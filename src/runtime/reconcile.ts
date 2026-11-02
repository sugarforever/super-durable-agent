import type { TaskRecord, Tx } from '@earendil-works/pi-durable';
import type { JsonValue } from '@earendil-works/chord';
import type { Engine } from '../domain/engine.js';
import type { AgentRun } from '../durable/tasks/agent-input.js';
import type { Assignment, CancelIntent, Ticket } from '../domain/model.js';
import type { Reservation } from './scheduler.js';
/** Endpoint Job consumers must persist this accounting shape; unknown remains capacity-bearing. */
export interface ExecutionAccounting { id: string; ticketId: string; assignmentId: string; epoch: number; status: 'pending' | 'running' | 'cancelPending' | 'unknown' | 'completed' | 'cancelled' | 'failed' }
export async function nativeTasks(tx: Tx): Promise<TaskRecord<JsonValue, JsonValue, JsonValue>[]> {
  const result: TaskRecord<JsonValue, JsonValue, JsonValue>[] = [];
  let cursor;
  do { const page = await tx.scanTasks({}, 256, cursor); result.push(...page.items.filter(x => x.state.status !== 'terminal')); cursor = page.next; } while (cursor);
  return result;
}
export async function reconcile(engine: Engine): Promise<void> {
  await engine.transaction(async tx => {
    const { store, scheduler } = engine;
    const tasks = await nativeTasks(tx);
    const runs = await store.listInTransaction<AgentRun>(tx, 'deliveryRun');
    const jobs = await store.listInTransaction<ExecutionAccounting>(tx, 'job');
    const cancels = await store.listInTransaction<CancelIntent>(tx, 'cancel');
    const reservations = await store.listInTransaction<Reservation>(tx, 'reservation');
    const tickets = await store.listInTransaction<Ticket>(tx, 'ticket');
    const nativeEpochs = new Map<string, number>();
    for (const ticket of tickets) {
      // The conversation can still be executing a revoked assignment. Attribute that
      // execution to its cancellation epoch, never grant the new assignment a slot.
      const cancel = cancels.find(x => x.ticketId === ticket.id && x.status !== 'completed');
      let placedEpoch: number | undefined;
      for (const run of runs.filter(x => x.ticketId === ticket.id && !x.settled)) {
        const submission = await tx.submissionByRequest(run.conversationId, run.requestId);
        if (submission?.status === 'placed') { placedEpoch = run.epoch; break; }
      }
      const epoch = cancel?.epoch ?? placedEpoch ?? ticket.assignmentEpoch;
      nativeEpochs.set(ticket.id, epoch);
      const liveNative = tasks.some(x => x.conversationId === ticket.workerConversationId);
      const epochs = new Set(jobs.filter(x => x.ticketId === ticket.id && !['completed','cancelled','failed'].includes(x.status)).map(x => x.epoch));
      if (liveNative) epochs.add(epoch);
      for (const executionEpoch of epochs) {
        if (reservations.some(x => x.ticketId === ticket.id && x.epoch === executionEpoch && x.status !== 'released')) continue;
        const assignmentId = jobs.find(x => x.ticketId === ticket.id && x.epoch === executionEpoch)?.assignmentId ?? cancel?.assignmentId ?? ticket.assignmentId;
        const assignment = await store.get<Assignment>(tx, 'assignment', assignmentId);
        const reservation: Reservation = { id: scheduler.key(ticket.id, executionEpoch), ticketId: ticket.id, assignmentId, epoch: executionEpoch, capacityClass: assignment?.capacityClass ?? 'ordinary', status: 'active', reservedAt: scheduler.clock.now() };
        await store.put(tx, 'reservation', reservation.id, reservation); reservations.push(reservation);
      }
    }
    for (const reservation of reservations) {
      if (reservation.status === 'released') continue;
      const ticket = tickets.find(x => x.id === reservation.ticketId);
      const cancel = cancels.find(x => x.ticketId === reservation.ticketId && x.epoch === reservation.epoch);
      const ownedRuns = runs.filter(x => x.ticketId === reservation.ticketId && x.epoch === reservation.epoch);
      const liveTasks = tasks.some(x => (x.conversationId === ticket?.workerConversationId && nativeEpochs.get(reservation.ticketId) === reservation.epoch) || ownedRuns.some(run => run.taskId === x.id));
      const liveJobs = jobs.filter(x => x.ticketId === reservation.ticketId && x.epoch === reservation.epoch && !['completed','cancelled','failed'].includes(x.status));
      const pendingRun = ownedRuns.some(x => x.status === 'pending' || x.status === 'submitted');
      const fresh = ticket?.lifecycle === 'open' && ticket.assignmentEpoch === reservation.epoch;
      // Preparation is a durable checkpoint until the first frozen run is claimed.
      const preparing = reservation.status === 'preparing' && fresh;
      const hold = liveTasks || liveJobs.length || (fresh && pendingRun) || preparing || (!!cancel && cancel.status !== 'completed');
      const status: Reservation['status'] = liveJobs.some(x => x.status === 'unknown') || cancel?.status === 'attention' ? 'quarantined' : hold ? (preparing ? 'preparing' : 'active') : 'released';
      await store.put(tx, 'reservation', reservation.id, { ...reservation, status });
    }
    for (const ticket of tickets) {
      const pending = cancels.some(x => x.ticketId === ticket.id && x.status !== 'completed');
      if (pending || ticket.cancelPending !== undefined) await store.put(tx, 'ticket', ticket.id, { ...ticket, cancelPending: pending });
    }
  });
}

/** Reserve first; Task06 may then persist a Job before executing its endpoint operation. */
export async function trackExecution(engine: Engine, tx: Tx, execution: ExecutionAccounting): Promise<void> {
  const { store } = engine;
  const ticket = await store.get<Ticket>(tx, 'ticket', execution.ticketId);
  if (!ticket || ticket.lifecycle !== 'open' || ticket.assignmentId !== execution.assignmentId || ticket.assignmentEpoch !== execution.epoch || !(await engine.scheduler.holds(tx, execution.ticketId, execution.epoch))) throw new Error('Stale or unreserved execution');
  const previous = await store.get<ExecutionAccounting>(tx, 'job', execution.id);
  if (previous) {
    if (previous.ticketId !== execution.ticketId || previous.assignmentId !== execution.assignmentId || previous.epoch !== execution.epoch) throw new Error('Execution identity conflict');
    return;
  }
  await store.put(tx, 'job', execution.id, execution);
}
/** Receipt-only reconciliation is allowed after revocation. The immutable identity fences newer work. */
export async function recordExecutionReceipt(engine: Engine, tx: Tx, receipt: ExecutionAccounting): Promise<void> {
  if (!['unknown','completed','cancelled','failed'].includes(receipt.status)) throw new Error('Expected an execution receipt');
  const previous = await engine.store.get<ExecutionAccounting>(tx, 'job', receipt.id);
  if (!previous || previous.ticketId !== receipt.ticketId || previous.assignmentId !== receipt.assignmentId || previous.epoch !== receipt.epoch) throw new Error('Execution receipt identity conflict');
  if (['completed','cancelled','failed'].includes(previous.status)) return;
  await engine.store.put(tx, 'job', receipt.id, { ...previous, status: receipt.status });
}
