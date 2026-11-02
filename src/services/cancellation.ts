import type { Context } from '@earendil-works/chord';
import type { ConversationId, TaskRuntime, Tx } from '@earendil-works/pi-durable';
import type { Engine } from '../domain/engine.js';
import { digest, type CancelIntent } from '../domain/model.js';
import type { AgentRun } from '../durable/tasks/agent-input.js';
import { nativeTasks, type ExecutionAccounting } from '../runtime/reconcile.js';
export interface CancellationRequest {
  cancelKey: string; payloadDigest: string; ticketId: string; assignmentId: string; epoch: number;
}
export interface CancellationReceipt { cancelKey: string; payloadDigest: string; outcome: 'confirmed' | 'unknown' }
/** Implementations must query/replay the same key+digest, never retry an unknown mutation under a new key. */
export type CancellationHandler = (request: CancellationRequest, context: Context) => Promise<CancellationReceipt>;
type CancelRuntime = Pick<TaskRuntime<{ intentId: string; conversationId?: ConversationId }, { phase: 'cancel' }, null, object>, 'signal' | 'conversation' | 'waitForTask'>;
export class Cancellation {
  constructor(readonly engine: Engine, readonly handler?: CancellationHandler) {}
  request(intent: CancelIntent): CancellationRequest {
    const payloadDigest = digest({ ticketId: intent.ticketId, assignmentId: intent.assignmentId, epoch: intent.epoch, reason: intent.reason });
    if (intent.payloadDigest && intent.payloadDigest !== payloadDigest) throw new Error('Cancel payload digest conflict');
    return { cancelKey: intent.cancelKey ?? intent.id, payloadDigest, ticketId: intent.ticketId, assignmentId: intent.assignmentId, epoch: intent.epoch };
  }
  async cancel(intent: CancelIntent, conversationId: ConversationId | undefined, context: Context, runtime: CancelRuntime): Promise<CancellationReceipt> {
    runtime.signal.throwIfAborted();
    const request = this.request(intent);
    const taskIds = await this.engine.transaction(async tx => {
      const current = await this.engine.store.get<CancelIntent>(tx, 'cancel', intent.id);
      if (!current || current.taskId !== intent.taskId || current.status !== 'pending') throw new Error('Stale cancellation claim');
      const tasks = await nativeTasks(tx);
      const runs = await this.engine.store.listInTransaction<AgentRun>(tx, 'deliveryRun');
      const redrives = await this.engine.store.listInTransaction<{ taskId?: number; input?: { runId: string } }>(tx, 'redrive');
      const jobs = await this.engine.store.listInTransaction<ExecutionAccounting & { taskId?: number }>(tx, 'job');
      const owned = runs.filter(x => x.ticketId === intent.ticketId && x.epoch === intent.epoch);
      const ids = new Set<number | undefined>([...owned.map(x => x.taskId), ...redrives.filter(x => owned.some(run => run.id === x.input?.runId)).map(x => x.taskId), ...jobs.filter(x => x.ticketId === intent.ticketId && x.epoch === intent.epoch).map(x => x.taskId)]);
      return tasks.filter(x => ids.has(x.id)).map(x => x.id);
    });
    // Mark only THIS Ticket's Control children. Router and unrelated Workers keep running.
    runtime.signal.throwIfAborted();
    await Promise.all(taskIds.map(id => this.engine.harness.abortTask(id, context)));
    if (conversationId !== undefined) {
      const conversation = await runtime.conversation(conversationId, context);
      await conversation?.abort(context, { background: true });
    }
    await Promise.all(taskIds.map(id => runtime.waitForTask(id, context)));
    await this.engine.transaction(async tx => {
      const current = await this.engine.store.get<CancelIntent>(tx, 'cancel', intent.id);
      if (current?.taskId === intent.taskId) await this.engine.store.put(tx, 'cancel', intent.id, { ...current, nativeConfirmed: true });
    });
    runtime.signal.throwIfAborted();
    if (this.handler) return this.handler(request, context);
    const jobs = await this.engine.store.list<ExecutionAccounting>('job');
    const unknown = jobs.some(x => x.ticketId === intent.ticketId && x.epoch === intent.epoch && !['completed','cancelled','failed'].includes(x.status));
    return { ...request, outcome: unknown ? 'unknown' : 'confirmed' };
  }
  /** Accounting only: receipts from revoked epochs never write Ticket progress or feedback. */
  async receipt(tx: Tx, intentId: string, receipt: CancellationReceipt, taskId?: number): Promise<void> {
    const { store } = this.engine;
    const intent = await store.get<CancelIntent>(tx, 'cancel', intentId);
    if (!intent || (taskId !== undefined && intent.taskId !== taskId)) return;
    const request = this.request(intent);
    if (request.cancelKey !== receipt.cancelKey || request.payloadDigest !== receipt.payloadDigest) throw new Error('Cancel receipt identity conflict');
    const endpointConfirmed = receipt.outcome === 'confirmed';
    await store.put(tx, 'cancel', intentId, { ...intent, endpointConfirmed, status: endpointConfirmed && intent.nativeConfirmed ? 'completed' : 'pending' });
    if (endpointConfirmed) {
      for (const job of await store.listInTransaction<ExecutionAccounting>(tx, 'job'))
        if (job.ticketId === intent.ticketId && job.epoch === intent.epoch && !['completed','cancelled','failed'].includes(job.status))
          await store.put(tx, 'job', job.id, { ...job, status: 'cancelled' });
    }
  }
}
