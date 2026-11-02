import type { ConversationId, TaskId, Tx } from '@earendil-works/pi-durable';
import type { Engine } from '../domain/engine.js';
import { canonicalJson, scopedId, type Ticket, type Mandate, type Policy, type Source, type CancelIntent } from '../domain/model.js';
import { trackExecution, recordExecutionReceipt, type ExecutionAccounting } from '../runtime/reconcile.js';
import { LocalEnvironmentBroker } from '../environments/local.js';
import { operationDigest, type EndpointIdentity, type EndpointGrant, type EndpointAction, type EnvironmentBroker, type ExecutionPayload, type ExecutionReceipt, type PreparedReceipt } from '../environments/types.js';
import { createJobTask } from '../durable/tasks/job.js';
import type { CancellationHandler } from './cancellation.js';
export type EnvironmentOptions = { repository: string; directory?: string; trustedLocalShell?: boolean } | { broker: EnvironmentBroker };
export interface StartJob { assignmentId: string; epoch: number; command: string; sandbox?: boolean }
export interface Job extends ExecutionAccounting {
  identity: EndpointIdentity; operationId: string; commandDigest: string; action: { kind: 'shell'; command: string; sandbox?: boolean };
  taskId: TaskId<null>; environmentId?: string; payloadDigest?: string; payload?: ExecutionPayload; receipt?: ExecutionReceipt; notified?: boolean;
}
export class Jobs {
  readonly task = createJobTask(this);
  readonly broker?: EnvironmentBroker;
  constructor(readonly engine: Engine, options: EnvironmentOptions | undefined, directory: string, readonly kick: () => void) {
    this.broker = options && ('broker' in options ? options.broker : new LocalEnvironmentBroker({ ...options, directory: options.directory ?? directory, authorize: (identity, action) => this.authorize(identity, action) }));
  }
  private endpoint(): EnvironmentBroker { if (!this.broker) throw new Error('Environment broker is not configured'); return this.broker; }
  private async bound(tx: Tx, conversationId: ConversationId, expected?: { assignmentId: string; epoch: number }): Promise<Ticket> {
    const binding = await this.engine.store.get<{ ticketId: string }>(tx, 'workerBinding', String(conversationId));
    if (!binding) throw new Error('Bound Worker role required');
    const ticket = await this.engine.store.get<Ticket>(tx, 'ticket', binding.ticketId);
    if (!ticket || ticket.workerConversationId !== conversationId || ticket.lifecycle !== 'open' || ticket.cancelPending ||
      (expected && (ticket.assignmentId !== expected.assignmentId || ticket.assignmentEpoch !== expected.epoch))) throw new Error('Stale or stopped Worker assignment');
    return ticket;
  }
  private async grant(tx: Tx, identity: EndpointIdentity, action: EndpointAction): Promise<EndpointGrant> {
    const { store } = this.engine;
    if (action === 'cancel') {
      const cancellation = (await store.listInTransaction<CancelIntent>(tx, 'cancel')).some(x => x.ticketId === identity.ticketId && x.assignmentId === identity.assignmentId && x.epoch === identity.epoch);
      if (cancellation) return { ...identity, capabilities: [] };
    }
    const ticket = await store.get<Ticket>(tx, 'ticket', identity.ticketId);
    const mandate = await store.get<Mandate>(tx, 'mandate', identity.mandateId);
    const policy = await store.get<Policy>(tx, 'policy', 'current');
    const source = mandate && await store.get<Source>(tx, 'source', mandate.sourceId);
    if (!ticket || ticket.lifecycle !== 'open' || ticket.assignmentId !== identity.assignmentId || ticket.assignmentEpoch !== identity.epoch || ticket.mandateId !== identity.mandateId || ticket.cancelPending || !mandate || mandate.withdrawnAt !== null || !policy || !source?.enabled || (source.listenedScopes && mandate.scope.some(x => !source.listenedScopes!.includes(x)))) throw new Error('Stale endpoint authority');
    const human = mandate.actor.kind === 'human' && mandate.actor.trusted === true && policy.allowedActors.includes(mandate.actor.id);
    let capabilities: string[];
    if (mandate.basis === 'explicit') {
      if (!human) throw new Error('Endpoint actor is no longer authorized');
      capabilities = policy.explicitCapabilities ?? ['repo.read', 'channel.reply'];
    } else {
      if (policy.acceptMode !== 'auto' || !(human || mandate.actor.kind === 'bot' && policy.botActors.includes(mandate.actor.id)) || !mandate.ruleIds.length) throw new Error('Endpoint rules no longer authorized');
      const rules = mandate.ruleIds.map(id => policy.rules.find(r => r.id === id && r.enabled));
      if (rules.some(x => !x)) throw new Error('Endpoint rules revoked');
      capabilities = rules.flatMap(x => x!.capabilities);
    }
    if (action !== 'cancel' && action !== 'release' && !(await this.engine.scheduler.holds(tx, ticket.id, ticket.assignmentEpoch))) throw new Error('Endpoint execution requires a reservation');
    return { ...identity, capabilities: mandate.capabilities.filter(cap => capabilities.includes(cap)) };
  }
  async authorize(identity: EndpointIdentity, action: EndpointAction): Promise<EndpointGrant> { return this.engine.transaction(tx => this.grant(tx, identity, action)); }
  private identity(ticket: Ticket): EndpointIdentity { return { ticketId: ticket.id, assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, mandateId: ticket.mandateId }; }
  async start(conversationId: ConversationId, args: StartJob, callId: string): Promise<{ jobId: string }> {
    this.endpoint();
    if (!args || typeof args.command !== 'string' || !args.command.trim() || (args.sandbox !== undefined && typeof args.sandbox !== 'boolean')) throw new Error('Invalid Job command');
    const id = scopedId(this.engine.instanceId, 'job', [String(conversationId), callId]);
    await this.engine.transaction(async tx => {
      const ticket = await this.bound(tx, conversationId, args), identity = this.identity(ticket);
      const action = { kind: 'shell' as const, command: args.command, ...(args.sandbox !== undefined ? { sandbox: args.sandbox } : {}) };
      const commandDigest = operationDigest(identity, action);
      const previous = await this.engine.store.get<Job>(tx, 'job', id);
      if (previous) { if (previous.commandDigest !== commandDigest) throw new Error('Job payload conflict'); return; }
      const grant = await this.grant(tx, identity, 'execute');
      if (!grant.capabilities.includes('command.execute')) throw new Error('Missing capability: command.execute');
      const accounting: ExecutionAccounting = { id, ticketId: ticket.id, assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, status: 'pending' };
      await trackExecution(this.engine, tx, accounting);
      const taskId = await tx.createTask(this.task, { jobId: id }, { ownership: { kind: 'conversation' }, background: true });
      await this.engine.store.put(tx, 'job', id, { ...accounting, identity, operationId: `execute:${id}`, commandDigest, action, taskId } satisfies Job);
    });
    return { jobId: id };
  }
  async get(conversationId: ConversationId, id: string): Promise<Job> {
    const job = await this.owned(conversationId, id);
    if (job.payloadDigest) {
      const receipt = await this.endpoint().query(job.operationId);
      if (receipt && ['completed','failed','cancelled','unknown'].includes(receipt.status)) await this.finish(id, receipt as ExecutionReceipt);
    }
    return (await this.engine.store.read<Job>('job', id))!;
  }
  private async owned(conversationId: ConversationId, id: string): Promise<Job> {
    return this.engine.transaction(async tx => {
      const ticket = await this.bound(tx, conversationId);
      const job = await this.engine.store.get<Job>(tx, 'job', id);
      if (!job || job.ticketId !== ticket.id || job.assignmentId !== ticket.assignmentId || job.epoch !== ticket.assignmentEpoch) throw new Error('Unrelated Job ownership');
      return job;
    });
  }
  async stop(conversationId: ConversationId, id: string): Promise<Job> {
    const job = await this.owned(conversationId, id);
    await this.engine.transaction(async tx => {
      const current = await this.engine.store.get<Job>(tx, 'job', id);
      if (current && !['completed','failed','cancelled'].includes(current.status)) await this.engine.store.put(tx, 'job', id, { ...current, status: 'cancelPending' });
    });
    await this.engine.harness.abortTask(job.taskId, this.engine.context);
    await this.engine.harness.waitForTask(job.taskId, this.engine.context);
    await this.cancelExecution(id);
    this.kick();
    return (await this.engine.store.read<Job>('job', id))!;
  }
  private async prepare(identity: EndpointIdentity): Promise<PreparedReceipt> {
    const payload = { kind: 'prepare' as const };
    return this.endpoint().prepare({ operationId: `prepare:${identity.ticketId}:${identity.assignmentId}:${identity.epoch}`, identity, payload, payloadDigest: operationDigest(identity, payload) });
  }
  async run(id: string, taskId: TaskId<null>, signal: AbortSignal): Promise<void> {
    let job = await this.engine.store.read<Job>('job', id);
    if (!job || job.taskId !== taskId || ['completed','failed','cancelled','unknown'].includes(job.status)) return;
    if (job.status === 'cancelPending' || signal.aborted) { await this.cancelExecution(id); return; }
    try {
      const prior = await this.endpoint().query(job.operationId);
      if (prior) { await this.finish(id, prior as ExecutionReceipt); return; }
      const environment = await this.prepare(job.identity);
      const payload: ExecutionPayload = { ...job.action, environmentId: environment.environmentId };
      const payloadDigest = operationDigest(job.identity, payload);
      let admitted = false;
      await this.engine.transaction(async tx => {
        const current = await this.engine.store.get<Job>(tx, 'job', id);
        if (!current || current.taskId !== taskId || !['pending','running'].includes(current.status) || signal.aborted) return;
        await this.grant(tx, job!.identity, 'execute');
        await this.engine.store.put(tx, 'job', id, { ...current, environmentId: environment.environmentId, payload, payloadDigest, status: 'running' }); admitted = true;
      });
      if (!admitted || signal.aborted) { await this.cancelExecution(id); return; }
      job = (await this.engine.store.read<Job>('job', id))!;
      const abort = () => { void this.cancelExecution(id).catch(() => {}); };
      signal.addEventListener('abort', abort, { once: true });
      try {
        signal.throwIfAborted();
        const receipt = await this.endpoint().execute({ operationId: job.operationId, identity: job.identity, payload, payloadDigest });
        await this.finish(id, receipt);
      } finally { signal.removeEventListener('abort', abort); }
    } catch (error) {
      const current = (await this.engine.store.read<Job>('job', id))!;
      const receipt = await this.endpoint().query(current.operationId);
      await this.finish(id, receipt ? receipt as ExecutionReceipt : { operationId: current.operationId, identity: current.identity, payloadDigest: current.payloadDigest ?? current.commandDigest, status: 'failed', reason: error instanceof Error ? error.message : String(error) });
    }
  }
  async cancelExecution(id: string): Promise<void> {
    const job = await this.engine.store.read<Job>('job', id);
    if (!job || ['completed','failed','cancelled'].includes(job.status)) return;
    const prior = await this.endpoint().query(job.operationId);
    if (prior) await this.finish(id, await this.endpoint().cancel(job.operationId, job.epoch));
    else {
      // No endpoint admission and the native task is being aborted: no external command was launched.
      await this.finish(id, { operationId: job.operationId, identity: job.identity, payloadDigest: job.payloadDigest ?? job.commandDigest, status: 'cancelled', reason: 'Cancelled before endpoint admission' });
    }
  }
  async finish(id: string, receipt: ExecutionReceipt): Promise<void> {
    if (!['completed','failed','cancelled','unknown'].includes(receipt.status)) return;
    await this.engine.transaction(async tx => {
      const { store } = this.engine;
      const job = await store.get<Job>(tx, 'job', id);
      if (!job || job.operationId !== receipt.operationId || canonicalJson(job.identity) !== canonicalJson(receipt.identity) || (job.payloadDigest ?? job.commandDigest) !== receipt.payloadDigest) throw new Error('Job receipt payload conflict');
      if (['completed','failed','cancelled'].includes(job.status)) return;
      await recordExecutionReceipt(this.engine, tx, { ...job, status: receipt.status as ExecutionAccounting['status'] });
      const next = (await store.get<Job>(tx, 'job', id))!;
      const ticket = await store.get<Ticket>(tx, 'ticket', job.ticketId);
      const current = ticket?.lifecycle === 'open' && ticket.assignmentId === job.assignmentId && ticket.assignmentEpoch === job.epoch && !ticket.cancelPending;
      await store.put(tx, 'job', id, { ...next, receipt, notified: job.notified || current });
      if (current && !job.notified) {
        const noteId = `job-result:${id}`;
        await store.put(tx, 'workerNote', noteId, { id: noteId, ticketId: job.ticketId, jobId: id, epoch: job.epoch, status: 'pending', text: canonicalJson({ kind: 'job_result', jobId: id, receipt }) });
        if (receipt.status === 'unknown') await store.put(tx, 'ticket', ticket!.id, { ...ticket!, attention: 'Job outcome unknown; inspect endpoint before continuing' });
      }
    });
    this.kick();
  }
  cancellationHandler(external?: CancellationHandler): CancellationHandler {
    return async (request, context) => {
      let confirmed = true;
      for (const job of await this.engine.store.list<Job>('job')) {
        if (job.ticketId !== request.ticketId || job.assignmentId !== request.assignmentId || job.epoch !== request.epoch || ['completed','failed','cancelled'].includes(job.status)) continue;
        await this.cancelExecution(job.id);
        if (!['completed','failed','cancelled'].includes((await this.engine.store.read<Job>('job', job.id))!.status)) confirmed = false;
      }
      const other = await external?.(request, context);
      if (other && (other.cancelKey !== request.cancelKey || other.payloadDigest !== request.payloadDigest)) throw new Error('External cancellation receipt conflict');
      return { ...request, outcome: confirmed && (!other || other.outcome === 'confirmed') ? 'confirmed' : 'unknown' };
    };
  }
  async file(conversationId: ConversationId, args: { assignmentId: string; epoch: number } & ({ kind: 'read'; path: string } | { kind: 'write'; path: string; text: string } | { kind: 'edit'; path: string; oldText: string; newText: string }), callId: string): Promise<ExecutionReceipt> {
    const identity = await this.engine.transaction(async tx => this.identity(await this.bound(tx, conversationId, args)));
    const environment = await this.prepare(identity);
    const { assignmentId: _, epoch: __, ...action } = args;
    const payload: ExecutionPayload = { ...action, environmentId: environment.environmentId };
    return this.endpoint().execute({ operationId: `file:${conversationId}:${callId}`, identity, payload, payloadDigest: operationDigest(identity, payload) });
  }
}
