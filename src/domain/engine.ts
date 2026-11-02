import type { Context, JsonValue } from '@earendil-works/chord';
import { defineTask, type Conversation, type Harness, type Tx } from '@earendil-works/pi-durable';
import { Scheduler, type Clock } from '../runtime/scheduler.js';
import { DomainStore } from './store.js';
import { PolicyController } from './policy.js';
import type { Actor, CommandMeta, DecisionOutcome, Policy } from './model.js';
import { digest, normalizeEvent, scopedId, type AuditEvent, type IntakeIntent, type NormalizedEvent,
  type ReceiveBatchResult, type ReceiveResult, type Source, type SourceCursor, type Thread, type Trigger, type PreparedEvent } from './model.js';
export type { Tx } from '@earendil-works/pi-durable';

class PendingCapacity extends Error {}
interface IntakeState { count: number; lastOrdinal: number }
interface CommandReceipt<T> { digest: string; hasResult: boolean; result: T | null }

export function createIntakeTask(store: DomainStore) {
  return defineTask<{ triggerId: string }, { phase: 'accept' }, null>({
    name: 'sa.intake', version: 1, initial: () => ({ phase: 'accept' }),
    phases: {
      accept: async (task, runtime, context) => {
        await runtime.commit(async (tx) => {
          const intent = await store.get<IntakeIntent>(tx, 'intake', task.input.triggerId);
          if (!intent) throw new Error(`Missing intake intent: ${task.input.triggerId}`);
          await store.put(tx, 'intake', intent.id, { ...intent, status: 'accepted' });
          return { status: 'terminal', outcome: { status: 'completed', result: null } };
        }, context);
      },
    },
    abort: async (_task, runtime, context) => {
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'aborted' } }), context);
    },
  });
}
export class Engine {
  readonly authorization = new PolicyController(this);
  readonly scheduler: Scheduler;
  /** Host preprocessing performs artifact IO before entering a transaction. */
  prepareEvent?: (event: NormalizedEvent) => Promise<PreparedEvent>;

  updatePolicy(policy: Policy, actor: Actor, meta: CommandMeta) { return this.authorization.updatePolicy(policy, actor, meta); }
  listCandidates() { return this.authorization.listCandidates(); }
  decide(evalId: string, epoch: number, outcome: DecisionOutcome, meta: CommandMeta) { return this.authorization.decide(evalId, epoch, outcome, meta); }
  subscribe(ticketId: string, sourceId: string, threadRef: string, own: boolean, meta: CommandMeta) { return this.authorization.subscribe(ticketId, sourceId, threadRef, own, meta); }
  unsubscribe(ticketId: string, sourceId: string, threadRef: string, meta: CommandMeta) { return this.authorization.unsubscribe(ticketId, sourceId, threadRef, meta); }
  handback(ticketId: string, triggerId: string, epoch: number, reason: string, meta: CommandMeta) { return this.authorization.handback(ticketId, triggerId, epoch, reason, meta); }
  stopTicket(ticketId: string, actor: Actor, meta: CommandMeta) { return this.authorization.stopTicket(ticketId, actor, meta); }
  retryTicket(ticketId: string, actor: Actor, meta: CommandMeta) { return this.authorization.retryTicket(ticketId, actor, meta); }
  endTicket(ticketId: string, epoch: number, meta: CommandMeta) { return this.authorization.endTicket(ticketId, epoch, meta); }

  constructor(
    readonly harness: Harness,
    readonly control: Conversation,
    readonly context: Context,
    readonly store: DomainStore,
    readonly instanceId: string,
    readonly maxPending = 10_000,
    private readonly intakeTask = createIntakeTask(store),
    clock?: Clock,
  ) { this.scheduler = new Scheduler(this, clock); }

  /** Commit callbacks perform domain edits and create intents only; external IO belongs to tasks. */
  transaction<T>(fn: (tx: Tx) => Promise<T>): Promise<T> { return this.control.commit(fn, this.context); }

  command<T>(commandId: string, payload: JsonValue, fn: (tx: Tx) => Promise<T>): Promise<T> {
    if (!commandId.trim()) return Promise.reject(new Error('commandId must be nonempty'));
    const hash = digest(payload);
    const key = scopedId(this.instanceId, 'command', [commandId]);
    return this.transaction(async (tx) => {
      const receipt = await this.store.get<CommandReceipt<T>>(tx, 'command', key);
      if (receipt) {
        if (receipt.digest !== hash) throw new Error(`Command payload conflict: ${commandId}`);
        return receipt.hasResult ? receipt.result as T : undefined as T;
      }
      const result = await fn(tx);
      await this.store.put(tx, 'command', key, { digest: hash, hasResult: result !== undefined, result: result === undefined ? null : result });
      return result;
    });
  }

  async configureSource(source: Omit<Source, 'cursor'>): Promise<void> {
    if (!source.id?.trim() || !source.kind?.trim() || typeof source.enabled !== 'boolean' ||
        (source.listenedScopes !== undefined && (!Array.isArray(source.listenedScopes) || source.listenedScopes.some((scope) => !scope?.trim())))) {
      throw new Error('Invalid source configuration');
    }
    await this.transaction(async (tx) => {
      const existing = await this.store.get<Source>(tx, 'source', source.id);
      await this.store.put(tx, 'source', source.id, {
        id: source.id, kind: source.kind, enabled: source.enabled,
        ...(source.listenedScopes === undefined ? {} : { listenedScopes: [...new Set(source.listenedScopes)] }),
        ...(existing?.cursor === undefined ? {} : { cursor: existing.cursor }),
      });
    });
  }

  private async prepareNormalized(clean: NormalizedEvent): Promise<PreparedEvent> {
    const { receivedAt: _receivedAt, ...stablePayload } = clean;
    const originalDigest = digest(stablePayload);
    const prepared = this.prepareEvent ? await this.prepareEvent(clean) : clean;
    return { ...prepared, originalDigest };
  }

  async receive(event: NormalizedEvent): Promise<ReceiveResult> {
    const clean = normalizeEvent(event);
    const normalized = await this.prepareNormalized(clean);
    try { return await this.transaction((tx) => this.receiveInTransaction(tx, normalized)); }
    catch (error) {
      if (error instanceof PendingCapacity) return { disposition: 'backpressure', retryable: true };
      throw error;
    }
  }

  async receiveBatch(events: NormalizedEvent[], cursor: SourceCursor): Promise<ReceiveBatchResult> {
    if (!cursor?.sourceId?.trim() || events.some((event) => event.sourceId !== cursor.sourceId)) {
      throw new Error('Batch events and cursor must have the same source');
    }
    digest(cursor.value);
    const normalized = await Promise.all(events.map(async event => {
      const clean = normalizeEvent(event);
      return this.prepareNormalized(clean);
    }));
    try {
      return await this.transaction(async (tx) => {
        const source = await this.store.get<Source>(tx, 'source', cursor.sourceId);
        if (!source?.enabled) throw new Error('Batch cursor source is not enabled');
        const results: ReceiveResult[] = [];
        for (const event of normalized) results.push(await this.receiveInTransaction(tx, event));
        await this.store.put(tx, 'source', source.id, { ...source, cursor: cursor.value });
        return { disposition: 'stored', results };
      });
    } catch (error) {
      if (error instanceof PendingCapacity) return { disposition: 'backpressure', retryable: true };
      throw error;
    }
  }

  async receiveInTransaction(tx: Tx, event: PreparedEvent): Promise<ReceiveResult> {
    const id = scopedId(this.instanceId, 'trigger', [event.sourceId, event.externalId, event.revision]);
    const { originalDigest, ...storedEvent } = event;
    const { receivedAt: _receivedAt, ...stablePayload } = storedEvent;
    const storageDigest = digest(stablePayload);
    const hash = originalDigest ?? storageDigest;
    const existing = await this.store.get<Trigger>(tx, 'trigger', id);
    if (existing) {
      if (existing.digest !== hash && existing.digest !== storageDigest) throw new Error(`Event payload conflict: ${id}`);
      return { disposition: 'duplicate', triggerId: id };
    }
    const source = await this.store.get<Source>(tx, 'source', event.sourceId);
    if (!source?.enabled || (source.listenedScopes !== undefined && !event.threads.some((thread) => source.listenedScopes!.includes(thread)))) {
      return { disposition: 'filtered', sourceId: event.sourceId };
    }
    const state = await this.store.get<IntakeState>(tx, 'state', 'intake') ?? { count: 0, lastOrdinal: 0 };
    if (state.count >= this.maxPending) throw new PendingCapacity('Pending intake capacity exceeded');
    const ordinal = state.lastOrdinal + 1;
    const trigger: Trigger = { ...storedEvent, id, ordinal, digest: hash };
    await this.store.put(tx, 'trigger', id, trigger);
    const audit: AuditEvent = { id, kind: 'trigger.received', triggerId: id, occurredAt: event.receivedAt };
    await this.store.put(tx, 'audit', id, audit);
    await tx.appendEntry(this.control.id, { kind: 'sa.trigger.received', data: { triggerId: id } });
    for (const externalRef of event.threads) {
      const threadId = scopedId(this.instanceId, 'thread', [event.sourceId, externalRef]);
      const thread = await this.store.get<Thread>(tx, 'thread', threadId) ?? {
        id: threadId, sourceId: event.sourceId, externalRef, triggerIds: [], lastOrdinal: 0,
      };
      if (thread.triggerIds.length >= this.store.maxIndexEntries) throw new Error('Thread membership capacity exceeded');
      await this.store.put(tx, 'thread', threadId, { ...thread, triggerIds: [...thread.triggerIds, id], lastOrdinal: ordinal });
    }
    const taskId = await tx.createTask(this.intakeTask, { triggerId: id }, { ownership: { kind: 'conversation' }, background: true });
    await this.store.put(tx, 'intake', id, { id, triggerId: id, status: 'pending', taskId } satisfies IntakeIntent);
    await this.store.put(tx, 'state', 'intake', { count: state.count + 1, lastOrdinal: ordinal });
    await this.authorization.routeInTransaction(tx, trigger);
    return { disposition: 'stored', triggerId: id };
  }
}
