import type { JsonValue } from '@earendil-works/chord';
import type { TaskId, Tx } from '@earendil-works/pi-durable';
import type { Engine } from '../domain/engine.js';
import { digest, scopedId, type Mandate, type Rule, type Source, type Ticket, type Report } from '../domain/model.js';
import type { Clock } from '../runtime/scheduler.js';
import { createOutboxTask } from '../durable/tasks/outbox.js';

export type MessagePurpose = 'ack' | 'progress' | 'answer' | 'question' | 'attention';
export interface MessageDestination { sourceId: string; threadRef: string }
export type ReceiptLookup = { status: 'found'; receipt: JsonValue } | { status: 'absent' } | { status: 'unknown' };
/** Trusted server configuration. Never install adapters from model/tool arguments. */
export interface SourceAdapter {
  readonly sourceId: string;
  readonly enabled: boolean;
  listened(destination: MessageDestination): boolean;
  send(message: Readonly<OutboundMessage>, key: string): Promise<JsonValue>;
  queryReceipt(key: string): Promise<ReceiptLookup>;
}
export interface OutboxOptions { adapters?: readonly SourceAdapter[]; random?: () => number }
export interface EnqueueMessage {
  ticketId: string; mandateId: string; assignmentEpoch: number;
  purpose: MessagePurpose; destination: MessageDestination; payload: JsonValue; idempotencyKey: string;
}
export interface OutboundMessage extends EnqueueMessage {
  id: string; payloadDigest: string;
  status: 'pending' | 'queued' | 'sending' | 'retrying' | 'sent' | 'uncertain' | 'invalid' | 'dropped' | 'attention';
  attempt: number; nextAttemptAt: number; taskId?: TaskId<null>; receipt?: JsonValue; error?: string;
}
/** An adapter may certify no effect occurred. Ordinary network errors never imply this. */
export class NoSendError extends Error { override readonly name = 'NoSendError'; }
const purposes: MessagePurpose[] = ['ack', 'progress', 'answer', 'question', 'attention'];
const communicates = (purpose: MessagePurpose, rules: Rule[]) => !['ack', 'progress'].includes(purpose) || rules.length > 0 && rules.every(r => r.communication?.[purpose === 'ack' ? 'acknowledgements' : 'progress'] === true);
const pending = (m: OutboundMessage) => ['pending', 'queued', 'retrying', 'sending'].includes(m.status);

export class Outbox {
  readonly task = createOutboxTask(this);
  private readonly adapters = new Map<string, SourceAdapter>();
  private timer?: ReturnType<typeof setTimeout>;
  private closed = false;
  private readonly random: () => number;
  constructor(readonly engine: Engine, options: OutboxOptions = {}, private readonly clock: Clock = { now: () => Date.now() }) {
    this.random = options.random ?? Math.random;
    for (const adapter of options.adapters ?? []) {
      if (!adapter.sourceId?.trim() || this.adapters.has(adapter.sourceId) || typeof adapter.enabled !== 'boolean' ||
        typeof adapter.listened !== 'function' || typeof adapter.send !== 'function' || typeof adapter.queryReceipt !== 'function') throw new Error('Invalid or duplicate SourceAdapter');
      this.adapters.set(adapter.sourceId, adapter);
    }
  }
  private async authority(tx: Tx, input: EnqueueMessage): Promise<{ mandate: Mandate; rules: Rule[]; source?: Source }> {
    const { store } = this.engine;
    const ticket = await store.get<Ticket>(tx, 'ticket', input.ticketId);
    const mandate = await store.get<Mandate>(tx, 'mandate', input.mandateId);
    if (!ticket || ticket.lifecycle !== 'open' || ticket.mandateId !== input.mandateId || ticket.assignmentEpoch !== input.assignmentEpoch ||
      !mandate || mandate.withdrawnAt !== null) throw new Error('Stale or revoked Outbox authority');
    if (mandate.sourceId !== input.destination.sourceId || !mandate.scope.includes(input.destination.threadRef)) throw new Error('Outbox destination is outside Mandate scope');
    const policy = await this.engine.authorization.policyInTransaction(tx);
    const human = mandate.actor.kind === 'human' && mandate.actor.trusted === true && !!mandate.actor.id.trim() && policy.allowedActors.includes(mandate.actor.id);
    let rules: Rule[] = [], capabilities: string[];
    if (mandate.basis === 'explicit') {
      if (!human) throw new Error('Outbox actor is no longer authorized');
      capabilities = policy.explicitCapabilities ?? ['repo.read', 'channel.reply'];
    } else {
      if (policy.acceptMode !== 'auto' || !mandate.ruleIds.length || !(human || mandate.actor.kind === 'bot' && policy.botActors.includes(mandate.actor.id))) throw new Error('Outbox rules are no longer authorized');
      for (const id of mandate.ruleIds) {
        const rule = policy.rules.find(r => r.id === id && r.enabled);
        if (!rule) throw new Error('Outbox rule is no longer authorized');
        rules.push(rule);
      }
      capabilities = rules.flatMap(rule => rule.capabilities);
    }
    if (!mandate.capabilities.includes('channel.reply') || !capabilities.includes('channel.reply')) throw new Error('Missing capability: channel.reply');
    return { mandate, rules, source: await store.get<Source>(tx, 'source', input.destination.sourceId) };
  }
  async enqueueMessage(tx: Tx, input: EnqueueMessage): Promise<OutboundMessage> {
    if (!input || ![input.ticketId, input.mandateId, input.idempotencyKey, input.destination?.sourceId, input.destination?.threadRef].every(x => typeof x === 'string' && !!x.trim()) ||
      !Number.isSafeInteger(input.assignmentEpoch) || input.assignmentEpoch < 1 || !purposes.includes(input.purpose)) throw new Error('Invalid outbound message');
    const stable: EnqueueMessage = { ticketId: input.ticketId, mandateId: input.mandateId, assignmentEpoch: input.assignmentEpoch,
      purpose: input.purpose, destination: { sourceId: input.destination.sourceId, threadRef: input.destination.threadRef }, payload: input.payload, idempotencyKey: input.idempotencyKey };
    const payloadDigest = digest(stable);
    const id = scopedId(this.engine.instanceId, 'outbox', [stable.idempotencyKey]);
    const existing = await this.engine.store.get<OutboundMessage>(tx, 'outbox', id);
    if (existing) {
      if (existing.payloadDigest !== payloadDigest) throw new Error('Outbox key payload conflict');
      return existing;
    }
    const { rules } = await this.authority(tx, stable);
    const permitted = communicates(stable.purpose, rules);
    const message: OutboundMessage = { ...stable, id, payloadDigest, status: permitted ? 'pending' : 'dropped', attempt: 0, nextAttemptAt: 0,
      ...(permitted ? {} : { error: 'Communication rule suppresses routine message' }) };
    await this.engine.store.put(tx, 'outbox', id, message);
    if (permitted && this.adapters.has(stable.destination.sourceId)) {
      message.taskId = await tx.createTask(this.task, { messageId: id }, { ownership: { kind: 'conversation' }, background: true });
      message.status = 'queued';
      await this.engine.store.put(tx, 'outbox', id, message);
    }
    return message;
  }
  /** Report, input consumption and outbound intent share the caller's Engine commit. */
  async relayReport(tx: Tx, ticket: Ticket, report: Report, reportId: string): Promise<void> {
    const purpose: MessagePurpose | undefined = report.questions?.length ? 'question' : report.status === 'ready' ? 'answer' : ['blocked','failed','dropped','attention'].includes(report.status) ? 'attention' : undefined;
    if (!purpose) return;
    const mandate = await this.engine.store.get<Mandate>(tx, 'mandate', ticket.mandateId);
    if (!mandate?.capabilities.includes('channel.reply')) return;
    // Only the explicit structured Report is relayed; native text/tool transcripts stay internal.
    const payload: JsonValue = { status: report.status, content: report.content, ...(report.questions?.length ? { questions: JSON.parse(JSON.stringify(report.questions)) as JsonValue } : {}) };
    for (const threadRef of mandate.scope) await this.enqueueMessage(tx, {
      ticketId: ticket.id, mandateId: mandate.id, assignmentEpoch: ticket.assignmentEpoch, purpose,
      destination: { sourceId: mandate.sourceId, threadRef }, payload,
      idempotencyKey: scopedId(this.engine.instanceId, 'report-message', [reportId, threadRef, purpose]),
    });
  }
  async reconcile(): Promise<void> {
    if (this.closed) return;
    await this.engine.transaction(async tx => {
      for (const message of await this.engine.store.listInTransaction<OutboundMessage>(tx, 'outbox')) {
        if (!pending(message) || !this.adapters.has(message.destination.sourceId) || message.nextAttemptAt > this.clock.now()) continue;
        const task = message.taskId ? await tx.task(message.taskId) : undefined;
        if (task && task.state.status !== 'terminal') continue;
        const taskId = await tx.createTask(this.task, { messageId: message.id }, { ownership: { kind: 'conversation' }, background: true });
        await this.engine.store.put(tx, 'outbox', message.id, { ...message, taskId, status: message.status === 'sending' ? 'sending' : 'queued' });
      }
    });
    await this.arm();
  }
  private async arm(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    if (this.closed) return;
    const messages = await this.engine.store.list<OutboundMessage>('outbox');
    const deadlines = messages.filter(m => pending(m) && this.adapters.has(m.destination.sourceId) && m.nextAttemptAt > this.clock.now()).map(m => m.nextAttemptAt);
    if (deadlines.length) {
      const delay = Math.max(10, Math.min(60_000, Math.min(...deadlines) - this.clock.now()));
      this.timer = setTimeout(() => { this.timer = undefined; void this.reconcile().catch(() => { /* Durable intents remain for explicit reconciliation. */ }); }, delay);
      this.timer.unref();
    }
  }
  private async lookup(adapter: SourceAdapter, key: string): Promise<ReceiptLookup> {
    try {
      const result = await adapter.queryReceipt(key);
      if (result.status === 'found') digest(result.receipt);
      if (!['found','absent','unknown'].includes(result.status)) return { status: 'unknown' };
      return result;
    } catch { return { status: 'unknown' }; }
  }
  private async receipt(id: string, taskId: TaskId<null>, receipt: JsonValue): Promise<void> {
    digest(receipt);
    await this.engine.transaction(async tx => {
      const message = await this.engine.store.get<OutboundMessage>(tx, 'outbox', id);
      if (!message || message.taskId !== taskId || message.status === 'sent') return;
      await this.engine.store.put(tx, 'outbox', id, { ...message, status: 'sent', receipt, nextAttemptAt: 0 });
    });
  }
  private async failure(id: string, taskId: TaskId<null>, knownAbsent: boolean, reason: string): Promise<void> {
    await this.engine.transaction(async tx => {
      const message = await this.engine.store.get<OutboundMessage>(tx, 'outbox', id);
      if (!message || message.taskId !== taskId || message.status !== 'sending') return;
      const exhausted = knownAbsent && message.attempt >= 8;
      const jitter = Math.max(0, Math.min(1, this.random()));
      const delay = Math.min(60_000, 2000 * 2 ** Math.max(0, message.attempt - 1) * (1 + (Number.isFinite(jitter) ? jitter : 0) * 0.25));
      await this.engine.store.put(tx, 'outbox', id, { ...message, status: !knownAbsent ? 'uncertain' : exhausted ? 'attention' : 'retrying', error: reason, nextAttemptAt: knownAbsent && !exhausted ? this.clock.now() + delay : 0 });
      if (!knownAbsent || exhausted) {
        const ticket = await this.engine.store.get<Ticket>(tx, 'ticket', message.ticketId);
        // Late old-epoch outcomes are accounting facts only.
        if (ticket?.lifecycle === 'open' && ticket.assignmentEpoch === message.assignmentEpoch && ticket.mandateId === message.mandateId)
          await this.engine.store.put(tx, 'ticket', ticket.id, { ...ticket, attention: !knownAbsent ? 'Outbound message outcome uncertain' : 'Outbound message exhausted 8 attempts' });
      }
    });
  }
  async run(id: string, taskId: TaskId<null>): Promise<void> {
    const initial = await this.engine.transaction(tx => this.engine.store.get<OutboundMessage>(tx, 'outbox', id));
    if (!initial || initial.taskId !== taskId || !pending(initial)) return;
    const adapter = this.adapters.get(initial.destination.sourceId);
    if (!adapter) return;
    // An interrupted send must be inspected even after stop; never infer no effect from process loss.
    // First-send lookup also repairs success when the local receipt was lost.
    const lookup = await this.lookup(adapter, initial.idempotencyKey);
    if (lookup.status === 'found') { await this.receipt(id, taskId, lookup.receipt); return; }
    if (initial.status === 'sending' && lookup.status === 'unknown') { await this.failure(id, taskId, false, 'Provider cannot establish interrupted send outcome'); return; }
    const message = await this.engine.transaction(async tx => {
      const current = await this.engine.store.get<OutboundMessage>(tx, 'outbox', id);
      if (!current || current.taskId !== taskId || !pending(current)) return;
      let source: Source | undefined, rules: Rule[];
      try { ({ source, rules } = await this.authority(tx, current)); }
      catch { await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'invalid', error: 'Fresh Outbox authority rejected', nextAttemptAt: 0 }); return; }
      if (!communicates(current.purpose, rules)) {
        await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'dropped', error: 'Communication rule suppresses routine message', nextAttemptAt: 0 }); return;
      }
      if (!source || source.listenedScopes !== undefined && !source.listenedScopes.includes(current.destination.threadRef) || !adapter.listened(current.destination)) {
        await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'dropped', error: 'Destination is no longer listened to', nextAttemptAt: 0 }); return;
      }
      if (!source.enabled || !adapter.enabled) {
        await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'pending', error: 'Source is disabled', nextAttemptAt: this.clock.now() + 2000 }); return;
      }
      if (current.nextAttemptAt > this.clock.now()) return;
      if (current.attempt >= 8) { await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'attention', error: 'Outbound message exhausted 8 attempts', nextAttemptAt: 0 }); return; }
      const next: OutboundMessage = { ...current, status: 'sending', attempt: current.attempt + 1, nextAttemptAt: 0 };
      await this.engine.store.put(tx, 'outbox', id, next); return next;
    });
    if (!message) return;
    // No awaited work between the fresh serialized claim and invoking the trusted sender.
    try { const receipt = await adapter.send(message, message.idempotencyKey); await this.receipt(id, taskId, receipt); }
    catch (error) {
      if (error instanceof NoSendError) await this.failure(id, taskId, true, 'Adapter certified no send');
      else {
        const result = await this.lookup(adapter, message.idempotencyKey);
        if (result.status === 'found') await this.receipt(id, taskId, result.receipt);
        else await this.failure(id, taskId, result.status === 'absent', 'Provider send outcome ' + result.status);
      }
    }
  }
  /** Trusted query-only repair. Absent/unknown never authorizes a retry of uncertain work. */
  async inspectReceipt(id: string): Promise<ReceiptLookup> {
    const message = await this.engine.store.read<OutboundMessage>('outbox', id);
    if (!message) throw new Error('Unknown outbound message');
    if (message.status === 'sent' && message.receipt !== undefined) return { status: 'found', receipt: message.receipt };
    const adapter = this.adapters.get(message.destination.sourceId);
    if (!adapter) return { status: 'unknown' };
    const result = await this.lookup(adapter, message.idempotencyKey);
    if (result.status === 'found') await this.engine.transaction(async tx => {
      const current = await this.engine.store.get<OutboundMessage>(tx, 'outbox', id);
      if (!current || current.payloadDigest !== message.payloadDigest || current.status === 'sent') return;
      await this.engine.store.put(tx, 'outbox', id, { ...current, status: 'sent', receipt: result.receipt, nextAttemptAt: 0 });
    });
    return result;
  }
  async completed(): Promise<void> { await this.arm(); }
  close(): void { this.closed = true; if (this.timer) clearTimeout(this.timer); }
}
