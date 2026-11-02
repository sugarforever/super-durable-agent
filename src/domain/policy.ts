import type { JsonValue } from '@earendil-works/chord';
import type { Tx } from '@earendil-works/pi-durable';
import type { Engine } from './engine.js';
import { canonicalJson, scopedId, type Actor, type Assignment, type CancelIntent, type CommandMeta, type Decision,
  type DecisionOutcome, type DeliveryIntent, type Disposition, type Eval, type IntakeIntent, type Mandate,
  type Policy, type Source, type Subscription, type Ticket, type Trigger } from './model.js';

interface EvalQueue { threadId: string; triggerIds: string[] }

const defaultExplicitCapabilities = ['repo.read', 'channel.reply'];
const defaultPolicy = (): Policy => ({ version: 0, acceptMode: 'observe', allowedActors: [], botActors: [], rules: [] });
const payload = (value: unknown): JsonValue => JSON.parse(canonicalJson(value)) as JsonValue;
function nonempty(value: string, name: string) { if (typeof value !== 'string' || !value.trim()) throw new Error(`${name} must be nonempty`); }
function human(actor: Actor) { return actor.kind === 'human' && actor.trusted; }
function allowedHuman(policy: Policy, actor: Actor) { return human(actor) && policy.allowedActors.includes(actor.id); }
function basis(policy: Policy, trigger: Trigger): Eval['basis'] | undefined {
  if (allowedHuman(policy, trigger.actor)) return trigger.addressed ? 'explicit' : 'inferred';
  if (trigger.actor.kind === 'bot' && policy.botActors.includes(trigger.actor.id)) return 'inferred';
  return undefined;
}

/** Authorization and routing edits share the Engine's existing pi transaction and durable command receipts. */
export class PolicyController {
  constructor(private readonly engine: Engine) {}
  private get store() { return this.engine.store; }
  private id(kind: string, ...parts: string[]) { return scopedId(this.engine.instanceId, kind, parts); }
  async policyInTransaction(tx: Tx): Promise<Policy> { return await this.store.get<Policy>(tx, 'policy', 'current') ?? defaultPolicy(); }

  async updatePolicy(policy: Policy, actor: Actor, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'updatePolicy', policy, actor, meta }), async tx => {
      if (!human(actor)) throw new Error('Policy updates require a trusted human');
      const current = await this.policyInTransaction(tx);
      if (meta.expectedRevision !== undefined && meta.expectedRevision !== current.version) throw new Error('Policy revision conflict');
      if (!Number.isSafeInteger(policy.version) || policy.version <= current.version) throw new Error('Policy version must strictly increase');
      if (!['off', 'observe', 'auto'].includes(policy.acceptMode) || !Array.isArray(policy.allowedActors) || !Array.isArray(policy.botActors) || !Array.isArray(policy.rules)) throw new Error('Invalid policy');
      for (const actorId of [...policy.allowedActors, ...policy.botActors]) nonempty(actorId, 'actor ID');
      if (policy.explicitCapabilities !== undefined) {
        if (!Array.isArray(policy.explicitCapabilities)) throw new Error('Invalid explicit capabilities');
        for (const capability of policy.explicitCapabilities) nonempty(capability, 'capability');
      }
      const ruleIds = new Set<string>();
      for (const rule of policy.rules) {
        nonempty(rule.id, 'rule ID'); nonempty(rule.instructions, 'rule instructions');
        if (typeof rule.enabled !== 'boolean' || !Array.isArray(rule.capabilities) || ruleIds.has(rule.id)) throw new Error('Invalid or duplicate rule');
        ruleIds.add(rule.id); for (const capability of rule.capabilities) nonempty(capability, 'capability');
      }
      await this.store.put(tx, 'policy', 'current', policy);
      await this.store.put(tx, 'policyRevision', String(policy.version), policy);
      for (const ticket of await this.store.listInTransaction<Ticket>(tx, 'ticket')) {
        if (ticket.lifecycle !== 'open') continue;
        const id = this.id('policyNotice', ticket.id, String(policy.version));
        await this.store.put(tx, 'workerNote', id, {
          id, ticketId: ticket.id, status: 'pending', kind: 'policy', policyVersion: policy.version,
          text: `Policy version ${policy.version} changed. Recall current instructions; this notice does not widen the original Mandate.`,
        });
      }
      const unresolved = new Set<string>();
      for (const evaluation of await this.store.listInTransaction<Eval>(tx, 'eval')) {
        if (evaluation.status !== 'pending') continue;
        await this.store.put(tx, 'eval', evaluation.id, { ...evaluation, status: 'invalid' });
        for (const triggerId of evaluation.triggerIds) unresolved.add(triggerId);
      }
      for (const queue of await this.store.listInTransaction<EvalQueue>(tx, 'evalQueue')) {
        for (const triggerId of queue.triggerIds) unresolved.add(triggerId);
        await this.store.put(tx, 'evalQueue', queue.threadId, { ...queue, triggerIds: [] });
      }
      const triggers: Trigger[] = [];
      for (const id of unresolved) triggers.push((await this.store.get<Trigger>(tx, 'trigger', id))!);
      for (const trigger of triggers.sort((a, b) => a.ordinal - b.ordinal)) {
        const disposition = (await this.store.get<Disposition>(tx, 'disposition', trigger.id))!;
        const epoch = disposition.epoch + 1;
        const next = { ...disposition, epoch, handbacks: disposition.handbacks.map(back => ({ ...back, epoch })) };
        await this.store.put(tx, 'disposition', trigger.id, next);
        await this.evaluateInTransaction(tx, trigger, next, policy);
        await this.releaseIntakeInTransaction(tx, trigger.id);
      }
    });
  }

  async listCandidates(): Promise<Eval[]> {
    return (await this.store.list<Eval>('eval')).filter(evaluation => evaluation.status === 'pending');
  }

  private async permittedThreads(tx: Tx, trigger: Trigger): Promise<string[]> {
    const source = await this.store.get<Source>(tx, 'source', trigger.sourceId);
    if (!source?.enabled) return [];
    return trigger.threads.filter(ref => source.listenedScopes === undefined || source.listenedScopes.includes(ref));
  }

  /** Revalidate the existing grant; resumption never expands scope or creates a replacement Mandate. */
  private async revalidateMandate(tx: Tx, mandate: Mandate, trigger: Trigger, policy: Policy): Promise<boolean> {
    if (mandate.withdrawnAt !== null || mandate.sourceId !== trigger.sourceId) return false;
    const permitted = await this.permittedThreads(tx, trigger);
    const source = await this.store.get<Source>(tx, 'source', mandate.sourceId);
    if (!source?.enabled || (source.listenedScopes !== undefined && mandate.scope.some(ref => !source.listenedScopes!.includes(ref)))) return false;
    if (!permitted.some(ref => mandate.scope.includes(ref))) return false;
    if (mandate.basis === 'explicit') return allowedHuman(policy, mandate.actor) && mandate.capabilities.every(cap => (policy.explicitCapabilities ?? defaultExplicitCapabilities).includes(cap));
    if (policy.acceptMode !== 'auto' || !mandate.ruleIds.length) return false;
    if (!(allowedHuman(policy, mandate.actor) || (mandate.actor.kind === 'bot' && policy.botActors.includes(mandate.actor.id)))) return false;
    const rules = mandate.ruleIds.map(id => policy.rules.find(rule => rule.id === id && rule.enabled));
    if (rules.some(rule => !rule)) return false;
    const capabilities = new Set(rules.flatMap(rule => rule!.capabilities));
    return mandate.capabilities.every(cap => capabilities.has(cap));
  }

  private remainingOwners(disposition: Disposition): string[] {
    return disposition.ownerTickets.filter(id => !disposition.handbacks.some(back => back.ticketId === id && back.epoch === disposition.epoch));
  }

  private async assignSubscriptions(tx: Tx, trigger: Trigger, disposition: Disposition, allowResume: boolean): Promise<Disposition> {
    const threadIds = new Set((await this.permittedThreads(tx, trigger)).map(ref => this.id('thread', trigger.sourceId, ref)));
    const owners = new Set<string>(), contexts = new Set<string>();
    const policy = await this.policyInTransaction(tx);
    for (const sub of await this.store.listInTransaction<Subscription>(tx, 'subscription')) {
      if (!sub.active || !threadIds.has(sub.threadId) || disposition.handbacks.some(back => back.ticketId === sub.ticketId)) continue;
      let ticket = await this.store.get<Ticket>(tx, 'ticket', sub.ticketId);
      if (!ticket || ticket.lifecycle === 'stopped') continue;
      const mandate = await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId);
      if (!mandate || mandate.withdrawnAt !== null) continue;
      if (ticket.lifecycle === 'ended' && sub.own && human(trigger.actor) && allowResume) {
        if (!await this.revalidateMandate(tx, mandate, trigger, policy)) {
          await this.store.put(tx, 'mandate', mandate.id, { ...mandate, withdrawnAt: Date.now() });
          continue;
        }
        const epoch = ticket.assignmentEpoch + 1, assignmentId = this.id('assignment', ticket.id, String(epoch));
        ticket = { ...ticket, lifecycle: 'open', assignmentEpoch: epoch, assignmentId, revision: ticket.revision + 1 };
        await this.store.put(tx, 'ticket', ticket.id, ticket);
        await this.store.put(tx, 'assignment', assignmentId, { id: assignmentId, ticketId: ticket.id, epoch, status: 'pending' } satisfies Assignment);
      }
      if (sub.own && ticket.lifecycle === 'open') owners.add(ticket.id); else contexts.add(ticket.id);
    }
    for (const id of owners) contexts.delete(id);
    const next: Disposition = { ...disposition, ownerTickets: [...new Set([...disposition.ownerTickets, ...owners])], contextTickets: [...new Set([...disposition.contextTickets, ...contexts])].filter(id => !owners.has(id)) };
    await this.store.put(tx, 'disposition', trigger.id, next);
    for (const ticketId of [...owners, ...contexts]) await this.deliverInTransaction(tx, (await this.store.get<Ticket>(tx, 'ticket', ticketId))!, trigger.id, owners.has(ticketId));
    return next;
  }

  /** Atomic receive hook: derive a durable disposition, deduplicated deliveries and one frozen Eval per Thread. */
  async routeInTransaction(tx: Tx, trigger: Trigger): Promise<void> {
    if (await this.store.get<Disposition>(tx, 'disposition', trigger.id)) return;
    const disposition = await this.assignSubscriptions(tx, trigger, { triggerId: trigger.id, ownerTickets: [], contextTickets: [], handbacks: [], acknowledged: false, epoch: 1, conflicts: 0 }, true);
    if (trigger.actor.kind !== 'human' || !this.remainingOwners(disposition).length) await this.evaluateInTransaction(tx, trigger, disposition, await this.policyInTransaction(tx));
    const intent = (await this.store.get<IntakeIntent>(tx, 'intake', trigger.id))!;
    await this.store.put(tx, 'intake', trigger.id, { ...intent, routed: true, released: false });
    await this.releaseIntakeInTransaction(tx, trigger.id);
  }

  private async retainIntakeInTransaction(tx: Tx, triggerId: string): Promise<void> {
    const intent = await this.store.get<IntakeIntent>(tx, 'intake', triggerId);
    if (!intent?.routed || !intent.released) return;
    const state = (await this.store.get<{ count: number; lastOrdinal: number }>(tx, 'state', 'intake'))!;
    if (state.count >= this.engine.maxPending) throw new Error('Pending intake capacity exceeded during requeue');
    await this.store.put(tx, 'state', 'intake', { ...state, count: state.count + 1 });
    await this.store.put(tx, 'intake', triggerId, { ...intent, released: false });
  }

  private async evaluateInTransaction(tx: Tx, trigger: Trigger, disposition: Disposition, policy: Policy): Promise<void> {
    const threads = await this.permittedThreads(tx, trigger), authority = basis(policy, trigger);
    if (!threads.length || !authority || (authority === 'inferred' && policy.acceptMode === 'off')) {
      await this.store.put(tx, 'disposition', trigger.id, { ...disposition, acknowledged: true }); return;
    }
    const pendingEvals = (await this.store.listInTransaction<Eval>(tx, 'eval')).filter(e => e.status === 'pending');
    if (pendingEvals.some(e => e.triggerIds.includes(trigger.id))) return;
    const threadIds = threads.map(ref => this.id('thread', trigger.sourceId, ref));
    const active = pendingEvals.find(e => (e.threadIds ?? [e.threadId]).some(id => threadIds.includes(id)));
    const { decisionId: _previousDecision, ...pending } = disposition;
    await this.store.put(tx, 'disposition', trigger.id, { ...pending, acknowledged: false });
    if (active) {
      const queue = await this.store.get<EvalQueue>(tx, 'evalQueue', active.threadId) ?? { threadId: active.threadId, triggerIds: [] };
      if (!queue.triggerIds.includes(trigger.id)) {
        if (queue.triggerIds.length >= this.store.maxIndexEntries) throw new Error('Thread evaluation queue capacity exceeded');
        await this.store.put(tx, 'evalQueue', queue.threadId, { ...queue, triggerIds: [...queue.triggerIds, trigger.id] });
      }
    } else {
      const id = this.id('eval', trigger.id, String(disposition.epoch));
      await this.store.put(tx, 'eval', id, { id, threadId: threadIds[0], threadIds, triggerIds: [trigger.id], policyVersion: policy.version, epoch: disposition.epoch, status: 'pending', basis: authority, actor: trigger.actor, sourceId: trigger.sourceId } satisfies Eval);
    }
    await this.retainIntakeInTransaction(tx, trigger.id);
  }

  private async drainQueuesInTransaction(tx: Tx): Promise<void> {
    for (const original of await this.store.listInTransaction<EvalQueue>(tx, 'evalQueue')) {
      let queue = (await this.store.get<EvalQueue>(tx, 'evalQueue', original.threadId))!;
      while (queue.triggerIds.length) {
        const pending = (await this.store.listInTransaction<Eval>(tx, 'eval')).some(e => e.status === 'pending' && (e.threadIds ?? [e.threadId]).includes(queue.threadId));
        if (pending) break;
        const [triggerId, ...remainder] = queue.triggerIds;
        await this.store.put(tx, 'evalQueue', queue.threadId, { ...queue, triggerIds: remainder });
        const trigger = (await this.store.get<Trigger>(tx, 'trigger', triggerId))!;
        const prior = (await this.store.get<Disposition>(tx, 'disposition', triggerId))!;
        const disposition = await this.assignSubscriptions(tx, trigger, prior, false);
        if (!disposition.acknowledged && (trigger.actor.kind !== 'human' || !this.remainingOwners(disposition).length)) await this.evaluateInTransaction(tx, trigger, disposition, await this.policyInTransaction(tx));
        await this.releaseIntakeInTransaction(tx, triggerId);
        queue = (await this.store.get<EvalQueue>(tx, 'evalQueue', original.threadId))!;
      }
    }
  }

  async decide(evalId: string, epoch: number, outcome: DecisionOutcome, meta: CommandMeta): Promise<string> {
    const result = await this.engine.command(meta.commandId, payload({ operation: 'decide', evalId, epoch, outcome, meta }), async tx => {
      const evaluation = await this.store.get<Eval>(tx, 'eval', evalId);
      const policy = await this.policyInTransaction(tx);
      if (!evaluation || evaluation.status !== 'pending' || evaluation.policyVersion !== policy.version) throw new Error('Invalid or stale policy evaluation');
      if (evaluation.epoch !== epoch) throw new Error('Stale evaluation epoch');
      const trigger = (await this.store.get<Trigger>(tx, 'trigger', evaluation.triggerIds[0]))!;
      let disposition = (await this.store.get<Disposition>(tx, 'disposition', trigger.id))!;
      const threads = await this.permittedThreads(tx, trigger);
      if (!threads.length) throw new Error('Source permissions no longer allow this decision');
      if (disposition.epoch !== epoch) throw new Error('Stale routing epoch');
      if (basis(policy, trigger) !== evaluation.basis) throw new Error('Evaluation authority is no longer valid');
      let recorded = outcome;
      if (outcome.kind === 'accept') {
        nonempty(outcome.goal, 'goal');
        if (!Array.isArray(outcome.ruleIds)) throw new Error('Rule IDs required');
        if (evaluation.basis === 'inferred' && policy.acceptMode === 'observe') recorded = { kind: 'ignore', reason: 'Observe mode: would accept', wouldAccept: true };
        else if (evaluation.basis === 'inferred') {
          if (policy.acceptMode !== 'auto') throw new Error('Inferred acceptance is off');
          if (outcome.ruleIds.length === 0 || outcome.ruleIds.some(id => !policy.rules.some(rule => rule.id === id && rule.enabled))) throw new Error('Enabled matching rule IDs required');
          const capabilities = new Set(policy.rules.filter(rule => outcome.ruleIds.includes(rule.id)).flatMap(rule => rule.capabilities));
          if (outcome.capabilities?.some(capability => !capabilities.has(capability))) throw new Error('Capabilities exceed matched rules');
        } else {
          if (outcome.ruleIds.some(id => !policy.rules.some(rule => rule.id === id && rule.enabled))) throw new Error('Unknown or disabled rule');
          const granted = policy.explicitCapabilities ?? defaultExplicitCapabilities;
          if (outcome.capabilities?.some(capability => !granted.includes(capability))) throw new Error('Explicit capabilities exceed trusted policy grants');
        }
      }
      const decisionId = this.id('decision', evalId, String(epoch));
      if (recorded.kind === 'attach') {
        const ticket = await this.requireOpenTicket(tx, recorded.ticketId, meta.expectedRevision);
        if (disposition.handbacks.some(back => back.ticketId === ticket.id && back.epoch === epoch)) {
          const conflicts = disposition.conflicts + 1;
          await this.store.put(tx, 'disposition', trigger.id, { ...disposition, conflicts, acknowledged: conflicts >= 3 });
          if (conflicts >= 3) {
            await this.store.put(tx, 'ticket', ticket.id, { ...ticket, attention: 'Repeated attach/handback conflict', revision: ticket.revision + 1 });
            await this.store.put(tx, 'eval', evalId, { ...evaluation, status: 'invalid' });
            await this.releaseIntakeInTransaction(tx, trigger.id);
            await this.drainQueuesInTransaction(tx);
          }
          return { error: 'Handback conflict: cannot attach to the same Ticket in this routing epoch' };
        }
        if (!disposition.ownerTickets.includes(ticket.id)) disposition.ownerTickets.push(ticket.id);
        disposition.contextTickets = disposition.contextTickets.filter(id => id !== ticket.id);
        await this.deliverInTransaction(tx, ticket, trigger.id, true);
      } else if (recorded.kind === 'accept') {
        const ticketId = this.id('ticket', decisionId), mandateId = this.id('mandate', decisionId), assignmentId = this.id('assignment', ticketId, '1');
        const capabilities = [...new Set(recorded.capabilities ?? (evaluation.basis === 'explicit' ? policy.explicitCapabilities ?? defaultExplicitCapabilities : policy.rules.filter(rule => recorded.ruleIds.includes(rule.id)).flatMap(rule => rule.capabilities)))];
        const mandate: Mandate = { id: mandateId, actor: evaluation.actor, basis: evaluation.basis, policyVersion: policy.version, ruleIds: [...new Set(recorded.ruleIds)], capabilities, rulesSnapshot: policy.rules.filter(rule => recorded.ruleIds.includes(rule.id)), sourceId: trigger.sourceId, scope: threads, withdrawnAt: null };
        const ticket: Ticket = { id: ticketId, goal: recorded.goal, mandateId, lifecycle: 'open', revision: 1, assignmentId, assignmentEpoch: 1 };
        await this.store.put(tx, 'mandate', mandateId, mandate);
        await this.store.put(tx, 'ticket', ticketId, ticket);
        await this.store.put(tx, 'assignment', assignmentId, { id: assignmentId, ticketId, epoch: 1, status: 'pending' } satisfies Assignment);
        await this.subscribeInTransaction(tx, ticketId, trigger.sourceId, threads[0], true);
        disposition.ownerTickets.push(ticketId);
        await this.deliverInTransaction(tx, ticket, trigger.id, true);
      } else if (recorded.kind === 'stop') {
        if (evaluation.basis !== 'explicit' || !allowedHuman(policy, evaluation.actor)) throw new Error('Stop requires an explicit authorized human');
        await this.stopInTransaction(tx, recorded.ticketId, recorded.reason, meta.expectedRevision, evaluation.triggerIds);
      } else if (recorded.kind === 'ignore') nonempty(recorded.reason, 'reason');
      else throw new Error('Unknown decision outcome');
      await this.store.put(tx, 'decision', decisionId, { id: decisionId, evalId, epoch, policyVersion: policy.version, outcome: recorded } satisfies Decision);
      await this.store.put(tx, 'eval', evalId, { ...evaluation, status: 'decided', decisionId });
      await this.store.put(tx, 'disposition', trigger.id, { ...disposition, decisionId, acknowledged: recorded.kind === 'ignore' || recorded.kind === 'stop' });
      await this.releaseIntakeInTransaction(tx, trigger.id);
      await this.drainQueuesInTransaction(tx);
      return { decisionId };
    });
    if ('error' in result) throw new Error(result.error);
    return result.decisionId;
  }

  private async requireOpenTicket(tx: Tx, ticketId: string, revision?: number): Promise<Ticket> {
    const ticket = await this.store.get<Ticket>(tx, 'ticket', ticketId);
    if (!ticket || ticket.lifecycle !== 'open') throw new Error('Ticket must be open (stopped Tickets cannot resume)');
    if (revision !== undefined && revision !== ticket.revision) throw new Error('Ticket revision conflict');
    const mandate = await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId);
    if (!mandate || mandate.withdrawnAt !== null) throw new Error('Ticket mandate is withdrawn');
    return ticket;
  }

  async subscribe(ticketId: string, sourceId: string, threadRef: string, own: boolean, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'subscribe', ticketId, sourceId, threadRef, own, meta }), tx => this.subscribeInTransaction(tx, ticketId, sourceId, threadRef, own, meta.expectedRevision));
  }
  async subscribeInTransaction(tx: Tx, ticketId: string, sourceId: string, threadRef: string, own: boolean, revision?: number): Promise<void> {
    nonempty(threadRef, 'threadRef'); if (typeof own !== 'boolean') throw new Error('own must be boolean');
    await this.requireOpenTicket(tx, ticketId, revision);
    const source = await this.store.get<Source>(tx, 'source', sourceId);
    if (!source?.enabled || (source.listenedScopes !== undefined && !source.listenedScopes.includes(threadRef))) throw new Error('Source permissions do not allow subscription');
    const threadId = this.id('thread', sourceId, threadRef), id = this.id('subscription', ticketId, threadId);
    await this.store.put(tx, 'subscription', id, { ticketId, threadId, own, active: true } satisfies Subscription);
  }
  async unsubscribe(ticketId: string, sourceId: string, threadRef: string, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'unsubscribe', ticketId, sourceId, threadRef, meta }), async tx => {
      const id = this.id('subscription', ticketId, this.id('thread', sourceId, threadRef));
      const sub = await this.store.get<Subscription>(tx, 'subscription', id);
      if (sub) await this.store.put(tx, 'subscription', id, { ...sub, active: false });
    });
  }

  async handback(ticketId: string, triggerId: string, epoch: number, reason: string, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'handback', ticketId, triggerId, epoch, reason, meta }), async tx => {
      nonempty(reason, 'reason');
      const disposition = await this.store.get<Disposition>(tx, 'disposition', triggerId);
      if (!disposition || disposition.epoch !== epoch) throw new Error('Stale handback epoch');
      if (!disposition.ownerTickets.includes(ticketId)) throw new Error('Ticket is not an assigned owner');
      if (disposition.handbacks.some(back => back.ticketId === ticketId && back.epoch === epoch)) return;
      await this.releaseOwnerInTransaction(tx, ticketId, triggerId, reason);
    });
  }

  private async releaseOwnerInTransaction(tx: Tx, ticketId: string, triggerId: string, reason: string): Promise<void> {
    const disposition = (await this.store.get<Disposition>(tx, 'disposition', triggerId))!;
    if (disposition.handbacks.some(back => back.ticketId === ticketId && back.epoch === disposition.epoch)) return;
    let next: Disposition = { ...disposition, handbacks: [...disposition.handbacks, { ticketId, epoch: disposition.epoch, reason }] };
    await this.store.put(tx, 'disposition', triggerId, next);
    for (const delivery of await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')) {
      if (delivery.ticketId === ticketId && delivery.triggerId === triggerId && delivery.status === 'pending') await this.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'invalid' });
    }
    if (!next.acknowledged && !(next.consumedTicketIds?.length) && !this.remainingOwners(next).length) {
      const evaluations = await this.store.listInTransaction<Eval>(tx, 'eval');
      const existing = evaluations.filter(e => e.triggerIds.includes(triggerId));
      if (!existing.some(e => e.status === 'pending')) {
        if (existing.length) {
          const epoch = next.epoch + 1;
          next = { ...next, epoch, handbacks: next.handbacks.map(back => ({ ...back, epoch })) };
          await this.store.put(tx, 'disposition', triggerId, next);
        }
        const trigger = (await this.store.get<Trigger>(tx, 'trigger', triggerId))!;
        await this.evaluateInTransaction(tx, trigger, next, await this.policyInTransaction(tx));
      }
    }
    await this.releaseIntakeInTransaction(tx, triggerId);
  }

  private async releaseTerminatedOwnership(tx: Tx, ticketId: string, reason: string, excludedTriggerIds: string[] = []): Promise<void> {
    for (const disposition of await this.store.listInTransaction<Disposition>(tx, 'disposition')) {
      if (!disposition.ownerTickets.includes(ticketId)) continue;
      if (excludedTriggerIds.includes(disposition.triggerId)) await this.store.put(tx, 'disposition', disposition.triggerId, { ...disposition, acknowledged: true });
      await this.releaseOwnerInTransaction(tx, ticketId, disposition.triggerId, reason);
    }
  }

  /** Application consumption is explicit; delivery transport receipts alone do not acknowledge an input. */
  async acknowledgeInputInTransaction(tx: Tx, ticketId: string, triggerId: string, epoch: number): Promise<void> {
    const ticket = await this.store.get<Ticket>(tx, 'ticket', ticketId);
    if (!ticket || ticket.lifecycle === 'stopped' || ticket.assignmentEpoch !== epoch) throw new Error('Stale input acknowledgement epoch');
    const disposition = await this.store.get<Disposition>(tx, 'disposition', triggerId);
    if (!disposition?.ownerTickets.includes(ticketId) || disposition.handbacks.some(back => back.ticketId === ticketId && back.epoch === disposition.epoch)) throw new Error('Input acknowledgement requires current ownership');
    const consumedTicketIds = [...new Set([...(disposition.consumedTicketIds ?? []), ticketId])];
    const acknowledged = this.remainingOwners(disposition).every(id => consumedTicketIds.includes(id));
    await this.store.put(tx, 'disposition', triggerId, { ...disposition, consumedTicketIds, acknowledged });
    for (const delivery of await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')) {
      if (delivery.ticketId === ticketId && delivery.triggerId === triggerId && delivery.status === 'pending') await this.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'delivered' });
    }
    await this.releaseIntakeInTransaction(tx, triggerId);
  }

  async stopTicket(ticketId: string, actor: Actor, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'stopTicket', ticketId, actor, meta }), async tx => {
      const policy = await this.policyInTransaction(tx);
      if (!allowedHuman(policy, actor) && !(human(actor) && actor.id === 'operator')) throw new Error('Stop requires an authorized trusted human');
      if (meta.triggerId !== undefined) {
        const trigger = await this.store.get<Trigger>(tx, 'trigger', meta.triggerId);
        if (!trigger || !human(trigger.actor) || trigger.actor.id !== actor.id) throw new Error('Stop input must belong to its authenticated human actor');
        const ticket = await this.store.get<Ticket>(tx, 'ticket', ticketId);
        const mandate = ticket ? await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId) : undefined;
        if (!mandate || mandate.sourceId !== trigger.sourceId || !trigger.threads.some(ref => mandate.scope.includes(ref)) || !(await this.permittedThreads(tx, trigger)).length) throw new Error('Stop input source scope denied');
      }
      await this.stopInTransaction(tx, ticketId, 'Stopped by human', meta.expectedRevision, meta.triggerId === undefined ? [] : [meta.triggerId]);
    });
  }
  async stopInTransaction(tx: Tx, ticketId: string, reason: string, revision?: number, excludedTriggerIds: string[] = []): Promise<void> {
    const ticket = await this.store.get<Ticket>(tx, 'ticket', ticketId);
    if (!ticket) throw new Error('Unknown Ticket');
    if (revision !== undefined && revision !== ticket.revision) throw new Error('Ticket revision conflict');
    for (const triggerId of excludedTriggerIds) {
      const disposition = await this.store.get<Disposition>(tx, 'disposition', triggerId);
      if (disposition) await this.store.put(tx, 'disposition', triggerId, { ...disposition, acknowledged: true });
      for (const evaluation of await this.store.listInTransaction<Eval>(tx, 'eval')) {
        if (evaluation.status === 'pending' && evaluation.triggerIds.includes(triggerId)) await this.store.put(tx, 'eval', evaluation.id, { ...evaluation, status: 'invalid' });
      }
      for (const queue of await this.store.listInTransaction<EvalQueue>(tx, 'evalQueue')) {
        if (queue.triggerIds.includes(triggerId)) await this.store.put(tx, 'evalQueue', queue.threadId, { ...queue, triggerIds: queue.triggerIds.filter(id => id !== triggerId) });
      }
    }
    if (ticket.lifecycle === 'stopped') {
      for (const triggerId of excludedTriggerIds) await this.releaseIntakeInTransaction(tx, triggerId);
      await this.drainQueuesInTransaction(tx);
      return;
    }
    const mandate = (await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId))!;
    await this.store.put(tx, 'mandate', mandate.id, { ...mandate, withdrawnAt: Date.now() });
    await this.store.put(tx, 'ticket', ticketId, { ...ticket, lifecycle: 'stopped', revision: ticket.revision + 1, assignmentEpoch: ticket.assignmentEpoch + 1 });
    const assignment = await this.store.get<Assignment>(tx, 'assignment', ticket.assignmentId);
    if (assignment) await this.store.put(tx, 'assignment', assignment.id, { ...assignment, status: 'stopped' });
    const id = this.id('cancel', ticketId, String(ticket.assignmentEpoch));
    await this.store.put(tx, 'cancel', id, { id, ticketId, assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, status: 'pending', reason } satisfies CancelIntent);
    await this.releaseTerminatedOwnership(tx, ticketId, reason, excludedTriggerIds);
    for (const kind of ['delivery', 'outbox']) {
      for (const record of await this.store.listInTransaction<{ id: string; ticketId?: string; triggerId?: string; status: string }>(tx, kind)) {
        if (record.ticketId !== ticketId || record.status !== 'pending') continue;
        await this.store.put(tx, kind, record.id, { ...record, status: 'invalid' });
        if (record.triggerId) await this.releaseIntakeInTransaction(tx, record.triggerId);
      }
    }
    for (const triggerId of excludedTriggerIds) await this.releaseIntakeInTransaction(tx, triggerId);
    await this.drainQueuesInTransaction(tx);
  }
  async endTicket(ticketId: string, epoch: number, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'endTicket', ticketId, epoch, meta }), async tx => {
      const ticket = await this.requireOpenTicket(tx, ticketId, meta.expectedRevision);
      if (ticket.assignmentEpoch !== epoch) throw new Error('Stale assignment epoch');
      await this.store.put(tx, 'ticket', ticket.id, { ...ticket, lifecycle: 'ended', revision: ticket.revision + 1 });
      const assignment = await this.store.get<Assignment>(tx, 'assignment', ticket.assignmentId);
      if (assignment) await this.store.put(tx, 'assignment', assignment.id, { ...assignment, status: 'ended' });
      await this.releaseTerminatedOwnership(tx, ticketId, 'Owner naturally ended');
      for (const delivery of await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')) {
        if (delivery.ticketId === ticketId && delivery.status === 'pending') {
          await this.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'invalid' });
          await this.releaseIntakeInTransaction(tx, delivery.triggerId);
        }
      }
    });
  }

  async deliverInTransaction(tx: Tx, ticket: Ticket, triggerId: string, own: boolean): Promise<void> {
    const id = this.id('delivery', ticket.id, triggerId, String(ticket.assignmentEpoch));
    const previous = await this.store.get<DeliveryIntent>(tx, 'delivery', id);
    if (previous) {
      const frozen = await this.store.get(tx, 'deliveryRun', id);
      if (own && !previous.own && previous.status === 'pending' && !frozen) await this.store.put(tx, 'delivery', id, { ...previous, own: true, resume: ticket.lifecycle === 'open' });
      if (own && !previous.own && (previous.status === 'delivered' || frozen)) {
        const ownershipId = this.id('delivery', ticket.id, triggerId, String(ticket.assignmentEpoch), 'ownership');
        if (!await this.store.get<DeliveryIntent>(tx, 'delivery', ownershipId)) await this.store.put(tx, 'delivery', ownershipId, { ...previous, id: ownershipId, ownershipOf: previous.id, own: true, resume: ticket.lifecycle === 'open', status: 'pending' });
      }
      return;
    }
    await this.store.put(tx, 'delivery', id, { id, ticketId: ticket.id, triggerId, epoch: ticket.assignmentEpoch, own, resume: own && ticket.lifecycle === 'open', status: 'pending' } satisfies DeliveryIntent);
  }
  /** Downstream receipt accounting: invalid/stale deliveries remain invalid and cannot resume work. */
  async acknowledgeDeliveryInTransaction(tx: Tx, deliveryId: string): Promise<void> {
    const delivery = await this.store.get<DeliveryIntent>(tx, 'delivery', deliveryId);
    if (!delivery) throw new Error('Unknown delivery');
    if (delivery.status === 'pending') await this.store.put(tx, 'delivery', deliveryId, { ...delivery, status: 'delivered' });
    await this.releaseIntakeInTransaction(tx, delivery.triggerId);
  }
  /** Capacity counts one outstanding routed Trigger, until all its Eval/Delivery work settles. */
  async releaseIntakeInTransaction(tx: Tx, triggerId: string): Promise<void> {
    const intent = await this.store.get<IntakeIntent>(tx, 'intake', triggerId);
    if (!intent?.routed || intent.released) return;
    if ((await this.store.listInTransaction<Eval>(tx, 'eval')).some(e => e.status === 'pending' && e.triggerIds.includes(triggerId))) return;
    if ((await this.store.listInTransaction<EvalQueue>(tx, 'evalQueue')).some(q => q.triggerIds.includes(triggerId))) return;
    if ((await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')).some(d => d.status === 'pending' && d.triggerId === triggerId)) return;
    const disposition = await this.store.get<Disposition>(tx, 'disposition', triggerId);
    if (disposition && this.remainingOwners(disposition).some(id => !disposition.consumedTicketIds?.includes(id))) return;
    const state = await this.store.get<{ count: number; lastOrdinal: number }>(tx, 'state', 'intake');
    if (!state || state.count < 1) throw new Error('Intake accounting underflow');
    await this.store.put(tx, 'state', 'intake', { ...state, count: state.count - 1 });
    await this.store.put(tx, 'intake', triggerId, { ...intent, released: true });
  }
}
