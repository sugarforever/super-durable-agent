import type { JsonValue } from '@earendil-works/chord';
import type { Tx } from '@earendil-works/pi-durable';
import type { Engine } from './engine.js';
import { canonicalJson, scopedId, type Actor, type Assignment, type CancelIntent, type CommandMeta, type Decision,
  type DecisionOutcome, type DeliveryIntent, type Disposition, type Eval, type IntakeIntent, type Mandate,
  type Policy, type Source, type Subscription, type Ticket, type Trigger } from './model.js';

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
      for (const evaluation of await this.store.listInTransaction<Eval>(tx, 'eval')) {
        if (evaluation.status !== 'pending') continue;
        await this.store.put(tx, 'eval', evaluation.id, { ...evaluation, status: 'invalid' });
        for (const triggerId of evaluation.triggerIds) await this.releaseIntakeInTransaction(tx, triggerId);
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

  /** Atomic receive hook: derive a durable disposition, deduplicated deliveries and at most one Eval. */
  async routeInTransaction(tx: Tx, trigger: Trigger): Promise<void> {
    if (await this.store.get<Disposition>(tx, 'disposition', trigger.id)) return;
    const threadIds = new Set((await this.permittedThreads(tx, trigger)).map(ref => this.id('thread', trigger.sourceId, ref)));
    const owners = new Set<string>(), contexts = new Set<string>();
    for (const sub of await this.store.listInTransaction<Subscription>(tx, 'subscription')) {
      if (!sub.active || !threadIds.has(sub.threadId)) continue;
      const ticket = await this.store.get<Ticket>(tx, 'ticket', sub.ticketId);
      if (!ticket || ticket.lifecycle === 'stopped') continue;
      const mandate = await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId);
      if (!mandate || mandate.withdrawnAt !== null) continue;
      if (sub.own && ticket.lifecycle === 'open') owners.add(ticket.id); else contexts.add(ticket.id);
    }
    for (const ticketId of owners) contexts.delete(ticketId);
    const disposition: Disposition = { triggerId: trigger.id, ownerTickets: [...owners], contextTickets: [...contexts], handbacks: [], acknowledged: false, epoch: 1, conflicts: 0 };
    await this.store.put(tx, 'disposition', trigger.id, disposition);
    for (const ticketId of [...owners, ...contexts]) {
      const ticket = (await this.store.get<Ticket>(tx, 'ticket', ticketId))!;
      await this.deliverInTransaction(tx, ticket, trigger.id, owners.has(ticketId));
    }
    const policy = await this.policyInTransaction(tx);
    if (trigger.actor.kind !== 'human' || owners.size === 0) await this.evaluateInTransaction(tx, trigger, disposition, policy);
    const intent = (await this.store.get<IntakeIntent>(tx, 'intake', trigger.id))!;
    await this.store.put(tx, 'intake', trigger.id, { ...intent, routed: true, released: false });
    await this.releaseIntakeInTransaction(tx, trigger.id);
  }

  private async evaluateInTransaction(tx: Tx, trigger: Trigger, disposition: Disposition, policy: Policy): Promise<void> {
    const threads = await this.permittedThreads(tx, trigger);
    const authority = basis(policy, trigger);
    if (!threads.length || !authority || (authority === 'inferred' && policy.acceptMode === 'off')) {
      await this.store.put(tx, 'disposition', trigger.id, { ...disposition, acknowledged: true }); return;
    }
    const id = this.id('eval', trigger.id, String(disposition.epoch));
    const evaluation: Eval = { id, threadId: this.id('thread', trigger.sourceId, threads[0]), triggerIds: [trigger.id], policyVersion: policy.version, epoch: disposition.epoch, status: 'pending', basis: authority, actor: trigger.actor, sourceId: trigger.sourceId };
    await this.store.put(tx, 'eval', id, evaluation);
    const { decisionId: _previousDecision, ...pending } = disposition;
    await this.store.put(tx, 'disposition', trigger.id, { ...pending, acknowledged: false });
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
          }
          return { error: 'Handback conflict: cannot attach to the same Ticket in this routing epoch' };
        }
        if (!disposition.ownerTickets.includes(ticket.id)) disposition.ownerTickets.push(ticket.id);
        disposition.contextTickets = disposition.contextTickets.filter(id => id !== ticket.id);
        await this.deliverInTransaction(tx, ticket, trigger.id, true);
      } else if (recorded.kind === 'accept') {
        const ticketId = this.id('ticket', decisionId), mandateId = this.id('mandate', decisionId), assignmentId = this.id('assignment', ticketId, '1');
        const capabilities = [...new Set(recorded.capabilities ?? (evaluation.basis === 'explicit' ? policy.explicitCapabilities ?? defaultExplicitCapabilities : policy.rules.filter(rule => recorded.ruleIds.includes(rule.id)).flatMap(rule => rule.capabilities)))];
        const mandate: Mandate = { id: mandateId, actor: evaluation.actor, basis: evaluation.basis, policyVersion: policy.version, ruleIds: [...new Set(recorded.ruleIds)], capabilities, sourceId: trigger.sourceId, scope: threads, withdrawnAt: null };
        const ticket: Ticket = { id: ticketId, goal: recorded.goal, mandateId, lifecycle: 'open', revision: 1, assignmentId, assignmentEpoch: 1 };
        await this.store.put(tx, 'mandate', mandateId, mandate);
        await this.store.put(tx, 'ticket', ticketId, ticket);
        await this.store.put(tx, 'assignment', assignmentId, { id: assignmentId, ticketId, epoch: 1, status: 'pending' } satisfies Assignment);
        await this.subscribeInTransaction(tx, ticketId, trigger.sourceId, threads[0], true);
        disposition.ownerTickets.push(ticketId);
        await this.deliverInTransaction(tx, ticket, trigger.id, true);
      } else if (recorded.kind === 'stop') {
        if (evaluation.basis !== 'explicit' || !allowedHuman(policy, evaluation.actor)) throw new Error('Stop requires an explicit authorized human');
        await this.stopInTransaction(tx, recorded.ticketId, recorded.reason, meta.expectedRevision);
      } else if (recorded.kind === 'ignore') nonempty(recorded.reason, 'reason');
      else throw new Error('Unknown decision outcome');
      await this.store.put(tx, 'decision', decisionId, { id: decisionId, evalId, epoch, policyVersion: policy.version, outcome: recorded } satisfies Decision);
      await this.store.put(tx, 'eval', evalId, { ...evaluation, status: 'decided', decisionId });
      await this.store.put(tx, 'disposition', trigger.id, { ...disposition, decisionId, acknowledged: recorded.kind === 'ignore' || recorded.kind === 'stop' });
      await this.releaseIntakeInTransaction(tx, trigger.id);
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
      const next: Disposition = { ...disposition, handbacks: [...disposition.handbacks, { ticketId, epoch, reason }] };
      await this.store.put(tx, 'disposition', triggerId, next);
      for (const delivery of await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')) {
        if (delivery.ticketId === ticketId && delivery.triggerId === triggerId && delivery.status === 'pending') await this.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'invalid' });
      }
      if (next.ownerTickets.every(id => next.handbacks.some(back => back.ticketId === id && back.epoch === epoch))) {
        const trigger = (await this.store.get<Trigger>(tx, 'trigger', triggerId))!;
        const previous = await this.store.get<Eval>(tx, 'eval', this.id('eval', triggerId, String(epoch)));
        if (previous?.status !== 'pending') {
          const routing = previous ? { ...next, epoch: epoch + 1, handbacks: next.handbacks.map(back => ({ ...back, epoch: epoch + 1 })) } : next;
          await this.store.put(tx, 'disposition', triggerId, routing);
          await this.evaluateInTransaction(tx, trigger, routing, await this.policyInTransaction(tx));
          const candidate = await this.store.get<Eval>(tx, 'eval', this.id('eval', triggerId, String(routing.epoch)));
          const intent = await this.store.get<IntakeIntent>(tx, 'intake', triggerId);
          if (candidate?.status === 'pending' && intent?.released) {
            const state = (await this.store.get<{ count: number; lastOrdinal: number }>(tx, 'state', 'intake'))!;
            if (state.count >= this.engine.maxPending) throw new Error('Pending intake capacity exceeded during handback');
            await this.store.put(tx, 'state', 'intake', { ...state, count: state.count + 1 });
            await this.store.put(tx, 'intake', triggerId, { ...intent, released: false });
          }
        }
      }
      await this.releaseIntakeInTransaction(tx, triggerId);
    });
  }

  async stopTicket(ticketId: string, actor: Actor, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'stopTicket', ticketId, actor, meta }), async tx => {
      const policy = await this.policyInTransaction(tx);
      if (!allowedHuman(policy, actor) && !(human(actor) && actor.id === 'operator')) throw new Error('Stop requires an authorized trusted human');
      await this.stopInTransaction(tx, ticketId, 'Stopped by human', meta.expectedRevision);
    });
  }
  async stopInTransaction(tx: Tx, ticketId: string, reason: string, revision?: number): Promise<void> {
    const ticket = await this.store.get<Ticket>(tx, 'ticket', ticketId);
    if (!ticket) throw new Error('Unknown Ticket');
    if (revision !== undefined && revision !== ticket.revision) throw new Error('Ticket revision conflict');
    if (ticket.lifecycle === 'stopped') return;
    const mandate = (await this.store.get<Mandate>(tx, 'mandate', ticket.mandateId))!;
    await this.store.put(tx, 'mandate', mandate.id, { ...mandate, withdrawnAt: Date.now() });
    await this.store.put(tx, 'ticket', ticketId, { ...ticket, lifecycle: 'stopped', revision: ticket.revision + 1, assignmentEpoch: ticket.assignmentEpoch + 1 });
    const assignment = await this.store.get<Assignment>(tx, 'assignment', ticket.assignmentId);
    if (assignment) await this.store.put(tx, 'assignment', assignment.id, { ...assignment, status: 'stopped' });
    const id = this.id('cancel', ticketId, String(ticket.assignmentEpoch));
    await this.store.put(tx, 'cancel', id, { id, ticketId, assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, status: 'pending', reason } satisfies CancelIntent);
    for (const kind of ['delivery', 'outbox']) {
      for (const record of await this.store.listInTransaction<{ id: string; ticketId?: string; triggerId?: string; status: string }>(tx, kind)) {
        if (record.ticketId !== ticketId || record.status !== 'pending') continue;
        await this.store.put(tx, kind, record.id, { ...record, status: 'invalid' });
        if (record.triggerId) await this.releaseIntakeInTransaction(tx, record.triggerId);
      }
    }
  }
  async endTicket(ticketId: string, epoch: number, meta: CommandMeta): Promise<void> {
    await this.engine.command(meta.commandId, payload({ operation: 'endTicket', ticketId, epoch, meta }), async tx => {
      const ticket = await this.requireOpenTicket(tx, ticketId, meta.expectedRevision);
      if (ticket.assignmentEpoch !== epoch) throw new Error('Stale assignment epoch');
      await this.store.put(tx, 'ticket', ticket.id, { ...ticket, lifecycle: 'ended', revision: ticket.revision + 1 });
      const assignment = await this.store.get<Assignment>(tx, 'assignment', ticket.assignmentId);
      if (assignment) await this.store.put(tx, 'assignment', assignment.id, { ...assignment, status: 'ended' });
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
      if (own && !previous.own && previous.status === 'pending') await this.store.put(tx, 'delivery', id, { ...previous, own: true, resume: ticket.lifecycle === 'open' });
      if (own && !previous.own && previous.status === 'delivered') {
        const ownershipId = this.id('delivery', ticket.id, triggerId, String(ticket.assignmentEpoch), 'ownership');
        if (!await this.store.get<DeliveryIntent>(tx, 'delivery', ownershipId)) await this.store.put(tx, 'delivery', ownershipId, { ...previous, id: ownershipId, own: true, resume: ticket.lifecycle === 'open', status: 'pending' });
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
    if ((await this.store.listInTransaction<DeliveryIntent>(tx, 'delivery')).some(d => d.status === 'pending' && d.triggerId === triggerId)) return;
    const state = await this.store.get<{ count: number; lastOrdinal: number }>(tx, 'state', 'intake');
    if (!state || state.count < 1) throw new Error('Intake accounting underflow');
    await this.store.put(tx, 'state', 'intake', { ...state, count: state.count - 1 });
    await this.store.put(tx, 'intake', triggerId, { ...intent, released: true });
  }
}
