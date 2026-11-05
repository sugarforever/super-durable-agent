import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openSuperAgent } from '../src/runtime/open.js';
const alice = { id: 'alice', kind: 'human' as const, trusted: true };
const bot = { id: 'bot', kind: 'bot' as const, trusted: true };
const dirs: string[] = []; const runtimes: Awaited<ReturnType<typeof openSuperAgent>>[] = [];
const event = (externalId: string, patch = {}) => ({ sourceId: 'cli', externalId, revision: '1', actor: alice, threads: ['root'], body: externalId, receivedAt: 1000, occurredAt: 900, addressed: true, ...patch });
async function open() {
  const directory = await mkdtemp(join(tmpdir(), 'sa-ownership-')); dirs.push(directory);
  const r = await openSuperAgent({ directory }); runtimes.push(r);
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: ['bot'], rules: [{ id: 'r', enabled: true, instructions: 'Maintain', capabilities: ['read'] }] }, alice, { commandId: 'policy' });
  return r;
}
async function ticket(r: Awaited<ReturnType<typeof openSuperAgent>>, id: string) {
  await r.receive(event(id, { threads: [id] }));
  const c = (await r.listCandidates()).find(c => c.status === 'pending')!;
  await r.decide(c.id, c.epoch, { kind: 'accept', goal: id, ruleIds: [] }, { commandId: `accept-${id}` });
  return (await r.list('ticket')).find(t => t.goal === id)!;
}
afterEach(async () => { await Promise.all(runtimes.splice(0).map(r => r.close())); await Promise.all(dirs.splice(0).map(d => rm(d, { recursive: true, force: true }))); });

describe('ownership routing on real pi SQLite', () => {
  it('deduplicates multiple threads per owner and requeues only after the last handback', async () => {
    const r = await open(); const a = await ticket(r, 'a'), b = await ticket(r, 'b');
    for (const t of [a, b]) for (const ref of ['one', 'two']) await r.subscribe(t.id, 'cli', ref, true, { commandId: `${t.goal}-${ref}` });
    const received = await r.receive(event('shared', { threads: ['one', 'two'] }));
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('delivery')).filter(d => d.triggerId === received.triggerId)).toHaveLength(2);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId)).toMatchObject({ ownerTickets: [a.id, b.id], epoch: 1 });
    await r.handback(a.id, received.triggerId, 1, 'not mine', { commandId: 'a-back' });
    expect(await r.listCandidates()).toEqual([]);
    await r.handback(b.id, received.triggerId, 1, 'not mine', { commandId: 'b-back' });
    const [c] = await r.listCandidates(); expect(c.triggerIds).toEqual([received.triggerId]);
    await expect(r.decide(c.id, c.epoch, { kind: 'attach', ticketId: a.id }, { commandId: 'bounce' })).rejects.toThrow(/handback/i);
    await r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'handled' }, { commandId: 'done' });
    expect(await r.listCandidates()).toEqual([]);
  });
  it('delivers context-only subscriptions without ownership and independently evaluates bots', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'context', false, { commandId: 'context' });
    const contextual = await r.receive(event('contextual', { threads: ['context'] }));
    expect((await r.list('disposition')).find(d => d.triggerId === contextual.triggerId)).toMatchObject({ ownerTickets: [], contextTickets: [a.id] });
    expect((await r.list('delivery')).find(d => d.triggerId === contextual.triggerId)).toMatchObject({ own: false, resume: false });
    const c = (await r.listCandidates())[0]; await r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'context' }, { commandId: 'ignore-context' });
    await r.subscribe(a.id, 'cli', 'owned', true, { commandId: 'owned' });
    const botInput = await r.receive(event('bot-input', { actor: bot, threads: ['owned'] }));
    expect(await r.listCandidates()).toMatchObject([{ triggerIds: [botInput.triggerId], basis: 'inferred' }]);
    expect((await r.list('delivery')).filter(d => d.triggerId === botInput.triggerId)).toHaveLength(1);
  });
  it('forwards unlisted human owner feedback without new authority and retains assigned ownership after unsubscribe', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'owned', true, { commandId: 'sub' });
    const received = await r.receive(event('stranger', { actor: { id: 'stranger', kind: 'human', trusted: false }, threads: ['owned'] }));
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('delivery')).find(d => d.triggerId === received.triggerId)).toMatchObject({ ticketId: a.id, own: true });
    await r.unsubscribe(a.id, 'cli', 'owned', { commandId: 'unsub' });
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId).ownerTickets).toEqual([a.id]);
    await r.handback(a.id, received.triggerId, 1, 'release', { commandId: 'back' });
    expect(await r.listCandidates()).toEqual([]); // unknown actor never gains authority through handback
  });
  it('stops atomically with authority checks, withdrawal, epoch fence and cancellation', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.engine.transaction(tx => r.engine.store.put(tx, 'outbox', 'pending-send', { id: 'pending-send', ticketId: a.id, status: 'pending', body: 'stale' }));
    for (const actor of [bot, { ...alice, trusted: false }, { ...alice, id: 'stranger' }]) await expect(r.stopTicket(a.id, actor, { commandId: `stop-${actor.id}-${actor.trusted}` })).rejects.toThrow(/authorized|trusted|human/i);
    await r.stopTicket(a.id, alice, { commandId: 'stop' });
    await r.stopTicket(a.id, alice, { commandId: 'stop' });
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'stopped', assignmentEpoch: 2 });
    expect((await r.list('mandate'))[0].withdrawnAt).toEqual(expect.any(Number));
    expect(await r.list('cancel')).toMatchObject([{ ticketId: a.id, epoch: 1, status: 'pending' }]);
    expect(await r.list('outbox')).toMatchObject([{ status: 'invalid' }]);
    expect((await r.list('delivery')).every(d => d.status === 'invalid')).toBe(true);
    const received = await r.receive(event('after-stop', { threads: ['a'] }));
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId).ownerTickets).toEqual([]);
    const [c] = await r.listCandidates();
    await expect(r.decide(c.id, c.epoch, { kind: 'attach', ticketId: a.id }, { commandId: 'resume-stopped' })).rejects.toThrow(/stopped|open/i);
  });
  it('bot stop decisions are refused even when addressed and an explicit allowed human can stop', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.receive(event('bot-stop', { actor: bot })); let c = (await r.listCandidates())[0];
    await expect(r.decide(c.id, c.epoch, { kind: 'stop', ticketId: a.id, reason: 'cancel' }, { commandId: 'bot-stop' })).rejects.toThrow(/explicit|human|authorized/i);
    await r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'bot' }, { commandId: 'ignore-bot' });
    await r.receive(event('human-stop')); c = (await r.listCandidates())[0];
    await r.decide(c.id, c.epoch, { kind: 'stop', ticketId: a.id, reason: 'cancel' }, { commandId: 'human-stop' });
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'stopped' });
  });
  it('a completed ticket can receive context but context cannot reopen it', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'context', false, { commandId: 'sub' });
    await r.endTicket(a.id, a.assignmentEpoch, { commandId: 'end' });
    await r.receive(event('late-context', { threads: ['context'] }));
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'ended' });
    expect((await r.list('delivery')).filter(d => d.triggerId !== (a as any).triggerId && d.resume === true)).toHaveLength(1); // initial acceptance only
  });
  it('transport receipts retain unread ownership capacity and handback uses a distinct Eval', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const original = (await r.list('eval'))[0];
    const delivery = (await r.list('delivery'))[0];
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeDeliveryInTransaction(tx, delivery.id));
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeDeliveryInTransaction(tx, delivery.id));
    expect((await r.engine.store.read('state', 'intake')).count).toBe(1);
    await r.handback(a.id, delivery.triggerId, 1, 'release', { commandId: 'return-initial' });
    const [fresh] = await r.listCandidates();
    expect(fresh.id).not.toBe(original.id);
    expect((await r.engine.store.read('state', 'intake')).count).toBe(1);
    await r.decide(fresh.id, fresh.epoch, { kind: 'ignore', reason: 'done' }, { commandId: 'ignore-returned' });
    expect(await r.list('decision')).toHaveLength(2);
    expect((await r.engine.store.read('state', 'intake')).count).toBe(0);
  });
  it('marks attention after three attach/handback conflicts without replay incrementing conflicts', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'shared', true, { commandId: 'sub' });
    const received = await r.receive(event('shared', { threads: ['shared'] }));
    await r.handback(a.id, received.triggerId, 1, 'release', { commandId: 'back' });
    const [c] = await r.listCandidates();
    for (const commandId of ['bounce-1', 'bounce-1', 'bounce-2', 'bounce-3']) await expect(r.decide(c.id, c.epoch, { kind: 'attach', ticketId: a.id }, { commandId })).rejects.toThrow(/handback/i);
    expect(await r.readTicket(a.id)).toMatchObject({ attention: expect.stringMatching(/conflict/i) });
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId)).toMatchObject({ conflicts: 3, acknowledged: true });
  });
  it('completion fences pending deliveries and late stop receipts remain accounting only', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [delivery] = await r.list('delivery');
    await r.endTicket(a.id, 1, { commandId: 'end' });
    expect((await r.list('delivery'))[0].status).toBe('invalid');
    await r.stopTicket(a.id, alice, { commandId: 'stop-ended' });
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeDeliveryInTransaction(tx, delivery.id));
    expect((await r.list('delivery'))[0].status).toBe('invalid');
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'stopped', assignmentEpoch: 2 });
    expect((await r.engine.store.read('state', 'intake')).count).toBe(1);
    expect(await r.listCandidates()).toHaveLength(1); // unread input was returned to Router, not discarded
  });
  it('current source permissions filter subscriptions on each thread before fanout', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'secret', true, { commandId: 'secret' });
    await r.engine.configureSource({ id: 'cli', kind: 'cli', enabled: true, listenedScopes: ['public'] });
    const received = await r.receive(event('mixed-scopes', { threads: ['secret', 'public'] }));
    expect((await r.list('delivery')).filter(d => d.triggerId === received.triggerId)).toEqual([]);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId).ownerTickets).toEqual([]);
    const [c] = await r.listCandidates();
    await r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Public task', ruleIds: [] }, { commandId: 'accept-public' });
    expect((await r.list('mandate')).find(m => m.id !== a.mandateId).scope).toEqual(['public']);
  });

  it('attaching a pending contextual input upgrades the same delivery to ownership without a second mandate', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'context', false, { commandId: 'context-sub' });
    const received = await r.receive(event('attach-context', { threads: ['context'] }));
    const [c] = await r.listCandidates();
    await r.decide(c.id, c.epoch, { kind: 'attach', ticketId: a.id, note: 'same work' }, { commandId: 'attach' });
    expect((await r.list('delivery')).filter(d => d.triggerId === received.triggerId)).toMatchObject([{ ticketId: a.id, own: true, resume: true }]);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId)).toMatchObject({ ownerTickets: [a.id], contextTickets: [] });
    expect(await r.list('mandate')).toHaveLength(1);
  });

  it('attachment after a context receipt still schedules an ownership delivery', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'context', false, { commandId: 'sub-context' });
    const received = await r.receive(event('already-context', { threads: ['context'] }));
    const context = (await r.list('delivery')).find(d => d.triggerId === received.triggerId)!;
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeDeliveryInTransaction(tx, context.id));
    const [c] = await r.listCandidates();
    await r.decide(c.id, c.epoch, { kind: 'attach', ticketId: a.id }, { commandId: 'attach-after-receipt' });
    expect((await r.list('delivery')).filter(d => d.triggerId === received.triggerId && d.status === 'pending')).toMatchObject([{ own: true, resume: true }]);
  });
  it('owner handback preserves a previous ignored bot evaluation acknowledgement', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'owned', true, { commandId: 'sub' });
    const received = await r.receive(event('bot-owned', { actor: bot, threads: ['owned'] }));
    const [c] = await r.listCandidates();
    await r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'handled by owner' }, { commandId: 'ignore' });
    await r.handback(a.id, received.triggerId, 1, 'cannot handle', { commandId: 'back' });
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId).acknowledged).toBe(true);
  });

  it('stop releases one unread owner and the final owner handback requeues once', async () => {
    const r = await open(); const a = await ticket(r, 'a'), b = await ticket(r, 'b');
    for (const t of [a, b]) await r.subscribe(t.id, 'cli', 'shared', true, { commandId: `${t.goal}-sub` });
    const received = await r.receive(event('shared-unread', { threads: ['shared'] }));
    await r.stopTicket(a.id, alice, { commandId: 'stop-a' });
    expect((await r.listCandidates()).filter(c => c.triggerIds.includes(received.triggerId))).toEqual([]);
    await r.handback(b.id, received.triggerId, 1, 'release', { commandId: 'b-back' });
    await r.handback(b.id, received.triggerId, 1, 'release', { commandId: 'b-back' });
    expect((await r.listCandidates()).filter(c => c.triggerIds.includes(received.triggerId))).toHaveLength(1);
    expect((await r.list('disposition')).find(d => d.triggerId === received.triggerId).handbacks).toHaveLength(2);
  });
  it('ending an owner requeues unread feedback but preserves acknowledged inputs', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    const unread = await r.receive(event('unread', { threads: ['a'] }));
    await r.endTicket(a.id, 1, { commandId: 'end' });
    await r.endTicket(a.id, 1, { commandId: 'end' });
    expect(await r.listCandidates()).toMatchObject([{ triggerIds: [unread.triggerId] }]);
    expect((await r.list('disposition')).find(d => d.triggerId === initial.triggerId).acknowledged).toBe(true);
  });
  it('naturally ended own-human feedback revalidates and resumes the same Ticket and Worker', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(async tx => {
      await r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1);
      await r.engine.store.put(tx, 'ticket', a.id, { ...a, workerConversationId: 'worker-binding' });
    });
    await r.endTicket(a.id, 1, { commandId: 'end' });
    const feedback = await r.receive(event('follow-up', { threads: ['a'] }));
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'open', mandateId: a.mandateId, workerConversationId: 'worker-binding', assignmentEpoch: 2 });
    expect(await r.list('ticket')).toHaveLength(1); expect(await r.list('mandate')).toHaveLength(1);
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('delivery')).find(d => d.triggerId === feedback.triggerId)).toMatchObject({ ticketId: a.id, epoch: 2, own: true, resume: true });
    expect(await r.list('assignment')).toHaveLength(2);
  });
  it('ended work cannot resume after its capability authority is revoked', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    await r.endTicket(a.id, 1, { commandId: 'end' });
    await r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: ['alice'], botActors: ['bot'], explicitCapabilities: [], rules: [] }, alice, { commandId: 'revoke' });
    await r.receive(event('new-request', { threads: ['a'] }));
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'ended', assignmentEpoch: 1 });
    expect((await r.list('mandate'))[0].withdrawnAt).toEqual(expect.any(Number));
    expect(await r.listCandidates()).toHaveLength(1);
  });

  it('an authenticated stop input is consumed rather than requeued as unread feedback', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    const stop = await r.receive(event('stop-message', { threads: ['a'] }));
    await r.stopTicket(a.id, alice, { commandId: 'stop-input', triggerId: stop.triggerId });
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.list('disposition')).find(d => d.triggerId === stop.triggerId).acknowledged).toBe(true);
    expect((await r.engine.store.read('state', 'intake')).count).toBe(0);
  });
  it('bot and context-only input cannot automatically reopen naturally ended work', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    await r.subscribe(a.id, 'cli', 'context', false, { commandId: 'ctx' });
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    await r.endTicket(a.id, 1, { commandId: 'end' });
    await r.receive(event('bot-followup', { actor: bot, threads: ['a'] }));
    await r.receive(event('context-followup', { threads: ['context'] }));
    expect(await r.readTicket(a.id)).toMatchObject({ lifecycle: 'ended', assignmentEpoch: 1 });
    expect((await r.list('delivery')).filter(d => d.triggerId !== initial.triggerId).every(d => !d.resume && !d.own)).toBe(true);
  });

  it('a new authenticated stop input is consumed even when the Ticket is already stopped', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    await r.stopTicket(a.id, alice, { commandId: 'stop-first' });
    const stop = await r.receive(event('stop-again', { threads: ['a'] }));
    await r.stopTicket(a.id, alice, { commandId: 'stop-again', triggerId: stop.triggerId });
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.engine.store.read('state', 'intake')).count).toBe(0);
    expect(await r.list('cancel')).toHaveLength(1);
  });

  it('multiple final-owner releases queue behind one Thread Eval across a policy update', async () => {
    const r = await open(); const a = await ticket(r, 'a');
    const [initial] = await r.list('delivery');
    await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, a.id, initial.triggerId, 1));
    const first = await r.receive(event('unread-one', { threads: ['a'] }));
    const second = await r.receive(event('unread-two', { threads: ['a'] }));
    await r.endTicket(a.id, 1, { commandId: 'end' });
    expect(await r.listCandidates()).toMatchObject([{ triggerIds: [first.triggerId] }]);
    await r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: ['alice'], botActors: ['bot'], rules: [] }, alice, { commandId: 'p2' });
    const [fresh] = await r.listCandidates(); expect(fresh.policyVersion).toBe(2);
    await r.decide(fresh.id, fresh.epoch, { kind: 'ignore', reason: 'first handled' }, { commandId: 'first-handled' });
    expect(await r.listCandidates()).toMatchObject([{ policyVersion: 2, triggerIds: [second.triggerId] }]);
  });

});
