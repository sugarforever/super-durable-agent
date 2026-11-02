import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { openSuperAgent } from '../src/runtime/open.js';

const dirs: string[] = [];
const runtimes: Awaited<ReturnType<typeof openSuperAgent>>[] = [];
const alice = { id: 'alice', kind: 'human' as const, trusted: true };
const bot = { id: 'buildbot', kind: 'bot' as const, trusted: true };
const rule = { id: 'maintenance', enabled: true, instructions: 'Maintain this project', capabilities: ['read', 'write'] };
const policy = (acceptMode: 'off' | 'observe' | 'auto', version = 1) => ({ version, acceptMode, allowedActors: ['alice'], botActors: ['buildbot'], explicitCapabilities: ['read', 'write'], rules: [rule] });
const event = (externalId: string, patch = {}) => ({ sourceId: 'cli', externalId, revision: '1', actor: alice, threads: ['discussion'], body: 'Please investigate', receivedAt: 1000, occurredAt: 900, ...patch });
async function open(options = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'sa-policy-')); dirs.push(directory);
  const runtime = await openSuperAgent({ directory, ...options }); runtimes.push(runtime); return runtime;
}
afterEach(async () => { await Promise.all(runtimes.splice(0).map(r => r.close())); await Promise.all(dirs.splice(0).map(d => rm(d, { recursive: true, force: true }))); });

describe('authorization decisions on real pi SQLite', () => {
  it('defaults to observe with no admitted event actors', async () => {
    const r = await open(); await r.receive(event('unlisted', { addressed: true }));
    expect(await r.listCandidates()).toEqual([]);
    expect(await r.list('ticket')).toEqual([]);
    expect((await r.list('disposition'))[0]).toMatchObject({ acknowledged: true, ownerTickets: [] });
  });
  it.each(['off', 'observe', 'auto'] as const)('explicit trusted allowed human accepts in %s mode atomically without a Worker', async mode => {
    const r = await open(); await r.updatePolicy(policy(mode), alice, { commandId: 'policy' });
    const received = await r.receive(event('explicit', { addressed: true }));
    const [candidate] = await r.listCandidates(); expect(candidate).toMatchObject({ status: 'pending', basis: 'explicit', triggerIds: [received.triggerId] });
    const meta = { commandId: 'accept' };
    const outcome = { kind: 'accept' as const, goal: 'Investigate', ruleIds: [], capabilities: ['read'] };
    const decisionId = await r.decide(candidate.id, candidate.epoch, outcome, meta);
    expect(await r.decide(candidate.id, candidate.epoch, outcome, meta)).toBe(decisionId);
    await expect(r.decide(candidate.id, candidate.epoch, { ...outcome, goal: 'Changed' }, meta)).rejects.toThrow(/conflict/i);
    const [ticket] = await r.list('ticket');
    expect(ticket).toMatchObject({ goal: 'Investigate', lifecycle: 'open', assignmentEpoch: 1 });
    expect(ticket.workerConversationId).toBeUndefined();
    expect(await r.list('mandate')).toMatchObject([{ actor: alice, basis: 'explicit', policyVersion: 1, capabilities: ['read'], withdrawnAt: null }]);
    expect(await r.list('assignment')).toMatchObject([{ ticketId: ticket.id, epoch: 1, status: 'pending' }]);
    expect(await r.list('subscription')).toMatchObject([{ ticketId: ticket.id, own: true }]);
    expect(await r.list('delivery')).toMatchObject([{ ticketId: ticket.id, triggerId: received.triggerId, status: 'pending' }]);
    expect(await r.listCandidates()).toEqual([]);
    expect((await r.engine.transaction(tx => tx.scanConversations({}, 100))).items).toHaveLength(1);
  });
  it('observe records inferred wouldAccept and off does not create inferred evaluations', async () => {
    const r = await open(); await r.updatePolicy(policy('observe'), alice, { commandId: 'p1' });
    await r.receive(event('inferred'));
    const [candidate] = await r.listCandidates();
    await r.decide(candidate.id, candidate.epoch, { kind: 'accept', goal: 'Maintain', ruleIds: ['maintenance'] }, { commandId: 'd1' });
    expect(await r.list('decision')).toMatchObject([{ outcome: { kind: 'ignore', wouldAccept: true } }]);
    expect(await r.list('ticket')).toEqual([]);
    await r.updatePolicy(policy('off', 2), alice, { commandId: 'p2' });
    await r.receive(event('off'));
    expect(await r.listCandidates()).toEqual([]);
  });
  it('auto inferred acceptance validates enabled rule IDs and capability bounds', async () => {
    const r = await open(); await r.updatePolicy({ ...policy('auto'), rules: [rule, { ...rule, id: 'disabled', enabled: false }] }, alice, { commandId: 'p' });
    await r.receive(event('bot', { actor: bot, addressed: true }));
    const [c] = await r.listCandidates(); expect(c.basis).toBe('inferred');
    for (const ruleIds of [[], ['missing'], ['disabled']]) await expect(r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Build', ruleIds }, { commandId: `bad-${ruleIds}` })).rejects.toThrow(/rule/i);
    await expect(r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Build', ruleIds: ['maintenance'], capabilities: ['admin'] }, { commandId: 'escalate' })).rejects.toThrow(/capabilit/i);
    await r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Build', ruleIds: ['maintenance'] }, { commandId: 'good' });
    expect(await r.list('mandate')).toMatchObject([{ actor: bot, basis: 'inferred', capabilities: ['read', 'write'] }]);
  });
  it('rejects unauthorized policy edits, stale policy/epoch decisions and non-increasing versions', async () => {
    const r = await open();
    for (const actor of [bot, { ...alice, trusted: false }]) await expect(r.updatePolicy(policy('auto'), actor, { commandId: actor.kind })).rejects.toThrow(/trusted human/i);
    await r.updatePolicy(policy('auto'), alice, { commandId: 'p1' });
    await r.receive(event('candidate')); const [c] = await r.listCandidates();
    await expect(r.decide(c.id, c.epoch + 1, { kind: 'ignore', reason: 'no' }, { commandId: 'bad-epoch' })).rejects.toThrow(/epoch|stale/i);
    await expect(r.updatePolicy(policy('auto'), alice, { commandId: 'same-version' })).rejects.toThrow(/version/i);
    await r.updatePolicy(policy('auto', 2), alice, { commandId: 'p2', expectedRevision: 1 });
    await expect(r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'no' }, { commandId: 'stale' })).rejects.toThrow(/policy|stale/i);
    await expect(r.updatePolicy(policy('auto', 3), bot, { commandId: 'bot-policy' })).rejects.toThrow(/trusted human/i);
  });
  it('bounds outstanding candidate work, releases ignored intake and preserves atomic batch rollback', async () => {
    const r = await open({ maxPending: 1 }); await r.updatePolicy(policy('auto'), alice, { commandId: 'p' });
    await r.receive(event('one')); expect((await r.receive(event('two'))).disposition).toBe('backpressure');
    const [c] = await r.listCandidates(); await r.decide(c.id, c.epoch, { kind: 'ignore', reason: 'irrelevant' }, { commandId: 'ignore' });
    expect((await r.receive(event('two'))).disposition).toBe('stored');
    expect((await r.receiveBatch([event('three')], { sourceId: 'cli', value: 'lost' })).disposition).toBe('backpressure');
    expect((await r.engine.store.read('source', 'cli')).cursor).toBeUndefined();
    expect(await r.list('trigger')).toHaveLength(2);
  });
  it('CLI init admits only operator and repeated init preserves a custom policy', async () => {
    const r = await open(); const directory = dirs[dirs.length - 1]; await r.close();
    const cli = promisify(execFile);
    await cli(process.execPath, ['dist/cli.js', '--directory', directory, 'init']);
    const initialized = await openSuperAgent({ directory }); runtimes.push(initialized);
    expect(await initialized.engine.store.read('policy', 'current')).toMatchObject({ acceptMode: 'observe', allowedActors: ['operator'], botActors: [] });
    await initialized.updatePolicy(policy('auto', 2), alice, { commandId: 'custom' });
    await initialized.close();
    await cli(process.execPath, ['dist/cli.js', '--directory', directory, 'init']);
    const reopened = await openSuperAgent({ directory }); runtimes.push(reopened);
    expect(await reopened.engine.store.read('policy', 'current')).toEqual(policy('auto', 2));
  });
  it('accept rollback leaves no partial authority, assignment, decision or delivery', async () => {
    const r = await open({ maxIndexEntries: 1 });
    await r.updatePolicy(policy('auto'), alice, { commandId: 'policy' });
    await r.engine.transaction(tx => r.engine.store.put(tx, 'ticket', 'existing', { id: 'existing' }));
    await r.receive(event('atomic', { addressed: true })); const [c] = await r.listCandidates();
    await expect(r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Atomic', ruleIds: [] }, { commandId: 'accept' })).rejects.toThrow(/capacity/i);
    for (const kind of ['mandate', 'assignment', 'decision', 'delivery', 'subscription']) expect(await r.list(kind)).toEqual([]);
    expect(await r.listCandidates()).toHaveLength(1);
    expect(await r.list('command')).toHaveLength(1);
  });
  it('disabled source permissions refuse an otherwise fresh decision', async () => {
    const r = await open(); await r.updatePolicy(policy('auto'), alice, { commandId: 'p' });
    await r.receive(event('fresh')); const [c] = await r.listCandidates();
    await r.engine.configureSource({ id: 'cli', kind: 'cli', enabled: false });
    await expect(r.decide(c.id, c.epoch, { kind: 'accept', goal: 'No access', ruleIds: ['maintenance'] }, { commandId: 'd' })).rejects.toThrow(/permission/i);
    expect(await r.list('ticket')).toEqual([]);
  });

  it('explicit capability proposals stay within trusted policy grants and defaults exclude merge', async () => {
    const r = await open();
    await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'p' });
    await r.receive(event('explicit-caps', { addressed: true })); const [c] = await r.listCandidates();
    await expect(r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Escalate', ruleIds: [], capabilities: ['pr.merge'] }, { commandId: 'escalate' })).rejects.toThrow(/capabilit/i);
    await r.decide(c.id, c.epoch, { kind: 'accept', goal: 'Safe defaults', ruleIds: [] }, { commandId: 'safe' });
    expect((await r.list('mandate'))[0].capabilities).toEqual(['repo.read', 'channel.reply']);
  });

});
