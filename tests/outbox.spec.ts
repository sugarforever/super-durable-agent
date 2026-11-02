import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { NoSendError } from '../src/services/outbox.js';
import { openSuperAgent } from '../src/runtime/open.js';
import { accept, alice, fixture, until } from './helpers/scheduling.js';

const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); });
class Sender {
  sourceId = 'cli'; enabled = true; listening = true;
  observed: { message: any; key: string }[] = [];
  receipts = new Map<string, any>();
  outcome: 'absent' | 'unknown' = 'absent';
  beforeQuery?: () => Promise<void>;
  operation?: (message: any, key: string) => Promise<any>;
  listened() { return this.listening; }
  async queryReceipt(key: string) { await this.beforeQuery?.(); return this.receipts.has(key) ? { status: 'found', receipt: this.receipts.get(key) } : { status: this.outcome }; }
  async send(message: any, key: string) {
    this.observed.push({ message: JSON.parse(JSON.stringify(message)), key });
    if (this.operation) return this.operation(message, key);
    const receipt = { providerId: String(this.observed.length) }; this.receipts.set(key, receipt); return receipt;
  }
}
async function setup(sender?: Sender, options: any = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'sa-outbox-'));
  let runtime = await openSuperAgent({ directory, ...options, ...(sender ? { outbox: { adapters: [sender], random: () => 0, ...options.outbox } } : {}) } as any);
  await runtime.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  const ticket = await accept(runtime, 'reply-thread');
  const f = { directory, ticket, get r() { return runtime; }, async reopen() { await runtime.close(); runtime = await openSuperAgent({ directory, ...options, ...(sender ? { outbox: { adapters: [sender], random: () => 0, ...options.outbox } } : {}) } as any); },
    async enqueue(payload: any = { text: 'Reviewable result' }, key = 'reply-key', purpose = 'answer') {
      return runtime.engine.transaction(tx => (runtime as any).outbox.enqueueMessage(tx, { ticketId: ticket.id, mandateId: ticket.mandateId, assignmentEpoch: ticket.assignmentEpoch, destination: { sourceId: 'cli', threadRef: 'reply-thread' }, purpose, payload, idempotencyKey: key }));
    } };
  cleanup.push(async () => { await runtime.close(); await rm(directory, { recursive: true, force: true }); }); return f;
}
async function state(f: any) { return (await f.r.list('outbox'))[0] as any; }
async function settled(f: any, expected: string) { await until(async () => (await state(f))?.status === expected); return state(f); }
function barrier() { let release!: () => void, enter!: () => void; const gate = new Promise<void>(r => release = r), entered = new Promise<void>(r => enter = r); return { release, entered, async wait() { enter(); await gate; } }; }

describe('transactional Outbox', () => {
  it('persists visible pending without a configured sender and binds keys to payload, purpose, destination and authority', async () => {
    const f = await setup(); const first = await f.enqueue();
    expect(first).toMatchObject({ status: 'pending', attempt: 0, payload: { text: 'Reviewable result' } });
    expect(await f.enqueue()).toEqual(first);
    await expect(f.enqueue({ text: 'Changed' })).rejects.toThrow(/conflict/i);
    await expect(f.enqueue({ text: 'Reviewable result' }, 'reply-key', 'question')).rejects.toThrow(/conflict/i);
    expect(await f.r.list('outbox')).toHaveLength(1); await f.reopen(); expect(await state(f)).toMatchObject({ status: 'pending', attempt: 0 });
  });
  it('rolls back outbound intent with its surrounding Engine commit', async () => {
    const f = await setup();
    await expect(f.r.engine.transaction(async tx => { await (f.r as any).outbox.enqueueMessage(tx, { ticketId: f.ticket.id, mandateId: f.ticket.mandateId, assignmentEpoch: f.ticket.assignmentEpoch, destination: { sourceId: 'cli', threadRef: 'reply-thread' }, purpose: 'answer', payload: {}, idempotencyKey: 'atomic' }); throw new Error('abort report'); })).rejects.toThrow('abort report');
    expect(await f.r.list('outbox')).toHaveLength(0);
  });
  it('rejects missing channel.reply and arbitrary destinations', async () => {
    const f = await setup();
    await f.r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], explicitCapabilities: ['repo.read'], rules: [] }, alice, { commandId: 'restrict' });
    await expect(f.enqueue()).rejects.toThrow(/channel.reply/);
    expect(await f.r.list('outbox')).toHaveLength(0);
  });
  it('sends through a native task and stores the provider receipt', async () => {
    const sender = new Sender(), f = await setup(sender); await f.enqueue();
    expect(await settled(f, 'sent')).toMatchObject({ attempt: 1, receipt: { providerId: '1' } });
    expect(sender.observed).toHaveLength(1); expect(sender.observed[0].key).toBe('reply-key');
    expect((await f.r.harness.getTask((await state(f)).taskId, f.r.engine.context))?.kind).toBe('sa.outbox');
  });
  it('fences queued intent after stop and SQLite reopen', async () => {
    const sender = new Sender(); sender.enabled = false; const f = await setup(sender); await f.enqueue();
    await until(async () => !!(await state(f))?.nextAttemptAt);
    await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-before-send' }); sender.enabled = true;
    await f.reopen(); await (f.r as any).outbox.reconcile();
    expect(sender.observed).toHaveLength(0); expect(await state(f)).toMatchObject({ status: 'invalid', attempt: 0 });
  });
  it('checks fresh stop authority after receipt lookup immediately before sending', async () => {
    const sender = new Sender(), b = barrier(); sender.beforeQuery = () => b.wait(); const f = await setup(sender); await f.enqueue();
    await b.entered; await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-during-query' }); b.release();
    await settled(f, 'invalid'); expect(sender.observed).toHaveLength(0);
  });
  it('records a successful already-in-flight send after stop without reviving Ticket work', async () => {
    const sender = new Sender(), b = barrier(); sender.operation = async () => { await b.wait(); return { providerId: 'inflight' }; };
    const f = await setup(sender); await f.enqueue(); await b.entered; await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-inflight' });
    const stopped = await f.r.readTicket(f.ticket.id); b.release();
    expect(await settled(f, 'sent')).toMatchObject({ receipt: { providerId: 'inflight' } }); expect(await f.r.readTicket(f.ticket.id)).toEqual(stopped); expect(sender.observed).toHaveLength(1);
  });
  it('queries a generic unknown failure and never blindly resends', async () => {
    const sender = new Sender(); sender.operation = async () => { sender.outcome = 'unknown'; throw new Error('connection reset after possible effect'); };
    const f = await setup(sender); await f.enqueue(); expect(await settled(f, 'uncertain')).toMatchObject({ attempt: 1 });
    await (f.r as any).outbox.reconcile(); await f.reopen(); await (f.r as any).outbox.reconcile();
    expect(sender.observed).toHaveLength(1); expect(await state(f)).toMatchObject({ status: 'uncertain' });
  });
  it('repairs accepted-send receipt loss from provider lookup without a second send', async () => {
    const sender = new Sender(); sender.operation = async (_m, key) => { sender.receipts.set(key, { providerId: 'accepted' }); throw new Error('lost response'); };
    const f = await setup(sender); await f.enqueue(); expect(await settled(f, 'sent')).toMatchObject({ receipt: { providerId: 'accepted' } }); expect(sender.observed).toHaveLength(1);
  });
  it('retries only authoritative absent with the same immutable key after durable delay', async () => {
    let now = Date.now(); const sender = new Sender(); sender.operation = async () => { throw new Error('failed delivery'); };
    const f = await setup(sender, { clock: { now: () => now } }); await f.enqueue();
    const pending = await settled(f, 'retrying'); expect(pending).toMatchObject({ attempt: 1, nextAttemptAt: now + 2000 });
    await (f.r as any).outbox.reconcile(); expect(sender.observed).toHaveLength(1);
    sender.operation = undefined; now += 2000; await (f.r as any).outbox.reconcile(); await settled(f, 'sent');
    expect(sender.observed.map(x => x.key)).toEqual(['reply-key', 'reply-key']); expect(sender.observed[0].message.payload).toEqual(sender.observed[1].message.payload);
  });
  it('defers disabled sources without failures or a live task and drops unlistened destinations', async () => {
    const sender = new Sender(); sender.enabled = false; const f = await setup(sender); await f.enqueue();
    await until(async () => (await state(f))?.nextAttemptAt > 0); expect(await state(f)).toMatchObject({ status: 'pending', attempt: 0 });
    expect(sender.observed).toHaveLength(0);
    await f.reopen(); sender.enabled = true; sender.listening = false; await (f.r as any).outbox.reconcile();
    // Advance the persisted deferral rather than sleeping in production backoff.
    await f.r.engine.transaction(async tx => { const m = await state(f); await f.r.engine.store.put(tx, 'outbox', m.id, { ...m, nextAttemptAt: 0 }); });
    await (f.r as any).outbox.reconcile(); expect(await settled(f, 'dropped')).toMatchObject({ attempt: 0, error: expect.stringMatching(/listened/) }); expect(sender.observed).toHaveLength(0);
  });
});


describe('Outbox recovery, bounded retry and integration', () => {
  it('persists jittered no-send retries, caps delays at 60s and raises attention after exactly 8 attempts', async () => {
    let now = Date.now(); const sender = new Sender(); sender.operation = async () => { throw new NoSendError('certified rejected before effect'); };
    const f = await setup(sender, { clock: { now: () => now }, outbox: { random: () => 1 } }); await f.enqueue();
    const delays: number[] = [];
    for (let attempt = 1; attempt <= 8; attempt++) {
      await until(async () => (await state(f))?.attempt === attempt && ['retrying', 'attention'].includes((await state(f))?.status));
      const m = await state(f); expect(m.attempt).toBe(attempt);
      if (attempt === 8) { expect(m.status).toBe('attention'); break; }
      delays.push(m.nextAttemptAt - now); await f.reopen(); expect((await state(f)).nextAttemptAt).toBe(m.nextAttemptAt);
      now = m.nextAttemptAt; await (f.r as any).outbox.reconcile();
    }
    expect(delays).toEqual([2500, 5000, 10000, 20000, 40000, 60000, 60000]);
    await (f.r as any).outbox.reconcile(); expect(sender.observed).toHaveLength(8);
    expect(new Set(sender.observed.map(x => x.key)).size).toBe(1);
    expect(await f.r.readTicket(f.ticket.id)).toMatchObject({ attention: expect.stringContaining('8 attempts') });
  });
  it('rechecks current Policy and listening after asynchronous receipt lookup', async () => {
    for (const revoke of ['policy', 'listening', 'source'] as const) {
      const sender = new Sender(), b = barrier(); sender.beforeQuery = () => b.wait(); const f = await setup(sender); await f.enqueue(); await b.entered;
      if (revoke === 'policy') await f.r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: [], botActors: [], rules: [] }, alice, { commandId: 'revoke-policy' });
      if (revoke === 'listening') sender.listening = false;
      if (revoke === 'source') await f.r.engine.configureSource({ id: 'cli', kind: 'cli', enabled: false });
      b.release();
      if (revoke === 'source') await until(async () => (await state(f))?.error === 'Source is disabled');
      else await settled(f, revoke === 'policy' ? 'invalid' : 'dropped');
      expect(sender.observed).toHaveLength(0); expect((await state(f)).attempt).toBe(0);
    }
  });
  it('rejects out-of-scope destinations and conflicts when authority/destination changes under the same key', async () => {
    const f = await setup(); await f.enqueue();
    const base = { ticketId: f.ticket.id, mandateId: f.ticket.mandateId, assignmentEpoch: f.ticket.assignmentEpoch, destination: { sourceId: 'cli', threadRef: 'other-thread' }, purpose: 'answer', payload: { text: 'Reviewable result' }, idempotencyKey: 'new-key' };
    await expect(f.r.engine.transaction(tx => (f.r as any).outbox.enqueueMessage(tx, base))).rejects.toThrow(/scope/);
    await expect(f.r.engine.transaction(tx => (f.r as any).outbox.enqueueMessage(tx, { ...base, idempotencyKey: 'reply-key' }))).rejects.toThrow(/conflict/);
    await expect(f.r.engine.transaction(tx => (f.r as any).outbox.enqueueMessage(tx, { ...base, assignmentEpoch: 999, idempotencyKey: 'reply-key' }))).rejects.toThrow(/conflict/);
  });
  it('suppresses routine acknowledgement/progress and validates explicit Rule communication flags', async () => {
    const f = await setup(); await f.enqueue({}, 'ack', 'ack'); await f.enqueue({}, 'progress', 'progress');
    expect((await f.r.list<any>('outbox')).map(x => x.status)).toEqual(['dropped', 'dropped']);
    await expect(f.r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [{ id: 'r', instructions: 'quiet', enabled: true, capabilities: ['channel.reply'], communication: { progress: 'yes' as any } }] }, alice, { commandId: 'bad-communication' })).rejects.toThrow(/communication/);
  });
  it('relays structured ready/questions/attention in the Report commit, keeps routine progress quiet and rejects stale Reports', async () => {
    const f = await fixture(1); cleanup.push(() => f.close()); await until(async () => f.calls.length === 1);
    const ticket = (await f.r.list<any>('ticket'))[0];
    const report = { reportId: 'progress', assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, inputIds: [], status: 'progress' as const, content: { summary: 'structured, no tool logs' } };
    await f.r.agents!.report(ticket.workerConversationId, report); expect(await f.r.list('outbox')).toHaveLength(0);
    await f.r.agents!.report(ticket.workerConversationId, { ...report, reportId: 'ready', status: 'ready' });
    await f.r.agents!.report(ticket.workerConversationId, { ...report, reportId: 'question', status: 'ready', questions: [{ id: 'choice', text: 'Which branch?' }] });
    await f.r.agents!.report(ticket.workerConversationId, { ...report, reportId: 'failed', status: 'failed' });
    const messages = await f.r.list<any>('outbox'); expect(messages.map(x => x.purpose)).toEqual(['answer', 'question', 'attention']);
    expect(messages.every(x => x.status === 'pending' && x.attempt === 0)).toBe(true);
    expect(messages[1].payload).toMatchObject({ status: 'waiting', questions: [{ id: 'choice', text: 'Which branch?' }] });
    await f.r.stopTicket(ticket.id, alice, { commandId: 'stop-report' });
    await expect(f.r.agents!.report(ticket.workerConversationId, { ...report, reportId: 'late', status: 'ready' })).rejects.toThrow(/Stale/);
    expect(await f.r.list('report')).toHaveLength(4); expect(await f.r.list('outbox')).toHaveLength(3); f.release(); await f.r.agents!.waitForIdle();
  });
  it('rolls back Report and application consumption when its outbound intent cannot commit', async () => {
    const f = await fixture(1); cleanup.push(() => f.close()); await until(async () => f.calls.length === 1);
    const ticket = (await f.r.list<any>('ticket'))[0], run = (await f.r.list<any>('deliveryRun'))[0];
    await until(async () => !!(await f.r.engine.transaction(tx => tx.submissionByRequest(run.conversationId, run.requestId)))?.entry);
    const original = f.r.outbox.enqueueMessage.bind(f.r.outbox);
    f.r.outbox.enqueueMessage = async (tx, input) => { await original(tx, input); throw new Error('outbound storage failure'); };
    await expect(f.r.agents!.report(ticket.workerConversationId, { reportId: 'atomic-ready', assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, inputIds: [run.id], status: 'ready', content: {} })).rejects.toThrow('outbound storage failure');
    expect(await f.r.list('report')).toHaveLength(0); expect(await f.r.list('outbox')).toHaveLength(0);
    expect(await f.r.engine.store.read('deliveryRun', run.id)).toMatchObject({ consumed: false });
    expect((await f.r.list<any>('disposition'))[0].acknowledged).toBe(false);
    f.r.outbox.enqueueMessage = original; f.release(); await f.r.agents!.waitForIdle();
  });
  it('accepts the exact previous registry, rejecting changed or missing required definitions', async () => {
    const f = await setup(); const metadata = await f.r.engine.store.read<any>('metadata', 'runtime');
    const prior = { ...metadata, tasks: Object.fromEntries(Object.entries(metadata.tasks).filter(([name]) => name !== 'sa.outbox')) };
    await f.r.engine.transaction(tx => f.r.engine.store.put(tx, 'metadata', 'runtime', prior)); await f.reopen();
    expect((await f.r.engine.store.read<any>('metadata', 'runtime')).tasks['sa.outbox']).toBe(1);
    const broken = { ...metadata, tasks: { ...metadata.tasks, 'sa.job': 99 } };
    await f.r.engine.transaction(tx => f.r.engine.store.put(tx, 'metadata', 'runtime', broken));
    await expect(f.reopen()).rejects.toThrow(/definitions/);
    // The failed open releases its lock; repair metadata through a deliberately paused preflight is unavailable by design.
  });
  it('recovers a real local HTTP accepted send after SIGKILL before the application receipt commit', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'sa-outbox-kill-')); const received: any[] = [], receipts = new Map<string, any>();
    const server = createServer(async (req, res) => {
      if (req.method === 'POST') {
        const chunks: Buffer[] = []; for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const message = JSON.parse(Buffer.concat(chunks).toString()); received.push(message);
        const receipt = { providerId: String(received.length) }; receipts.set(message.key, receipt); res.end(JSON.stringify(receipt));
      } else { const key = decodeURIComponent(req.url!.slice(1)); res.end(JSON.stringify(receipts.has(key) ? { status: 'found', receipt: receipts.get(key) } : { status: 'absent' })); }
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve)); const url = `http://127.0.0.1:${(server.address() as any).port}`;
    let runtime: Awaited<ReturnType<typeof openSuperAgent>> | undefined;
    cleanup.push(async () => { await runtime?.close(); await new Promise<void>(resolve => server.close(() => resolve())); await rm(directory, { recursive: true, force: true }); });
    const child = spawn(process.execPath, ['tests/helpers/outbox-crash.mjs', directory, url], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; child.stderr.on('data', chunk => stderr += chunk);
    const exit = await new Promise<{ code: number | null; signal: string | null }>(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
    expect(exit, stderr).toEqual({ code: null, signal: 'SIGKILL' }); expect(received).toHaveLength(1);
    const adapter = { sourceId: 'cli', enabled: true, listened: () => true,
      async queryReceipt(key: string) { return (await (await fetch(url + '/' + encodeURIComponent(key))).json()) as any; },
      async send(message: any, key: string) { return (await (await fetch(url, { method: 'POST', body: JSON.stringify({ key, payload: message.payload }) })).json()) as any; } };
    runtime = await openSuperAgent({ directory, outbox: { adapters: [adapter] } });
    await until(async () => (await runtime!.list<any>('outbox'))[0]?.status === 'sent');
    expect(received).toHaveLength(1); expect((await runtime.list<any>('outbox'))[0]).toMatchObject({ attempt: 1, receipt: { providerId: '1' } });
  });
});


describe('fresh communication fences', () => {
  it('applies an updated silence rule to an already queued progress message', async () => {
    const sender = new Sender(), b = barrier(); sender.beforeQuery = () => b.wait(); const f = await setup(sender);
    const rule = { id: 'automatic', enabled: true, instructions: 'Report progress only when configured', capabilities: ['channel.reply'], communication: { progress: true } };
    await f.r.updatePolicy({ version: 2, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [rule] }, alice, { commandId: 'progress-policy' });
    const received = await f.r.receive({ sourceId: 'cli', externalId: 'auto', revision: '1', actor: alice, threads: ['automatic-thread'], body: 'handle automatically', receivedAt: Date.now(), occurredAt: Date.now(), addressed: false });
    const evaluation = (await f.r.listCandidates()).find(x => x.triggerIds.includes((received as any).triggerId))!;
    await f.r.decide(evaluation.id, evaluation.epoch, { kind: 'accept', goal: 'auto', ruleIds: [rule.id] }, { commandId: 'accept-auto' });
    const ticket = (await f.r.list<any>('ticket')).find(x => x.goal === 'auto');
    await f.r.engine.transaction(tx => f.r.outbox.enqueueMessage(tx, { ticketId: ticket.id, mandateId: ticket.mandateId, assignmentEpoch: ticket.assignmentEpoch, destination: { sourceId: 'cli', threadRef: 'automatic-thread' }, purpose: 'progress', payload: { text: 'working' }, idempotencyKey: 'rule-progress' }));
    await b.entered;
    await f.r.updatePolicy({ version: 3, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [{ ...rule, communication: { progress: false } }] }, alice, { commandId: 'silence-policy' });
    b.release(); await settled(f, 'dropped'); expect(sender.observed).toHaveLength(0);
  });
});

describe('native finite sender coordination', () => {
  it('joins an in-flight sender in explicit agent idle while leaving disabled or missing-adapter intent pending', async () => {
    const sender = new Sender(), b = barrier(); sender.operation = async () => { await b.wait(); return { providerId: 'joined' }; };
    const f = await fixture(1, { outbox: { adapters: [sender] } } as any); cleanup.push(() => f.close()); await until(async () => f.calls.length === 1);
    const ticket = (await f.r.list<any>('ticket'))[0];
    await f.r.agents!.report(ticket.workerConversationId, { reportId: 'join-sender', assignmentId: ticket.assignmentId, epoch: ticket.assignmentEpoch, inputIds: [], status: 'ready', content: { summary: 'done' } });
    await b.entered; f.release(); let idle = false; const waiting = f.r.agents!.waitForIdle().then(() => { idle = true; });
    try { await new Promise(resolve => setTimeout(resolve, 30)); expect(idle).toBe(false); }
    finally { b.release(); await waiting; }
    expect((await f.r.list<any>('outbox'))[0]).toMatchObject({ status: 'sent', receipt: { providerId: 'joined' } });
  });
});

describe('trusted provider receipt inspection', () => {
  it('repairs later-known uncertain receipts without resending or changing a stopped Ticket', async () => {
    const sender = new Sender(); sender.operation = async () => { sender.outcome = 'unknown'; throw new Error('ambiguous transport'); };
    const f = await setup(sender); await f.enqueue(); const message = await settled(f, 'uncertain');
    await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-uncertain' }); const stopped = await f.r.readTicket(f.ticket.id);
    expect(await (f.r.outbox as any).inspectReceipt(message.id)).toEqual({ status: 'unknown' });
    sender.receipts.set(message.idempotencyKey, { providerId: 'later-confirmed' });
    expect(await (f.r.outbox as any).inspectReceipt(message.id)).toEqual({ status: 'found', receipt: { providerId: 'later-confirmed' } });
    expect(await state(f)).toMatchObject({ status: 'sent', receipt: { providerId: 'later-confirmed' } });
    expect(sender.observed).toHaveLength(1); expect(await f.r.readTicket(f.ticket.id)).toEqual(stopped);
  });
});
