import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, writeFile, symlink, rm, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { LocalEnvironmentBroker } from '../src/environments/local.js';
import { operationDigest, type EndpointIdentity } from '../src/environments/types.js';
const run = promisify(execFile);
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });
const identity: EndpointIdentity = { ticketId: 'ticket', assignmentId: 'assignment', epoch: 1, mandateId: 'mandate' };
async function repo(options: { trustedLocalShell?: boolean; capabilities?: string[] } = {}) {
  const root = await mkdtemp(join(tmpdir(), 'sa-environments-'));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  const repository = join(root, 'repo'); await mkdir(repository);
  await run('git', ['init', '-b', 'main', repository]);
  await run('git', ['-C', repository, 'config', 'user.email', 'tests@example.invalid']);
  await run('git', ['-C', repository, 'config', 'user.name', 'Tests']);
  await writeFile(join(repository, 'source.txt'), 'original');
  await run('git', ['-C', repository, 'add', '.']); await run('git', ['-C', repository, 'commit', '-m', 'base']);
  const configuration = { directory: join(root, 'broker'), repository, trustedLocalShell: options.trustedLocalShell, authorize: async () => ({ ...identity, capabilities: options.capabilities ?? ['repo.read', 'repo.write', 'command.execute'] }) };
  const broker = new LocalEnvironmentBroker(configuration);
  const prepare = { operationId: 'prepare', identity, payload: { kind: 'prepare' as const }, payloadDigest: operationDigest(identity, { kind: 'prepare' }) };
  const prepared = await broker.prepare(prepare);
  if (prepared.status !== 'prepared') throw new Error('Preparation failed');
  const request = (operationId: string, action: any) => {
    const payload = { environmentId: prepared.environmentId, ...action };
    return { operationId, identity, payload, payloadDigest: operationDigest(identity, payload) };
  };
  return { root, repository, broker, configuration, prepared, prepare, request };
}
describe('local environment broker', () => {
  it('prepares one isolated git worktree and repeats without resetting its changes', async () => {
    const f = await repo();
    await writeFile(join(f.prepared.cwd, 'source.txt'), 'isolated');
    expect(await f.broker.prepare(f.prepare)).toEqual(f.prepared);
    expect(await readFile(join(f.repository, 'source.txt'), 'utf8')).toBe('original');
    expect(await readFile(join(f.prepared.cwd, 'source.txt'), 'utf8')).toBe('isolated');
    expect((await run('git', ['-C', f.prepared.cwd, 'rev-parse', 'HEAD'])).stdout.trim()).toBe(f.prepared.baseCommit);
    expect((await run('git', ['-C', f.repository, 'branch', '--show-current'])).stdout.trim()).toBe('main');
  });
  it('binds operation keys to payloads and applies structured writes and edits once', async () => {
    const f = await repo(); const request = f.request('write', { kind: 'write', path: 'source.txt', text: 'edited' });
    expect(await f.broker.execute(request)).toMatchObject({ status: 'completed' });
    await expect(f.broker.execute(f.request('write', { kind: 'write', path: 'source.txt', text: 'other' }))).rejects.toThrow(/conflict/i);
    await f.broker.execute(f.request('edit', { kind: 'edit', path: 'source.txt', oldText: 'edited', newText: 'changed' }));
    await f.broker.execute(f.request('edit', { kind: 'edit', path: 'source.txt', oldText: 'edited', newText: 'changed' }));
    expect(await f.broker.execute(f.request('read', { kind: 'read', path: 'source.txt' }))).toMatchObject({ status: 'completed', result: { text: 'changed' } });
  });
  it('rejects traversal, symlink escapes, git metadata writes and cwd escape', async () => {
    const f = await repo(); await symlink(f.root, join(f.prepared.cwd, 'escape'));
    await symlink(join(f.root, 'outside.txt'), join(f.prepared.cwd, 'dangling'));
    for (const path of ['../outside.txt', 'escape/outside.txt', 'dangling', '.git']) {
      await expect(f.broker.execute(f.request(`write-${path}`, { kind: 'write', path, text: 'unsafe' }))).rejects.toThrow(/path|escape|metadata/i);
      await expect(f.broker.execute(f.request(`read-${path}`, { kind: 'read', path }))).rejects.toThrow();
    }
    await expect(f.broker.execute(f.request('cwd', { kind: 'read', path: 'source.txt', cwd: f.repository }))).rejects.toThrow(/cwd/i);
    await expect(readFile(join(f.root, 'outside.txt'))).rejects.toThrow();
  });
  it('fails closed for default shell, missing capabilities and unavailable sandbox', async () => {
    const f = await repo();
    await expect(f.broker.execute(f.request('shell', { kind: 'shell', command: 'echo unsafe' }))).rejects.toThrow(/trustedLocalShell/);
    const limited = await repo({ trustedLocalShell: true, capabilities: ['repo.read'] });
    await expect(limited.broker.execute(limited.request('shell', { kind: 'shell', command: 'echo unsafe' }))).rejects.toThrow(/capability/);
    await expect(limited.broker.execute(limited.request('write', { kind: 'write', path: 'file', text: 'unsafe' }))).rejects.toThrow(/capability/);
    const trusted = await repo({ trustedLocalShell: true });
    await expect(trusted.broker.execute(trusted.request('sandbox', { kind: 'shell', command: 'echo unsafe', sandbox: true }))).rejects.toThrow(/sandbox/);
    await expect(trusted.broker.execute(trusted.request('sandbox-write', { kind: 'write', path: 'file', text: 'unsafe', sandbox: true }))).rejects.toThrow(/sandbox/);
  });
  it('records bounded subprocess output, exit status and idempotent receipt', async () => {
    const f = await repo({ trustedLocalShell: true });
    const request = f.request('shell', { kind: 'shell', command: "node -e \"process.stdout.write('x'.repeat(100000));process.stderr.write('oops');process.exit(7)\"" });
    const receipt = await f.broker.execute(request);
    expect(receipt).toMatchObject({ status: 'failed', result: { exitCode: 7, stderr: 'oops', truncated: true } });
    if ('result' in receipt) expect(Buffer.byteLength((receipt.result as any).stdout)).toBeLessThanOrEqual(16384);
    expect(await f.broker.execute(request)).toEqual(receipt);
    expect(await f.broker.query('shell')).toEqual(receipt);
  });
});

import { openSuperAgent } from '../src/runtime/open.js';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { accept, alice, event, until, isRouter } from './helpers/scheduling.js';
async function jobRuntime(cancellationHandler?: any, commandFor?: (fixture: any) => string) {
  const f = await repo({ trustedLocalShell: true });
  const directory = join(f.root, 'runtime');
  let r = await openSuperAgent({ directory });
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], explicitCapabilities: ['repo.read', 'repo.write', 'command.execute'], rules: [] }, alice, { commandId: 'policy' });
  const ticket = await accept(r, 'work'); await r.close();
  const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
  const model = { provider: faux.getModel().provider, modelId: faux.getModel().id };
  let first = true;
  const command = commandFor?.(f) ?? `printf begun > '${join(f.root, 'begun')}'; while [ ! -f '${join(f.root, 'finish')}' ]; do sleep 0.02; done; printf finished`;
  faux.setResponses(Array.from({ length: 100 }, () => (ctx: any) => {
    if (isRouter(ctx)) return fauxAssistantMessage('No decision');
    if (first) { first = false; return fauxAssistantMessage(fauxToolCall('start_job', { assignmentId: ticket.assignmentId, epoch: 1, command }), { stopReason: 'toolUse' }); }
    return fauxAssistantMessage('Worker available');
  }));
  r = await openSuperAgent({ directory, models, model, environment: { repository: f.repository, trustedLocalShell: true }, cancellationHandler });
  cleanups.push(async () => { await writeFile(join(f.root, 'finish'), 'done'); await r.agents?.waitForIdle(); await r.close(); });
  return { ...f, get r() { return r; }, ticket, faux, models, model, directory, async reopen(startPaused = false) { await r.close(); r = await openSuperAgent({ directory, startPaused, models, model, environment: { repository: f.repository, trustedLocalShell: true }, cancellationHandler }); } };
}
describe('native asynchronous Jobs', () => {
  it('starts from a real Worker tool, keeps capacity, accepts feedback, and delivers completion once', async () => {
    const f = await jobRuntime();
    await until(async () => (await f.r.list<any>('job'))[0]?.status === 'running');
    await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
    await until(async () => (await f.r.list<any>('deliveryRun'))[0]?.settled === true);
    const job = (await f.r.list<any>('job'))[0];
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(1);
    await f.r.receive(event('feedback', ['work'])); await f.r.agents!.pump();
    await until(async () => (await f.r.list<any>('deliveryRun')).length === 2 && (await f.r.list<any>('deliveryRun')).every(x => x.settled));
    expect((await f.r.agents!.jobs.get(f.ticket.workerConversationId ?? (await f.r.readTicket<any>(f.ticket.id)).workerConversationId, job.id)).status).toBe('running');
    await writeFile(join(f.root, 'finish'), 'go'); await f.r.agents!.waitForIdle();
    const done = (await f.r.list<any>('job'))[0]; expect(done).toMatchObject({ status: 'completed', receipt: { result: { stdout: 'finished', exitCode: 0 } } });
    await f.r.agents!.jobs.finish(job.id, done.receipt); await f.r.agents!.pump(); await f.r.agents!.waitForIdle();
    expect((await f.r.list<any>('workerNote')).filter(x => x.jobId === job.id)).toHaveLength(1);
    expect((await f.r.list<any>('deliveryRun')).filter(x => x.id === `job-result:${job.id}`)).toHaveLength(1);
  });
  it('stops only the owned Job and rejects unrelated Worker access', async () => {
    const f = await jobRuntime(); await until(async () => (await f.r.list<any>('job'))[0]?.status === 'running');
    await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
    const ticket = await f.r.readTicket<any>(f.ticket.id), first = (await f.r.list<any>('job'))[0];
    const second = await f.r.agents!.jobs.start(ticket.workerConversationId, { assignmentId: ticket.assignmentId, epoch: 1, command: `sleep 0.2; printf other` }, 'other-job');
    await expect(f.r.agents!.jobs.stop(f.r.agents!.routerConversationId, first.id)).rejects.toThrow(/Worker/);
    await f.r.agents!.jobs.stop(ticket.workerConversationId, first.id);
    expect((await f.r.agents!.jobs.get(ticket.workerConversationId, first.id)).status).toBe('cancelled');
    await f.r.agents!.waitForIdle();
    expect((await f.r.agents!.jobs.get(ticket.workerConversationId, second.jobId)).status).toBe('completed');
  });
  it('stopTicket cancels broker work, composes configured cancellation, and suppresses stale completion input', async () => {
    let externalCalls = 0;
    const f = await jobRuntime(async (request: any) => { externalCalls++; return { ...request, outcome: 'confirmed' }; }); await until(async () => (await f.r.list<any>('job'))[0]?.status === 'running');
    await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
    const job = (await f.r.list<any>('job'))[0];
    await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop' }); await f.r.agents!.waitForIdle();
    expect((await f.r.list<any>('job'))[0].status).toBe('cancelled');
    expect(externalCalls).toBe(1);
    expect((await f.r.list<any>('workerNote')).filter(x => x.jobId === job.id)).toHaveLength(0);
    expect((await f.r.list<any>('cancel'))[0]).toMatchObject({ nativeConfirmed: true, endpointConfirmed: true, status: 'completed' });
    expect((await f.r.list<any>('reservation')).every(x => x.status === 'released')).toBe(true);
    const ticket = await f.r.readTicket<any>(f.ticket.id);
    await f.r.agents!.jobs.finish(job.id, (await f.r.list<any>('job'))[0].receipt);
    expect(await f.r.readTicket<any>(f.ticket.id)).toEqual(ticket);
  });
});

describe('endpoint restart and cancellation boundaries', () => {
  it('runs concurrent same-payload executions only once and fences cancellation epoch', async () => {
    const f = await repo({ trustedLocalShell: true });
    const counter = join(f.root, 'counter');
    const request = f.request('concurrent', { kind: 'shell', command: `printf x >> '${counter}'; sleep 0.1; printf done` });
    const receipts = await Promise.all(Array.from({ length: 6 }, () => f.broker.execute(request)));
    expect(await readFile(counter, 'utf8')).toBe('x');
    expect(receipts.every(x => x.status === 'completed')).toBe(true);
    await expect(f.broker.cancel('concurrent', 2)).rejects.toThrow(/epoch/);
  });
  it('cancels an operation already entering endpoint admission before it can spawn', async () => {
    const f = await repo({ trustedLocalShell: true });
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(r => enter = r), gate = new Promise<void>(r => release = r);
    const original = f.configuration.authorize;
    f.configuration.authorize = async (_identity: any, action: any) => { if (action === 'execute') { enter(); await gate; } return original(); };
    const execution = f.broker.execute(f.request('cancel-admission', { kind: 'shell', command: `printf bad > '${join(f.root, 'bad')}'` }));
    await entered;
    const cancellation = f.broker.cancel('cancel-admission', 1);
    // Let cancellation finish its independent authorization and set the live cancellation flag.
    await new Promise(r => setTimeout(r, 10)); release();
    expect(await execution).toMatchObject({ status: 'cancelled' });
    expect(await cancellation).toMatchObject({ status: 'cancelled' });
    await expect(readFile(join(f.root, 'bad'))).rejects.toThrow();
  });
  it('refuses an unknown preexisting preparation path and releases only clean known worktrees', async () => {
    const f = await repo();
    await writeFile(join(f.prepared.cwd, 'source.txt'), 'dirty');
    await expect(f.broker.release(f.prepared.environmentId, 'release-dirty')).rejects.toThrow();
    expect(await readFile(join(f.prepared.cwd, 'source.txt'), 'utf8')).toBe('dirty');
    await writeFile(join(f.prepared.cwd, 'source.txt'), 'original');
    const released = await f.broker.release(f.prepared.environmentId, 'release-clean');
    expect(released.status).toBe('released');
    expect(await f.broker.release(f.prepared.environmentId, 'release-clean')).toEqual(released);
    await mkdir(f.prepared.cwd); await writeFile(join(f.prepared.cwd, 'precious'), 'keep');
    await expect(f.broker.prepare(f.prepare)).rejects.toThrow();
    expect(await readFile(join(f.prepared.cwd, 'precious'), 'utf8')).toBe('keep');
  });
  it('recovers a SIGKILLed native Job as unknown without incrementing its file a second time', async () => {
    const { spawn } = await import('node:child_process'); const { once } = await import('node:events');
    const { withoutSqliteExperimentalWarning } = await import('./helpers/scheduling.js');
    const f = await repo({ trustedLocalShell: true });
    const directory = join(f.root, 'runtime'), counter = join(f.root, 'counter');
    const child = spawn(process.execPath, ['tests/helpers/jobs-crash.mjs', directory, f.repository, counter], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; child.stderr.on('data', chunk => stderr += chunk);
    const [code, signal] = await once(child, 'exit');
    expect({ code, signal, stderr: withoutSqliteExperimentalWarning(stderr) }).toEqual({ code: null, signal: 'SIGKILL', stderr: '' });
    expect(await readFile(counter, 'utf8')).toBe('x');
    const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
    faux.setResponses(Array.from({ length: 20 }, () => fauxAssistantMessage('Inspect unknown endpoint')));
    const r = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, environment: { repository: f.repository, trustedLocalShell: true } });
    cleanups.push(() => r.close());
    await r.agents!.waitForIdle();
    expect((await r.list<any>('job'))[0]).toMatchObject({ status: 'unknown', receipt: { status: 'unknown' } });
    expect((await r.list<any>('reservation'))[0].status).toBe('quarantined');
    await new Promise(r => setTimeout(r, 650));
    expect(await readFile(counter, 'utf8')).toBe('x');
    await r.agents!.pump();
    expect(await readFile(counter, 'utf8')).toBe('x');
  });
});

describe('durable endpoint records', () => {
  it('detects a corrupted ledger instead of trusting its changed receipt', async () => {
    const { digest } = await import('../src/domain/model.js');
    const f = await repo();
    await f.broker.execute(f.request('integrity', { kind: 'write', path: 'new.txt', text: 'safe' }));
    const ledgerPath = join(f.configuration.directory, 'operations', `${digest('integrity')}.json`);
    const record = JSON.parse(await readFile(ledgerPath, 'utf8')); record.receipt.status = 'unknown';
    await writeFile(ledgerPath, JSON.stringify(record));
    await expect(f.broker.query('integrity')).rejects.toThrow(/integrity|checksum|corrupt/i);
  });
  it('repairs a missing marker only from durable admission plus the registered detached git base', async () => {
    const { digest } = await import('../src/domain/model.js');
    const f = await repo();
    const directory = join(f.root, 'interrupted');
    const { spawn } = await import('node:child_process'); const { once } = await import('node:events');
    const child = spawn(process.execPath, ['tests/helpers/prepare-crash.mjs', directory, f.repository], { stdio: ['ignore','pipe','pipe'] });
    let stderr = ''; child.stderr.on('data', c => stderr += c);
    const [code, signal] = await once(child, 'exit'); expect({ code, signal, stderr }).toEqual({ code: null, signal: 'SIGKILL', stderr: '' });
    const record = JSON.parse(await readFile(join(directory, 'operations', `${digest('prepare')}.json`), 'utf8'));
    await writeFile(join(record.receipt.cwd, 'source.txt'), 'preserve changes');
    const broker = new LocalEnvironmentBroker({ ...f.configuration, directory });
    const receipt = await broker.prepare(f.prepare);
    expect(receipt).toMatchObject({ status: 'prepared', baseCommit: f.prepared.baseCommit });
    expect(await readFile(join(receipt.cwd, 'source.txt'), 'utf8')).toBe('preserve changes');
    expect(await readFile(join(f.repository, 'source.txt'), 'utf8')).toBe('original');
  });
});

describe('Job and broker authority fences', () => {
  it('refuses release with a live process and bounds multibyte subprocess output in bytes', async () => {
    const f = await repo({ trustedLocalShell: true });
    const execution = f.broker.execute(f.request('live-release', { kind: 'shell', command: `sleep 0.1; node -e "process.stdout.write('中'.repeat(10000))"` }));
    await until(async () => (await f.broker.query('live-release'))?.status === 'running');
    await expect(f.broker.release(f.prepared.environmentId, 'while-live')).rejects.toThrow(/active|running/);
    const receipt = await execution;
    expect(receipt.status).toBe('completed');
    if ('result' in receipt && receipt.result && 'stdout' in receipt.result) expect(Buffer.byteLength(receipt.result.stdout)).toBeLessThanOrEqual(16384);
  });
  it('refuses case-insensitive git metadata access and stale fresh grants', async () => {
    const f = await repo();
    await expect(f.broker.execute(f.request('metadata', { kind: 'write', path: '.GIT', text: 'unsafe' }))).rejects.toThrow(/metadata/);
    f.configuration.authorize = async () => ({ ...identity, epoch: 2, capabilities: ['repo.read','repo.write'] });
    await expect(f.broker.execute(f.request('stale', { kind: 'write', path: 'source.txt', text: 'unsafe' }))).rejects.toThrow(/Stale/);
    expect(await readFile(join(f.prepared.cwd, 'source.txt'), 'utf8')).toBe('original');
  });
  it('never delivers a prior-epoch Job result note after explicit retry', async () => {
    const f = await jobRuntime();
    await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
    // A no-op kick does not drain an already scheduled timer/pump. Close the
    // coordinator before completing the Job so its real claim path is quiescent.
    await f.r.agents!.close();
    await writeFile(join(f.root, 'finish'), 'done');
    await until(async () => (await f.r.list<any>('job'))[0]?.status === 'completed');
    const job = (await f.r.list<any>('job'))[0];
    expect((await f.r.list<any>('workerNote')).find(x => x.jobId === job.id)?.status).toBe('pending');
    await f.reopen(true);
    expect((await f.r.list<any>('workerNote')).find(x => x.jobId === job.id)?.status).toBe('pending');
    await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-before-result' });
    await f.r.activate(); await f.r.agents!.waitForIdle();
    await f.r.retryTicket(f.ticket.id, alice, { commandId: 'retry-before-result' });
    await f.r.agents!.pump(); await f.r.agents!.waitForIdle();
    expect((await f.r.list<any>('deliveryRun')).some(x => x.id === `job-result:${job.id}`)).toBe(false);
    const latest = await f.r.readTicket<any>(f.ticket.id);
    await expect(f.r.agents!.jobs.start(latest.workerConversationId, { assignmentId: f.ticket.assignmentId, epoch: 1, command: 'echo stale' }, 'stale-call')).rejects.toThrow(/Stale/);
  });
});

it('resumes native running intent only when the endpoint has no admission record', async () => {
  const { spawn } = await import('node:child_process'); const { once } = await import('node:events');
  const { withoutSqliteExperimentalWarning } = await import('./helpers/scheduling.js');
  const f = await repo(); const directory = join(f.root, 'runtime'), counter = join(f.root, 'counter');
  const child = spawn(process.execPath, ['tests/helpers/jobs-crash.mjs', directory, f.repository, counter, 'preendpoint'], { stdio: ['ignore','pipe','pipe'] });
  let stderr = ''; child.stderr.on('data', c => stderr += c);
  const [code, signal] = await once(child, 'exit'); expect({ code, signal, stderr: withoutSqliteExperimentalWarning(stderr) }).toEqual({ code: null, signal: 'SIGKILL', stderr: '' });
  await expect(readFile(counter)).rejects.toThrow();
  const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
  faux.setResponses(Array.from({ length: 20 }, () => fauxAssistantMessage('Observe receipt')));
  const r = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, environment: { repository: f.repository, trustedLocalShell: true } });
  cleanups.push(() => r.close());
  await r.agents!.waitForIdle();
  expect(await readFile(counter, 'utf8')).toBe('x');
  expect((await r.list<any>('job'))[0].status).toBe('completed');
});

it('records a real broker completion delayed across stop as old-epoch accounting only', async () => {
  const f = await jobRuntime();
  await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
  const jobs = f.r.agents!.jobs, original = jobs.finish.bind(jobs);
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(r => enter = r), gate = new Promise<void>(r => release = r);
  let actual: any;
  jobs.finish = async (id, receipt) => {
    if (receipt.status === 'completed') { actual = receipt; enter(); await gate; }
    return original(id, receipt);
  };
  try {
    await writeFile(join(f.root, 'finish'), 'go'); await entered;
    expect(actual).toMatchObject({ status: 'completed', result: { stdout: 'finished', exitCode: 0 } });
    await f.r.stopTicket(f.ticket.id, alice, { commandId: 'stop-before-actual-receipt' });
    const stopped = await f.r.readTicket<any>(f.ticket.id);
    release(); await f.r.agents!.waitForIdle();
    expect((await f.r.list<any>('job'))[0]).toMatchObject({ status: 'completed', receipt: actual });
    expect((await f.r.list<any>('workerNote')).filter(x => x.jobId)).toHaveLength(0);
    expect(await f.r.readTicket<any>(f.ticket.id)).toMatchObject({ lifecycle: 'stopped', revision: stopped.revision, assignmentEpoch: stopped.assignmentEpoch });
    expect((await f.r.list<any>('reservation')).every(x => x.status === 'released')).toBe(true);
  } finally { release(); jobs.finish = original; }
});

describe('review regressions: endpoint truth and repository identity', () => {
  it('supervises a same-group TERM-ignoring descendant after the shell leader closes', async () => {
    const f = await repo({ trustedLocalShell: true });
    const ready = join(f.root, 'group-ready'), effect = join(f.root, 'group-effect');
    const execution = f.broker.execute(f.request('same-group', { kind: 'shell', command: `(trap '' TERM; printf ready > '${ready}'; sleep 1.2; printf survived > '${effect}') >/dev/null 2>&1 & wait` }));
    await until(async () => readFile(ready, 'utf8').then(x => x === 'ready', () => false));
    const receipt = await f.broker.cancel('same-group', 1);
    expect(['cancelled','unknown']).toContain(receipt.status);
    await execution;
    await new Promise(resolve => setTimeout(resolve, 1400));
    await expect(readFile(effect, 'utf8')).rejects.toThrow();
    expect(await f.broker.query('same-group')).toEqual(receipt);
  });
  it('refuses prepared replay and normal operations when a retained broker is reopened for another repository', async () => {
    const a = await repo(), b = await repo();
    await writeFile(join(a.prepared.cwd, 'source.txt'), 'repo A');
    const reopened = new LocalEnvironmentBroker({ ...a.configuration, repository: b.repository });
    await expect(reopened.prepare(a.prepare)).rejects.toThrow(/repository/i);
    await expect(reopened.execute(a.request('wrong-repository', { kind: 'read', path: 'source.txt' }))).rejects.toThrow(/repository/i);
    expect(await readFile(join(a.prepared.cwd, 'source.txt'), 'utf8')).toBe('repo A');
    expect(await readFile(join(b.repository, 'source.txt'), 'utf8')).toBe('original');
  });
  it('accepts a canonical alias of the repository while preserving legitimate worktree edits and commits', async () => {
    const f = await repo(); const alias = join(f.root, 'repository-alias'); await symlink(f.repository, alias);
    await writeFile(join(f.prepared.cwd, 'source.txt'), 'committed work');
    await run('git', ['-C', f.prepared.cwd, 'add', 'source.txt']);
    await run('git', ['-C', f.prepared.cwd, 'commit', '-m', 'legitimate worker commit']);
    const head = (await run('git', ['-C', f.prepared.cwd, 'rev-parse', 'HEAD'])).stdout.trim();
    expect(head).not.toBe(f.prepared.baseCommit);
    await writeFile(join(f.prepared.cwd, 'source.txt'), 'further edits');
    const reopened = new LocalEnvironmentBroker({ ...f.configuration, repository: alias });
    expect(await reopened.prepare(f.prepare)).toEqual(f.prepared);
    expect(await reopened.execute(f.request('alias-read', { kind: 'read', path: 'source.txt' }))).toMatchObject({ status: 'completed', result: { text: 'further edits' } });
    expect((await run('git', ['-C', f.prepared.cwd, 'rev-parse', 'HEAD'])).stdout.trim()).toBe(head);
    expect(await readFile(join(f.repository, 'source.txt'), 'utf8')).toBe('original');
  });
});

it('quarantines a native Job when live group termination cannot be established', async () => {
  const f = await jobRuntime(undefined, f => `(trap '' TERM; printf begun > '${join(f.root, 'begun')}'; while [ ! -f '${join(f.root, 'finish')}' ]; do /bin/sleep 0.02; done; printf uncertain > '${join(f.root, 'uncertain-effect')}') & wait`);
  await until(async () => readFile(join(f.root, 'begun'), 'utf8').then(x => x === 'begun', () => false));
  await until(async () => (await f.r.list<any>('deliveryRun'))[0]?.settled === true);
  const ticket = await f.r.readTicket<any>(f.ticket.id), job = (await f.r.list<any>('job'))[0];
  const originalPath = process.env.PATH;
  const noTools = join(f.root, 'no-inspection-tools'); await mkdir(noTools);
  try {
    // Real OS failure: ps cannot be launched, while the same-group child is alive.
    process.env.PATH = noTools;
    expect(await f.r.agents!.jobs.stop(ticket.workerConversationId, job.id)).toMatchObject({ status: 'unknown', receipt: { status: 'unknown' } });
  } finally { process.env.PATH = originalPath; }
  await f.r.agents!.waitForIdle();
  expect((await f.r.list<any>('reservation')).find(x => x.ticketId === ticket.id)?.status).toBe('quarantined');
  // The unknown receipt promises no rollback or cessation of possible effects.
  await writeFile(join(f.root, 'finish'), 'finish uncertain child');
  await until(async () => readFile(join(f.root, 'uncertain-effect'), 'utf8').then(x => x === 'uncertain', () => false));
  expect((await f.r.list<any>('job'))[0].status).toBe('unknown');
});

it('verifies legacy prepared descriptors against the canonical repository without rewriting them', async () => {
  const { digest, canonicalJson } = await import('../src/domain/model.js');
  const f = await repo(), other = await repo();
  const receipt: any = { ...f.prepared }; delete receipt.repository; delete receipt.gitCommonDirectory;
  const descriptorPath = join(f.configuration.directory, 'environments', `${receipt.environmentId}.json`);
  const markerPath = join(receipt.cwd, '.super-agent-environment.json');
  await writeFile(descriptorPath, canonicalJson(receipt)); await writeFile(markerPath, canonicalJson(receipt));
  const ledgerPath = join(f.configuration.directory, 'operations', `${digest('prepare')}.json`);
  const ledger = { request: f.prepare, receipt };
  await writeFile(ledgerPath, canonicalJson({ ...ledger, checksum: digest(ledger) }));
  const reopened = new LocalEnvironmentBroker(f.configuration);
  expect(await reopened.prepare(f.prepare)).toEqual(receipt);
  expect(await reopened.execute(f.request('legacy-read', { kind: 'read', path: 'source.txt' }))).toMatchObject({ status: 'completed', result: { text: 'original' } });
  const wrong = new LocalEnvironmentBroker({ ...f.configuration, repository: other.repository });
  await expect(wrong.prepare(f.prepare)).rejects.toThrow(/repository/);
  await expect(wrong.execute(f.request('legacy-wrong-read', { kind: 'read', path: 'source.txt' }))).rejects.toThrow(/repository/);
  expect(await readFile(descriptorPath, 'utf8')).toBe(canonicalJson(receipt));
  expect(await readFile(markerPath, 'utf8')).toBe(canonicalJson(receipt));
});

describe('Ticket workspace continuity',()=>{
 it('preserves commits and dirty files across fresh Assignment authority and fences old leases',async()=>{
   const f=await repo({trustedLocalShell:true});
   await writeFile(join(f.prepared.cwd,'source.txt'),'committed work');
   await run('git',['-C',f.prepared.cwd,'add','source.txt']);
   await run('git',['-C',f.prepared.cwd,'commit','-m','worker change']);
   const head=(await run('git',['-C',f.prepared.cwd,'rev-parse','HEAD'])).stdout.trim();
   await writeFile(join(f.prepared.cwd,'dirty.txt'),'unsaved work');
   const next={...identity,assignmentId:'resumed',epoch:2,mandateId:'fresh'};
   f.configuration.authorize=async(i:any)=>({...i,capabilities:['repo.read','repo.write','command.execute']});
   const payload={kind:'prepare' as const};
   const prepared=await f.broker.prepare({operationId:'prepare-next',identity:next,payload,payloadDigest:operationDigest(next,payload)});
   expect(prepared.cwd).toBe(f.prepared.cwd);
   expect(prepared.environmentId).not.toBe(f.prepared.environmentId);
   expect((await run('git',['-C',prepared.cwd,'rev-parse','HEAD'])).stdout.trim()).toBe(head);
   expect(await readFile(join(prepared.cwd,'dirty.txt'),'utf8')).toBe('unsaved work');
   await expect(f.broker.release(f.prepared.environmentId,'old-release')).rejects.toThrow();
   await expect(f.broker.execute(f.request('old-write',{kind:'write',path:'dirty.txt',text:'bad'}))).rejects.toThrow();
   expect(await f.broker.query('prepare')).toEqual(f.prepared);
   const p={kind:'write' as const,environmentId:prepared.environmentId,path:'fresh.txt',text:'new authority'};
   expect(await f.broker.execute({operationId:'fresh-write',identity:next,payload:p,payloadDigest:operationDigest(next,p)})).toMatchObject({status:'completed'});
 });
});

it('preserves existing modes through atomic structured writes and edits without following symlinks', async () => {
  const { chmod, stat } = await import('node:fs/promises');
  const f = await repo();
  for (const kind of ['write', 'edit'] as const) for (const mode of [0o755, 0o640]) {
    const path = `${kind}-${mode}`; await writeFile(join(f.prepared.cwd, path), 'original'); await chmod(join(f.prepared.cwd, path), mode);
    const action = kind === 'write' ? { kind, path, text: 'updated' } : { kind, path, oldText: 'original', newText: 'updated' };
    expect(await f.broker.execute(f.request(path, action))).toMatchObject({ status: 'completed' });
    expect((await stat(join(f.prepared.cwd, path))).mode & 0o777).toBe(mode);
    expect(await readFile(join(f.prepared.cwd, path), 'utf8')).toBe('updated');
  }
  expect(await f.broker.execute(f.request('new-conservative', { kind: 'write', path: 'new-conservative', text: 'new' }))).toMatchObject({ status: 'completed' });
  expect((await stat(join(f.prepared.cwd, 'new-conservative'))).mode & 0o777).toBe(0o600);
  await symlink(join(f.prepared.cwd, 'source.txt'), join(f.prepared.cwd, 'linked'));
  for (const kind of ['write', 'edit']) await expect(f.broker.execute(f.request(`linked-${kind}`, { kind, path: 'linked', text: 'unsafe', oldText: 'original', newText: 'unsafe' }))).rejects.toThrow(/symlink/);
  expect(await readFile(join(f.prepared.cwd, 'source.txt'), 'utf8')).toBe('original');
});
