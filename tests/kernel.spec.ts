import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { defineTask } from '@earendil-works/pi-durable';
import { openSuperAgent } from '../src/runtime/open.js';

const directories: string[] = [];
const runtimes: Awaited<ReturnType<typeof openSuperAgent>>[] = [];
async function open(options: Record<string, unknown> = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'sa-kernel-'));
  directories.push(directory);
  const runtime = await openSuperAgent({ directory, ...options });
  runtimes.push(runtime);
  return { runtime, directory };
}
const event = (patch = {}) => ({
  sourceId: 'cli', externalId: 'message-1', revision: '1',
  actor: { id: 'alice', kind: 'human' as const, trusted: true },
  threads: ['discussion', 'discussion'], body: 'Please investigate',
  receivedAt: 1000, occurredAt: 900, ...patch,
});
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.close()));
  await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

describe('persistent domain kernel', () => {
  it('deduplicates retries, retains edits, and separates source namespaces', async () => {
    const { runtime } = await open();
    const first = await runtime.receive(event());
    expect(first.disposition).toBe('stored');
    expect(await runtime.receive(event({ receivedAt: 2000 }))).toEqual({ ...first, disposition: 'duplicate' });
    const edited = await runtime.receive(event({ revision: '2', body: 'Correction' }));
    expect(edited.triggerId).not.toBe(first.triggerId);
    await runtime.engine.configureSource({ id: 'github', kind: 'github', enabled: true });
    const other = await runtime.receive(event({ sourceId: 'github' }));
    expect(other.triggerId).not.toBe(first.triggerId);
    const triggers = await runtime.list('trigger');
    expect(triggers).toHaveLength(3);
    expect(triggers[0].threads).toEqual(['discussion']);
    expect(await runtime.list('audit')).toHaveLength(3);
    const thread = (await runtime.list('thread')).find((item: any) => item.sourceId === 'cli');
    expect(thread.triggerIds).toEqual([first.triggerId, edited.triggerId]);
    expect(await runtime.list('intake')).toHaveLength(3);
    const tasks = await runtime.engine.transaction((tx: any) => tx.scanTasks({ kind: 'sa.intake' }, 100));
    expect(tasks.items).toHaveLength(3);
    expect(tasks.items.every((task: any) => task.background && task.owner === undefined && task.conversationId === runtime.control.id)).toBe(true);
    await runtime.harness.waitForTask(tasks.items[0].id, runtime.engine.context);
    expect((await runtime.engine.store.read('intake', first.triggerId)).status).toBe('accepted');
  });

  it('rejects conflicting normalized payload and malformed events before any write', async () => {
    const { runtime } = await open();
    await runtime.receive(event());
    await expect(runtime.receive(event({ body: 'different' }))).rejects.toThrow(/conflict/i);
    await expect(runtime.receive(event({ revision: '2', threads: [] }))).rejects.toThrow(/thread/i);
    await expect(runtime.receive(event({ revision: '2', receivedAt: NaN }))).rejects.toThrow(/time|receivedAt/i);
    expect(await runtime.list('trigger')).toHaveLength(1);
    expect(await runtime.list('audit')).toHaveLength(1);
  });

  it('does not allocate unindexed event documents for filtered inputs', async () => {
    const { runtime } = await open();
    const createdKinds: string[] = [];
    const unsubscribe = runtime.harness.subscribeCommits((publication: any) => {
      for (const change of publication.changes) if (change.type === 'document') createdKinds.push(change.record.kind);
    });
    try {
      expect((await runtime.receive(event({ sourceId: 'unregistered' }))).disposition).toBe('filtered');
      expect(createdKinds).not.toContain('sa.trigger');
      expect(createdKinds).not.toContain('sa.source');
    } finally { unsubscribe(); }
  });

  it('atomically binds canonical command payload to a durable result', async () => {
    const { runtime, directory } = await open();
    let calls = 0;
    const result = await runtime.engine.command('create', { b: 2, a: 1 }, async (tx: any) => {
      calls++;
      await runtime.engine.store.put(tx, 'ticket', 'ticket-1', { id: 'ticket-1', goal: 'Fix it' });
      return { ticketId: 'ticket-1' };
    });
    expect(result).toEqual({ ticketId: 'ticket-1' });
    expect(await runtime.engine.command('create', { a: 1, b: 2 }, async () => { calls++; return null; })).toEqual(result);
    expect(calls).toBe(1);
    await expect(runtime.engine.command('create', { a: 3 }, async () => null)).rejects.toThrow(/conflict/i);
    await runtime.close();
    const reopened = await openSuperAgent({ directory }); runtimes.push(reopened);
    expect(await reopened.readTicket('ticket-1')).toEqual({ id: 'ticket-1', goal: 'Fix it' });
    expect(await reopened.engine.command('create', { a: 1, b: 2 }, async () => null)).toEqual(result);
  });

  it('serializes concurrent duplicate commands and accepts void command receipts', async () => {
    const { runtime } = await open();
    let calls = 0;
    const results = await Promise.all(Array.from({ length: 8 }, () => runtime.engine.command('concurrent', {}, async () => { calls++; return 'one result'; })));
    expect(results).toEqual(Array(8).fill('one result'));
    expect(calls).toBe(1);
    await runtime.engine.command('void', {}, async () => { calls++; });
    expect(await runtime.engine.command('void', {}, async () => { calls++; })).toBeUndefined();
    expect(calls).toBe(2);
  });

  it('rejects lossy JSON command payloads without executing the command', async () => {
    const { runtime } = await open();
    let calls = 0;
    for (const payload of [NaN, { missing: undefined }, new Array(2)]) {
      await expect(Promise.resolve().then(() => runtime.engine.command('invalid', payload, async () => { calls++; }))).rejects.toThrow(/JSON/i);
    }
    expect(calls).toBe(0);
  });

  it('never permits an immutable audit record to be rewritten', async () => {
    const { runtime } = await open();
    const result = await runtime.receive(event());
    await expect(runtime.engine.transaction((tx: any) => runtime.engine.store.put(tx, 'audit', result.triggerId, { id: result.triggerId, kind: 'tampered' }))).rejects.toThrow(/immutable|conflict/i);
    expect((await runtime.list('audit'))[0].kind).toBe('trigger.received');
    const history = await runtime.control.entries({}, 100, undefined, runtime.engine.context);
    expect(history.items.filter((entry: any) => entry.kind === 'sa.trigger.received')).toHaveLength(1);
  });

  it('rolls back a domain record, its index, task intent, and command receipt together', async () => {
    const { runtime } = await open();
    const Task = defineTask({ name: 'test.rollback', version: 1, initial: () => ({ phase: 'done' }), phases: {
      done: async () => {},
    }, abort: async () => {} });
    await expect(runtime.engine.command('rollback', {}, async (tx: any) => {
      await runtime.engine.store.put(tx, 'ticket', 'transient', { id: 'transient' });
      await tx.createTask(Task, {}, { ownership: { kind: 'conversation' }, background: true });
      throw new Error('deliberate rollback');
    })).rejects.toThrow('deliberate rollback');
    expect(await runtime.readTicket('transient')).toBeUndefined();
    expect(await runtime.list('ticket')).toEqual([]);
    expect((await runtime.harness.inspect(runtime.engine.context)).tasks).toHaveLength(0);
    expect(await runtime.engine.command('rollback', {}, async () => 'retry works')).toBe('retry works');
  });

  it('persists source filters and commits a batch cursor only with its events', async () => {
    const { runtime, directory } = await open({ maxPending: 2 });
    expect((await runtime.receive(event({ sourceId: 'github' }))).disposition).toBe('filtered');
    await runtime.engine.configureSource({ id: 'cli', kind: 'cli', enabled: true, listenedScopes: ['allowed'] });
    expect((await runtime.receive(event())).disposition).toBe('filtered');
    await runtime.engine.configureSource({ id: 'cli', kind: 'cli', enabled: true });
    await runtime.receiveBatch([event()], { sourceId: 'cli', value: 'cursor-1' });
    expect((await runtime.engine.store.read('source', 'cli')).cursor).toBe('cursor-1');
    const full = await runtime.receiveBatch([event({ externalId: 'two' }), event({ externalId: 'three' })], { sourceId: 'cli', value: 'cursor-2' });
    expect(full).toMatchObject({ disposition: 'backpressure', retryable: true });
    expect(await runtime.list('trigger')).toHaveLength(1);
    expect((await runtime.engine.store.read('source', 'cli')).cursor).toBe('cursor-1');
    expect((await runtime.receive(event({ externalId: 'two' }))).disposition).toBe('stored');
    expect((await runtime.receive(event({ externalId: 'three' }))).disposition).toBe('backpressure');
    expect((await runtime.receive(event())).disposition).toBe('duplicate');
    await runtime.engine.configureSource({ id: 'cli', kind: 'cli', enabled: false });
    await runtime.close();
    const reopened = await openSuperAgent({ directory }); runtimes.push(reopened);
    expect((await reopened.receive(event({ externalId: 'four' }))).disposition).toBe('filtered');
    expect((await reopened.engine.store.read('source', 'cli')).cursor).toBe('cursor-1');
  });

  it('rolls back batch conflicts and never advances a different source cursor', async () => {
    const { runtime } = await open();
    await runtime.receive(event());
    await expect(runtime.receiveBatch([event({ externalId: 'new' }), event({ body: 'conflict' })], { sourceId: 'cli', value: 'lost' })).rejects.toThrow(/conflict/i);
    expect(await runtime.list('trigger')).toHaveLength(1);
    expect((await runtime.engine.store.read('source', 'cli')).cursor).toBeUndefined();
    await expect(runtime.receiveBatch([event()], { sourceId: 'other', value: 'wrong' })).rejects.toThrow(/source/i);
    await runtime.receiveBatch([event()], { sourceId: 'cli', value: 'same' });
    expect(await runtime.list('trigger')).toHaveLength(1);
    expect((await runtime.engine.store.read('source', 'cli')).cursor).toBe('same');
  });

  it('bounds each kind index independently and leaves the previous records intact', async () => {
    const { runtime } = await open({ maxIndexEntries: 2 });
    await runtime.engine.transaction(async (tx: any) => {
      await runtime.engine.store.put(tx, 'ticket', 'one', { id: 'one' });
      await runtime.engine.store.put(tx, 'ticket', 'two', { id: 'two' });
    });
    await expect(runtime.engine.transaction((tx: any) => runtime.engine.store.put(tx, 'ticket', 'three', { id: 'three' }))).rejects.toThrow(/capacity/i);
    expect(await runtime.list('ticket')).toEqual([{ id: 'one' }, { id: 'two' }]);
    expect(await runtime.readTicket('three')).toBeUndefined();
  });

  it('checks schema and extension/task metadata before scheduling recovered work', async () => {
    const { runtime, directory } = await open();
    const metadata = await runtime.engine.store.read('metadata', 'runtime');
    expect(metadata.schemaVersion).toBe(1);
    expect(metadata.extensions).toContain('sa.kernel');
    expect(metadata.tasks['sa.intake']).toBe(1);
    await runtime.engine.transaction((tx: any) => runtime.engine.store.put(tx, 'metadata', 'runtime', { ...metadata, schemaVersion: 99 }));
    await runtime.close();
    await expect(openSuperAgent({ directory })).rejects.toThrow(/schema/i);
  });

  it.each(['extensions', 'tasks'])('rejects mismatched %s before startup preflight', async (field) => {
    const { runtime, directory } = await open();
    const metadata = await runtime.engine.store.read('metadata', 'runtime');
    await runtime.engine.transaction((tx: any) => runtime.engine.store.put(tx, 'metadata', 'runtime', { ...metadata,
      [field]: field === 'extensions' ? [] : { 'sa.intake': 99 },
    }));
    await runtime.close();
    let preflightCalled = false;
    await expect(openSuperAgent({ directory, preflight: async () => { preflightCalled = true; } })).rejects.toThrow(/extension|task|definition/i);
    expect(preflightCalled).toBe(false);
  });

  it('rejects an instance namespace change across reopen', async () => {
    const { runtime, directory } = await open({ instanceId: 'stable' });
    await runtime.close();
    await expect(openSuperAgent({ directory, instanceId: 'different' })).rejects.toThrow(/instanceId/i);
  });

  it('runs startup preflight while paused and releases its lock on a failed preflight', async () => {
    const { runtime, directory } = await open();
    await runtime.close();
    await expect(openSuperAgent({ directory, preflight: async (engine: any) => {
      expect((await engine.harness.inspect(engine.context)).scheduling).toBe('paused');
      throw new Error('preflight refused');
    } })).rejects.toThrow('preflight refused');
    const reopened = await openSuperAgent({ directory }); runtimes.push(reopened);
    expect(await reopened.list('trigger')).toEqual([]);
  });

  it('rejects duplicate writers in one process and in a subprocess', async () => {
    const { directory } = await open();
    await expect(openSuperAgent({ directory })).rejects.toThrow(/lock|writer|open/i);
    const child = spawn(process.execPath, ['tests/helpers/lock-child.mjs', directory], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; child.stderr.on('data', (value) => stderr += value);
    const [code] = await once(child, 'exit');
    expect(code).toBe(1);
    expect(stderr).toMatch(/lock|writer|open/i);
  });

  it('reclaims an OS lock after SIGKILL and resumes the committed intake intent', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'sa-killed-')); directories.push(directory);
    const child = spawn(process.execPath, ['tests/helpers/lock-child.mjs', directory, 'hold'], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; child.stderr.on('data', (value) => stderr += value);
    const ready = await Promise.race([
      once(child.stdout, 'data').then(([value]) => String(value)),
      once(child, 'exit').then(([code]) => { throw new Error(`child exited ${code}: ${stderr}`); }),
    ]);
    expect(ready).toContain('ready');
    const exited = once(child, 'exit'); child.kill('SIGKILL');
    expect((await exited)[1]).toBe('SIGKILL');
    const runtime = await openSuperAgent({ directory, preflight: async (engine) => {
      expect((await engine.store.list<{ status: string }>('intake'))[0].status).toBe('pending');
    } }); runtimes.push(runtime);
    expect(await runtime.list('trigger')).toHaveLength(1);
    const tasks = await runtime.engine.transaction((tx: any) => tx.scanTasks({ kind: 'sa.intake' }, 10));
    await runtime.harness.waitForTask(tasks.items[0].id, runtime.engine.context);
    expect((await runtime.list('intake'))[0].status).toBe('accepted');
  });

  it('provides init, receive, list, show and nonzero CLI failures without a model', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'sa-cli-')); directories.push(directory);
    async function cli(args: string[], input = '') {
      const child = spawn(process.execPath, ['dist/cli.js', ...args], {
        env: { ...process.env, SUPER_AGENT_DIRECTORY: directory }, stdio: ['pipe', 'pipe', 'pipe'],
      });
      let stdout = '', stderr = '';
      child.stdout.on('data', (data) => stdout += data);
      child.stderr.on('data', (data) => stderr += data);
      child.stdin.end(input);
      const [code] = await once(child, 'exit');
      return { code, stdout, stderr };
    }
    expect((await cli(['init'])).code).toBe(0);
    const received = await cli(['receive', '--directory', directory], JSON.stringify(event()));
    expect(received.code).toBe(0);
    const triggerId = JSON.parse(received.stdout).triggerId;
    expect(JSON.parse((await cli(['list', 'trigger'])).stdout)).toHaveLength(1);
    expect(JSON.parse((await cli(['show', 'trigger', triggerId])).stdout).body).toBe('Please investigate');
    expect((await cli(['receive'], '{broken')).code).toBe(1);
    expect((await cli(['unknown'])).code).toBe(1);
    expect((await cli(['--help'])).stdout).toContain('receive');
    expect((await cli(['show', 'trigger', 'missing'])).code).toBe(1);
  });
});
