// Executable architecture probes, not an implementation of the super agent.
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import {
  Harness, MemoryStorage, createRegistry, defineDoc, defineDocFamily,
  defineExtension, defineTask,
} from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';

const State = defineDoc({
  kind: 'probe.state', version: 1, scope: 'session',
  initial: () => ({ prepared: 0, finished: 0 }),
});
const Tickets = defineDocFamily({
  kind: 'probe.ticket', version: 1, scope: 'session', family: true,
  initial: (id) => ({ id, status: 'queued' }),
});
const Checkpoint = defineTask({
  name: 'probe.checkpoint', version: 1,
  initial: () => ({ phase: 'prepare' }),
  phases: {
    prepare: async (_task, runtime, ctx) => {
      await runtime.commit(async (tx) => {
        (await tx.doc(State)).prepared++;
        return { status: 'running', checkpoint: { phase: 'finish' } };
      }, ctx);
      if (process.argv[2] === '--crash-child') process.kill(process.pid, 'SIGKILL');
    },
    finish: async (_task, runtime, ctx) => {
      await runtime.commit(async (tx) => {
        (await tx.doc(State)).finished++;
        return { status: 'terminal', outcome: { status: 'completed', result: 'ok' } };
      }, ctx);
    },
  },
  abort: async (_task, runtime, ctx) => {
    await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'aborted' } }), ctx);
  },
});
const Anchor = defineTask({
  name: 'probe.anchor', version: 1,
  initial: () => ({ phase: 'hold' }),
  phases: {
    hold: async (_task, runtime, ctx) => {
      await runtime.sleep(runtime.now() + 86_400_000, ctx);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'completed', result: null } }), ctx);
    },
  },
  abort: Checkpoint.definition.abort,
});

async function open(storage) {
  const registry = createRegistry();
  registry.install(defineExtension({ name: 'probe', tasks: [Checkpoint, Anchor] }));
  return Harness.open(storage, { models: createModels(), registry }, context);
}

if (process.argv[2] === '--crash-child') {
  const harness = await open(await openNodeSqliteStorage(process.argv[3]));
  const root = await harness.root(context);
  await root.commit(async (tx) => {
    await tx.doc(State);
    await tx.createTask(Checkpoint, {}, { ownership: { kind: 'conversation' } });
  }, context);
  harness.resume();
  await harness.waitForIdle(context); // prepare deliberately kills this process
  throw new Error('Crash child unexpectedly survived');
}

const harness = await open(new MemoryStorage());
try {
  const root = await harness.root(context);
  await assert.rejects(root.commit(async (tx) => {
    (await tx.doc(Tickets, 'rollback', 'rollback')).status = 'working';
    await tx.createTask(Checkpoint, {}, { ownership: { kind: 'conversation' } });
    throw new Error('rollback');
  }, context), /rollback/);
  assert.equal(await harness.snapshot(Tickets, 'rollback', context), undefined);
  assert.equal((await harness.inspect(context)).tasks.length, 0);
  console.log('PASS: domain document and execution intent roll back together');

  const submission = { type: 'write', requestId: 'delivery:1', entry: { kind: 'probe.input', data: 'feedback' } };
  const first = await root.submit(submission, context);
  const again = await root.submit(submission, context);
  assert.equal(first.id, again.id);
  const entries = await root.entries({ order: 'ascending' }, 100, undefined, context);
  assert.equal(entries.items.filter((entry) => entry.kind === 'probe.input').length, 1);
  console.log('PASS: repeated requestId produces one submission and one entry');

  const { anchor, worker } = await root.commit(async (tx) => {
    const anchor = await tx.createTask(Anchor, {}, { ownership: { kind: 'conversation' }, background: true });
    const worker = await tx.createConversation({ ownership: { kind: 'task', taskId: anchor } });
    (await tx.doc(Tickets, 'ticket-1', 'ticket-1')).status = 'working';
    return { anchor, worker };
  }, context);
  await root.abort(context);
  assert.notEqual((await harness.getTask(anchor, context)).state.status, 'terminal');
  assert.ok(await harness.conversation(worker.id, context));
  console.log('PASS: background anchor and worker survive ordinary Router abort');
  await root.abort(context, { background: true });
  assert.equal((await harness.getTask(anchor, context)).state.outcome.status, 'aborted');
  assert.equal((await harness.snapshot(Tickets, 'ticket-1', context)).status, 'working');
  console.log('PASS: explicit background abort stops execution but does not rewrite Ticket state');

  const independent = await harness.createConversation({ ownership: { kind: 'ownerless' } }, context);
  const work = await independent.commit((tx) => tx.createTask(Anchor, {}, {
    ownership: { kind: 'conversation' },
  }), context);
  await root.abort(context, { background: true });
  assert.notEqual((await harness.getTask(work, context)).state.status, 'terminal');
  await independent.abort(context);
  assert.equal((await harness.getTask(work, context)).state.outcome.status, 'aborted');
  console.log('PASS: ownerless Worker execution survives Router-wide abort and stops independently');
} finally {
  await harness.close(context);
}

const directory = await mkdtemp(join(tmpdir(), 'super-durable-probe-'));
try {
  const database = join(directory, 'session.sqlite');
  const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--crash-child', database], {
    encoding: 'utf8', timeout: 15_000,
  });
  assert.equal(child.signal, 'SIGKILL', child.stderr);
  const recovered = await open(await openNodeSqliteStorage(database));
  try {
    assert.deepEqual(await recovered.snapshot(State, context), { prepared: 1, finished: 0 });
    recovered.resume();
    await recovered.waitForIdle(context);
    assert.deepEqual(await recovered.snapshot(State, context), { prepared: 1, finished: 1 });
    console.log('PASS: SQLite recovers after SIGKILL at the committed phase without replaying prepare');
    const root = await recovered.root(context);
    const draft = { type: 'write', requestId: 'persisted-delivery', entry: { kind: 'probe.persisted', data: 'one' } };
    const first = await root.submit(draft, context);
    const id = first.id;
    await recovered.close(context);
    const reopened = await open(await openNodeSqliteStorage(database));
    try {
      const second = await (await reopened.root(context)).submit(draft, context);
      assert.equal(second.id, id);
      console.log('PASS: submission deduplication survives reopening SQLite');
    } finally {
      await reopened.close(context);
    }
  } finally {
    await recovered.close(context);
  }
} finally {
  await rm(directory, { recursive: true, force: true });
}
