import { mkdir, open, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import fsExt from 'fs-ext';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels, type Models } from '@earendil-works/pi-ai/models';
import { Harness, createRegistry, defineExtension, type ModelRef, type Conversation } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { DomainStore } from '../domain/store.js';
import { Engine, createIntakeTask } from '../domain/engine.js';
import { canonicalJson, type NormalizedEvent, type ReceiveResult, type ReceiveBatchResult, type RuntimeMetadata, type Source, type SourceCursor } from '../domain/model.js';

const openDirectories = new Set<string>();
export interface OpenOptions {
  directory: string;
  models?: Models;
  model?: ModelRef;
  instanceId?: string;
  maxPending?: number;
  maxIndexEntries?: number;
  /** Additional recovery checks run while scheduling is paused. Throwing closes storage and releases the lock. */
  preflight?: (engine: Engine) => Promise<void>;
}
export interface SuperAgentRuntime {
  readonly engine: Engine;
  readonly harness: Harness;
  readonly control: Conversation;
  close(): Promise<void>;
  receive(event: NormalizedEvent): Promise<ReceiveResult>;
  receiveBatch(events: NormalizedEvent[], cursor: SourceCursor): Promise<ReceiveBatchResult>;
  readTicket<T = Record<string, unknown>>(id: string): Promise<T | undefined>;
  list<T = Record<string, unknown>>(kind: string): Promise<T[]>;
}

export async function openSuperAgent(options: OpenOptions): Promise<SuperAgentRuntime> {
  if (!options.directory?.trim()) throw new Error('directory is required');
  for (const [name, value] of Object.entries({ maxPending: options.maxPending, maxIndexEntries: options.maxIndexEntries })) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) throw new Error(`${name} must be a positive integer`);
  }
  if (options.instanceId !== undefined && !options.instanceId.trim()) throw new Error('instanceId must be nonempty');
  await mkdir(options.directory, { recursive: true });
  const directory = await realpath(options.directory);
  if (openDirectories.has(directory)) throw new Error('Directory already has an open writer lock');
  openDirectories.add(directory);
  let lock: Awaited<ReturnType<typeof open>> | undefined;
  let harness: Harness | undefined;
  let storage: Awaited<ReturnType<typeof openNodeSqliteStorage>> | undefined;
  const release = async () => {
    try { await lock?.close(); } finally { openDirectories.delete(directory); }
  };
  try {
    // Keep this inode permanently: unlinking a lock file would permit two owners on different inodes.
    lock = await open(join(directory, '.writer.lock'), 'a+', 0o600);
    try { fsExt.flockSync(lock.fd, 'exnb'); }
    catch (cause) { throw new Error('Directory writer lock is held by another process', { cause }); }
    storage = await openNodeSqliteStorage(join(directory, 'session.sqlite'));
    const registry = createRegistry();
    harness = await Harness.open(storage, { models: options.models ?? createModels(), registry }, context);
    // Reading metadata precedes all domain writes and task scheduling.
    const initialStore = new DomainStore(harness, context);
    const previous = await initialStore.read<RuntimeMetadata>('metadata', 'runtime');
    const maxPending = options.maxPending ?? previous?.maxPending ?? 10_000;
    const maxIndexEntries = options.maxIndexEntries ?? previous?.maxIndexEntries ?? 10_000;
    const store = new DomainStore(harness, context, maxIndexEntries);
    const intakeTask = createIntakeTask(store);
    registry.install(defineExtension({ name: 'sa.kernel', tasks: [intakeTask] }));
    const snapshot = registry.snapshot();
    const definitions = {
      extensions: snapshot.installed().map((extension) => extension.name).sort(),
      tasks: Object.fromEntries(snapshot.tasks().map((task) => [task.definition.name, task.definition.version])),
    };
    if (previous) {
      if (previous.schemaVersion !== 1) throw new Error(`Unsupported schema version: ${previous.schemaVersion}`);
      if (canonicalJson(previous.extensions) !== canonicalJson(definitions.extensions) || canonicalJson(previous.tasks) !== canonicalJson(definitions.tasks)) {
        throw new Error('Installed extension/task definitions do not match persisted metadata');
      }
      if (options.instanceId !== undefined && options.instanceId !== previous.instanceId) throw new Error('instanceId differs from persisted metadata');
    }
    const instanceId = previous?.instanceId ?? options.instanceId ?? randomUUID();
    const control = await harness.root(context, { agent: { extensions: [], ...(options.model ? { model: options.model } : {}) } });
    const engine = new Engine(harness, control, context, store, instanceId, maxPending, intakeTask);
    await engine.transaction(async (tx) => {
      await store.put(tx, 'metadata', 'runtime', { schemaVersion: 1, instanceId, ...definitions, maxPending, maxIndexEntries } satisfies RuntimeMetadata);
      if (!await store.get<Source>(tx, 'source', 'cli')) await store.put(tx, 'source', 'cli', { id: 'cli', kind: 'cli', enabled: true });
    });
    await options.preflight?.(engine);
    harness.resume();
    const openedHarness = harness;
    let closing: Promise<void> | undefined;
    return {
      engine, harness, control,
      close: () => closing ??= (async () => { try { await openedHarness.close(context); } finally { await release(); } })(),
      receive: (event) => engine.receive(event),
      receiveBatch: (events, cursor) => engine.receiveBatch(events, cursor),
      readTicket: <T>(id: string) => store.read<T>('ticket', id),
      list: <T>(kind: string) => store.list<T>(kind),
    };
  } catch (error) {
    try { if (harness) await harness.close(context); else await storage?.close(context); }
    finally { await release(); }
    throw error;
  }
}
