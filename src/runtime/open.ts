import { Outbox, type OutboxOptions } from '../services/outbox.js';
import type { EnvironmentOptions } from "../services/jobs.js";
import { reconcile } from "./reconcile.js";
import type { Clock } from "./scheduler.js";
import type { CancellationHandler } from "../services/cancellation.js";
import { mkdir, open, realpath } from "node:fs/promises";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import fsExt from "fs-ext";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { createModels, type Models } from "@earendil-works/pi-ai/models";
import {
  Harness,
  createRegistry,
  defineExtension,
  type ModelRef,
  type Conversation,
} from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { DomainStore } from "../domain/store.js";
import { AgentCoordinator } from "../agents/coordinator.js";
import { Artifacts } from "./artifacts.js";
import { Engine, createIntakeTask } from "../domain/engine.js";
import {
  canonicalJson,
  type NormalizedEvent,
  type ReceiveResult,
  type ReceiveBatchResult,
  type RuntimeMetadata,
  type Source,
  type SourceCursor,
  type Actor,
  type Policy,
  type CommandMeta,
  type DecisionOutcome,
  type Eval,
} from "../domain/model.js";

const openDirectories = new Set<string>();
export interface OpenOptions {
  directory: string;
  models?: Models;
  model?: ModelRef;
  instanceId?: string;
  maxPending?: number;
  maxIndexEntries?: number;
  clock?: Clock;
  cancellationHandler?: CancellationHandler;
  environment?: EnvironmentOptions;
  outbox?: OutboxOptions;
  /** Defer native effects until host listeners and credentials are ready. Call activate() explicitly. */
  startPaused?: boolean;
  /** Additional recovery checks run while scheduling is paused. Throwing closes storage and releases the lock. */
  preflight?: (engine: Engine) => Promise<void>;
}
export interface SuperAgentRuntime {
  readonly engine: Engine;
  readonly outbox: Outbox;
  readonly harness: Harness;
  readonly control: Conversation;
  readonly agents: AgentCoordinator | undefined;
  readonly artifactRoot: string;
  activate(): Promise<void>;
  close(): Promise<void>;
  updatePolicy(policy: Policy, actor: Actor, meta: CommandMeta): Promise<void>;
  listCandidates(): Promise<Eval[]>;
  decide(
    evalId: string,
    epoch: number,
    outcome: DecisionOutcome,
    meta: CommandMeta,
  ): Promise<string>;
  subscribe(
    ticketId: string,
    sourceId: string,
    threadRef: string,
    own: boolean,
    meta: CommandMeta,
  ): Promise<void>;
  unsubscribe(
    ticketId: string,
    sourceId: string,
    threadRef: string,
    meta: CommandMeta,
  ): Promise<void>;
  handback(
    ticketId: string,
    triggerId: string,
    epoch: number,
    reason: string,
    meta: CommandMeta,
  ): Promise<void>;
  stopTicket(ticketId: string, actor: Actor, meta: CommandMeta): Promise<void>;
  retryTicket(ticketId: string, actor: Actor, meta: CommandMeta): Promise<void>;
  endTicket(ticketId: string, epoch: number, meta: CommandMeta): Promise<void>;
  receive(event: NormalizedEvent): Promise<ReceiveResult>;
  receiveBatch(
    events: NormalizedEvent[],
    cursor: SourceCursor,
  ): Promise<ReceiveBatchResult>;
  readTicket<T = Record<string, unknown>>(id: string): Promise<T | undefined>;
  list<T = Record<string, unknown>>(kind: string): Promise<T[]>;
}

export async function openSuperAgent(
  options: OpenOptions,
): Promise<SuperAgentRuntime> {
  if (options.model && !options.models)
    throw new Error("model requires an explicit models collection");
  if (!options.directory?.trim()) throw new Error("directory is required");
  for (const [name, value] of Object.entries({
    maxPending: options.maxPending,
    maxIndexEntries: options.maxIndexEntries,
  })) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1))
      throw new Error(`${name} must be a positive integer`);
  }
  if (options.instanceId !== undefined && !options.instanceId.trim())
    throw new Error("instanceId must be nonempty");
  await mkdir(options.directory, { recursive: true });
  const directory = await realpath(options.directory);
  if (openDirectories.has(directory))
    throw new Error("Directory already has an open writer lock");
  openDirectories.add(directory);
  let lock: Awaited<ReturnType<typeof open>> | undefined;
  let harness: Harness | undefined;
  let storage: Awaited<ReturnType<typeof openNodeSqliteStorage>> | undefined;
  const release = async () => {
    try {
      await lock?.close();
    } finally {
      openDirectories.delete(directory);
    }
  };
  try {
    // Keep this inode permanently: unlinking a lock file would permit two owners on different inodes.
    lock = await open(join(directory, ".writer.lock"), "a+", 0o600);
    try {
      fsExt.flockSync(lock.fd, "exnb");
    } catch (cause) {
      throw new Error("Directory writer lock is held by another process", {
        cause,
      });
    }
    storage = await openNodeSqliteStorage(join(directory, "session.sqlite"));
    const registry = createRegistry();
    harness = await Harness.open(
      storage,
      { models: options.models ?? createModels(), registry },
      context,
    );
    // Reading metadata precedes all domain writes and task scheduling.
    const initialStore = new DomainStore(harness, context);
    const previous = await initialStore.read<RuntimeMetadata>(
      "metadata",
      "runtime",
    );
    const maxPending = options.maxPending ?? previous?.maxPending ?? 10_000;
    const maxIndexEntries =
      options.maxIndexEntries ?? previous?.maxIndexEntries ?? 10_000;
    const store = new DomainStore(harness, context, maxIndexEntries);
    const intakeTask = createIntakeTask(store);
    registry.install(
      defineExtension({ name: "sa.kernel", tasks: [intakeTask] }),
    );
    const baseline = registry.snapshot();
    const baselineDefinitions = {
      extensions: baseline
        .installed()
        .map((extension) => extension.name)
        .sort(),
      tasks: Object.fromEntries(
        baseline
          .tasks()
          .map((task) => [task.definition.name, task.definition.version]),
      ),
    };
    const control = await harness.root(context, { agent: { extensions: [] } });
    const instanceId =
      previous?.instanceId ?? options.instanceId ?? randomUUID();
    const engine = new Engine(
      harness,
      control,
      context,
      store,
      instanceId,
      maxPending,
      intakeTask,
      options.clock,
    );
    const artifacts = new Artifacts(directory);
    engine.prepareEvent = async (event) => {
      const body = await artifacts.spill(event.body);
      return {
        ...event,
        body: body.text,
        ...(body.artifact ? { bodyArtifact: body.artifact } : {}),
      };
    };
    const outbox = new Outbox(engine, options.outbox, options.clock);
    engine.outbox = outbox;
    const coordinator = new AgentCoordinator(engine, artifacts, options.model, options.cancellationHandler, options.environment);
    registry.install(coordinator.routerExtension);
    registry.install(coordinator.workerExtension);
    registry.install(
      defineExtension({
        name: "sa.agents",
        tasks: [
          coordinator.inputTask,
          coordinator.cancelTask,
          coordinator.redriveTask,
          coordinator.jobs.task,
          outbox.task,
        ],
      }),
    );
    const snapshot = registry.snapshot();
    const definitions = {
      extensions: snapshot
        .installed()
        .map((extension) => extension.name)
        .sort(),
      tasks: Object.fromEntries(
        snapshot
          .tasks()
          .map((task) => [task.definition.name, task.definition.version]),
      ),
    };
    if (previous) {
      if (previous.schemaVersion !== 1)
        throw new Error(
          `Unsupported schema version: ${previous.schemaVersion}`,
        );
      const previousDefinitions = {
        extensions: previous.extensions,
        tasks: previous.tasks,
      };
      // Explicit additive compatibility: the exact reviewed kernel registry can
      // upgrade to this known agents registry. Missing/changed old definitions fail.
      if (
        canonicalJson(previousDefinitions) !== canonicalJson(definitions) &&
        canonicalJson(previousDefinitions) !==
          canonicalJson(baselineDefinitions) &&
        canonicalJson(previousDefinitions) !== canonicalJson({ ...definitions, tasks: Object.fromEntries(Object.entries(definitions.tasks).filter(([name]) => name !== "sa.outbox")) }) &&
        canonicalJson(previousDefinitions) !== canonicalJson({ ...definitions, tasks: Object.fromEntries(Object.entries(definitions.tasks).filter(([name]) => !["sa.job", "sa.outbox"].includes(name))) })
      ) {
        throw new Error(
          "Installed extension/task definitions do not match persisted metadata",
        );
      }
      if (
        options.instanceId !== undefined &&
        options.instanceId !== previous.instanceId
      )
        throw new Error("instanceId differs from persisted metadata");
    }
    await engine.transaction(async (tx) => {
      if (!(await store.get<Source>(tx, "source", "cli")))
        await store.put(tx, "source", "cli", {
          id: "cli",
          kind: "cli",
          enabled: true,
        });
    });
    await options.preflight?.(engine);
    await coordinator.initialize();
    await engine.transaction(async (tx) => {
      await store.put(tx, "metadata", "runtime", {
        schemaVersion: 1,
        instanceId,
        ...definitions,
        maxPending,
        maxIndexEntries,
      } satisfies RuntimeMetadata);
      if (!(await store.get<Source>(tx, "source", "cli")))
        await store.put(tx, "source", "cli", {
          id: "cli",
          kind: "cli",
          enabled: true,
        });
    });
    await reconcile(engine);
    await engine.transaction(tx => engine.lifecycle.completeInTransaction(tx));
    await outbox.reconcile();
    // An explicit model activates execution. A model-free recovery keeps live
    // agent tasks durable and paused; domain receives/commands still commit.
    const recoveredAgentWork =
      !options.model &&
      (await harness.inspect(context)).tasks.some(
        (task) =>
          (task.record.kind.startsWith("sa.") &&
            task.record.kind !== "sa.intake" && task.record.kind !== "sa.outbox") ||
          task.record.kind === "pi.generation",
      );
    const agents = options.model ? coordinator : undefined;
    let activated = false;
    let activation: Promise<void> | undefined;
    const activate = () => activation ??= (async () => {
      if (!recoveredAgentWork) harness!.resume();
      activated = true;
      await agents?.pump();
    })();
    if (!options.startPaused) await activate();
    const openedHarness = harness;
    let closing: Promise<void> | undefined;
    return {
      engine,
      activate,
      outbox,
      harness,
      control,
      agents,
      artifactRoot: artifacts.root,
      updatePolicy: async (policy, actor, meta) => {
        await engine.updatePolicy(policy, actor, meta);
        if (activated) agents?.kick();
      },
      listCandidates: () => engine.listCandidates(),
      decide: async (evalId, epoch, outcome, meta) => {
        const result = await engine.decide(evalId, epoch, outcome, meta);
        if (activated) agents?.kick();
        return result;
      },
      subscribe: (ticketId, sourceId, threadRef, own, meta) =>
        engine.subscribe(ticketId, sourceId, threadRef, own, meta),
      unsubscribe: (ticketId, sourceId, threadRef, meta) =>
        engine.unsubscribe(ticketId, sourceId, threadRef, meta),
      handback: (ticketId, triggerId, epoch, reason, meta) =>
        engine.handback(ticketId, triggerId, epoch, reason, meta),
      stopTicket: async (ticketId, actor, meta) => {
        await engine.stopTicket(ticketId, actor, meta);
        if (activated) agents?.kick();
      },
      retryTicket: async (ticketId, actor, meta) => {
        await engine.retryTicket(ticketId, actor, meta);
        if (activated) agents?.kick();
      },
      endTicket: (ticketId, epoch, meta) =>
        engine.endTicket(ticketId, epoch, meta),
      close: () =>
        (closing ??= (async () => {
          try {
            outbox.close();
            await coordinator.close();
            await openedHarness.close(context);
          } finally {
            await release();
          }
        })()),
      receive: async (event) => {
        const result = await engine.receive(event);
        if (activated) agents?.kick();
        return result;
      },
      receiveBatch: async (events, cursor) => {
        const result = await engine.receiveBatch(events, cursor);
        if (activated) agents?.kick();
        return result;
      },
      readTicket: <T>(id: string) => store.read<T>("ticket", id),
      list: <T>(kind: string) => store.list<T>(kind),
    };
  } catch (error) {
    try {
      if (harness) await harness.close(context);
      else await storage?.close(context);
    } finally {
      await release();
    }
    throw error;
  }
}
