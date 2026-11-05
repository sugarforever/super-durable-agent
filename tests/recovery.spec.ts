import { afterEach, expect, it } from "vitest";
import { mkdtemp, rm, writeFile, readFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { defineTask, defineExtension } from "@earendil-works/pi-durable";
import { openSuperAgent } from "../src/runtime/open.js";
const cleanup: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
async function offlineBackup(directory: string, destination: string) {
  const { promisify } = await import("node:util");
  const { execFile } = await import("node:child_process");
  await promisify(execFile)(process.execPath, [
    "dist/cli.js",
    "--directory",
    directory,
    "backup",
    destination,
  ]);
}
async function root() {
  const path = await mkdtemp(join(tmpdir(), "sa-recovery-"));
  cleanup.push(() => rm(path, { recursive: true, force: true }));
  return path;
}
function checkpointTask(version: number, migrate?: any, onRun = () => {}) {
  return defineTask({
    name: "test.checkpoint",
    version,
    initial: () => ({ phase: "finish", counter: 1 }),
    ...(migrate ? { migrate } : {}),
    phases: {
      finish: async (task: any, runtime: any, context: any) => {
        onRun();
        await runtime.commit(
          () => ({
            status: "terminal",
            outcome: {
              status: "completed",
              result: task.state.checkpoint.counter,
            },
          }),
          context,
        );
      },
    },
    abort: async (_task: any, runtime: any, context: any) => {
      await runtime.commit(
        () => ({ status: "terminal", outcome: { status: "aborted" } }),
        context,
      );
    },
  });
}
it("preflights live native task definitions and permits explicit N to N+1 checkpoint migration exactly once execution", async () => {
  const directory = await root(),
    v1 = checkpointTask(1);
  const extensions = [defineExtension({ name: "test.extension", tasks: [v1] })];
  let r = await openSuperAgent({ directory, startPaused: true, extensions });
  const id = await r.engine.transaction((tx) =>
    tx.createTask(
      v1,
      {},
      { ownership: { kind: "conversation" }, background: true },
    ),
  );
  await r.close();
  await expect(openSuperAgent({ directory })).rejects.toThrow(
    /definition|task/i,
  );
  const invalid = checkpointTask(2);
  await expect(
    openSuperAgent({
      directory,
      extensions: [
        defineExtension({ name: "test.extension", tasks: [invalid] }),
      ],
      allowTaskMigrations: true,
    }),
  ).rejects.toThrow(/migrat|definition/i);
  let executed = 0;
  const v2 = checkpointTask(
    2,
    (_input: any, checkpoint: any, from: number) => {
      expect(from).toBe(1);
      return {
        input: {},
        checkpoint: { ...checkpoint, counter: checkpoint.counter + 1 },
      };
    },
    () => executed++,
  );
  r = await openSuperAgent({
    directory,
    extensions: [defineExtension({ name: "test.extension", tasks: [v2] })],
    allowTaskMigrations: true,
  });
  cleanup.push(() => r.close());
  const terminal = await r.harness.waitForTask(id, r.engine.context);
  expect((await r.harness.getTask(id, r.engine.context))?.state).toMatchObject({
    status: "terminal",
    outcome: { result: 2 },
  });
  expect(executed).toBe(1);
  await r.close();
  r = await openSuperAgent({
    directory,
    extensions: [defineExtension({ name: "test.extension", tasks: [v2] })],
  });
  expect(executed).toBe(1);
});
it("failed migration prevents every effect before another task can start", async () => {
  const directory = await root(),
    old = checkpointTask(1);
  let effects = 0;
  let r = await openSuperAgent({
    directory,
    startPaused: true,
    extensions: [defineExtension({ name: "test.extension", tasks: [old] })],
  });
  await r.engine.transaction((tx) =>
    tx.createTask(
      old,
      {},
      { ownership: { kind: "conversation" }, background: true },
    ),
  );
  await r.close();
  const broken = checkpointTask(
    2,
    () => {
      throw Error("incompatible checkpoint");
    },
    () => effects++,
  );
  await expect(
    openSuperAgent({
      directory,
      extensions: [
        defineExtension({ name: "test.extension", tasks: [broken] }),
      ],
      allowTaskMigrations: true,
    }),
  ).rejects.toThrow(/checkpoint/);
  expect(effects).toBe(0);
});
it("offline backup refuses a held writer, restores receipts and artifacts to a new directory, and detects tampering", async () => {
  const { backupRuntime, restoreRuntime } =
    await import("../src/runtime/backup.js");
  const home = await root(),
    directory = join(home, "primary"),
    backup = join(home, "backup"),
    restored = join(home, "restored");
  let r = await openSuperAgent({ directory });
  const body = "large content".repeat(2000);
  const result = await r.receive({
    sourceId: "cli",
    externalId: "backup-event",
    revision: "1",
    actor: { id: "alice", kind: "human", trusted: true },
    threads: ["thread"],
    body,
    receivedAt: 1,
    occurredAt: 1,
  });
  await r.engine.command("receipt", {}, async () => ({ counter: 1 }));
  await expect(backupRuntime(directory, backup)).rejects.toThrow(/lock/i);
  await r.close();
  await offlineBackup(directory, backup);
  await restoreRuntime(backup, restored);
  r = await openSuperAgent({ directory: restored });
  cleanup.push(() => r.close());
  expect(
    await r.engine.command("receipt", {}, async () => {
      throw Error("duplicate effect");
    }),
  ).toEqual({ counter: 1 });
  expect((await r.list<any>("trigger"))[0].id).toBe(result.triggerId);
  const artifact = (await r.list<any>("trigger"))[0].bodyArtifact;
  expect(await readFile(join(restored, "artifacts", artifact), "utf8")).toBe(
    body,
  );
  await expect(restoreRuntime(backup, restored)).rejects.toThrow(/exist|new/i);
  await writeFile(join(backup, "session.sqlite"), "tampered");
  await expect(restoreRuntime(backup, join(home, "bad"))).rejects.toThrow(
    /integrity|checksum/i,
  );
});
it("restored local receipts use the original broker and never replay a completed counter; missing ledger blocks startup", async () => {
  const { backupRuntime, restoreRuntime } =
    await import("../src/runtime/backup.js");
  const { LocalEnvironmentBroker } =
    await import("../src/environments/local.js");
  const { operationDigest } = await import("../src/environments/types.js");
  const { promisify } = await import("node:util");
  const { execFile } = await import("node:child_process");
  const git = promisify(execFile);
  const home = await root(),
    repository = join(home, "repo"),
    directory = join(home, "primary"),
    brokerDirectory = join(home, "broker");
  await mkdir(repository);
  await git("git", ["init", "-b", "main", repository]);
  await git("git", ["-C", repository, "config", "user.name", "Recovery"]);
  await git("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "recovery@example.invalid",
  ]);
  await writeFile(join(repository, "file"), "base");
  await git("git", ["-C", repository, "add", "."]);
  await git("git", ["-C", repository, "commit", "-m", "base"]);
  const identity = {
      ticketId: "t",
      assignmentId: "a",
      epoch: 1,
      mandateId: "m",
    },
    authorize = async () => ({
      ...identity,
      capabilities: ["repo.read", "repo.write", "command.execute"],
    });
  const broker = new LocalEnvironmentBroker({
    directory: brokerDirectory,
    repository,
    authorize,
    trustedLocalShell: true,
  });
  const pp = { kind: "prepare" as const },
    prepared = await broker.prepare({
      operationId: "prepare",
      identity,
      payload: pp,
      payloadDigest: operationDigest(identity, pp),
    });
  const payload = {
      kind: "shell" as const,
      environmentId: prepared.environmentId,
      command: "printf x >> count",
    },
    request = {
      operationId: "count",
      identity,
      payload,
      payloadDigest: operationDigest(identity, payload),
    };
  await broker.execute(request);
  const environment = {
    directory: brokerDirectory,
    repository,
    trustedLocalShell: true,
  };
  let r = await openSuperAgent({ directory, environment });
  await r.close();
  const backup = join(home, "backup");
  await offlineBackup(directory, backup);
  const restored = join(home, "restored");
  await restoreRuntime(backup, restored);
  await expect(openSuperAgent({ directory: restored })).rejects.toThrow(
    /original/i,
  );
  r = await openSuperAgent({ directory: restored, environment });
  await r.close();
  expect(await broker.execute(request)).toMatchObject({ status: "completed" });
  expect(await readFile(join(prepared.cwd, "count"), "utf8")).toBe("x");
  const broken = join(home, "broken");
  await restoreRuntime(backup, broken);
  await rm(join(brokerDirectory, "operations"), { recursive: true });
  await expect(
    openSuperAgent({ directory: broken, environment }),
  ).rejects.toThrow(/Missing original endpoint dependency/i);
  expect(await readFile(join(prepared.cwd, "count"), "utf8")).toBe("x");
}, 15000);
it("migrates an actually executed native checkpoint and rejects incompatible Documents before effects", async () => {
  const { defineDoc } = await import("@earendil-works/pi-durable");
  const directory = await root();
  let checkpointed!: () => void;
  const reached = new Promise<void>((r) => (checkpointed = r));
  const old = defineTask({
    name: "test.executed",
    version: 1,
    initial: () => ({ phase: "prepare", value: 0 }),
    phases: {
      prepare: async (_task: any, run: any, ctx: any) => {
        await run.commit(
          () => ({
            status: "running",
            checkpoint: { phase: "finish", value: 41 },
          }),
          ctx,
        );
        checkpointed();
        await run.sleep(Date.now() + 60000, ctx);
      },
      finish: async () => {
        throw Error("old finish must not run");
      },
    },
    abort: async () => {},
  });
  let r = await openSuperAgent({
    directory,
    startPaused: true,
    extensions: [defineExtension({ name: "test.executed", tasks: [old] })],
  });
  const id = await r.engine.transaction((tx) =>
    tx.createTask(
      old,
      {},
      { ownership: { kind: "conversation" }, background: true },
    ),
  );
  await r.activate();
  await reached;
  await r.close();
  let count = 0;
  const updated = defineTask({
    ...old.definition,
    version: 2,
    migrate: (input: any, checkpoint: any) => ({
      input,
      checkpoint: { phase: "finish", value: checkpoint.value + 1 },
    }),
    phases: {
      prepare: old.definition.phases.prepare,
      finish: async (task: any, run: any, ctx: any) => {
        count++;
        await run.commit(
          () => ({
            status: "terminal",
            outcome: {
              status: "completed",
              result: task.state.checkpoint.value,
            },
          }),
          ctx,
        );
      },
    },
  });
  r = await openSuperAgent({
    directory,
    extensions: [defineExtension({ name: "test.executed", tasks: [updated] })],
    allowTaskMigrations: true,
  });
  await r.harness.waitForTask(id, r.engine.context);
  expect((await r.harness.getTask(id, r.engine.context))?.state).toMatchObject({
    outcome: { result: 42 },
  });
  expect(count).toBe(1);
  await r.engine.transaction(async (tx) => {
    await tx.doc(
      defineDoc({
        kind: "sa.future",
        version: 2,
        scope: "session",
        initial: () => ({ value: 1 }),
      }),
    );
  });
  await r.close();
  await expect(
    openSuperAgent({
      directory,
      extensions: [
        defineExtension({ name: "test.executed", tasks: [updated] }),
      ],
    }),
  ).rejects.toThrow(/Document.*version/i);
  expect(count).toBe(1);
});
it("refuses missing artifacts referenced by a conversation Document before completing a backup", async () => {
  const { InboxDoc } = await import("@earendil-works/pi-durable");
  const home = await root(), directory = join(home, "primary");
  const r = await openSuperAgent({ directory, startPaused: true });
  // Native queued passive-write context can contain arbitrary host entry data.
  await r.engine.transaction(async tx => {
    const inbox = await tx.doc(InboxDoc, r.control.id);
    inbox.items.push({ id: 1 as any, mode: "write", entry: { kind: "fixture.artifact", data: { artifact: "0".repeat(64) } } });
  });
  await r.close();
  await expect(offlineBackup(directory, join(home, "backup"))).rejects.toThrow(
    /Referenced artifact is missing/i,
  );
  await expect(
    readFile(join(home, "backup", "manifest.json")),
  ).rejects.toThrow();
});
it("marks custom endpoint restoration inspection-only with no activation override", async () => {
  const { restoreRuntime, verifyBackup } =
    await import("../src/runtime/backup.js");
  const home = await root(),
    directory = join(home, "primary");
  const r = await openSuperAgent({ directory });
  await r.engine.transaction((tx) =>
    r.engine.store.put(tx, "metadata", "environment", { kind: "custom" }),
  );
  await r.close();
  const backup = join(home, "backup");
  await offlineBackup(directory, backup);
  expect((await verifyBackup(backup)).inspectionOnly).toBe(true);
  const restored = join(home, "restored");
  await restoreRuntime(backup, restored);
  await expect(openSuperAgent({ directory: restored })).rejects.toThrow(
    /inspection-only/,
  );
});
it("refuses restored activation after the original broker ledger gains a post-backup operation", async () => {
  const { restoreRuntime } = await import("../src/runtime/backup.js");
  const { LocalEnvironmentBroker } =
    await import("../src/environments/local.js");
  const { operationDigest } = await import("../src/environments/types.js");
  const { promisify } = await import("node:util");
  const { execFile } = await import("node:child_process");
  const git = promisify(execFile);
  const home = await root(),
    repository = join(home, "repo"),
    directory = join(home, "primary"),
    endpoint = join(home, "endpoint");
  await mkdir(repository);
  await git("git", ["init", "-b", "main", repository]);
  await git("git", ["-C", repository, "config", "user.name", "Test"]);
  await git("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "test@example.invalid",
  ]);
  await writeFile(join(repository, "file"), "base");
  await git("git", ["-C", repository, "add", "."]);
  await git("git", ["-C", repository, "commit", "-m", "base"]);
  const identity = {
      ticketId: "t",
      assignmentId: "a",
      epoch: 1,
      mandateId: "m",
    },
    broker = new LocalEnvironmentBroker({
      directory: endpoint,
      repository,
      authorize: async () => ({
        ...identity,
        capabilities: ["repo.read", "repo.write"],
      }),
    });
  const p = { kind: "prepare" as const },
    prepared = await broker.prepare({
      operationId: "prepare",
      identity,
      payload: p,
      payloadDigest: operationDigest(identity, p),
    });
  const environment = { directory: endpoint, repository };
  const r = await openSuperAgent({ directory, environment });
  await r.close();
  const archive = join(home, "backup");
  await offlineBackup(directory, archive);
  const payload = {
    kind: "write" as const,
    environmentId: prepared.environmentId,
    path: "later",
    text: "post-backup",
  };
  await broker.execute({
    operationId: "later",
    identity,
    payload,
    payloadDigest: operationDigest(identity, payload),
  });
  const restored = join(home, "restored");
  await restoreRuntime(archive, restored);
  await expect(
    openSuperAgent({ directory: restored, environment }),
  ).rejects.toThrow(/changed|reconcil/i);
  expect(await readFile(join(prepared.cwd, "later"), "utf8")).toBe(
    "post-backup",
  );
});

it.each(["session", "conversation", "task"] as const)(
  "rejects unknown sa version-one Documents in %s scope before a runnable Task effect",
  async (scope) => {
    const { defineDoc } = await import("@earendil-works/pi-durable");
    const directory = await root();
    let effects = 0;
    const task = checkpointTask(1, undefined, () => effects++);
    const extensions = [defineExtension({ name: "test.extension", tasks: [task] })];
    const r = await openSuperAgent({ directory, startPaused: true, extensions });
    await r.engine.transaction(async (tx) => {
      const id = await tx.createTask(task, {}, { ownership: { kind: "conversation" }, background: true });
      const doc = defineDoc({ kind: "sa.unknownFutureDefinition", version: 1,
        ...(scope === "conversation" ? { scope, history: "latest", fork: "initial" } as const : { scope }),
        initial: () => ({ value: 1 }) } as any);
      await (tx.doc as any)(doc, scope === "task" ? id : scope === "conversation" ? r.control.id : undefined);
    });
    await r.close();
    await expect(openSuperAgent({ directory, extensions }).then(async (opened) => { await opened.close(); }))
      .rejects.toThrow(/Unsupported Document definition\/version: sa.unknownFutureDefinition/);
    expect(effects).toBe(0);
  },
);

async function localRecoveryFixture() {
  const { promisify } = await import("node:util");
  const { execFile } = await import("node:child_process");
  const { LocalEnvironmentBroker } = await import("../src/environments/local.js");
  const { operationDigest } = await import("../src/environments/types.js");
  const git = promisify(execFile), home = await root(), repository = join(home, "repo"), directory = join(home, "primary"), endpoint = join(home, "endpoint");
  await mkdir(repository);
  await git("git", ["init", "-b", "main", repository]);
  await git("git", ["-C", repository, "config", "user.name", "Recovery"]);
  await git("git", ["-C", repository, "config", "user.email", "recovery@example.invalid"]);
  await writeFile(join(repository, "file"), "base");
  await git("git", ["-C", repository, "add", "."]);
  await git("git", ["-C", repository, "commit", "-m", "base"]);
  const environment = { directory: endpoint, repository };
  const broker = new LocalEnvironmentBroker({ ...environment, authorize: async identity => ({ ...identity, capabilities: ["repo.read", "repo.write"] }) });
  async function prepare(epoch: number) {
    const identity = { ticketId: "t", assignmentId: `a${epoch}`, epoch, mandateId: "m" }, payload = { kind: "prepare" as const };
    return broker.prepare({ operationId: `prepare:${epoch}`, identity, payload, payloadDigest: operationDigest(identity, payload) });
  }
  return { home, directory, endpoint, environment, broker, prepare };
}

it.each([1, 3])("backs up and restores retained release history after %i workspace leases", async (epochs) => {
  const { restoreRuntime, verifyBackup } = await import("../src/runtime/backup.js");
  const f = await localRecoveryFixture();
  const leases = [];
  for (let epoch = 1; epoch <= epochs; epoch++) leases.push(await f.prepare(epoch));
  const latest = leases.at(-1)!;
  expect(new Set(leases.map(p => p.cwd)).size).toBe(1);
  expect(await f.broker.release(latest.environmentId, "release")).toMatchObject({ status: "released" });
  const r = await openSuperAgent({ directory: f.directory, environment: f.environment });
  await r.close();
  const archive = join(f.home, "archive");
  await offlineBackup(f.directory, archive);
  const manifest = await verifyBackup(archive);
  expect(manifest.workspaces).toEqual([]);
  expect(manifest.external.filter(x => x.path.startsWith("dependencies/environments/"))).toHaveLength(epochs);
  expect(manifest.external.filter(x => x.path.startsWith("dependencies/operations/"))).toHaveLength(epochs + 1);
  const restored = join(f.home, "restored");
  await restoreRuntime(archive, restored);
  const reopened = await openSuperAgent({ directory: restored, environment: f.environment });
  await reopened.close();
  expect(await f.broker.release(latest.environmentId, "release")).toMatchObject({ status: "released" });
  // A confirmed release of a different physical workspace cannot excuse this loss.
  const retained = await f.prepare(epochs + 1);
  await rm(retained.cwd, { recursive: true });
  await expect(offlineBackup(f.directory, join(f.home, "missing-workspace"))).rejects.toThrow(/ENOENT|workspace/i);
}, 15000);

it("backs up a safely unused configured local endpoint and activates the restored primary without recreating missing history", async () => {
  const { restoreRuntime, verifyBackup } = await import("../src/runtime/backup.js");
  const f = await localRecoveryFixture();
  const r = await openSuperAgent({ directory: f.directory, environment: f.environment });
  await r.close();
  const archive = join(f.home, "archive");
  await offlineBackup(f.directory, archive);
  expect((await verifyBackup(archive)).external).toEqual([]);
  const restored = join(f.home, "restored");
  await restoreRuntime(archive, restored);
  const reopened = await openSuperAgent({ directory: restored, environment: f.environment });
  await reopened.close();
  await f.prepare(1);
  await rm(f.endpoint, { recursive: true });
  await expect(offlineBackup(f.directory, join(f.home, "missing-ledger"))).rejects.toThrow(/ENOENT|endpoint|dependency/i);
  await expect(openSuperAgent({ directory: f.directory, environment: f.environment }).then(async opened => { await opened.close(); })).rejects.toThrow(/ENOENT|endpoint|dependency/i);
}, 15000);


it("refuses a missing frozen native delivery promptArtifact before completing backup", async () => {
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider } = await import("@earendil-works/pi-ai/providers/faux");
  const { alice, event } = await import("./helpers/scheduling.js");
  const home = await root(), directory = join(home, "runtime");
  let r = await openSuperAgent({ directory });
  await r.updatePolicy({ version: 1, acceptMode: "auto", allowedActors: ["alice"], botActors: [], rules: [] }, alice, { commandId: "policy" });
  await r.receive({ ...event("large"), body: "x".repeat(16000) });
  const candidate = (await r.listCandidates())[0];
  await r.decide(candidate.id, candidate.epoch, { kind: "accept", goal: "large", ruleIds: [] }, { commandId: "accept" });
  await r.close();
  const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
  r = await openSuperAgent({ directory, startPaused: true, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id } });
  await r.agents!.pump();
  const artifact = (await r.list<any>("deliveryRun"))[0].promptArtifact;
  expect(artifact).toMatch(/^[a-f0-9]{64}$/);
  await r.close();
  await rm(join(directory, "artifacts", artifact));
  const archive = join(home, "archive");
  await expect(offlineBackup(directory, archive)).rejects.toThrow(/Referenced artifact is missing/);
  await expect(readFile(join(archive, "manifest.json"))).rejects.toThrow();
});
