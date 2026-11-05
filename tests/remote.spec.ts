import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { RemoteEnvironmentBroker } from "../src/environments/remote.js";
import {
  RemoteDaemon,
  startDaemonServer,
  runDaemonPolling,
} from "../src/daemon/server.js";
import {
  operationDigest,
  type EndpointIdentity,
} from "../src/environments/types.js";
const git = promisify(execFile);
const cleanup: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
const identity: EndpointIdentity = {
  ticketId: "t",
  assignmentId: "a",
  epoch: 1,
  mandateId: "m",
};
const capabilities = ["repo.read", "repo.write", "command.execute"];
async function fixture(mode: "http" | "poll" = "http", timeoutMs = 3000) {
  const root = await mkdtemp(join(tmpdir(), "remote-test-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const repository = join(root, "repo");
  await mkdir(repository);
  await git("git", ["init", "-b", "main", repository]);
  await git("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "test@example.invalid",
  ]);
  await git("git", ["-C", repository, "config", "user.name", "Tests"]);
  await writeFile(join(repository, "file"), "base");
  await git("git", ["-C", repository, "add", "."]);
  await git("git", ["-C", repository, "commit", "-m", "base"]);
  let allowed = true;
  const authorize = async (i: EndpointIdentity) => {
    if (!allowed || i.epoch !== identity.epoch)
      throw new Error("Stale endpoint authority");
    return { ...i, capabilities };
  };
  const daemonOptions = {
    endpointId: "device",
    token: "daemon-token",
    directory: join(root, "daemon"),
    repository,
    allowedRepositoryRoots: [repository],
    capabilities,
    trustedLocalShell: true,
    authorize,
  };
  const daemon = new RemoteDaemon(daemonOptions);
  let server: Awaited<ReturnType<typeof startDaemonServer>> | undefined;
  let polling: Awaited<ReturnType<typeof runDaemonPolling>> | undefined;
  let resumePolling: (() => Promise<void>) | undefined;
  const broker = new RemoteEnvironmentBroker({
    endpointId: "device",
    token: "daemon-token",
    directory: join(root, "controller"),
    transport: mode,
    url: "http://127.0.0.1:1",
    allowInsecureLoopback: true,
    timeoutMs: Math.max(3000, timeoutMs),
    authorize,
  });
  if (mode === "http") {
    server = await startDaemonServer(daemon, {
      host: "127.0.0.1",
      port: 0,
      allowInsecureLoopback: true,
    });
    broker.options.url = server.address;
    cleanup.push(() => server!.close());
  } else {
    const control = createServer((req, res) => {
      void broker.handleControl(req, res).then((handled) => {
        if (!handled) {
          res.statusCode = 404;
          res.end();
        }
      });
    });
    await new Promise<void>((resolve) =>
      control.listen(0, "127.0.0.1", resolve),
    );
    const address = control.address() as { port: number };
    cleanup.push(
      () =>
        new Promise<void>((resolve) => {
          control.close(() => resolve());
          control.closeAllConnections();
        }),
    );
    resumePolling = async () => {
      polling = await runDaemonPolling(daemon, {
        url: `http://127.0.0.1:${address.port}`,
        token: "daemon-token",
        allowInsecureLoopback: true,
        intervalMs: 10,
      });
    };
    await resumePolling();
    cleanup.push(() => polling!.close());
  }
  const prepare = {
    operationId: "prepare",
    identity,
    payload: { kind: "prepare" as const },
    payloadDigest: operationDigest(identity, { kind: "prepare" }),
  };
  const prepared = await broker.prepare(prepare);
  broker.options.timeoutMs = timeoutMs;
  const request = (operationId: string, action: any) => {
    const payload = { environmentId: prepared.environmentId, ...action };
    return {
      operationId,
      identity,
      payload,
      payloadDigest: operationDigest(identity, payload),
    };
  };
  return {
    pausePolling: () => polling!.close(),
    resumePolling: () => resumePolling!(),
    root,
    daemon,
    daemonOptions,
    broker,
    server,
    prepared,
    request,
    revoke: () => {
      allowed = false;
    },
  };
}
describe("remote endpoint protocol", () => {
  for (const mode of ["http", "poll"] as const) {
    it(`${mode}: concurrent duplicate shell executes once and conflicts are rejected`, async () => {
      const f = await fixture(mode);
      const request = f.request("increment", {
        kind: "shell",
        command: "printf x >> count; sleep 0.05; cat count",
      });
      const receipts = await Promise.all([
        f.broker.execute(request),
        f.broker.execute(request),
      ]);
      expect(receipts.every((r) => r.status === "completed")).toBe(true);
      expect(await readFile(join(f.prepared.cwd, "count"), "utf8")).toBe("x");
      await expect(
        f.broker.execute(
          f.request("increment", { kind: "shell", command: "echo conflict" }),
        ),
      ).rejects.toThrow(/conflict/i);
      expect(await f.broker.query("increment")).toMatchObject({
        status: "completed",
      });
    });
  }
  it("requires explicit loopback exception and TLS for remote hosts", async () => {
    const options = {
      endpointId: "e",
      token: "t",
      directory: "/tmp/unopened",
      transport: "http" as const,
      authorize: async () => ({ ...identity, capabilities }),
    };
    expect(
      () =>
        new RemoteEnvironmentBroker({
          ...options,
          url: "http://example.com",
          allowInsecureLoopback: true,
        }),
    ).toThrow(/TLS|HTTPS/i);
    expect(
      () =>
        new RemoteEnvironmentBroker({ ...options, url: "http://127.0.0.1" }),
    ).toThrow(/TLS|HTTPS/i);
  });
  it("authenticates query and cancellation and bounds request bodies", async () => {
    const f = await fixture();
    for (const action of ["query", "cancel"]) {
      const response = await fetch(
        `${f.server!.address}/v1/endpoints/device/${action}`,
        {
          method: "POST",
          body: JSON.stringify({ operationId: "prepare", epoch: 1 }),
        },
      );
      expect(response.status).toBe(401);
    }
    const response = await fetch(
      `${f.server!.address}/v1/endpoints/device/execute`,
      {
        method: "POST",
        headers: { authorization: "Bearer daemon-token" },
        body: "x".repeat(1_048_577),
      },
    );
    expect(response.status).toBe(413);
  });
  it("rejects stale grants, forged capabilities and revoked endpoints", async () => {
    const f = await fixture();
    const request = f.request("stale", {
      kind: "write",
      path: "file",
      text: "bad",
    });
    request.identity = { ...identity, epoch: 2 };
    request.payloadDigest = operationDigest(request.identity, request.payload);
    await expect(f.broker.execute(request)).rejects.toThrow(/Stale/);
    f.revoke();
    const response = await fetch(
      `${f.server!.address}/v1/endpoints/device/execute`,
      {
        method: "POST",
        headers: { authorization: "Bearer daemon-token" },
        body: JSON.stringify({
          ...f.request("forged", { kind: "write", path: "file", text: "bad" }),
          capabilities,
          trusted: true,
        }),
      },
    );
    expect(response.status).toBe(403);
    expect(await readFile(join(f.prepared.cwd, "file"), "utf8")).toBe("base");
  });
  it("retains uncertain operation across controller restart and queries without repeating", async () => {
    const f = await fixture("http", 100);
    const request = f.request("slow", {
      kind: "shell",
      command: "printf x >> count; sleep 0.3",
    });
    expect(await f.broker.execute(request)).toMatchObject({
      status: "unknown",
    });
    await delay(350);
    const restarted = new RemoteEnvironmentBroker(f.broker.options);
    expect(await restarted.query("slow")).toMatchObject({
      status: "completed",
    });
    expect(await restarted.execute(request)).toMatchObject({
      status: "completed",
    });
    expect(await readFile(join(f.prepared.cwd, "count"), "utf8")).toBe("x");
    await f.server!.close();
    expect(await restarted.query("slow")).toMatchObject({
      status: "completed",
    });
    expect(restarted.diagnostics()).toMatchObject({
      endpointId: "device",
      state: "offline",
    });
  });
  it("fences future same-epoch operations after cancellation and reconciles old completion", async () => {
    const f = await fixture();
    f.daemonOptions.authorize = async (i) => ({
      ...i,
      capabilities,
      revokeEpoch: true,
    });
    const request = f.request("cancelled", {
      kind: "shell",
      command: "printf started > cancel-started; sleep 2",
    });
    const result = f.broker.execute(request);
    for (let i = 0; i < 1000; i++) {
      if (
        await readFile(join(f.prepared.cwd, "cancel-started"), "utf8").then(
          (value) => value === "started",
          () => false,
        )
      )
        break;
      await delay(5);
    }
    expect(await readFile(join(f.prepared.cwd, "cancel-started"), "utf8")).toBe(
      "started",
    );
    expect(await f.broker.cancel("cancelled", 1)).toMatchObject({
      status: "cancelled",
    });
    await result;
    await expect(
      f.broker.execute(
        f.request("after-cancel", {
          kind: "write",
          path: "file",
          text: "unsafe",
        }),
      ),
    ).rejects.toThrow(/fenced|revoked/i);
    expect(await f.broker.query("cancelled")).toMatchObject({
      status: "cancelled",
    });
  });
});

it("rejects ledger replacement and never treats a fresh daemon as proof of absence", async () => {
  const f = await fixture();
  const request = f.request("completed-before-disk-loss", {
    kind: "shell",
    command: "printf x >> count",
  });
  await f.broker.execute(request);
  await f.server!.close();
  const replacement = new RemoteDaemon({
    ...f.daemonOptions,
    directory: join(f.root, "replacement-ledger"),
  });
  const server = await startDaemonServer(replacement, {
    port: 0,
    allowInsecureLoopback: true,
  });
  cleanup.push(() => server.close());
  f.broker.options.url = server.address;
  expect(await f.broker.query("completed-before-disk-loss")).toMatchObject({
    status: "unknown",
  });
  await expect(
    f.broker.execute(
      f.request("new-after-loss", { kind: "write", path: "file", text: "bad" }),
    ),
  ).rejects.toThrow(/ledger|continuity/i);
  expect(f.broker.diagnostics()).toMatchObject({ state: "unknown" });
});

it("opens remote service paused until authority/listener are ready, then activates once", async () => {
  const { openSuperAgent } = await import("../src/runtime/open.js");
  const root = await mkdtemp(join(tmpdir(), "remote-activation-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const runtime = await openSuperAgent({ directory: root, startPaused: true });
  cleanup.push(() => runtime.close());
  await runtime.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: ["operator"],
      botActors: [],
      rules: [],
    },
    { id: "operator", kind: "human", trusted: true },
    { commandId: "policy" },
  );
  await runtime.receive({
    addressed: true,
    sourceId: "cli",
    externalId: "one",
    revision: "one",
    actor: { id: "operator", kind: "human", trusted: true },
    threads: ["one"],
    body: "test",
    receivedAt: Date.now(),
    occurredAt: Date.now(),
  });
  await delay(20);
  expect((await runtime.list<any>("intake"))[0].status).toBe("pending");
  await runtime.activate();
  await runtime.activate();
  for (
    let i = 0;
    i < 100 && (await runtime.list<any>("intake"))[0].status !== "accepted";
    i++
  )
    await delay(5);
  expect((await runtime.list<any>("intake"))[0].status).toBe("accepted");
});

it("rechecks the epoch fence after a delayed controller grant resolves", async () => {
  const f = await fixture();
  await f.broker.execute(
    f.request("prior", { kind: "write", path: "file", text: "prior" }),
  );
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>((resolve) => (enter = resolve)),
    gate = new Promise<void>((resolve) => (release = resolve));
  let executeGrants = 0;
  f.daemonOptions.authorize = async (i, action) => {
    if (action === "execute" && ++executeGrants === 2) {
      enter();
      await gate;
    }
    return {
      ...i,
      capabilities,
      ...(action === "cancel" ? { revokeEpoch: true as const } : {}),
    };
  };
  const pending = f.broker.execute(
    f.request("grant-race", { kind: "write", path: "file", text: "unsafe" }),
  );
  const assertion = expect(pending).resolves.toMatchObject({
    status: "failed",
    reason: expect.stringMatching(/fenced|revoked/i),
  });
  await entered;
  await f.broker.cancel("prior", 1);
  release();
  await assertion;
  expect(await readFile(join(f.prepared.cwd, "file"), "utf8")).toBe("prior");
});

it("intersects daemon capability and repository bounds with current controller grants", async () => {
  const f = await fixture();
  f.daemonOptions.capabilities = ["repo.read"];
  await expect(
    f.broker.execute(
      f.request("denied-write", {
        kind: "write",
        path: "file",
        text: "unsafe",
      }),
    ),
  ).rejects.toThrow(/capability/i);
  const denied = new RemoteDaemon({
    ...f.daemonOptions,
    directory: join(f.root, "denied-ledger"),
    allowedRepositoryRoots: [join(f.root, "daemon")],
  });
  await expect(denied.instanceId()).rejects.toThrow(/root/i);
});

it("quarantines a native Job when remote preparation delivery is uncertain", async () => {
  const { openSuperAgent } = await import("../src/runtime/open.js");
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } =
    await import("@earendil-works/pi-ai/providers/faux");
  const { accept, alice, isRouter, until } =
    await import("./helpers/scheduling.js");
  const root = await mkdtemp(join(tmpdir(), "remote-prepare-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  let r = await openSuperAgent({ directory: join(root, "runtime") });
  await r.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: ["alice"],
      botActors: [],
      explicitCapabilities: ["repo.read", "command.execute"],
      rules: [],
    },
    alice,
    { commandId: "policy" },
  );
  const ticket = await accept(r, "work");
  await r.close();
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  let first = true;
  faux.setResponses(
    Array.from({ length: 30 }, () => (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage("No decision");
      if (first) {
        first = false;
        return fauxAssistantMessage(
          fauxToolCall("start_job", {
            assignmentId: ticket.assignmentId,
            epoch: 1,
            command: "echo must-not-run",
          }),
          { stopReason: "toolUse" },
        );
      }
      return fauxAssistantMessage("Inspect uncertain endpoint");
    }),
  );
  const broker = new RemoteEnvironmentBroker({
    endpointId: "device",
    token: "token",
    transport: "poll",
    directory: join(root, "transport"),
    timeoutMs: 20,
    authorize: async () => {
      throw new Error("not bound");
    },
  });
  r = await openSuperAgent({
    directory: join(root, "runtime"),
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
    environment: { broker },
  });
  cleanup.push(() => r.close());
  await until(async () =>
    ["failed", "unknown"].includes((await r.list<any>("job"))[0]?.status),
  );
  expect((await r.list<any>("job"))[0]).toMatchObject({
    status: "unknown",
    receipt: { status: "unknown" },
  });
  await until(
    async () => (await r.list<any>("reservation"))[0]?.status === "quarantined",
  );
  expect((await r.list<any>("reservation"))[0]).toMatchObject({
    status: "quarantined",
  });
  expect(broker.diagnostics()).toMatchObject({
    state: "unknown",
    queued: 1,
    active: 0,
  });
});

it("checks ledger continuity before returning cached execution receipts", async () => {
  const f = await fixture();
  const request = f.request("cached", {
    kind: "write",
    path: "file",
    text: "safe",
  });
  await f.broker.execute(request);
  await f.server!.close();
  const replacement = new RemoteDaemon({
    ...f.daemonOptions,
    directory: join(f.root, "fresh"),
  });
  const server = await startDaemonServer(replacement, {
    port: 0,
    allowInsecureLoopback: true,
  });
  cleanup.push(() => server.close());
  f.broker.options.url = server.address;
  await expect(f.broker.execute(request)).rejects.toThrow(/continuity/i);
});

it("detects controller transport receipt corruption on restart", async () => {
  const { digest } = await import("../src/domain/model.js");
  const f = await fixture();
  await f.broker.execute(
    f.request("integrity", { kind: "write", path: "file", text: "safe" }),
  );
  const path = join(
    f.root,
    "controller",
    "device",
    `${digest("integrity")}.json`,
  );
  const stored = JSON.parse(await readFile(path, "utf8"));
  stored.value.receipt.status = "unknown";
  await writeFile(path, JSON.stringify(stored));
  const restarted = new RemoteEnvironmentBroker(f.broker.options);
  await expect(restarted.query("integrity")).rejects.toThrow(
    /integrity|conflict/i,
  );
});

it("bounds streamed endpoint responses and times out while the body is still open", async () => {
  const { requestJson } = await import("../src/daemon/protocol.js");
  const server = createServer((req, res) => {
    req.resume();
    res.setHeader("content-type", "application/json");
    if (req.url === "/oversize") res.write("x".repeat(1_048_577));
    else res.write('{"open":');
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      }),
  );
  const url = `http://127.0.0.1:${(server.address() as any).port}`;
  await expect(requestJson(url, "/oversize", "token", {}, 500)).rejects.toThrow(
    /limit/i,
  );
  await expect(
    requestJson(url, "/stalled-body", "token", {}, 30),
  ).rejects.toThrow(/abort/i);
});

it("splits outbound poll batches before the response byte limit without losing queued operations", async () => {
  const f = await fixture("poll");
  const text = "x".repeat(600000);
  const results = await Promise.all([
    f.broker.execute(f.request("large-a", { kind: "write", path: "a", text })),
    f.broker.execute(f.request("large-b", { kind: "write", path: "b", text })),
  ]);
  expect(results.map((r) => r.status)).toEqual(["completed", "completed"]);
  expect((await readFile(join(f.prepared.cwd, "a"))).length).toBe(600000);
  expect((await readFile(join(f.prepared.cwd, "b"))).length).toBe(600000);
});

it("fails closed when previously delivered controller state loses its pinned daemon identity", async () => {
  const { digest } = await import("../src/domain/model.js");
  const f = await fixture();
  await f.broker.execute(
    f.request("retained-receipt", {
      kind: "write",
      path: "file",
      text: "safe",
    }),
  );
  await rm(
    join(
      f.root,
      "controller",
      "device",
      "identity",
      `${digest("daemon-instance")}.json`,
    ),
  );
  const restarted = new RemoteEnvironmentBroker(f.broker.options);
  await expect(restarted.query("retained-receipt")).rejects.toThrow(
    /identity|continuity/i,
  );
});

async function nativeRemote(mode: "http" | "poll", lateReceipt = false) {
  const { openSuperAgent } = await import("../src/runtime/open.js");
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } =
    await import("@earendil-works/pi-ai/providers/faux");
  const { accept, alice, isRouter, until } =
    await import("./helpers/scheduling.js");
  const f = await fixture(mode);
  const directory = join(f.root, "native-runtime");
  let r = await openSuperAgent({ directory });
  await r.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: ["alice"],
      botActors: [],
      explicitCapabilities: ["repo.read", "repo.write", "command.execute"],
      rules: [],
    },
    alice,
    { commandId: "policy" },
  );
  const accepted = await accept(r, "native-work");
  await r.close();
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  let first = true,
    release!: () => void;
  const holdGeneration = new Promise<void>((resolve) => (release = resolve));
  faux.setResponses(
    Array.from({ length: 50 }, () => async (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage("No decision");
      if (first) {
        first = false;
        return fauxAssistantMessage(
          fauxToolCall("start_job", {
            assignmentId: accepted.assignmentId,
            epoch: 1,
            command: lateReceipt
              ? `printf begun >> '${join(f.root, "native-begun")}'; while [ ! -f '${join(f.root, "finish-job")}' ]; do sleep 0.01; done`
              : `printf begun > '${join(f.root, "native-begun")}'; sleep 10`,
          }),
          { stopReason: "toolUse" },
        );
      }
      await holdGeneration;
      return fauxAssistantMessage("Available");
    }),
  );
  r = await openSuperAgent({
    directory,
    startPaused: true,
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
    environment: { broker: f.broker },
  });
  cleanup.push(async () => {
    release();
    await r.close();
  });
  f.daemonOptions.authorize = (i, action) =>
    r.agents!.jobs.authorize(i, action);
  await r.activate();
  await until(async () =>
    readFile(join(f.root, "native-begun"), "utf8").then(
      (value) => value === "begun",
      () => false,
    ),
  );
  const firstJob = (await r.list<any>("job"))[0],
    ticket = await r.readTicket<any>(accepted.id),
    jobs = r.agents!.jobs;
  return { ...f, r, firstJob, ticket, jobs, release, until,
    async restart(startPaused = false) {
      release(); await r.close();
      r = await openSuperAgent({ directory, models, startPaused,
        model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
        environment: { broker: f.broker } });
      this.r = r;
    },
  };
}

for (const mode of ["http", "poll"] as const) {
  it(`${mode}: stopping one native Job preserves same-Assignment file access and another Job`, async () => {
    const f = await nativeRemote(mode);
    const { r, firstJob, ticket, jobs, until } = f;
    expect(
      (await jobs.stop(ticket.workerConversationId, firstJob.id)).status,
    ).toBe("cancelled");
    const bound = {
      assignmentId: ticket.assignmentId,
      epoch: ticket.assignmentEpoch,
    };
    expect(
      await jobs.file(
        ticket.workerConversationId,
        { ...bound, kind: "write", path: "after-stop", text: "safe" },
        "write-after-stop",
      ),
    ).toMatchObject({ status: "completed" });
    expect(
      await jobs.file(
        ticket.workerConversationId,
        { ...bound, kind: "read", path: "after-stop" },
        "read-after-stop",
      ),
    ).toMatchObject({ status: "completed", result: { text: "safe" } });
    const second = await jobs.start(
      ticket.workerConversationId,
      { ...bound, command: "printf next" },
      "next-job",
    );
    await until(
      async () =>
        (await r.engine.store.read<any>("job", second.jobId))?.status ===
        "completed",
    );
    expect(
      await jobs.get(ticket.workerConversationId, second.jobId),
    ).toMatchObject({
      status: "completed",
      epoch: ticket.assignmentEpoch,
      receipt: { result: { stdout: "next" } },
    });
    expect((await r.readTicket<any>(ticket.id)).assignmentEpoch).toBe(
      ticket.assignmentEpoch,
    );
    f.release();
  });
}

for (const mode of ["http", "poll"] as const) {
  it(`${mode}: actual Ticket stop derives epoch revocation and fences delayed/future admissions`, async () => {
    const f = await nativeRemote(mode);
    const { alice } = await import("./helpers/scheduling.js");
    const original = f.daemonOptions.authorize;
    let enter!: () => void,
      release!: () => void,
      calls = 0,
      revokedGrant = false;
    const entered = new Promise<void>((resolve) => (enter = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    f.daemonOptions.authorize = async (i, action) => {
      const grant = await original(i, action);
      if (action === "cancel" && grant.revokeEpoch === true)
        revokedGrant = true;
      if (action === "execute" && ++calls === 2) {
        enter();
        await gate;
      }
      return grant;
    };
    const payload = {
      kind: "write" as const,
      environmentId: f.firstJob.environmentId,
      path: "must-not-write",
      text: "unsafe",
    };
    const request = {
      operationId: "delayed-ticket-stop",
      identity: f.firstJob.identity,
      payload,
      payloadDigest: operationDigest(f.firstJob.identity, payload),
    };
    const delayed = f.broker.execute(request);
    await entered;
    f.release();
    await f.r.stopTicket(f.ticket.id, alice, { commandId: "stop-ticket" });
    await f.until(
      async () =>
        revokedGrant &&
        (await f.r.engine.store.read<any>("job", f.firstJob.id))?.status ===
          "cancelled",
    );
    release();
    expect(await delayed).toMatchObject({
      status: "failed",
      reason: expect.stringMatching(/fenced|revoked/i),
    });
    await expect(
      f.broker.execute({ ...request, operationId: "future-ticket-stop" }),
    ).rejects.toThrow(/Stale|revoked/i);
    const prepared = await f.broker.query(
      `prepare:${f.firstJob.identity.ticketId}:${f.firstJob.identity.assignmentId}:${f.firstJob.identity.epoch}`,
    );
    expect(prepared?.status).toBe("prepared");
    if (!prepared || !("cwd" in prepared))
      throw new Error("Missing prepared receipt");
    await expect(
      readFile(join(prepared.cwd, "must-not-write")),
    ).rejects.toThrow();
    expect((await f.r.readTicket<any>(f.ticket.id)).lifecycle).toBe("stopped");
  });
}

describe("pre-admission operation cancellation", () => {
  it("poll: cancels eight queued commands before any daemon admission and keeps the epoch usable", async () => {
    const f = await fixture("poll", 30);
    await f.pausePolling();
    for (let n = 0; n < 8; n++) {
      await f.broker.execute(
        f.request(`queued-${n}`, {
          kind: "shell",
          command: "printf x >> forbidden",
        }),
      );
      await f.broker.cancel(`queued-${n}`, identity.epoch);
    }
    for (let n = 0; n < 8; n++)
      expect(await f.broker.query(`queued-${n}`)).toMatchObject({
        status: "cancelled",
      });
    await f.resumePolling();
    const deadline = Date.now() + 4000;
    while (
      (await f.daemon.broker.query("queued-7"))?.status !== "cancelled" &&
      Date.now() < deadline
    )
      await delay(10);
    expect(await f.daemon.broker.query("queued-7")).toMatchObject({
      status: "cancelled",
    });
    // Delayed already-captured envelopes also hit the durable daemon tombstone.
    for (let n = 0; n < 8; n++) {
      const target = f.request(`queued-${n}`, {
        kind: "shell",
        command: "printf x >> forbidden",
      });
      await f.daemon.dispatch("cancel", {
        operationId: `transport-cancel:queued-${n}:1`,
        targetOperationId: target.operationId,
        epoch: 1,
        target,
      });
      expect(await f.daemon.dispatch("execute", target)).toMatchObject({
        status: "cancelled",
      });
    }
    await expect(readFile(join(f.prepared.cwd, "forbidden"))).rejects.toThrow();
    expect(
      await f.daemon.dispatch(
        "execute",
        f.request("other", { kind: "write", path: "allowed", text: "yes" }),
      ),
    ).toMatchObject({ status: "completed" });
  });
  it("http: a cancel arriving before the delayed execute fences only its exact immutable payload", async () => {
    const f = await fixture("http");
    const target = f.request("delayed", {
      kind: "shell",
      command: "printf bad > forbidden",
    });
    expect(
      await f.daemon.dispatch("cancel", {
        operationId: "cancel-delayed",
        targetOperationId: target.operationId,
        epoch: 1,
        target,
      }),
    ).toMatchObject({ status: "cancelled" });
    expect(await f.broker.execute(target)).toMatchObject({
      status: "cancelled",
    });
    await expect(readFile(join(f.prepared.cwd, "forbidden"))).rejects.toThrow();
    await expect(
      f.daemon.dispatch(
        "execute",
        f.request("delayed", { kind: "shell", command: "echo changed" }),
      ),
    ).rejects.toThrow(/conflict/i);
    expect(
      await f.broker.execute(
        f.request("unrelated", { kind: "write", path: "good", text: "ok" }),
      ),
    ).toMatchObject({ status: "completed" });
  });
});

it("controlled full stack: Router, two Workers, Git workspace, followup, long/queued stop_job, restart, Outbox and PR closure", async () => {
  const { openSuperAgent } = await import("../src/runtime/open.js");
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } =
    await import("@earendil-works/pi-ai/providers/faux");
  const { GitHubSource } = await import("../src/sources/github.js");
  const { createHmac } = await import("node:crypto");
  const { isRouter } = await import("./helpers/scheduling.js");
  const f = await fixture("poll", 3000);
  const comments: any[] = [];
  const api = createServer(async (req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.method === "POST") {
      const chunks = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const value = {
        id: comments.length + 1,
        ...JSON.parse(Buffer.concat(chunks).toString()),
      };
      comments.push(value);
      res.end(JSON.stringify(value));
    } else res.end(JSON.stringify(comments));
  });
  await new Promise<void>((r) => api.listen(0, "127.0.0.1", r));
  cleanup.push(() => new Promise<void>((r) => api.close(() => r())));
  const github = new GitHubSource({
    repositories: ["acme/repo"],
    webhookSecret: "fixture",
    apiToken: "fixture",
    apiBase: `http://127.0.0.1:${(api.address() as any).port}`,
    ownLogins: ["robot"],
    addressed: true,
  });
  cleanup.push(async () => github.close());
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  const model = {
    provider: faux.getModel().provider,
    modelId: faux.getModel().id,
  };
  const directory = join(f.root, "e2e-runtime");
  let r = await openSuperAgent({
    directory,
    startPaused: true,
    models,
    model,
    environment: { broker: f.broker },
    outbox: { adapters: [github] },
  });
  cleanup.push(() => r.close());
  f.daemonOptions.authorize = (identity, action) =>
    r.agents!.jobs.authorize(identity, action);
  const operator = {
    id: "github:alice",
    kind: "human" as const,
    trusted: true,
  };
  await r.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: [operator.id],
      botActors: [],
      explicitCapabilities: [
        "repo.read",
        "repo.write",
        "command.execute",
        "channel.reply",
        "pr.create",
      ],
      rules: [],
    },
    operator,
    { commandId: "p" },
  );
  await github.configure(r);
  const issued = new Set<string>(),
    stops = new Set<string>();
  const wait = async (predicate: () => Promise<boolean>) => {
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline) {
      if (await predicate()) return;
      await delay(5);
    }
    throw Error(
      "Full-stack condition did not settle " +
        JSON.stringify({
          jobs: (await r.list<any>("job")).map((j) => ({
            status: j.status,
            epoch: j.epoch,
          })),
          tickets: (await r.list<any>("ticket")).map((t) => ({
            lifecycle: t.lifecycle,
            epoch: t.assignmentEpoch,
          })),
        }),
    );
  };
  const response = async (ctx: any) => {
    const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
    let input: any;
    try {
      input = JSON.parse(
        typeof user?.content === "string" ? user.content : user.content[0].text,
      );
    } catch {
      return fauxAssistantMessage("No input");
    }
    if (isRouter(ctx))
      return ctx.messages.at(-1)?.role === "user" && input.candidates?.length
        ? fauxAssistantMessage(
            fauxToolCall("decide_batch", {
              decisions: input.candidates.map((c: any) => ({
                evalId: c.id,
                epoch: c.epoch,
                outcome: { kind: "accept", goal: `Task ${c.id}`, ruleIds: [] },
              })),
            }),
            { stopReason: "toolUse" },
          )
        : fauxAssistantMessage("Routed");
    if (!input.inputId) return fauxAssistantMessage("Waiting");
    const body = String(input.body ?? "");
    let command: string | undefined;
    if (body.includes("primary-work"))
      command =
        "printf committed > code; git add code; git commit -m worker; printf dirty > dirty";
    else if (body.includes("long-work"))
      command = `printf begun > '${join(f.root, "e2e-begun")}'; sleep 20`;
    else if (body.includes("queued-work"))
      command = "printf forbidden > forbidden";
    else if (body.includes("resume-work") || body.includes("retry-work"))
      command =
        'test "$(cat code)" = committed && test "$(cat dirty)" = dirty && printf preserved > resumed';
    if (command && !issued.has(input.inputId)) {
      issued.add(input.inputId);
      return fauxAssistantMessage(
        fauxToolCall("start_job", {
          assignmentId: input.assignmentId,
          epoch: input.epoch,
          command,
        }),
        { stopReason: "toolUse" },
      );
    }
    if (
      (body.includes("queued-work") || body.includes("stop-long")) &&
      !stops.has(input.inputId)
    ) {
      await wait(async () => {
        const jobs = await r.list<any>("job");
        return jobs.some((j) =>
          body.includes("queued-work")
            ? j.action.command === "printf forbidden > forbidden" &&
              !!j.payloadDigest
            : j.action.command.includes("e2e-begun"),
        );
      });
      if (body.includes("queued-work"))
        await wait(async () => f.broker.diagnostics().queued > 0);
      const job = (await r.list<any>("job")).find((j) =>
        body.includes("queued-work")
          ? j.action.command === "printf forbidden > forbidden"
          : j.action.command.includes("e2e-begun"),
      );
      stops.add(input.inputId);
      return fauxAssistantMessage(fauxToolCall("stop_job", { jobId: job.id }), {
        stopReason: "toolUse",
      });
    }
    if (
      ctx.messages.at(-1)?.role === "toolResult" &&
      ctx.messages.at(-1)?.toolName === "report"
    )
      return fauxAssistantMessage("Report recorded");
    if (
      ctx.messages.at(-1)?.role !== "user" &&
      ctx.messages.some(
        (m: any) =>
          m.role === "toolResult" &&
          m.toolName === "report" &&
          JSON.stringify(m).includes(input.inputId),
      )
    )
      return fauxAssistantMessage("Done");
    return fauxAssistantMessage(
      fauxToolCall("report", {
        reportId: `e2e:${input.inputId}`,
        assignmentId: input.assignmentId,
        epoch: input.epoch,
        inputIds: [input.inputId],
        status: "ready",
        content: { summary: "Controlled result" },
      }),
      { stopReason: "toolUse" },
    );
  };
  faux.setResponses(Array.from({ length: 500 }, () => response));
  let delivery = 0;
  async function webhook(number: number, body: string, action = "opened") {
    const raw = Buffer.from(
      JSON.stringify({
        action,
        repository: { full_name: "acme/repo" },
        sender: { login: "alice", type: "User" },
        pull_request: {
          id: number,
          number,
          state: action === "closed" ? "closed" : "open",
          merged: action === "closed",
          title: "Fixture",
          body,
          updated_at: new Date(1800000000000 + ++delivery * 1000).toISOString(),
        },
      }),
    );
    return github.webhook(r, raw, {
      "x-github-event": "pull_request",
      "x-github-delivery": `e2e-${delivery}`,
      "x-hub-signature-256":
        "sha256=" + createHmac("sha256", "fixture").update(raw).digest("hex"),
    });
  }
  await r.activate();
  await webhook(1, "primary-work");
  await webhook(2, "secondary-work");
  await wait(
    async () =>
      (await r.list<any>("ticket")).length === 2 &&
      (await r.list<any>("job")).some((j) => j.status === "completed"),
  );
  await r.agents!.waitForIdle();
  const ticket =
    (await r.list<any>("ticket")).find((t) =>
      (t.latestReport as any)?.inputIds.some((id: string) => issued.has(id)),
    ) ?? (await r.list<any>("ticket"))[0];
  const first = (await r.list<any>("job")).find((j) =>
    j.action.command.includes("git commit"),
  );
  const firstTicket = await r.readTicket<any>(first.ticketId);
  const worker = firstTicket.workerConversationId;
  expect(
    new Set((await r.list<any>("ticket")).map((t) => t.workerConversationId))
      .size,
  ).toBe(2);
  const prepared = (await f.daemon.broker.query(
    first.preparationOperationId,
  )) as any;
  const workHead = (
    await git("git", ["-C", prepared.cwd, "rev-parse", "HEAD"])
  ).stdout.trim();
  expect(
    (
      await git("git", ["-C", prepared.cwd, "log", "-1", "--pretty=%s"])
    ).stdout.trim(),
  ).toBe("worker");
  expect(await readFile(join(prepared.cwd, "dirty"), "utf8")).toBe("dirty");
  await webhook(1, "long-work", "synchronize");
  await wait(async () =>
    readFile(join(f.root, "e2e-begun"), "utf8").then(
      (x) => x === "begun",
      () => false,
    ),
  );
  await webhook(1, "stop-long", "synchronize");
  await wait(
    async () =>
      (await r.list<any>("job")).find((j) =>
        j.action.command.includes("e2e-begun"),
      )?.status === "cancelled",
  );
  await r.agents!.waitForIdle();
  await f.pausePolling();
  await webhook(1, "queued-work", "synchronize");
  await wait(
    async () =>
      (await r.list<any>("job")).find(
        (j) => j.action.command === "printf forbidden > forbidden",
      )?.status === "cancelled",
  );
  await f.resumePolling();
  await r.agents!.waitForIdle();
  const queued = (await r.list<any>("job")).find(
    (j) => j.action.command === "printf forbidden > forbidden",
  );
  await wait(
    async () =>
      (await f.daemon.broker.query(queued.operationId))?.status === "cancelled",
  );
  await expect(readFile(join(prepared.cwd, "forbidden"))).rejects.toThrow();
  const before = comments.length;
  await r.close();
  r = await openSuperAgent({
    directory,
    startPaused: true,
    models,
    model,
    environment: { broker: f.broker },
    outbox: { adapters: [github] },
  });
  await r.activate();
  await r.agents!.waitForIdle();
  expect(comments.length).toBe(before);
  await github.trackPullRequest(r, first.ticketId, "acme/repo#1", {
    commandId: "track",
  });
  await webhook(1, "completed", "closed");
  await r.agents!.waitForIdle();
  await wait(
    async () => (await r.readTicket<any>(first.ticketId)).lifecycle === "ended",
  );
  await webhook(1, "resume-work", "reopened");
  await wait(async () =>
    (await r.list<any>("job")).some(
      (j) => j.epoch > 1 && j.status === "completed",
    ),
  );
  await r.agents!.waitForIdle();
  const resumed = (await r.list<any>("job")).find((j) => j.epoch > 1);
  const retained = (await f.daemon.broker.query(
    resumed.preparationOperationId,
  )) as any;
  expect(retained.cwd).toBe(prepared.cwd);
  expect(
    (await git("git", ["-C", prepared.cwd, "rev-parse", "HEAD"])).stdout.trim(),
  ).toBe(workHead);
  expect((await r.readTicket<any>(first.ticketId)).workerConversationId).toBe(
    worker,
  );
  expect(await readFile(join(prepared.cwd, "resumed"), "utf8")).toBe(
    "preserved",
  );
  await r.stopTicket(first.ticketId, operator, { commandId: "explicit-stop" });
  await r.agents!.waitForIdle();
  await r.retryTicket(first.ticketId, operator, {
    commandId: "explicit-retry",
  });
  await webhook(1, "retry-work", "synchronize");
  await wait(async () =>
    (await r.list<any>("job")).some(
      (j) => j.epoch > resumed.epoch && j.status === "completed",
    ),
  );
  await r.agents!.waitForIdle();
  expect(await readFile(join(prepared.cwd, "dirty"), "utf8")).toBe("dirty");
  expect(
    (await r.list<any>("outbox")).every((m) =>
      ["sent", "invalid"].includes(m.status),
    ),
  ).toBe(true);
  expect(comments.length).toBeGreaterThan(2);
}, 30000);
it("HTTP per-operation cancellation fences an execute suspended in its async authority lookup", async () => {
  const f = await fixture("http");
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>((r) => (enter = r)),
    gate = new Promise<void>((r) => (release = r));
  const authorize = f.daemonOptions.authorize;
  let hold = true;
  f.daemonOptions.authorize = async (i, action) => {
    if (action === "execute" && hold) {
      enter();
      await gate;
    }
    return authorize(i);
  };
  const request = f.request("barrier-cancel", {
    kind: "shell",
    command: "printf forbidden > forbidden",
  });
  const execution = f.broker.execute(request);
  await entered;
  const cancellation = f.broker.cancel(request.operationId, identity.epoch);
  try {
    const deadline = Date.now() + 3000;
    while (
      (await f.daemon.broker.query(request.operationId))?.status !==
        "cancelled" &&
      Date.now() < deadline
    )
      await delay(5);
    expect(await f.daemon.broker.query(request.operationId)).toMatchObject({
      status: "cancelled",
    });
  } finally {
    hold = false;
    release();
  }
  expect(await execution).toMatchObject({ status: "cancelled" });
  expect(await cancellation).toMatchObject({ status: "cancelled" });
  await expect(readFile(join(f.prepared.cwd, "forbidden"))).rejects.toThrow();
  expect(
    await f.broker.execute(
      f.request("after-barrier", { kind: "write", path: "safe", text: "yes" }),
    ),
  ).toMatchObject({ status: "completed" });
});


for (const mode of ["http", "poll"] as const) {
  for (const restart of [false, true]) it(`${mode}: late Job completion reconciles without get_job${restart ? " across restart" : ""}`, async () => {
    const f = await nativeRemote(mode, true);
    async function waitFor(fn: () => Promise<boolean>) {
      for (let n = 0; n < 1000; n++) { if (await fn()) return; await delay(10); }
      throw Error("Late receipt did not reconcile");
    }
    await waitFor(async () => (await f.r.engine.store.read<any>("job", f.firstJob.id))?.status === "unknown");
    if (restart) f.release();
    await f.r.agents!.pump();
    if (!restart) expect((await f.r.readTicket<any>(f.ticket.id)).attention).toBe("Job outcome unknown; inspect endpoint before continuing");
    const unknownNotes = (await f.r.list<any>("workerNote")).filter(n => n.jobId === f.firstJob.id);
    expect(unknownNotes).toHaveLength(1);
    if (restart) {
      await f.restart(true);
      // Persist a genuinely unfinished native query claim, then reopen with its
      // future deadline. A live claim must resume; due time only gates new claims.
      await f.r.engine.transaction(async tx => {
        const job = (await f.r.engine.store.get<any>(tx, "job", f.firstJob.id))!;
        const old = job.receiptTaskId ? await tx.task(job.receiptTaskId) : undefined;
        const taskId = old && old.state.status !== "terminal" ? old.id : await tx.createTask(f.r.agents!.jobs.task,
          { jobId: job.id, queryOnly: true }, { ownership: { kind: "conversation" }, background: true });
        await f.r.engine.store.put(tx, "job", job.id, { ...job, receiptTaskId: taskId, receiptQueryAt: Date.now() + 60000 });
      });
      await writeFile(join(f.root, "finish-job"), "go");
      await f.restart();
    }
    await writeFile(join(f.root, "finish-job"), "go");
    await waitFor(async () => (await f.r.engine.store.read<any>("job", f.firstJob.id))?.status === "completed");
    if (!restart) expect((await f.r.readTicket<any>(f.ticket.id)).attention).toBeUndefined();
    f.release();
    await f.r.agents!.pump();
    const notes = (await f.r.list<any>("workerNote")).filter(n => n.jobId === f.firstJob.id);
    expect(notes.map(n => JSON.parse(n.text).receipt.status).sort()).toEqual(["completed", "unknown"]);
    expect(new Set(notes.map(n => n.id)).size).toBe(2);
    await waitFor(async () => (await f.r.list<any>("deliveryRun")).filter(run => notes.some(n => n.id === run.id) && run.submissionId).length === 2);
    expect((await f.r.readTicket<any>(f.ticket.id)).attention).not.toBe("Job outcome unknown; inspect endpoint before continuing");
    await f.r.agents!.pump();
    expect((await f.r.list<any>("workerNote")).filter(n => n.jobId === f.firstJob.id)).toHaveLength(2);
    expect(await readFile(join(f.root, "native-begun"), "utf8")).toBe("begun");
    expect((await f.broker.query(f.firstJob.operationId))?.status).toBe("completed");
    const completed = await f.r.engine.store.read<any>("job", f.firstJob.id);
    const query = f.broker.query.bind(f.broker);
    let queries = 0;
    f.broker.query = async id => { queries++; return query(id); };
    try {
      // Replaying either the old execution claim or terminal query claim is inert.
      await f.r.agents!.jobs.receipts.run(completed.id, completed.taskId);
      await f.r.agents!.jobs.receipts.run(completed.id, completed.receiptTaskId);
      expect(queries).toBe(0);
    } finally { f.broker.query = query; }
  }, 25000);
}


for (const mode of ["http", "poll"] as const) it(`${mode}: late query receipt after stop remains accounting-only`, async () => {
  const { alice } = await import("./helpers/scheduling.js");
  const f = await nativeRemote(mode, true);
  const queryObservations: { status?: string; at: number }[] = [];
  const waitFor = async (fn: () => Promise<boolean>) => {
    for (let n = 0; n < 1000; n++) { if (await fn()) return; await delay(10); }
    const job = await f.r.engine.store.read<any>('job', f.firstJob.id);
    throw Error(`Late accounting did not settle: ${JSON.stringify({ status: job?.status, queryAttempts: job?.receiptQueryAttempts, queryDueIn: job?.receiptQueryAt - Date.now(), queryObservations })}`);
  };
  await waitFor(async () => (await f.r.engine.store.read<any>("job", f.firstJob.id))?.status === "unknown");
  f.release();
  // Install the observation barrier before any recovered native query can run.
  await f.restart(true);
  const query = f.broker.query.bind(f.broker);
  let entered = false, release!: () => void;
  const gate = new Promise<void>(resolve => release = resolve);
  f.broker.query = async id => {
    const receipt = await query(id);
    if (id === f.firstJob.operationId) queryObservations.push({ status: receipt?.status, at: Date.now() });
    if (id === f.firstJob.operationId && receipt?.status === "completed" && !entered) {
      entered = true; await gate;
    }
    return receipt;
  };
  try {
    await writeFile(join(f.root, "finish-job"), "go");
    await waitFor(async () => (await f.daemon.broker.query(f.firstJob.operationId))?.status === 'completed');
    // This test exercises late accounting, not wall-clock exponential backoff.
    await f.r.engine.transaction(async tx => {
      const job = (await f.r.engine.store.get<any>(tx, 'job', f.firstJob.id))!;
      await f.r.engine.store.put(tx, 'job', job.id, { ...job, receiptQueryAt: 0 });
    });
    await f.r.agents!.jobs.receipts.reconcile();
    await f.r.activate();
    await waitFor(async () => entered);
    expect((await f.r.engine.store.read<any>("job", f.firstJob.id)).receiptTaskId).toBeTruthy();
    await f.r.agents!.close();
    await f.r.stopTicket(f.ticket.id, alice, { commandId: "stop-before-late-query" });
    const stopped = await f.r.readTicket<any>(f.ticket.id);
    release();
    await waitFor(async () => (await f.r.engine.store.read<any>("job", f.firstJob.id))?.status === "completed");
    expect(await f.r.readTicket<any>(f.ticket.id)).toEqual(stopped);
    const notes = (await f.r.list<any>("workerNote")).filter(n => n.jobId === f.firstJob.id);
    expect(notes).toHaveLength(1);
    expect(JSON.parse(notes[0].text).receipt.status).toBe("unknown");
    expect(await readFile(join(f.root, "native-begun"), "utf8")).toBe("begun");
  } finally { release(); f.broker.query = query; }
}, 25000);

for (const mode of ['http', 'poll'] as const) {
  it(`${mode}: durably cancels a target before delayed transport authorization admits it`, async () => {
    const f = await fixture(mode);
    const request = f.request('pre-admission-cancel', { kind: 'shell', command: 'printf unsafe > late-effect' });
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(r => enter = r), gate = new Promise<void>(r => release = r);
    const authorize = f.broker.options.authorize;
    f.broker.options.authorize = async (i, action) => {
      const grant = await authorize(i, action);
      if (action === 'execute') { enter(); await gate; }
      return grant;
    };
    const execution = f.broker.execute(request);
    await entered;
    try {
      expect(await f.broker.cancel(request.operationId, 1, request)).toMatchObject({ status: 'cancelled' });
      expect(await f.broker.query(request.operationId)).toMatchObject({ status: 'cancelled' });
    } finally { release(); await execution; }
    const reopened = new RemoteEnvironmentBroker({ ...f.broker.options, authorize });
    expect(await reopened.query(request.operationId)).toMatchObject({ status: 'cancelled' });
    expect(await reopened.execute(request)).toMatchObject({ status: 'cancelled' });
    await expect(readFile(join(f.prepared.cwd, 'late-effect'))).rejects.toThrow();
    expect(await f.broker.execute(f.request('unrelated-after-operation-cancel', { kind: 'write', path: 'file', text: 'still authorized' }))).toMatchObject({ status: 'completed' });
  });

  it(`${mode}: stop_job fences a native Job while its transport grant is delayed`, async () => {
    const { openSuperAgent } = await import('../src/runtime/open.js');
    const { createModels } = await import('@earendil-works/pi-ai/models');
    const { fauxProvider, fauxAssistantMessage, fauxToolCall } = await import('@earendil-works/pi-ai/providers/faux');
    const { accept, alice, isRouter, until } = await import('./helpers/scheduling.js');
    const f = await fixture(mode), directory = join(f.root, 'runtime');
    let r = await openSuperAgent({ directory });
    await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], explicitCapabilities: capabilities, rules: [] }, alice, { commandId: 'policy' });
    const ticket = await accept(r, 'native-cancel');
    await r.close();
    const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
    let first = true;
    const effect = join(f.root, 'job-late-effect');
    faux.setResponses(Array.from({ length: 50 }, () => (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage('No decision');
      if (first) { first = false; return fauxAssistantMessage(fauxToolCall('start_job', { assignmentId: ticket.assignmentId, epoch: 1, command: `printf unsafe > '${effect}'` }), { stopReason: 'toolUse' }); }
      return fauxAssistantMessage('Observe cancellation');
    }));
    r = await openSuperAgent({ directory, startPaused: true, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, environment: { broker: f.broker } });
    cleanup.push(() => r.close());
    f.daemonOptions.authorize = (i, action) => r.agents!.jobs.authorize(i, action);
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => enter = resolve), gate = new Promise<void>(resolve => release = resolve);
    const authorize = f.broker.options.authorize;
    f.broker.options.authorize = async (i, action) => { const grant = await authorize(i, action); if (action === 'execute') { enter(); await gate; } return grant; };
    await r.activate(); await entered;
    const job = (await r.list<any>('job'))[0], current = await r.readTicket<any>(ticket.id);
    const stopping = r.agents!.jobs.stop(current.workerConversationId, job.id);
    try {
      await until(async () => (await r.list<any>('job'))[0]?.status === 'cancelled');
      expect(await f.broker.query(job.operationId)).toMatchObject({ status: 'cancelled' });
    } finally { release(); await stopping; await r.agents!.waitForIdle(); }
    expect((await r.list<any>('job'))[0]).toMatchObject({ status: 'cancelled', receipt: { status: 'cancelled' } });
    await expect(readFile(effect)).rejects.toThrow();
  });
}

for (const mode of ['http', 'poll'] as const) it(`${mode}: pre-admission Ticket cancellation still revokes the whole epoch`, async () => {
  const f = await fixture(mode);
  f.daemonOptions.authorize = async (i, action) => ({ ...i, capabilities, ...(action === 'cancel' ? { revokeEpoch: true as const } : {}) });
  const request = f.request('pre-admission-ticket-stop', { kind: 'shell', command: 'printf unsafe > ticket-stop-effect' });
  expect(await f.broker.cancel(request.operationId, 1, request)).toMatchObject({ status: 'cancelled' });
  await expect(f.broker.execute(f.request('other-old-epoch-operation', { kind: 'write', path: 'file', text: 'unsafe' }))).rejects.toThrow(/fenced|revoked|rejected/i);
  expect(await readFile(join(f.prepared.cwd, 'file'), 'utf8')).toBe('base');
  await expect(readFile(join(f.prepared.cwd, 'ticket-stop-effect'))).rejects.toThrow();
});
