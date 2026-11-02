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
    polling = await runDaemonPolling(daemon, {
      url: `http://127.0.0.1:${address.port}`,
      token: "daemon-token",
      allowInsecureLoopback: true,
      intervalMs: 10,
    });
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
    return { ...i, capabilities };
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
