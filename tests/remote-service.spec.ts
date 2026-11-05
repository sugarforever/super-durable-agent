import { afterEach, expect, it } from "vitest";
import { mkdtemp, mkdir, writeFile, rm, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  spawn,
  execFile,
  type ChildProcessWithoutNullStreams,
} from "node:child_process";
import { promisify } from "node:util";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import {
  operationDigest,
  type EndpointIdentity,
} from "../src/environments/types.js";
import { RemoteEnvironmentBroker } from "../src/environments/remote.js";
import {
  configuredRemote,
  configuredFeishu,
} from "../src/runtime/adapter-config.js";
const cleanup: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
const run = promisify(execFile);
async function root() {
  const directory = await mkdtemp(join(tmpdir(), "remote-cli-"));
  cleanup.push(() => rm(directory, { force: true, recursive: true }));
  return directory;
}
async function launch(
  file: string,
  config: string,
  env: Record<string, string> = {},
) {
  const child = spawn(
    process.execPath,
    [file, "--config", config, ...(file === "dist/cli.js" ? ["serve"] : [])],
    {
      env: {
        PATH: process.env.PATH,
        ENDPOINT_TOKEN: "remote-secret",
        MANAGEMENT_TOKEN: "management-secret",
        FEISHU_VERIFY: "verify",
        FEISHU_ENCRYPT: "encrypt",
        ...env,
      },
    },
  );
  let stdout = "",
    stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  cleanup.push(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGKILL");
      await exited;
    }
  });
  for (
    let i = 0;
    i < 500 && !stdout.includes("\n") && child.exitCode === null;
    i++
  )
    await delay(5);
  expect(stdout, stderr).toContain("\n");
  expect(stdout + stderr).not.toContain("remote-secret");
  return { child, startup: JSON.parse(stdout.split("\n")[0]) };
}
async function waitForFile(path: string, expected: string) {
  for (let i = 0; i < 1000; i++) {
    if (
      await readFile(path, "utf8").then(
        (value) => value === expected,
        () => false,
      )
    )
      return;
    await delay(5);
  }
  throw new Error("Expected observable daemon shell effect did not arrive");
}
async function stop(
  child: ChildProcessWithoutNullStreams,
  signal: NodeJS.Signals = "SIGTERM",
) {
  const done = new Promise<number | null>((resolve) =>
    child.once("exit", resolve),
  );
  child.kill(signal);
  return done;
}
it("runs configured service with remote poll control and Feishu verified route, without a wrapper", async () => {
  const directory = await root(),
    file = join(directory, "service.json");
  await writeFile(
    file,
    JSON.stringify({
      directory: join(directory, "runtime"),
      port: 0,
      tokenEnv: "MANAGEMENT_TOKEN",
      remoteEnvironmentBroker: {
        endpointId: "device",
        transport: "poll",
        tokenEnv: "ENDPOINT_TOKEN",
        allowInsecureLoopback: true,
      },
      feishu: {
        verificationTokenEnv: "FEISHU_VERIFY",
        encryptKeyEnv: "FEISHU_ENCRYPT",
        appId: "app",
        chats: ["chat"],
      },
    }),
  );
  const { child, startup } = await launch("dist/cli.js", file);
  const response = await fetch(startup.listening + "/health", {
    headers: { authorization: "Bearer management-secret" },
  });
  expect(await response.json()).toMatchObject({
    endpoints: [
      {
        endpointId: "device",
        transport: "poll",
        state: "unknown",
        queued: 0,
        active: 0,
      },
    ],
  });
  expect(
    (
      await fetch(startup.listening + "/remote/device/poll", {
        method: "POST",
        body: "{}",
      })
    ).status,
  ).toBe(401);
  expect(
    (
      await fetch(startup.listening + "/webhooks/feishu", {
        method: "POST",
        body: "{}",
      })
    ).status,
  ).toBe(401);
  expect(await stop(child)).toBe(0);
});
it("validates nested remote/Feishu settings and refuses inline credentials", async () => {
  const old = process.env.ENDPOINT_TOKEN;
  process.env.ENDPOINT_TOKEN = "remote-secret";
  try {
    for (const config of [
      {
        endpointId: "device",
        transport: "poll",
        tokenEnv: "ENDPOINT_TOKEN",
        token: "inline",
      },
      {
        endpointId: "device",
        transport: "http",
        tokenEnv: "ENDPOINT_TOKEN",
        url: "http://example.com",
        allowInsecureLoopback: true,
      },
      {
        endpointId: "device",
        transport: "poll",
        tokenEnv: "ENDPOINT_TOKEN",
        directory: 12,
      },
      {
        endpointId: "device",
        transport: "poll",
        tokenEnv: "ENDPOINT_TOKEN",
        allowInsecureLoopback: "true",
      },
    ])
      expect(() => configuredRemote(config, "/unused")).toThrow();
    expect(() => configuredFeishu({ verificationToken: "inline" })).toThrow();
    expect(() =>
      configuredFeishu({
        verificationTokenEnv: "V",
        encryptKeyEnv: "E",
        appId: "a",
        chats: ["c"],
        accessTokenEnv: "T",
      }),
    ).toThrow(/expiry/i);
  } finally {
    if (old === undefined) delete process.env.ENDPOINT_TOKEN;
    else process.env.ENDPOINT_TOKEN = old;
  }
});
it("daemon CLI preserves launched operation uncertainty across SIGKILL/restart without rerunning a shell", async () => {
  const directory = await root(),
    repository = join(directory, "repo");
  await mkdir(repository);
  await run("git", ["init", "-b", "main", repository]);
  await run("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "tests@example.invalid",
  ]);
  await run("git", ["-C", repository, "config", "user.name", "Tests"]);
  await writeFile(join(repository, "base"), "base");
  await run("git", ["-C", repository, "add", "."]);
  await run("git", ["-C", repository, "commit", "-m", "base"]);
  const identity: EndpointIdentity = {
    ticketId: "t",
    assignmentId: "a",
    epoch: 1,
    mandateId: "m",
  };
  const broker = new RemoteEnvironmentBroker({
    directory: join(directory, "controller"),
    endpointId: "device",
    token: "remote-secret",
    transport: "http",
    url: "http://127.0.0.1:1",
    allowInsecureLoopback: true,
    timeoutMs: 3000,
    authorize: async (i) => ({
      ...i,
      capabilities: ["repo.read", "command.execute"],
    }),
  });
  const controller = createServer((req, res) => {
    void broker.handleControl(req, res);
  });
  await new Promise<void>((resolve) =>
    controller.listen(0, "127.0.0.1", resolve),
  );
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        controller.close(() => resolve());
        controller.closeAllConnections();
      }),
  );
  const config = join(directory, "daemon.json");
  await writeFile(
    config,
    JSON.stringify({
      endpointId: "device",
      tokenEnv: "ENDPOINT_TOKEN",
      controllerTokenEnv: "ENDPOINT_TOKEN",
      controllerUrl: `http://127.0.0.1:${(controller.address() as any).port}`,
      directory: join(directory, "daemon"),
      repository,
      allowedRepositoryRoots: [repository],
      capabilities: ["repo.read", "command.execute"],
      trustedLocalShell: true,
      mode: "http",
      port: 0,
      allowInsecureLoopback: true,
    }),
  );
  let daemon = await launch("dist/daemon/cli.js", config);
  broker.options.url = daemon.startup.listening;
  const payload = { kind: "prepare" as const },
    prepared = await broker.prepare({
      operationId: "prepare",
      identity,
      payload,
      payloadDigest: operationDigest(identity, payload),
    });
  broker.options.timeoutMs = 3000;
  const command = {
    kind: "shell" as const,
    environmentId: prepared.environmentId,
    command: `printf x >> count; while [ ! -f '${join(directory, "release-shell")}' ]; do sleep 0.02; done; printf done > '${join(directory, "shell-finished")}'`,
  };
  const request = {
    operationId: "interrupted",
    identity,
    payload: command,
    payloadDigest: operationDigest(identity, command),
  };
  cleanup.push(() => writeFile(join(directory, "release-shell"), "done"));
  const execution = broker.execute(request);
  await waitForFile(join(prepared.cwd, "count"), "x");
  await stop(daemon.child, "SIGKILL");
  expect(await execution).toMatchObject({ status: "unknown" });
  await writeFile(join(directory, "release-shell"), "done");
  await waitForFile(join(directory, "shell-finished"), "done");
  daemon = await launch("dist/daemon/cli.js", config);
  broker.options.url = daemon.startup.listening;
  const restarted = new RemoteEnvironmentBroker(broker.options);
  expect(await restarted.query("interrupted")).toMatchObject({
    status: "unknown",
  });
  expect(await restarted.execute(request)).toMatchObject({ status: "unknown" });
  expect(await readFile(join(prepared.cwd, "count"), "utf8")).toBe("x");
  expect(await stop(daemon.child)).toBe(0);
});

it("runs the daemon CLI outbound poll mode against a controller, including restart receipt recovery", async () => {
  const directory = await root(),
    repository = join(directory, "repo");
  await mkdir(repository);
  await run("git", ["init", "-b", "main", repository]);
  await run("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "tests@example.invalid",
  ]);
  await run("git", ["-C", repository, "config", "user.name", "Tests"]);
  await writeFile(join(repository, "base"), "base");
  await run("git", ["-C", repository, "add", "."]);
  await run("git", ["-C", repository, "commit", "-m", "base"]);
  const identity: EndpointIdentity = {
    ticketId: "t",
    assignmentId: "a",
    epoch: 1,
    mandateId: "m",
  };
  const broker = new RemoteEnvironmentBroker({
    directory: join(directory, "controller"),
    endpointId: "device",
    token: "remote-secret",
    transport: "poll",
    allowInsecureLoopback: true,
    timeoutMs: 3000,
    authorize: async (i) => ({
      ...i,
      capabilities: ["repo.read", "command.execute"],
    }),
  });
  const controller = createServer((req, res) => {
    void broker.handleControl(req, res);
  });
  await new Promise<void>((resolve) =>
    controller.listen(0, "127.0.0.1", resolve),
  );
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        controller.close(() => resolve());
        controller.closeAllConnections();
      }),
  );
  const config = join(directory, "daemon.json");
  await writeFile(
    config,
    JSON.stringify({
      endpointId: "device",
      tokenEnv: "ENDPOINT_TOKEN",
      controllerTokenEnv: "ENDPOINT_TOKEN",
      controllerUrl: `http://127.0.0.1:${(controller.address() as any).port}`,
      directory: join(directory, "daemon"),
      repository,
      allowedRepositoryRoots: [repository],
      capabilities: ["repo.read", "command.execute"],
      trustedLocalShell: true,
      mode: "poll",
      intervalMs: 10,
      allowInsecureLoopback: true,
    }),
  );
  const daemon = await launch("dist/daemon/cli.js", config);
  expect(daemon.startup).toMatchObject({ polling: true, mode: "poll" });
  const payload = { kind: "prepare" as const },
    prepared = await broker.prepare({
      operationId: "prepare",
      identity,
      payload,
      payloadDigest: operationDigest(identity, payload),
    });
  const command = {
    kind: "shell" as const,
    environmentId: prepared.environmentId,
    command: "printf x >> count",
  };
  expect(
    await broker.execute({
      operationId: "command",
      identity,
      payload: command,
      payloadDigest: operationDigest(identity, command),
    }),
  ).toMatchObject({ status: "completed" });
  expect(await readFile(join(prepared.cwd, "count"), "utf8")).toBe("x");
  expect(await stop(daemon.child)).toBe(0);
});

it("poll reconnect queries a previously delivered shell even when its daemon admission file was lost", async () => {
  const { digest } = await import("../src/domain/model.js");
  const directory = await root(),
    repository = join(directory, "repo");
  await mkdir(repository);
  await run("git", ["init", "-b", "main", repository]);
  await run("git", [
    "-C",
    repository,
    "config",
    "user.email",
    "tests@example.invalid",
  ]);
  await run("git", ["-C", repository, "config", "user.name", "Tests"]);
  await writeFile(join(repository, "base"), "base");
  await run("git", ["-C", repository, "add", "."]);
  await run("git", ["-C", repository, "commit", "-m", "base"]);
  const identity: EndpointIdentity = {
    ticketId: "t",
    assignmentId: "a",
    epoch: 1,
    mandateId: "m",
  };
  const broker = new RemoteEnvironmentBroker({
    directory: join(directory, "controller"),
    endpointId: "device",
    token: "remote-secret",
    transport: "poll",
    allowInsecureLoopback: true,
    timeoutMs: 3000,
    authorize: async (i) => ({
      ...i,
      capabilities: ["repo.read", "command.execute"],
    }),
  });
  const controller = createServer((req, res) => {
    void broker.handleControl(req, res);
  });
  await new Promise<void>((resolve) =>
    controller.listen(0, "127.0.0.1", resolve),
  );
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        controller.close(() => resolve());
        controller.closeAllConnections();
      }),
  );
  const config = join(directory, "daemon.json");
  await writeFile(
    config,
    JSON.stringify({
      endpointId: "device",
      tokenEnv: "ENDPOINT_TOKEN",
      controllerTokenEnv: "ENDPOINT_TOKEN",
      controllerUrl: `http://127.0.0.1:${(controller.address() as any).port}`,
      directory: join(directory, "daemon"),
      repository,
      allowedRepositoryRoots: [repository],
      capabilities: ["repo.read", "command.execute"],
      trustedLocalShell: true,
      mode: "poll",
      intervalMs: 10,
      allowInsecureLoopback: true,
    }),
  );
  let daemon = await launch("dist/daemon/cli.js", config);
  const payload = { kind: "prepare" as const },
    prepared = await broker.prepare({
      operationId: "prepare",
      identity,
      payload,
      payloadDigest: operationDigest(identity, payload),
    });
  broker.options.timeoutMs = 150;
  const command = {
    kind: "shell" as const,
    environmentId: prepared.environmentId,
    command: `printf x >> count; while [ ! -f '${join(directory, "release-shell")}' ]; do sleep 0.02; done; printf done > '${join(directory, "shell-finished")}'`,
  };
  const request = {
    operationId: "lost-file",
    identity,
    payload: command,
    payloadDigest: operationDigest(identity, command),
  };
  cleanup.push(() => writeFile(join(directory, "release-shell"), "done"));
  expect(await broker.execute(request)).toMatchObject({ status: "unknown" });
  await waitForFile(join(prepared.cwd, "count"), "x");
  await stop(daemon.child, "SIGKILL");
  await writeFile(join(directory, "release-shell"), "done");
  await waitForFile(join(directory, "shell-finished"), "done");
  await rm(
    join(
      directory,
      "daemon",
      "endpoint-ledger",
      "operations",
      `${digest("lost-file")}.json`,
    ),
  );
  daemon = await launch("dist/daemon/cli.js", config);
  await delay(500);
  expect(await broker.query("lost-file")).toMatchObject({ status: "unknown" });
  expect(await readFile(join(prepared.cwd, "count"), "utf8")).toBe("x");
  expect(await stop(daemon.child)).toBe(0);
});
