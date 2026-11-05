#!/usr/bin/env node
import { readFile, mkdir, open } from "node:fs/promises";
import { join } from "node:path";
import fsExt from "fs-ext";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { RemoteDaemon, runDaemonPolling, startDaemonServer } from "./server.js";
import { requestJson, secureUrl } from "./protocol.js";
import { configObject, envSecret } from "../runtime/adapter-config.js";
export const help = `Super Durable Agent endpoint daemon
Usage: node dist/daemon/cli.js --config PATH

Config: endpointId, tokenEnv, directory, repository, allowedRepositoryRoots,
capabilities, trustedLocalShell (explicit opt-in), controllerUrl,
controllerTokenEnv, mode (http or poll), host, port, tls {keyFile, certFile}.
Polling mode initiates all connections outbound to the controller.
Nonloopback requires TLS. allowInsecureLoopback is an explicit test-only flag.
All credentials are environment references; no credentials or commands are logged.
`;
export async function main(args = process.argv.slice(2)) {
  if (args.length === 1 && ["--help", "-h"].includes(args[0])) {
    process.stdout.write(help);
    return;
  }
  if (args.length !== 2 || args[0] !== "--config")
    throw new Error("Use --config PATH or --help");
  const config = configObject(
    JSON.parse(await readFile(args[1], "utf8")),
    [
      "endpointId",
      "tokenEnv",
      "directory",
      "repository",
      "allowedRepositoryRoots",
      "capabilities",
      "trustedLocalShell",
      "controllerUrl",
      "controllerTokenEnv",
      "mode",
      "host",
      "port",
      "tls",
      "allowInsecureLoopback",
      "timeoutMs",
      "intervalMs",
    ],
    "daemon",
  );
  if (
    !["http", "poll"].includes(config.mode) ||
    (config.trustedLocalShell !== undefined &&
      typeof config.trustedLocalShell !== "boolean") ||
    (config.allowInsecureLoopback !== undefined &&
      typeof config.allowInsecureLoopback !== "boolean")
  )
    throw new Error("Invalid daemon mode or flags");
  if (
    config.port !== undefined &&
    (!Number.isSafeInteger(config.port) ||
      config.port < 0 ||
      config.port > 65535)
  )
    throw new Error("Invalid daemon port");
  secureUrl(config.controllerUrl, config.allowInsecureLoopback);
  const controllerToken = envSecret(config.controllerTokenEnv);
  const daemon = new RemoteDaemon({
    endpointId: config.endpointId,
    token: envSecret(config.tokenEnv),
    directory: config.directory,
    repository: config.repository,
    allowedRepositoryRoots: config.allowedRepositoryRoots,
    capabilities: config.capabilities,
    trustedLocalShell: config.trustedLocalShell,
    authorize: async (identity, action) =>
      (
        await requestJson(
          config.controllerUrl,
          `/remote/${config.endpointId}/authorize`,
          controllerToken,
          { identity, action },
          config.timeoutMs,
        )
      ).grant,
  });
  await mkdir(config.directory, { recursive: true, mode: 0o700 });
  const lock = await open(join(config.directory, ".daemon.lock"), "a+", 0o600);
  try {
    fsExt.flockSync(lock.fd, "exnb");
  } catch {
    await lock.close();
    throw new Error("Daemon writer lock is held");
  }
  try {
    await daemon.instanceId();
    let host: { close(): Promise<void>; address?: string };
    if (config.mode === "poll")
      host = await runDaemonPolling(daemon, {
        url: config.controllerUrl,
        token: controllerToken,
        allowInsecureLoopback: config.allowInsecureLoopback,
        intervalMs: config.intervalMs,
        timeoutMs: config.timeoutMs,
      });
    else {
      let tls;
      if (config.tls) {
        const value = configObject(
          config.tls,
          ["keyFile", "certFile"],
          "daemon TLS",
        );
        tls = {
          key: await readFile(value.keyFile),
          cert: await readFile(value.certFile),
        };
      }
      host = await startDaemonServer(daemon, {
        port: config.port ?? 8788,
        host: config.host,
        tls,
        allowInsecureLoopback: config.allowInsecureLoopback,
      });
    }
    process.stdout.write(
      JSON.stringify({
        endpointId: config.endpointId,
        mode: config.mode,
        ...(host.address ? { listening: host.address } : { polling: true }),
      }) + "\n",
    );
    try {
      await new Promise<void>((resolve) => {
        const close = () => {
          process.off("SIGINT", close);
          process.off("SIGTERM", close);
          resolve();
        };
        process.on("SIGINT", close);
        process.on("SIGTERM", close);
      });
    } finally {
      await host.close();
    }
  } finally {
    await lock.close();
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
)
  main().catch(() => {
    process.stderr.write(
      "Daemon failed; check validated configuration, references and endpoint reachability\n",
    );
    process.exitCode = 1;
  });
