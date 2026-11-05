#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { configObject, envSecret } from "../runtime/adapter-config.js";
import { configuredModels } from "../cli.js";
import { WatchdogLedger, type WatchdogConfig } from "./core.js";
import { pollWatchdog } from "./runner.js";
import { semanticEvaluator } from "./semantic.js";
export async function watchdogMain(args = process.argv.slice(2)) {
  if (
    args[0] !== "--config" ||
    !args[1] ||
    args.slice(2).some((a) => !["--once", "watch"].includes(a))
  )
    throw Error(
      "Usage: node dist/watchdog/cli.js --config PATH [watch | --once]",
    );
  const config = configObject(
    JSON.parse(await readFile(args[1], "utf8")),
    [
      "directory",
      "url",
      "tokenEnv",
      "allowInsecureLoopback",
      "policy",
      "semantic",
    ],
    "watchdog",
  );
  if (typeof config.directory !== "string" || !config.directory.trim())
    throw Error("Separate watchdog directory required");
  const policy = configObject(
    config.policy ?? {},
    [
      "thresholdMs",
      "intervalMs",
      "maxCalls",
      "maxTokens",
      "maxCost",
      "callTokens",
      "callCost",
      "allowPause",
      "allowSafeRetry",
      "costAnomalyThreshold",
    ],
    "watchdog policy",
  ) as WatchdogConfig;
  const interval = policy.intervalMs ?? 60000;
  if (!Number.isSafeInteger(interval) || interval < 10 || interval > 3600000)
    throw Error("Invalid watchdog interval");
  const connection = {
    url: config.url,
    token: envSecret(config.tokenEnv),
    allowInsecureLoopback: config.allowInsecureLoopback,
  };
  const ledger = new WatchdogLedger(config.directory);
  let evaluator;
  if (config.semantic) {
    const semantic = configObject(
      config.semantic,
      ["provider", "model", "modelKeyEnv"],
      "watchdog semantic",
    );
    const selected = await configuredModels(semantic);
    if (!selected.models || !selected.model)
      throw Error("Explicit watchdog model required");
    evaluator = semanticEvaluator(selected.models, selected.model);
  }
  let stopped = false;
  let wake: (() => void) | undefined;
  const shutdown = () => {
    stopped = true;
    wake?.();
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  let lastError = "";
  try {
    do {
      try {
        await pollWatchdog(connection, policy, ledger, evaluator);
        lastError = "";
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Watchdog polling failed";
        if (message !== lastError)
          process.stderr.write(
            JSON.stringify({ kind: "watchdogFailure", message }) + "\n",
          );
        lastError = message;
        if (args.includes("--once")) throw error;
      }
      if (stopped || args.includes("--once")) break;
      await new Promise<void>((r) => {
        const timer = setTimeout(r, interval);
        wake = () => {
          clearTimeout(timer);
          r();
        };
      });
    } while (!stopped);
  } finally {
    process.off("SIGINT", shutdown);
    process.off("SIGTERM", shutdown);
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
)
  void watchdogMain().catch((error) => {
    process.stderr.write(
      (error instanceof Error ? error.message : "Watchdog failed") + "\n",
    );
    process.exitCode = 1;
  });
