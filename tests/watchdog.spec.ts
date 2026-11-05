import { afterEach, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { WatchdogLedger, watchdogTick } from "../src/watchdog/core.js";
const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0))
    await rm(root, { recursive: true, force: true });
});
async function ledger() {
  const root = await mkdtemp(join(tmpdir(), "sa-watchdog-"));
  roots.push(root);
  return new WatchdogLedger(root);
}
const snapshot = (patch: any = {}) => ({
  tickets: [{ id: "t", lifecycle: "open", revision: 1 }],
  jobs: [],
  endpoints: [],
  ...patch,
});
it("waits fifteen minutes and deduplicates candidate diagnostics across restart", async () => {
  const l = await ledger();
  expect(await watchdogTick(snapshot(), {}, l, undefined, 0)).toEqual([]);
  expect(await watchdogTick(snapshot(), {}, l, undefined, 899999)).toEqual([]);
  const actions = await watchdogTick(snapshot(), {}, l, undefined, 900000);
  expect(actions.map((a) => a.kind)).toEqual(["requestStatus", "notify"]);
  expect(actions.at(-1)?.reason).toContain("evaluator unavailable");
  expect(
    await watchdogTick(
      snapshot(),
      {},
      new WatchdogLedger(l.directory),
      undefined,
      1800000,
    ),
  ).toEqual([]);
});
it.each([
  [
    { latestReport: { status: "waiting", waitingFor: "human" } },
    [],
    [],
    "waitingForPerson",
  ],
  [{}, [{ id: "j", ticketId: "t", status: "running" }], [], "activeJob"],
  [{}, [], [{ state: "offline" }], "endpointOffline"],
])(
  "classifies actual waiting reasons without semantic stall calls",
  async (ticket, jobs, endpoints, classification) => {
    const l = await ledger(),
      s = snapshot({
        tickets: [{ id: "t", lifecycle: "open", revision: 1, ...ticket }],
        jobs,
        endpoints,
      });
    await watchdogTick(s, {}, l, undefined, 0);
    const actions = await watchdogTick(
      s,
      {},
      l,
      async () => {
        throw Error("must not evaluate ordinary wait");
      },
      900000,
    );
    expect(actions).toHaveLength(1);
    expect(actions[0].classification).toBe(classification);
  },
);
it("persists conservative semantic budget and refuses duplicate concurrent ticks", async () => {
  const l = await ledger(),
    config = {
      maxCalls: 1,
      maxTokens: 200,
      maxCost: 0.02,
      callTokens: 200,
      callCost: 0.02,
    };
  await watchdogTick(snapshot(), config, l, undefined, 0);
  const evaluate = async () => ({
    verdict: "stalled" as const,
    reason: "Repeated command has no new artifact",
  });
  const results = await Promise.all([
    watchdogTick(snapshot(), config, l, evaluate, 900000),
    watchdogTick(
      snapshot(),
      config,
      new WatchdogLedger(l.directory),
      evaluate,
      900000,
    ),
  ]);
  expect(results.flat().filter((a) => a.kind === "notify")).toHaveLength(1);
  await watchdogTick(
    snapshot({ tickets: [{ id: "t", lifecycle: "open", revision: 2 }] }),
    config,
    l,
    evaluate,
    900001,
  );
  const later = await watchdogTick(
    snapshot({ tickets: [{ id: "t", lifecycle: "open", revision: 2 }] }),
    config,
    l,
    evaluate,
    1800001,
  );
  expect(later.at(-1)?.reason).toContain("budget exhausted");
  expect((await l.read()).budget.calls).toBe(1);
});
it("records completion once and constrains evaluator output to configured actions", async () => {
  const l = await ledger();
  await watchdogTick(snapshot(), {}, l, undefined, 0);
  const ended = snapshot({
    tickets: [{ id: "t", lifecycle: "ended", revision: 2 }],
  });
  expect(
    (await watchdogTick(ended, {}, l, undefined, 1))[0].classification,
  ).toBe("completed");
  expect(await watchdogTick(ended, {}, l, undefined, 2)).toEqual([]);
});
it("validates separate management principal token references and refuses inline credentials", async () => {
  const { configuredPrincipals } =
    await import("../src/runtime/adapter-config.js");
  process.env.TEST_WATCHDOG_TOKEN = "secret-watchdog";
  expect(
    configuredPrincipals([
      {
        id: "supervisor",
        role: "watchdog",
        tokenEnv: "TEST_WATCHDOG_TOKEN",
        allowPause: false,
      },
    ]),
  ).toEqual([
    {
      id: "supervisor",
      role: "watchdog",
      token: "secret-watchdog",
      allowPause: false,
    },
  ]);
  expect(() =>
    configuredPrincipals([
      { id: "supervisor", role: "watchdog", token: "inline" },
    ]),
  ).toThrow();
  expect(() =>
    configuredPrincipals([
      { id: "supervisor", role: "admin", tokenEnv: "TEST_WATCHDOG_TOKEN" },
    ]),
  ).toThrow();
});
it("runs separate service and watchdog processes with constrained credentials and quiet durable restart", async () => {
  const { openSuperAgent } = await import("../src/runtime/open.js");
  const { accept, alice, withoutSqliteExperimentalWarning } =
    await import("./helpers/scheduling.js");
  const { writeFile, readFile } = await import("node:fs/promises");
  const { spawn } = await import("node:child_process");
  const { once } = await import("node:events");
  const root = await mkdtemp(join(tmpdir(), "sa-watchdog-process-"));
  roots.push(root);
  const directory = join(root, "primary"),
    watchDirectory = join(root, "watchdog");
  const r = await openSuperAgent({ directory });
  await r.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: ["alice"],
      botActors: [],
      rules: [],
    },
    alice,
    { commandId: "p" },
  );
  await accept(r, "work");
  await r.close();
  const serviceConfig = join(root, "service.json");
  await writeFile(
    serviceConfig,
    JSON.stringify({
      directory,
      port: 0,
      managementPrincipals: [
        { id: "watchdog", role: "watchdog", tokenEnv: "TEST_WATCH_TOKEN" },
        { id: "operator", role: "operator", tokenEnv: "TEST_OP_TOKEN" },
      ],
    }),
  );
  const env = {
    ...process.env,
    TEST_WATCH_TOKEN: "watchdog-secret",
    TEST_OP_TOKEN: "operator-secret",
  };
  const service = spawn(
    process.execPath,
    ["dist/cli.js", "--config", serviceConfig, "serve"],
    { env, stdio: ["ignore", "pipe", "pipe"] },
  );
  let stdout = "";
  service.stdout.on("data", (b) => (stdout += b));
  let stderr = "";
  service.stderr.on("data", (b) => (stderr += b));
  try {
    const deadline = Date.now() + 5000;
    while (!stdout.includes("\n") && Date.now() < deadline)
      await new Promise((r) => setTimeout(r, 10));
    if (!stdout) throw Error(stderr || "service did not listen");
    const url = JSON.parse(stdout.trim()).listening;
    for (const route of ["/policy", "/receive", "/tickets/x/retry"])
      expect(
        (
          await fetch(url + route, {
            method: "POST",
            headers: { authorization: "Bearer watchdog-secret" },
            body: "{}",
          })
        ).status,
      ).toBe(403);
    const config = join(root, "watch.json");
    await writeFile(
      config,
      JSON.stringify({
        directory: watchDirectory,
        url,
        tokenEnv: "TEST_WATCH_TOKEN",
        allowInsecureLoopback: true,
        policy: { thresholdMs: 0 },
      }),
    );
    async function runWatch() {
      const child = spawn(
        process.execPath,
        ["dist/watchdog/cli.js", "--config", config, "--once"],
        { env, stdio: ["ignore", "pipe", "pipe"] },
      );
      let output = "",
        errors = "";
      child.stdout.on("data", (b) => (output += b));
      child.stderr.on("data", (b) => (errors += b));
      const [code] = await once(child, "exit");
      expect(code, errors).toBe(0);
      expect(withoutSqliteExperimentalWarning(errors)).toBe("");
      return output;
    }
    expect(
      JSON.parse((await Promise.all([runWatch(), runWatch()])).join("").trim())
        .kind,
    ).toBe("notify");
    expect(await runWatch()).toBe("");
    await expect(openSuperAgent({ directory })).rejects.toThrow(/lock/i);
    await expect(
      readFile(join(watchDirectory, "session.sqlite")),
    ).rejects.toThrow();
    expect(
      (await new WatchdogLedger(watchDirectory).read()).actions,
    ).toBeTruthy();
  } finally {
    const exited = once(service, "exit");
    service.kill("SIGTERM");
    await exited;
  }
}, 15000);
it("uses the optional real pi model API with a controlled faux provider and reserved budget", async () => {
  const { semanticEvaluator } = await import("../src/watchdog/semantic.js");
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage } =
    await import("@earendil-works/pi-ai/providers/faux");
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  faux.setResponses([
    fauxAssistantMessage(
      JSON.stringify({
        verdict: "stalled",
        reason: "The same work has no new evidence.",
      }),
    ),
  ]);
  const evaluate = semanticEvaluator(models, {
    provider: faux.getModel().provider,
    modelId: faux.getModel().id,
  });
  const l = await ledger(),
    config = {
      maxCalls: 1,
      maxTokens: 8192,
      maxCost: 1,
      callTokens: 8192,
      callCost: 1,
      allowPause: true,
    };
  await watchdogTick(snapshot(), config, l, evaluate, 0);
  expect(
    (await watchdogTick(snapshot(), config, l, evaluate, 900000)).map(
      (a) => a.kind,
    ),
  ).toEqual(["pause", "notify"]);
  expect((await l.read()).budget).toEqual({ calls: 1, tokens: 8192, cost: 1 });
});
it.each([
  [{ status: "waiting", waitingFor: "endpoint" }, "waitingForEndpoint"],
  [{ status: "waiting", waitingFor: "input" }, "waitingForInput"],
  [{ status: "ready" }, "reviewable"],
  [
    { status: "progress", questions: [{ id: "q", text: "Choose an option" }] },
    "waitingForPerson",
  ],
])(
  "does not diagnose a legitimate typed or review wait as a stall",
  async (report, classification) => {
    const l = await ledger(),
      s = snapshot({
        tickets: [
          { id: "t", lifecycle: "open", revision: 1, latestReport: report },
        ],
      });
    await watchdogTick(
      s,
      {
        allowPause: true,
        maxCalls: 1,
        maxTokens: 1000,
        maxCost: 1,
        callTokens: 1000,
        callCost: 1,
      },
      l,
      undefined,
      0,
    );
    const actions = await watchdogTick(
      s,
      {
        allowPause: true,
        maxCalls: 1,
        maxTokens: 1000,
        maxCost: 1,
        callTokens: 1000,
        callCost: 1,
      },
      l,
      async () => ({ verdict: "stalled", reason: "must not be consulted" }),
      900000,
    );
    expect(actions).toHaveLength(1);
    expect(actions[0]).toMatchObject({ kind: "notify", classification });
    expect((await l.read()).budget.calls).toBe(0);
  },
);
