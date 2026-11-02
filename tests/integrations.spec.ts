import { afterEach, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import { spawnSync } from "node:child_process";
import { openSuperAgent } from "../src/runtime/open.js";
import * as api from "../src/api/server.js";
import * as gh from "../src/sources/github.js";
const resources: (() => Promise<any>)[] = [];
afterEach(async () => {
  for (const close of resources.splice(0).reverse()) await close();
});
async function setup(maxPending?: number) {
  const directory = await mkdtemp(join(tmpdir(), "sa-integration-"));
  resources.push(() => rm(directory, { recursive: true, force: true }));
  const r = await openSuperAgent({ directory, maxPending });
  resources.push(() => r.close());
  return { r, directory };
}
const policy = {
  version: 1,
  acceptMode: "auto" as const,
  allowedActors: ["operator", "github:alice"],
  botActors: [],
  rules: [],
};
const operator = { id: "operator", kind: "human" as const, trusted: true };
const payload = (patch: any = {}) => ({
  action: "opened",
  repository: { full_name: "acme/repo" },
  sender: { login: "alice", type: "User" },
  issue: {
    id: 1,
    number: 1,
    title: "Fix export",
    body: "Please fix",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  },
  ...patch,
});
const raw = (p: any) => Buffer.from(JSON.stringify(p));
const headers = (p: Buffer, event = "issues", delivery = "d1") => ({
  "x-hub-signature-256":
    "sha256=" + createHmac("sha256", "secret").update(p).digest("hex"),
  "x-github-event": event,
  "x-github-delivery": delivery,
});
async function github(r: any, extra: any = {}) {
  const adapter = new gh.GitHubSource({
    sourceId: "github",
    repositories: ["acme/repo"],
    webhookSecret: "secret",
    ...extra,
  });
  await adapter.configure(r);
  return adapter;
}
async function server(r: any, options: any = {}) {
  const s = await api.startManagementServer(r, {
    port: 0,
    token: "operator-secret",
    ...options,
  });
  resources.push(() => s.close());
  return s;
}
it("requires explicit management credentials", async () => {
  const { r } = await setup();
  await expect(
    api.startManagementServer(r, { port: 0, token: "" }),
  ).rejects.toThrow(/token/i);
});
it("authenticates before malformed JSON and restricts methods and body size", async () => {
  const { r } = await setup();
  const s = await server(r);
  const url = s.address;
  expect(
    (await fetch(url + "/receive", { method: "POST", body: "{" })).status,
  ).toBe(401);
  expect(
    (
      await fetch(url + "/health", {
        method: "POST",
        headers: { authorization: "Bearer operator-secret" },
        body: "{}",
      })
    ).status,
  ).toBe(405);
  expect(
    (
      await fetch(url + "/receive", {
        method: "POST",
        headers: { authorization: "Bearer operator-secret" },
        body: "x".repeat(1048577),
      })
    ).status,
  ).toBe(413);
  expect(
    (
      await fetch(url + "/health", {
        headers: { authorization: "Bearer operator-secret" },
      })
    ).status,
  ).toBe(200);
});
it("server derives intake actor and enforces viewer/watchdog whitelist", async () => {
  const { r } = await setup();
  await r.updatePolicy(policy, operator, { commandId: "p" });
  const s = await server(r, {
    principals: [
      { token: "view", role: "viewer", id: "viewer" },
      { token: "watch", role: "watchdog", id: "watchdog" },
    ],
  });
  for (const token of ["view", "watch"])
    for (const route of ["receive", "policy", "tickets/x/retry"])
      expect(
        (
          await fetch(s.address + "/" + route, {
            method: "POST",
            headers: { authorization: "Bearer " + token },
            body: "{}",
          })
        ).status,
      ).toBe(403);
  const event = {
    sourceId: "cli",
    externalId: "one",
    revision: "1",
    actor: { id: "forged", kind: "system", trusted: false },
    threads: ["one"],
    body: "fix",
    receivedAt: 1,
    occurredAt: 1,
    addressed: true,
  };
  expect(
    (
      await fetch(s.address + "/receive", {
        method: "POST",
        headers: { authorization: "Bearer operator-secret" },
        body: JSON.stringify(event),
      })
    ).status,
  ).toBe(200);
  expect((await r.list<any>("trigger"))[0].actor).toEqual(operator);
  expect(
    (
      await fetch(s.address + "/reconcile", {
        method: "POST",
        headers: { authorization: "Bearer watch" },
        body: "{}",
      })
    ).status,
  ).toBe(200);
});
it("redacts credential keys and bearer strings from safe management exports", async () => {
  const { r } = await setup();
  await r.engine.transaction((tx) =>
    r.engine.store.put(tx, "ticket", "x", {
      id: "x",
      attention: "Bearer hidden",
      token: "secret",
      latestReport: { credentialRef: "env:TOKEN" },
    }),
  );
  const s = await server(r);
  const text = await (
    await fetch(s.address + "/tickets", {
      headers: { authorization: "Bearer operator-secret" },
    })
  ).text();
  expect(text).not.toContain("secret");
  expect(text).not.toContain("hidden");
  expect(text).toContain("env:TOKEN");
});
it("bad webhook signature and disallowed repository write nothing", async () => {
  const { r } = await setup();
  const a = await github(r);
  const p = raw(payload());
  await expect(
    a.webhook(r, p, { "x-hub-signature-256": "sha256=bad" }),
  ).rejects.toThrow(/signature/i);
  const wrong = raw(payload({ repository: { full_name: "evil/repo" } }));
  await expect(a.webhook(r, wrong, headers(wrong))).rejects.toThrow(
    /repository/i,
  );
  expect(await r.list("trigger")).toEqual([]);
});
it("deduplicates semantic payload despite unsigned delivery changes and source qualifies actors", async () => {
  const { r } = await setup();
  const a = await github(r);
  const p = raw(payload({ sender: { login: "operator", type: "User" } }));
  await a.webhook(r, p, headers(p));
  await a.webhook(r, p, headers(p, "issues", "d2"));
  expect(await r.list<any>("trigger")).toHaveLength(1);
  expect((await r.list<any>("trigger"))[0].actor.id).toBe("github:operator");
});
it("suppresses own send markers and respects source listening/disabled state", async () => {
  const { r } = await setup();
  const a = await github(r);
  const p = raw(
    payload({
      comment: {
        id: 2,
        body: "answer " + a.marker("own"),
        created_at: "2026-01-01T00:00:00Z",
      },
    }),
  );
  await a.webhook(r, p, headers(p, "issue_comment"));
  expect(await r.list("trigger")).toEqual([]);
  await r.engine.configureSource({
    id: "github",
    kind: "github",
    enabled: false,
  });
  const q = raw(payload());
  await a.webhook(r, q, headers(q));
  expect(await r.list("trigger")).toEqual([]);
});
it("webhook backpressure is retryable and does not commit receipt or polling cursor", async () => {
  const { r } = await setup(1);
  await r.updatePolicy(policy, operator, { commandId: "p" });
  const a = await github(r);
  const p = raw(payload());
  await a.webhook(r, p, headers(p));
  const q = raw(payload({ issue: { ...payload().issue, id: 2, number: 2 } }));
  expect((await a.webhook(r, q, headers(q, "issues", "d2"))).disposition).toBe(
    "backpressure",
  );
  expect(await r.list("githubDelivery")).toHaveLength(1);
  expect(
    (
      await a.receivePoll(
        r,
        [
          {
            event: "pull_request",
            payload: payload({
              action: "closed",
              issue: undefined,
              pull_request: {
                id: 2,
                number: 2,
                state: "closed",
                updated_at: "2026-01-01T00:01:00Z",
              },
            }),
          },
        ],
        "next",
      )
    ).disposition,
  ).toBe("backpressure");
  expect(
    (await r.engine.store.read<any>("source", "github"))?.cursor,
  ).toBeUndefined();
  expect(
    await r.engine.store.read("sourceThreadState", "github:acme/repo#2"),
  ).toBeUndefined();
});
it("poll intake and cursor roll back together and deduplicate on retry", async () => {
  const { r } = await setup();
  const a = await github(r);
  await a.receivePoll(r, [{ event: "issues", payload: payload() }], "cursor1");
  await a.receivePoll(r, [{ event: "issues", payload: payload() }], "cursor2");
  expect(await r.list("trigger")).toHaveLength(1);
  expect((await r.engine.store.read<any>("source", "github")).cursor).toBe(
    "cursor2",
  );
});
it("HTTP webhook returns durable errors and never acknowledges capacity exhaustion", async () => {
  const { r } = await setup(1);
  await r.updatePolicy(policy, operator, { commandId: "p" });
  const a = await github(r);
  const s = await server(r, { github: a });
  const p = raw(payload());
  expect(
    (
      await fetch(s.address + "/webhooks/github", {
        method: "POST",
        headers: headers(p),
        body: p,
      })
    ).status,
  ).toBe(200);
  const q = raw(payload({ issue: { ...payload().issue, id: 2, number: 2 } }));
  expect(
    (
      await fetch(s.address + "/webhooks/github", {
        method: "POST",
        headers: headers(q, "issues", "d2"),
        body: q,
      })
    ).status,
  ).toBe(503);
});
it("GitHub transport uses real local HTTP marker receipts and no invented idempotency header", async () => {
  const requests: any[] = [];
  const comments: any[] = [];
  const fixture = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    requests.push({
      method: req.method,
      path: req.url,
      headers: req.headers,
      body,
    });
    res.setHeader("content-type", "application/json");
    if (req.method === "POST") {
      comments.push({ id: 10, body: JSON.parse(body).body });
      res.statusCode = 201;
      res.end(JSON.stringify(comments[0]));
    } else res.end(JSON.stringify(comments));
  });
  await new Promise<void>((resolve) => fixture.listen(0, "127.0.0.1", resolve));
  resources.push(
    () => new Promise<void>((resolve) => fixture.close(() => resolve())),
  );
  const { r } = await setup();
  const a = await github(r, {
    apiBase: `http://127.0.0.1:${(fixture.address() as any).port}`,
    apiToken: "private",
  });
  expect(await a.queryReceipt("k")).toEqual({ status: "unknown" });
  const message: any = {
    destination: { sourceId: "github", threadRef: "acme/repo#1" },
    payload: { content: "Result" },
    purpose: "answer",
  };
  await a.send(message, "k");
  expect((await a.queryReceipt("k")).status).toBe("found");
  expect(
    requests.find((x) => x.method === "POST").headers["idempotency-key"],
  ).toBeUndefined();
  expect(requests.find((x) => x.method === "POST").path).toBe(
    "/repos/acme/repo/issues/1/comments",
  );
});
it("credential-free demo subprocess reports native Router Worker feedback and distinct task", async () => {
  const { directory } = await setup();
  const result = spawnSync(
    process.execPath,
    ["dist/cli.js", "--directory", directory + "-demo", "demo"],
    { encoding: "utf8", timeout: 20000, env: { PATH: process.env.PATH } },
  );
  resources.push(() =>
    rm(directory + "-demo", { recursive: true, force: true }),
  );
  expect(result.status, result.stderr).toBe(0);
  const output = JSON.parse(result.stdout);
  expect(output.demo).toBe("native-faux");
  expect(output.tickets).toHaveLength(2);
  expect(output.feedbackSameTicket).toBe(true);
  expect(
    output.tickets.every((x: any) => x.latestReport?.status === "ready"),
  ).toBe(true);
});

it("ready PR stays open; authenticated merge waits for unread work then naturally ends and human feedback resumes the same Ticket", async () => {
  const { r, directory } = await setup();
  await r.updatePolicy(policy, operator, { commandId: "p" });
  const a = await github(r, { addressed: true });
  const pr = (action: string, time: string, state = "open") =>
    payload({
      action,
      issue: undefined,
      pull_request: {
        id: 5,
        number: 5,
        title: "Fix",
        body: "Fix export",
        state,
        merged: state === "closed",
        created_at: "2026-01-01T00:00:00Z",
        updated_at: time,
      },
    });
  const opened = raw(pr("opened", "2026-01-01T00:00:00Z"));
  await a.webhook(r, opened, headers(opened, "pull_request"));
  const candidate = (await r.listCandidates())[0];
  await r.decide(
    candidate.id,
    candidate.epoch,
    { kind: "accept", goal: "PR fix", ruleIds: [] },
    { commandId: "accept" },
  );
  const [ticket] = await r.list<any>("ticket");
  await r.engine.trackThread(ticket.id, "github", "acme/repo#5", {
    commandId: "track",
  });
  await r.close();
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } = await import(
    "@earendil-works/pi-ai/providers/faux"
  );
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  const response = async (ctx: any) => {
    if (ctx.messages.at(-1)?.role !== "user")
      return fauxAssistantMessage("Done");
    const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
    const input = JSON.parse(
      typeof user.content === "string" ? user.content : user.content[0].text,
    );
    if (!input.inputId) return fauxAssistantMessage("No decisions");
    return fauxAssistantMessage(
      fauxToolCall("report", {
        reportId: input.inputId,
        assignmentId: input.assignmentId,
        epoch: input.epoch,
        inputIds: [input.inputId],
        status: "ready",
        content: "Ready for review",
      }),
      { stopReason: "toolUse" },
    );
  };
  faux.setResponses(Array.from({ length: 100 }, () => response));
  let active = await openSuperAgent({
    directory,
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
  });
  resources.push(() => active.close());
  await active.agents!.waitForIdle();
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("open");
  await active.close();
  active = await openSuperAgent({ directory });
  const comment = raw(
    payload({
      issue: { ...payload().issue, number: 5 },
      comment: {
        id: 8,
        body: "Use the filename result.csv",
        created_at: "2026-01-01T00:01:00Z",
        updated_at: "2026-01-01T00:01:00Z",
      },
    }),
  );
  await a.webhook(
    active,
    comment,
    headers(comment, "issue_comment", "feedback"),
  );
  const merged = raw(pr("closed", "2026-01-01T00:02:00Z", "closed"));
  await a.webhook(active, merged, headers(merged, "pull_request", "merged"));
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("open");
  expect(
    await active.engine.store.read(
      "sourceThreadState",
      `${"github"}:acme/repo#5`,
    ),
  ).toMatchObject({ state: "closed" });
  await active.close();
  active = await openSuperAgent({
    directory,
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
  });
  await active.agents!.waitForIdle();
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("ended");
  expect(
    (await active.engine.store.read<any>("mandate", ticket.mandateId))
      .withdrawnAt,
  ).toBeNull();
  const bot = raw(
    payload({
      sender: { login: "ci[bot]", type: "Bot" },
      issue: { ...payload().issue, number: 5 },
      comment: {
        id: 9,
        body: "Bot result",
        created_at: "2026-01-01T00:03:00Z",
      },
    }),
  );
  await a.webhook(active, bot, headers(bot, "issue_comment", "bot"));
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("ended");
  const human = raw(
    payload({
      issue: { ...payload().issue, number: 5 },
      comment: {
        id: 10,
        body: "Please adjust more",
        created_at: "2026-01-01T00:04:00Z",
      },
    }),
  );
  await a.webhook(active, human, headers(human, "issue_comment", "human"));
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("open");
  expect(await active.list("ticket")).toHaveLength(1);
  await active.stopTicket(ticket.id, operator, { commandId: "stop" });
  const later = raw(
    payload({
      issue: { ...payload().issue, number: 5 },
      comment: {
        id: 11,
        body: "More feedback",
        created_at: "2026-01-01T00:05:00Z",
      },
    }),
  );
  await a.webhook(active, later, headers(later, "issue_comment", "later"));
  expect((await active.readTicket<any>(ticket.id)).lifecycle).toBe("stopped");
});
it("suppresses own PR text without losing independently authenticated closure facts", async () => {
  const { r } = await setup();
  const a = await github(r);
  const p = raw(
    payload({
      action: "closed",
      issue: undefined,
      pull_request: {
        id: 15,
        number: 15,
        state: "closed",
        body: a.marker("own"),
        updated_at: "2026-01-01T00:00:00Z",
      },
    }),
  );
  await a.webhook(r, p, headers(p, "pull_request"));
  expect(await r.list("trigger")).toHaveLength(0);
  expect(
    await r.engine.store.read("sourceThreadState", "github:acme/repo#15"),
  ).toMatchObject({ state: "closed" });
});
it("trusted PR binding enforces configured repository and fresh original pr.create authority", async () => {
  const { r } = await setup();
  await r.updatePolicy(
    {
      ...policy,
      explicitCapabilities: ["repo.read", "channel.reply", "pr.create"],
    },
    operator,
    { commandId: "p" },
  );
  const a = await github(r, { addressed: true });
  const p = raw(payload());
  await a.webhook(r, p, headers(p));
  const c = (await r.listCandidates())[0];
  await r.decide(
    c.id,
    c.epoch,
    { kind: "accept", goal: "Create PR", ruleIds: [] },
    { commandId: "accept" },
  );
  const [t] = await r.list<any>("ticket");
  await expect(
    a.trackPullRequest(r, t.id, "evil/repo#2", { commandId: "evil" }),
  ).rejects.toThrow(/repository|destination/i);
  await a.trackPullRequest(r, t.id, "acme/repo#2", { commandId: "safe" });
  expect(
    (await r.engine.store.read<any>("mandate", t.mandateId)).scope,
  ).toContain("acme/repo#2");
  expect(
    (await r.list<any>("subscription")).some(
      (s) => s.ticketId === t.id && s.own,
    ),
  ).toBe(true);
  await r.updatePolicy(
    {
      ...policy,
      version: 2,
      explicitCapabilities: ["repo.read", "channel.reply"],
    },
    operator,
    { commandId: "p2" },
  );
  await expect(
    a.trackPullRequest(r, t.id, "acme/repo#3", { commandId: "revoked" }),
  ).rejects.toThrow(/capability|authority/i);
});
it("configured watchdog pause only revokes an existing Ticket and cannot retry stopped work", async () => {
  const { r } = await setup();
  await r.updatePolicy(policy, operator, { commandId: "p" });
  const event = {
    sourceId: "cli",
    externalId: "pause",
    revision: "1",
    actor: operator,
    threads: ["pause"],
    body: "fix",
    receivedAt: 1,
    occurredAt: 1,
    addressed: true,
  };
  await r.receive(event);
  const c = (await r.listCandidates())[0];
  await r.decide(
    c.id,
    c.epoch,
    { kind: "accept", goal: "pause", ruleIds: [] },
    { commandId: "accept" },
  );
  const [t] = await r.list<any>("ticket");
  const s = await server(r, {
    principals: [
      { token: "watch", role: "watchdog", id: "watchdog", allowPause: true },
    ],
  });
  const options = {
    method: "POST",
    headers: { authorization: "Bearer watch" },
    body: JSON.stringify({ commandId: "pause", actor: operator }),
  };
  expect(
    (
      await fetch(
        s.address + `/tickets/${encodeURIComponent(t.id)}/pause`,
        options,
      )
    ).status,
  ).toBe(200);
  expect((await r.readTicket<any>(t.id)).lifecycle).toBe("stopped");
  expect(
    (
      await fetch(
        s.address + `/tickets/${encodeURIComponent(t.id)}/retry`,
        options,
      )
    ).status,
  ).toBe(403);
  expect((await fetch(s.address + "/reconcile", options)).status).toBe(200);
  expect((await r.readTicket<any>(t.id)).lifecycle).toBe("stopped");
});
it("native service subprocess serves authenticated health and closes on SIGTERM", async () => {
  const { directory } = await setup();
  const { spawn } = await import("node:child_process");
  const child = spawn(
    process.execPath,
    [
      "dist/cli.js",
      "--directory",
      directory + "-service",
      "--port",
      "0",
      "--token-env",
      "TEST_TOKEN",
      "serve",
    ],
    {
      env: { PATH: process.env.PATH, TEST_TOKEN: "subprocess-secret" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stdout = "",
    stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  resources.push(async () => {
    if (child.exitCode === null) child.kill("SIGKILL");
    await rm(directory + "-service", { recursive: true, force: true });
  });
  for (let i = 0; i < 500 && !stdout.includes("\n"); i++)
    await new Promise((resolve) => setTimeout(resolve, 5));
  expect(stdout, stderr).toContain("listening");
  const address = JSON.parse(stdout.split("\n")[0]).listening;
  expect(
    (
      await fetch(address + "/health", {
        headers: { authorization: "Bearer subprocess-secret" },
      })
    ).status,
  ).toBe(200);
  expect(stdout).not.toContain("subprocess-secret");
  const exited = new Promise<number | null>((resolve) =>
    child.once("exit", resolve),
  );
  child.kill("SIGTERM");
  expect(await exited).toBe(0);
});
it("closed-before-ready waits for current generation, every tracked PR, and registered active execution", async () => {
  const { r, directory } = await setup();
  await r.updatePolicy(
    {
      ...policy,
      explicitCapabilities: ["repo.read", "channel.reply", "pr.create"],
    },
    operator,
    { commandId: "p" },
  );
  const a = await github(r, { addressed: true });
  const p = raw(payload());
  await a.webhook(r, p, headers(p));
  const c = (await r.listCandidates())[0];
  await r.decide(
    c.id,
    c.epoch,
    { kind: "accept", goal: "all settled", ruleIds: [] },
    { commandId: "accept" },
  );
  const [t] = await r.list<any>("ticket");
  await a.trackPullRequest(r, t.id, "acme/repo#2", { commandId: "track2" });
  await a.trackPullRequest(r, t.id, "acme/repo#3", { commandId: "track3" });
  await r.close();
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } = await import(
    "@earendil-works/pi-ai/providers/faux"
  );
  const { trackExecution, recordExecutionReceipt } = await import(
    "../src/runtime/reconcile.js"
  );
  const { until } = await import("./helpers/scheduling.js");
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  let before = false,
    after = false;
  let releaseBefore!: () => void, releaseAfter!: () => void;
  const beforeGate = new Promise<void>((resolve) => (releaseBefore = resolve)),
    afterGate = new Promise<void>((resolve) => (releaseAfter = resolve));
  resources.push(async () => {
    releaseBefore();
    releaseAfter();
  });
  faux.setResponses(
    Array.from({ length: 100 }, () => async (ctx: any) => {
      const router = ctx.messages.some(
        (m: any) =>
          m.role === "system" &&
          m.toolsAdded?.some((x: any) => x.name === "list_candidates"),
      );
      if (router) return fauxAssistantMessage("No decisions");
      if (ctx.messages.at(-1)?.role === "user") {
        before = true;
        await beforeGate;
        const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
        const input = JSON.parse(
          typeof user.content === "string"
            ? user.content
            : user.content[0].text,
        );
        return fauxAssistantMessage(
          fauxToolCall("report", {
            reportId: input.inputId,
            assignmentId: input.assignmentId,
            epoch: input.epoch,
            inputIds: [input.inputId],
            status: "ready",
            content: "Ready",
          }),
          { stopReason: "toolUse" },
        );
      }
      after = true;
      await afterGate;
      return fauxAssistantMessage("Finished generation");
    }),
  );
  const active = await openSuperAgent({
    directory,
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
  });
  resources.push(async () => {
    releaseBefore();
    releaseAfter();
    await active.close();
  });
  await until(async () => before);
  const close = async (number: number, id: string) => {
    const p = raw(
      payload({
        action: "closed",
        issue: undefined,
        pull_request: {
          id: number,
          number,
          state: "closed",
          merged: true,
          body: a.marker("own"),
          updated_at: "2026-01-01T00:02:00Z",
        },
      }),
    );
    await a.webhook(active, p, headers(p, "pull_request", id));
  };
  await close(2, "closed2");
  expect((await active.readTicket<any>(t.id)).latestReport).toBeUndefined();
  expect((await active.readTicket<any>(t.id)).lifecycle).toBe("open");
  releaseBefore();
  await until(async () => after);
  expect((await active.readTicket<any>(t.id)).latestReport.status).toBe(
    "ready",
  );
  expect((await active.readTicket<any>(t.id)).lifecycle).toBe("open");
  const execution = {
    id: "registered-job",
    ticketId: t.id,
    assignmentId: t.assignmentId,
    epoch: t.assignmentEpoch,
    status: "running" as const,
  };
  await active.engine.transaction((tx) =>
    trackExecution(active.engine, tx, execution),
  );
  await close(3, "closed3");
  expect((await active.readTicket<any>(t.id)).lifecycle).toBe("open");
  releaseAfter();
  await active.agents!.waitForIdle();
  expect((await active.readTicket<any>(t.id)).lifecycle).toBe("open");
  await active.engine.transaction((tx) =>
    recordExecutionReceipt(active.engine, tx, {
      ...execution,
      status: "completed",
    }),
  );
  await active.agents!.pump();
  await active.agents!.waitForIdle();
  expect((await active.readTicket<any>(t.id)).lifecycle).toBe("ended");
});
it("conflicting delivery replay rolls back intake and old close facts cannot overwrite a newer reopen", async () => {
  const { r } = await setup();
  const a = await github(r);
  const pr = (state: string, time: string) =>
    payload({
      action: state === "closed" ? "closed" : "reopened",
      issue: undefined,
      pull_request: { id: 4, number: 4, state, body: "PR", updated_at: time },
    });
  const closed = raw(pr("closed", "2026-01-01T00:00:00Z"));
  await a.webhook(r, closed, headers(closed, "pull_request", "close"));
  const reopened = raw(pr("open", "2026-01-01T00:01:00Z"));
  await expect(
    a.webhook(r, reopened, headers(reopened, "pull_request", "close")),
  ).rejects.toThrow(/conflict/i);
  expect(await r.list("trigger")).toHaveLength(1);
  await a.webhook(r, reopened, headers(reopened, "pull_request", "reopen"));
  await a.webhook(r, closed, headers(closed, "pull_request", "old-close"));
  expect(
    await r.engine.store.read("sourceThreadState", "github:acme/repo#4"),
  ).toMatchObject({ state: "open" });
});
it("normalizes signed review and CI events, handles ghost conservatively, and verifies raw Unicode bytes", async () => {
  const { r } = await setup();
  const a = await github(r);
  const unicode = Buffer.from(
    "  " +
      JSON.stringify(
        payload({ issue: { ...payload().issue, body: "Fix 导出" } }),
      ) +
      "\n",
  );
  await a.webhook(r, unicode, headers(unicode));
  const review = raw(
    payload({
      issue: undefined,
      pull_request: { number: 1 },
      review: {
        id: 30,
        body: "Please adjust",
        state: "changes_requested",
        submitted_at: "2026-01-01T00:00:00Z",
      },
    }),
  );
  await a.webhook(r, review, headers(review, "pull_request_review", "review"));
  for (const status of ["queued", "in_progress", "completed"]) {
    const check = raw(
      payload({
        sender: { login: "ci[bot]", type: "Bot" },
        issue: undefined,
        check_run: {
          id: 31,
          status,
          conclusion: status === "completed" ? "success" : null,
          head_sha: "abc",
          pull_requests: [{ number: 1 }, { number: 2 }],
        },
      }),
    );
    await a.webhook(r, check, headers(check, "check_run", "ci-" + status));
  }
  const ghost = raw(
    payload({
      sender: { login: "ghost", type: "User" },
      issue: { ...payload().issue, id: 7, number: 7 },
    }),
  );
  await a.webhook(r, ghost, headers(ghost, "issues", "ghost"));
  const triggers = await r.list<any>("trigger");
  expect(triggers).toHaveLength(6);
  expect(
    triggers.find((t) => t.actor.id === "github:ghost").actor,
  ).toMatchObject({ kind: "system", trusted: false });
  expect(
    triggers
      .filter((t) => t.actor.kind === "bot")
      .every((t) => t.threads.includes("acme/repo#2")),
  ).toBe(true);
  expect(triggers[0].body).toContain("导出");
});
it("GitHub close aborts a stalled response body instead of leaving an unbounded transport", async () => {
  let started!: () => void;
  const entered = new Promise<void>((resolve) => (started = resolve));
  const fixture = createServer((req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.write("[");
    started();
  });
  await new Promise<void>((resolve) => fixture.listen(0, "127.0.0.1", resolve));
  resources.push(
    () =>
      new Promise<void>((resolve) => {
        fixture.closeAllConnections();
        fixture.close(() => resolve());
      }),
  );
  const { r } = await setup();
  const a = await github(r, {
    apiBase: `http://127.0.0.1:${(fixture.address() as any).port}`,
  });
  const query = a.queryReceipt("k");
  await entered;
  a.close();
  expect(await query).toEqual({ status: "unknown" });
});

it("registers only the selected official pi provider without resolving credentials", async () => {
  const { configuredModels } = await import("../src/cli.js");
  const { openaiProvider } = await import(
    "@earendil-works/pi-ai/providers/openai"
  );
  const catalog = openaiProvider().getModels();
  expect(catalog.length).toBeGreaterThan(0);
  const configured = await configuredModels({
    provider: "openai",
    model: catalog[0].id,
    modelKeyEnv: "UNSET_TEST_MODEL_KEY",
  });
  expect(
    configured.models!.getProviders().map((provider) => provider.id),
  ).toEqual(["openai"]);
  expect(configured.model).toEqual({
    provider: "openai",
    modelId: catalog[0].id,
  });
  await expect(
    configuredModels({ provider: "openai", model: "missing-model" }),
  ).rejects.toThrow(/catalog/i);
});
it("service config activates the existing local broker and rejects unknown environment keys/types before opening", async () => {
  const { directory } = await setup();
  const { writeFile } = await import("node:fs/promises");
  const { openaiProvider } = await import(
    "@earendil-works/pi-ai/providers/openai"
  );
  const { spawn } = await import("node:child_process");
  const file = join(directory, "service.json");
  const serviceDirectory = join(directory, "configured-runtime");
  const config = {
    directory: serviceDirectory,
    port: 0,
    tokenEnv: "TEST_TOKEN",
    provider: "openai",
    model: openaiProvider().getModels()[0].id,
    environment: {
      repository: directory,
      directory: join(directory, "broker"),
      trustedLocalShell: false,
    },
  };
  await writeFile(file, JSON.stringify(config));
  const child = spawn(
    process.execPath,
    ["dist/cli.js", "--config", file, "run"],
    {
      env: { PATH: process.env.PATH, TEST_TOKEN: "configured-secret" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stdout = "",
    stderr = "";
  child.stdout.on("data", (data) => (stdout += data));
  child.stderr.on("data", (data) => (stderr += data));
  resources.push(async () => {
    if (child.exitCode === null) {
      const done = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGKILL");
      await done;
    }
  });
  for (
    let i = 0;
    i < 500 && !stdout.includes("\n") && child.exitCode === null;
    i++
  )
    await new Promise((resolve) => setTimeout(resolve, 5));
  expect(stdout, stderr).toContain("listening");
  const address = JSON.parse(stdout.split("\n")[0]).listening;
  const status = await (
    await fetch(address + "/status", {
      headers: { authorization: "Bearer configured-secret" },
    })
  ).json();
  expect(status.environmentConfigured).toBe(true);
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  expect(await exited).toBe(0);
  for (const environment of [
    { repository: directory, trustedLocalShell: "true" },
    { repository: directory, arbitrary: true },
    { repository: "" },
  ]) {
    await writeFile(file, JSON.stringify({ ...config, environment }));
    const result = spawnSync(
      process.execPath,
      ["dist/cli.js", "--config", file, "run"],
      {
        env: { PATH: process.env.PATH, TEST_TOKEN: "configured-secret" },
        encoding: "utf8",
        timeout: 5000,
      },
    );
    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/environment/i);
  }
});
