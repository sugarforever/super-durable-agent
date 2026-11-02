import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm, realpath } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { openSuperAgent } from "../src/runtime/open.js";
const alice = { id: "alice", kind: "human" as const, trusted: true };
const event = (id: string, patch = {}) => ({
  sourceId: "cli",
  externalId: id,
  revision: "1",
  actor: alice,
  threads: [id],
  body: id,
  receivedAt: 1000,
  occurredAt: 900,
  addressed: true,
  ...patch,
});
const dirs: string[] = [];
const runtimes: any[] = [];
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((r) => r.close()));
  await Promise.all(
    dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })),
  );
});
async function setup() {
  const directory = await mkdtemp(join(tmpdir(), "sa-conversation-"));
  dirs.push(directory);
  const r = await openSuperAgent({ directory });
  runtimes.push(r);
  await r.updatePolicy(
    {
      version: 1,
      acceptMode: "auto",
      allowedActors: ["alice"],
      botActors: [],
      rules: [],
    },
    alice,
    { commandId: "policy" },
  );
  return { r, directory };
}
async function accept(r: any, id: string, body = id) {
  const received = await r.receive(event(id, { body }));
  const c = (await r.listCandidates()).find((c: any) =>
    c.triggerIds.includes(received.triggerId),
  );
  await r.decide(
    c.id,
    c.epoch,
    { kind: "accept", goal: id, ruleIds: [] },
    { commandId: `accept-${id}` },
  );
  return (await r.list("ticket")).find((t: any) => t.goal === id);
}
async function until(fn: () => Promise<boolean>) {
  for (let i = 0; i < 500; i++) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 1));
  }
  throw new Error("Controlled condition did not settle");
}
const userText = (ctx: any) => {
  const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
  return typeof user?.content === "string"
    ? user.content
    : (user?.content?.[0]?.text ?? "");
};
const isRouter = (ctx: any) =>
  ctx.messages.some(
    (m: any) =>
      m.role === "system" &&
      m.toolsAdded?.some((t: any) => t.name === "list_candidates"),
  );
async function activate(
  r: any,
  directory: string,
  responses: any[] = Array.from({ length: 100 }, () =>
    fauxAssistantMessage("Waiting for structured work."),
  ),
) {
  await r.close();
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux.provider);
  faux.setResponses(responses);
  const model = {
    provider: faux.getModel().provider,
    modelId: faux.getModel().id,
  };
  const active = await openSuperAgent({ directory, models, model });
  runtimes.push(active);
  return { active, faux, models, model };
}
describe("native Router, ownerless Workers and durable delivery", () => {
  it("keeps the kernel model-free and activates an explicit Router tool allowlist", async () => {
    const { r, directory } = await setup();
    expect(r.agents).toBeUndefined();
    const { active } = await activate(r, directory);
    expect(active.agents).toBeDefined();
    const router = await active.harness.conversation(
      active.agents.routerConversationId,
      active.engine.context,
    );
    expect(
      (await router.agent(active.engine.context)).tools
        .map((t: any) => t.name)
        .sort(),
    ).toEqual([
      "decide_batch",
      "list_candidates",
      "note_worker",
      "recall",
      "request_status",
    ]);
  });
  it("atomically binds one Worker per Ticket and isolates its context from Router and siblings", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a", "A secret"),
      b = await accept(r, "b", "B secret");
    const { active } = await activate(r, directory);
    const agents = active.agents;
    const ids = await Promise.all(
      Array.from({ length: 6 }, () => agents.ensureWorker(a.id)),
    );
    expect(new Set(ids).size).toBe(1);
    const bid = await agents.ensureWorker(b.id);
    expect(bid).not.toBe(ids[0]);
    await agents.waitForIdle();
    const conv = await active.harness.conversation(
      ids[0],
      active.engine.context,
    );
    const text = JSON.stringify(
      (await conv.context(active.engine.context)).messages,
    );
    expect(text).toContain("A secret");
    expect(text).not.toContain("B secret");
    expect(
      (await conv.agent(active.engine.context)).tools
        .map((t: any) => t.name)
        .sort(),
    ).toEqual([
      "follow_for_context",
      "handback",
      "recall",
      "report",
      "subscribe",
    ]);
    const records = await active.engine.transaction((tx: any) =>
      tx.scanConversations({}, 20),
    );
    expect(
      records.items.find((c: any) => c.id === ids[0]).owner,
    ).toBeUndefined();
  });
  it("persists exact payload-bound input admission and keeps final prose unconsumed", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    const { active } = await activate(r, directory);
    await active.agents.waitForIdle();
    const [delivery] = await active.list("deliveryRun");
    expect(delivery).toMatchObject({
      admitted: true,
      placed: true,
      settled: true,
      consumed: false,
      mode: "followUp",
    });
    expect(delivery.promptDigest).toHaveLength(64);
    expect(delivery.submissionId).toEqual(expect.any(Number));
    expect(delivery.taskId).toEqual(expect.any(Number));
    expect(await active.readTicket(a.id)).toMatchObject({
      attention: expect.stringMatching(/report/i),
    });
    expect((await active.list("disposition"))[0].acknowledged).toBe(false);
    await expect(
      active.agents.submitDelivery(delivery.id, delivery.prompt + "tampered"),
    ).rejects.toThrow(/payload|digest|immutable/i);
  });
  it("uses native structured Report tools to consume known inputs and rejects stale or cross-ticket reports", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    const { active } = await activate(r, directory, [
      async (ctx: any) => {
        const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
        const input = JSON.parse(
          typeof user.content === "string"
            ? user.content
            : user.content[0].text,
        );
        return fauxAssistantMessage(
          fauxToolCall("report", {
            reportId: "report-a",
            assignmentId: a.assignmentId,
            epoch: 1,
            inputIds: [input.inputId],
            status: "waiting",
            content: "Acknowledged",
          }),
          { stopReason: "toolUse" },
        );
      },
      fauxAssistantMessage("Done"),
    ]);
    await active.agents.waitForIdle();
    const [d] = await active.list("deliveryRun");
    expect(d.consumed).toBe(true);
    expect((await active.list("disposition"))[0].acknowledged).toBe(true);
    const report = {
      reportId: "r2",
      assignmentId: a.assignmentId,
      epoch: 1,
      inputIds: [d.inputId],
      status: "ready",
      content: "done",
    };
    await expect(
      active.agents.report(active.agents.routerConversationId, report),
    ).rejects.toThrow(/Worker|bound|role/i);
    await active.stopTicket(a.id, alice, { commandId: "stop" });
    await expect(
      active.agents.report(d.conversationId, report),
    ).rejects.toThrow(/stale|stopped/i);
  });
  it("spills long source content and recalls it after reopen with role-scoped cursor pages", async () => {
    const { r, directory } = await setup();
    const body = "Large long body. ".repeat(7000);
    const a = await accept(r, "a", body),
      b = await accept(r, "b");
    const { active, models, model } = await activate(r, directory);
    await active.agents.waitForIdle();
    const [trigger] = await active.list("trigger");
    expect(JSON.stringify(trigger).length).toBeLessThan(5000);
    expect(trigger.bodyArtifact).toMatch(/^[a-f0-9]{64}$/);
    const d = (await active.list("deliveryRun")).find(
      (d: any) => d.ticketId === a.id,
    );
    expect(d.prompt.length).toBeLessThan(6000);
    const bid = await active.agents.ensureWorker(b.id);
    await expect(active.agents.recall(bid, trigger.id)).rejects.toThrow(
      /access|scope|denied/i,
    );
    const forged = await active.receive(
      event("forged-artifact", {
        threads: ["b"],
        body: "Only my own source text",
        bodyArtifact: trigger.bodyArtifact,
      }),
    );
    await active.agents.waitForIdle();
    expect(
      (await active.engine.store.read("trigger", forged.triggerId))
        .bodyArtifact,
    ).toBeUndefined();
    expect((await active.agents.recall(bid, forged.triggerId)).text).toBe(
      "Only my own source text",
    );
    await expect(
      active.agents.recall(bid, trigger.bodyArtifact),
    ).rejects.toThrow(/access|denied/i);
    await active.close();
    const reopened = await openSuperAgent({ directory, models, model });
    runtimes.push(reopened);
    let restored = "",
      cursor: string | undefined;
    do {
      const p = await reopened.agents.recall(
        d.conversationId,
        trigger.id,
        cursor,
      );
      expect(p.estimatedTokens).toBeLessThanOrEqual(8000);
      restored += p.text;
      cursor = p.cursor;
    } while (cursor);
    expect(restored).toBe(body);
    expect(reopened.agents.artifactRoot).toBe(
      join(await realpath(directory), "artifacts"),
    );
  });
  it("bounds Router batches and continues after an independently rejected decision", async () => {
    const { r, directory } = await setup();
    for (let i = 0; i < 34; i++) await r.receive(event(`candidate-${i}`));
    const { active } = await activate(r, directory, [
      async (ctx: any) => {
        const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
        const input = JSON.parse(
          typeof user.content === "string"
            ? user.content
            : user.content[0].text,
        );
        return fauxAssistantMessage(
          fauxToolCall("decide_batch", {
            decisions: [
              {
                evalId: input.candidates[0].id,
                epoch: 999,
                outcome: { kind: "ignore", reason: "stale" },
              },
              {
                evalId: input.candidates[1].id,
                epoch: input.candidates[1].epoch,
                outcome: { kind: "ignore", reason: "valid" },
              },
            ],
          }),
          { stopReason: "toolUse" },
        );
      },
      ...Array.from({ length: 10 }, () =>
        fauxAssistantMessage("No more decisions"),
      ),
    ]);
    await active.agents.waitForIdle();
    const batches = await active.list("routerBatch");
    expect(batches.length).toBeGreaterThanOrEqual(2);
    expect(
      batches.every(
        (b: any) => b.evalIds.length <= 32 && b.estimatedTokens <= 12000,
      ),
    ).toBe(true);
    expect(
      (await active.list("decision")).some(
        (d: any) => d.outcome.reason === "valid",
      ),
    ).toBe(true);
  });
  it("retains unread multi-owner capacity after only the first owner consumes", async () => {
    const { r } = await setup();
    const a = await accept(r, "a"),
      b = await accept(r, "b");
    for (const t of [a, b])
      await r.subscribe(t.id, "cli", "shared", true, {
        commandId: `sub-${t.id}`,
      });
    const input = await r.receive(
      event("shared-input", { threads: ["shared"] }),
    );
    await r.engine.transaction((tx: any) =>
      r.engine.authorization.acknowledgeInputInTransaction(
        tx,
        a.id,
        input.triggerId,
        1,
      ),
    );
    expect(await r.engine.store.read("intake", input.triggerId)).toMatchObject({
      released: false,
    });
    expect(
      await r.engine.store.read("disposition", input.triggerId),
    ).toMatchObject({ consumedTicketIds: [a.id] });
    await r.handback(b.id, input.triggerId, 1, "release", {
      commandId: "back-b",
    });
    expect(
      (await r.listCandidates()).some((e: any) =>
        e.triggerIds.includes(input.triggerId),
      ),
    ).toBe(false);
  });
  it("Router abort leaves an ownerless Worker generation running", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    let enter!: () => void,
      release!: () => void,
      signal: AbortSignal | undefined;
    const entered = new Promise<void>((resolve) => (enter = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    const { active } = await activate(r, directory, [
      async (_ctx: any, options: any) => {
        signal = options.signal;
        enter();
        await gate;
        return fauxAssistantMessage("still working");
      },
    ]);
    try {
      await entered;
      const router = await active.harness.conversation(
        active.agents.routerConversationId,
        active.engine.context,
      );
      await router.abort(active.engine.context, { background: true });
      expect(signal?.aborted).toBe(false);
      expect((await active.readTicket(a.id)).lifecycle).toBe("open");
    } finally {
      release();
    }
    await active.agents.waitForIdle();
  });
  it.each(["followUp", "steer"])(
    "uses native busy inbox %s boundaries",
    async (mode) => {
      const { r, directory } = await setup();
      const a = await accept(r, "a");
      let enter!: () => void,
        release!: () => void,
        observed: string[] = [];
      const entered = new Promise<void>((resolve) => (enter = resolve)),
        gate = new Promise<void>((resolve) => (release = resolve));
      let workerCalls = 0;
      const response = async (ctx: any) => {
        if (isRouter(ctx)) return fauxAssistantMessage("No stop requested");
        workerCalls++;
        if (workerCalls === 1) {
          enter();
          await gate;
          return fauxAssistantMessage(
            fauxToolCall("recall", {
              ref: JSON.parse(userText(ctx)).triggerId,
            }),
            { stopReason: "toolUse" },
          );
        }
        observed.push(JSON.stringify(ctx.messages));
        return fauxAssistantMessage("waiting");
      };
      const { active } = await activate(
        r,
        directory,
        Array.from({ length: 20 }, () => response),
      );
      try {
        await entered;
        await active.agents.noteWorker(
          active.agents.routerConversationId,
          a.id,
          "Feedback boundary marker",
          `boundary-${mode}`,
          mode,
        );
        await active.agents.deliverPending();
        await until(async () =>
          (await active.list("deliveryRun")).some(
            (d: any) => !d.intentId && d.admitted,
          ),
        );
        const queued = (await active.list("deliveryRun")).find(
          (d: any) => !d.intentId,
        );
        await expect(
          active.agents.report(queued.conversationId, {
            reportId: `unplaced-${mode}`,
            assignmentId: a.assignmentId,
            epoch: 1,
            inputIds: [queued.inputId],
            status: "waiting",
            content: "not visible yet",
          }),
        ).rejects.toThrow(/placed/i);
      } finally {
        release();
      }
      await active.agents.waitForIdle();
      expect(observed[0].includes("Feedback boundary marker")).toBe(
        mode === "steer",
      );
      expect(
        observed.some((text) => text.includes("Feedback boundary marker")),
      ).toBe(true);
      const note = (await active.list("deliveryRun")).find(
        (d: any) => !d.intentId,
      );
      expect(note.mode).toBe(mode);
      expect(note.settled).toBe(true);
    },
  );
  it("fails boundedly after three native generation failures and redrives queued feedback", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>((resolve) => (enter = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    let calls = 0;
    const response = async (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage("No stop");
      calls++;
      if (calls === 1) {
        enter();
        await gate;
      }
      return fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "Controlled non-retryable invalid request",
      });
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 50 }, () => response),
    );
    try {
      await entered;
      await active.agents.noteWorker(
        active.agents.routerConversationId,
        a.id,
        "queued after failure",
        "queued",
      );
      await active.agents.deliverPending();
      await until(async () =>
        (await active.list("deliveryRun")).some(
          (d: any) => !d.intentId && d.admitted,
        ),
      );
    } finally {
      release();
    }
    await active.agents.waitForIdle();
    const runs = await active.list("deliveryRun");
    expect(runs).toHaveLength(2);
    expect(
      runs.every(
        (d: any) => d.attempt === 3 && d.status === "attention" && d.settled,
      ),
    ).toBe(true);
    expect((await active.readTicket(a.id)).attention).toMatch(/3 attempts/);
  });
  it("policy updates durably notify Workers without widening their original Mandate", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    const { active } = await activate(r, directory);
    await active.agents.waitForIdle();
    const before = await active.engine.store.read("mandate", a.mandateId);
    await active.updatePolicy(
      {
        version: 2,
        acceptMode: "auto",
        allowedActors: ["alice"],
        botActors: [],
        explicitCapabilities: ["repo.read", "pr.merge"],
        rules: [
          {
            id: "new",
            enabled: true,
            instructions: "Changed operating guidance",
            capabilities: ["pr.merge"],
          },
        ],
      },
      alice,
      { commandId: "policy2" },
    );
    await active.agents.waitForIdle();
    expect(await active.engine.store.read("mandate", a.mandateId)).toEqual(
      before,
    );
    const conv = await active.harness.conversation(
      (await active.readTicket(a.id)).workerConversationId,
      active.engine.context,
    );
    expect(
      JSON.stringify((await conv.context(active.engine.context)).messages),
    ).toContain("Changed operating guidance");
    expect(
      (await active.list("workerNote")).some(
        (n: any) => n.kind === "policy" && n.policyVersion === 2,
      ),
    ).toBe(true);
  });
  it.each(["admission", "provider"])(
    "recovers SIGKILL at %s with one original input and stable request ID",
    async (mode) => {
      const directory = await mkdtemp(join(tmpdir(), "sa-delivery-crash-"));
      dirs.push(directory);
      const child = spawn(
        process.execPath,
        ["tests/helpers/delivery-crash.mjs", directory, mode],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let stderr = "";
      child.stderr.on("data", (v) => (stderr += v));
      const [code, signal] = await once(child, "exit");
      expect({ code, signal, stderr }).toMatchObject({
        code: null,
        signal: "SIGKILL",
      });
      const faux = fauxProvider(),
        models = createModels();
      models.setProvider(faux.provider);
      faux.setResponses(
        Array.from({ length: 20 }, () => fauxAssistantMessage("Recovered")),
      );
      let originalId: number | undefined, requestId: string | undefined;
      const active = await openSuperAgent({
        directory,
        models,
        model: {
          provider: faux.getModel().provider,
          modelId: faux.getModel().id,
        },
        preflight: async (engine) => {
          const [run] = await engine.store.list<any>("deliveryRun");
          requestId = run.requestId;
          const old = await engine.transaction((tx) =>
            tx.submissionByRequest(run.conversationId, run.requestId),
          );
          expect(old).toBeDefined();
          originalId = old!.id;
          if (mode === "admission") expect(run.submissionId).toBeUndefined();
        },
      });
      runtimes.push(active);
      await active.agents.waitForIdle();
      const [run] = await active.list("deliveryRun");
      expect(run.submissionId).toBe(originalId);
      expect(run.requestId).toBe(requestId);
      const conv = await active.harness.conversation(
        run.conversationId,
        active.engine.context,
      );
      const entries = await conv.entries(
        {},
        100,
        undefined,
        active.engine.context,
      );
      expect(
        entries.items.filter((e: any) => e.kind === "pi.user"),
      ).toHaveLength(1);
      expect(run).toMatchObject({
        admitted: true,
        placed: true,
        settled: true,
      });
    },
  );

  it("keeps candidates arriving during an active Router generation for the next frozen batch", async () => {
    const { r, directory } = await setup();
    await r.receive(event("first"));
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>((resolve) => (enter = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    let calls = 0;
    const response = async (ctx: any) => {
      if (!isRouter(ctx)) return fauxAssistantMessage("Worker waiting");
      calls++;
      if (calls === 1) {
        enter();
        await gate;
      }
      const input = JSON.parse(userText(ctx));
      return input.candidates.length && ctx.messages.at(-1).role === "user"
        ? fauxAssistantMessage(
            fauxToolCall("decide_batch", {
              decisions: input.candidates.map((c: any) => ({
                evalId: c.id,
                epoch: c.epoch,
                outcome: { kind: "ignore", reason: "handled" },
              })),
            }),
            { stopReason: "toolUse" },
          )
        : fauxAssistantMessage("Done");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 20 }, () => response),
    );
    try {
      await entered;
      await active.receive(event("second"));
      await active.agents.pump();
      const [batch] = await active.list("routerBatch");
      expect(batch.evalIds).toHaveLength(1);
      expect(await active.list("routerBatch")).toHaveLength(1);
    } finally {
      release();
    }
    await active.agents.waitForIdle();
    const batches = await active.list("routerBatch");
    expect(batches).toHaveLength(2);
    expect(batches[0].evalIds[0]).not.toBe(batches[1].evalIds[0]);
    expect(await active.list("decision")).toHaveLength(2);
  });
  it("derives stop-only notice authority from the stored actor and refuses untrusted stops", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    let results: any[] = [];
    const response = async (ctx: any) => {
      if (!isRouter(ctx)) return fauxAssistantMessage("waiting");
      const input = JSON.parse(userText(ctx));
      if (input.notices.length && ctx.messages.at(-1).role === "user")
        return fauxAssistantMessage(
          fauxToolCall("decide_batch", {
            decisions: input.notices.map((n: any) => ({
              triggerId: n.triggerId,
              outcome: { kind: "stop", ticketId: a.id, reason: "Requested" },
            })),
          }),
          { stopReason: "toolUse" },
        );
      return fauxAssistantMessage("Done");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 50 }, () => response),
    );
    await active.agents.waitForIdle();
    await active.receive(
      event("untrusted-stop", {
        threads: ["a"],
        actor: { id: "operator", kind: "human", trusted: false },
        body: "stop",
      }),
    );
    await active.agents.waitForIdle();
    expect((await active.readTicket(a.id)).lifecycle).toBe("open");
    const before = await active.listCandidates();
    expect(before).toEqual([]);
    await active.receive(
      event("trusted-stop", { threads: ["a"], body: "stop" }),
    );
    await active.agents.waitForIdle();
    expect((await active.readTicket(a.id)).lifecycle).toBe("stopped");
    const stopTrigger = (await active.list("trigger")).find(
      (t: any) => t.externalId === "trusted-stop",
    );
    expect(
      (await active.listCandidates()).some((c: any) =>
        c.triggerIds.includes(stopTrigger.id),
      ),
    ).toBe(false);
  });
  it("binds Report deduplication to payload and does not consume sibling Ticket inputs", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a"),
      b = await accept(r, "b");
    for (const t of [a, b])
      await r.subscribe(t.id, "cli", "shared", true, {
        commandId: `sub2-${t.id}`,
      });
    const shared = await r.receive(event("shared", { threads: ["shared"] }));
    const { active } = await activate(r, directory);
    await active.agents.waitForIdle();
    const runs = (await active.list("deliveryRun")).filter(
      (d: any) => d.triggerId === shared.triggerId,
    );
    const da = runs.find((d: any) => d.ticketId === a.id),
      db = runs.find((d: any) => d.ticketId === b.id);
    const report = {
      reportId: "dedup",
      assignmentId: a.assignmentId,
      epoch: 1,
      inputIds: [da.inputId],
      status: "waiting",
      content: "only A",
    };
    await active.agents.report(da.conversationId, report);
    await active.agents.report(da.conversationId, report);
    await expect(
      active.agents.report(da.conversationId, {
        ...report,
        content: "changed",
      }),
    ).rejects.toThrow(/conflict/i);
    expect(
      (await active.engine.store.read("deliveryRun", db.id)).consumed,
    ).toBe(false);
    expect(
      await active.engine.store.read("intake", shared.triggerId),
    ).toMatchObject({ released: false });
    await expect(
      active.agents.report(da.conversationId, {
        ...report,
        reportId: "cross",
        inputIds: [db.inputId],
      }),
    ).rejects.toThrow(/unrelated|unknown/i);
  });
  it("keeps unread owner input after a separate bot Eval is ignored and transport is placed", async () => {
    const { r } = await setup();
    const a = await accept(r, "a");
    await r.updatePolicy(
      {
        version: 2,
        acceptMode: "auto",
        allowedActors: ["alice"],
        botActors: ["bot"],
        rules: [],
      },
      alice,
      { commandId: "bots" },
    );
    await r.subscribe(a.id, "cli", "owned", true, { commandId: "bot-owned" });
    const received = await r.receive(
      event("bot-owned", {
        actor: { id: "bot", kind: "bot", trusted: true },
        threads: ["owned"],
      }),
    );
    const [c] = await r.listCandidates();
    await r.decide(
      c.id,
      c.epoch,
      { kind: "ignore", reason: "already owned" },
      { commandId: "ignore-owned-bot" },
    );
    const d = (await r.list("delivery")).find(
      (d: any) => d.triggerId === received.triggerId,
    );
    await r.engine.transaction((tx: any) =>
      r.engine.authorization.acknowledgeDeliveryInTransaction(tx, d.id),
    );
    expect(
      await r.engine.store.read("intake", received.triggerId),
    ).toMatchObject({ released: false });
  });
  it("gracefully closes a live provider wait and resumes its durable work on reopen", async () => {
    const { r, directory } = await setup();
    await accept(r, "a");
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>((resolve) => (enter = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    const { active, models, model, faux } = await activate(r, directory, [
      async () => {
        enter();
        await gate;
        return fauxAssistantMessage("interrupted");
      },
    ]);
    await entered;
    const waiting = active.agents
      .waitForIdle()
      .catch((error: unknown) => error);
    const closing = active.close();
    release();
    await closing;
    await waiting;
    faux.setResponses([fauxAssistantMessage("resumed")]);
    const reopened = await openSuperAgent({ directory, models, model });
    runtimes.push(reopened);
    await reopened.agents.waitForIdle();
    expect((await reopened.list("deliveryRun"))[0].settled).toBe(true);
  });

  it("refuses Router stop proposals from an unrelated source-qualified candidate", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "a");
    await r.engine.configureSource({
      id: "other",
      kind: "other",
      enabled: true,
    });
    await r.receive(
      event("wrong-source", { sourceId: "other", threads: ["a"] }),
    );
    let attempted = false;
    const response = async (ctx: any) => {
      if (!isRouter(ctx) || ctx.messages.at(-1).role !== "user")
        return fauxAssistantMessage("waiting");
      const input = JSON.parse(userText(ctx));
      const c = input.candidates[0];
      if (!c) return fauxAssistantMessage("done");
      attempted = true;
      return fauxAssistantMessage(
        fauxToolCall("decide_batch", {
          decisions: [
            {
              evalId: c.id,
              epoch: c.epoch,
              outcome: {
                kind: "stop",
                ticketId: a.id,
                reason: "stop from other source",
              },
            },
          ],
        }),
        { stopReason: "toolUse" },
      );
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 20 }, () => response),
    );
    await active.agents.waitForIdle();
    expect(attempted).toBe(true);
    expect((await active.readTicket(a.id)).lifecycle).toBe("open");
  });
  it("upgrades only the known additive registry after recovery preflight succeeds", async () => {
    const { r, directory } = await setup();
    const metadata = await r.engine.store.read("metadata", "runtime");
    const old = {
      ...metadata,
      extensions: ["sa.kernel"],
      tasks: Object.fromEntries(
        Object.entries(metadata.tasks).filter(
          ([name]) =>
            !["sa.agentInput", "sa.cancel", "sa.redrive"].includes(name),
        ),
      ),
    };
    await r.engine.transaction((tx) =>
      r.engine.store.put(tx, "metadata", "runtime", old),
    );
    await r.close();
    await expect(
      openSuperAgent({
        directory,
        preflight: async (engine) => {
          expect(await engine.store.read("metadata", "runtime")).toEqual(old);
          throw new Error("Preflight refusal");
        },
      }),
    ).rejects.toThrow("Preflight refusal");
    const reopened = await openSuperAgent({ directory });
    runtimes.push(reopened);
    expect(
      (await reopened.engine.store.read("metadata", "runtime")).extensions,
    ).toContain("sa.agents");
  });
  it("leaves recovered model work paused when reopened without an agent model", async () => {
    const directory = await mkdtemp(join(tmpdir(), "sa-model-free-recovery-"));
    dirs.push(directory);
    const child = spawn(
      process.execPath,
      ["tests/helpers/delivery-crash.mjs", directory, "admission"],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    const [, signal] = await once(child, "exit");
    expect(signal).toBe("SIGKILL");
    const runtime = await openSuperAgent({ directory });
    runtimes.push(runtime);
    expect(runtime.agents).toBeUndefined();
    expect(
      (await runtime.harness.inspect(runtime.engine.context)).scheduling,
    ).toBe("paused");
    expect((await runtime.receive(event("model-free-input"))).disposition).toBe(
      "stored",
    );
    expect((await runtime.list("deliveryRun"))[0]).toMatchObject({
      status: "pending",
      admitted: false,
    });
  });
});
