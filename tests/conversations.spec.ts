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
      "edit_file",
      "follow_for_context",
      "get_job",
      "handback",
      "read_file",
      "recall",
      "report",
      "start_job",
      "stop_job",
      "subscribe",
      "write_file",
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
            !["sa.agentInput", "sa.cancel", "sa.redrive", "sa.job"].includes(name),
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
  it("replays an unchanged pre-spill large event after upgrade and rejects forged source digests", async () => {
    const { r, directory } = await setup();
    const old = event("old-large", { body: "original body ".repeat(5000) });
    r.engine.prepareEvent = undefined;
    const first = await r.receive(old);
    await r.close();
    const reopened = await openSuperAgent({ directory });
    runtimes.push(reopened);
    expect(await reopened.receive({ ...old, receivedAt: 2000 })).toEqual({
      disposition: "duplicate",
      triggerId: first.triggerId,
    });
    const batch = await reopened.receiveBatch([old], {
      sourceId: "cli",
      value: "replayed-old-large",
    });
    expect(batch.disposition).toBe("stored");
    const original = await reopened.engine.store.read(
      "trigger",
      first.triggerId,
    );
    await expect(
      reopened.receive({
        ...old,
        body: "changed",
        originalDigest: original.digest,
        bodyArtifact: "a".repeat(64),
      } as any),
    ).rejects.toThrow(/conflict/i);
  });
  it("fences stale native inspection and late input settlement against a newer retry claim", async () => {
    const { r, directory } = await setup();
    await accept(r, "retry-race");
    let entered!: () => void,
      releaseFirst!: () => void,
      releaseRetry!: () => void;
    const firstEntered = new Promise<void>((resolve) => (entered = resolve)),
      firstGate = new Promise<void>((resolve) => (releaseFirst = resolve)),
      retryGate = new Promise<void>((resolve) => (releaseRetry = resolve));
    let calls = 0;
    const response = async () => {
      if (++calls === 1) {
        entered();
        await firstGate;
        return fauxAssistantMessage("", {
          stopReason: "error",
          errorMessage: "Controlled invalid request",
        });
      }
      await retryGate;
      return fauxAssistantMessage("waiting");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 20 }, () => response),
    );
    await firstEntered;
    const [original] = await active.list("deliveryRun");
    let inspected!: () => void, releaseInspection!: () => void;
    const captured = new Promise<void>((resolve) => (inspected = resolve)),
      inspectionGate = new Promise<void>(
        (resolve) => (releaseInspection = resolve),
      );
    const inspect = active.harness.inspect.bind(active.harness);
    let intercept = true;
    active.harness.inspect = async (...args: any[]) => {
      const result = await inspect(...args);
      if (intercept) {
        intercept = false;
        inspected();
        await inspectionGate;
      }
      return result;
    };
    try {
      const pumping = active.agents.pump();
      await captured;
      releaseFirst();
      await until(
        async () =>
          (await active.engine.store.read("deliveryRun", original.id))
            .attempt === 2,
      );
      releaseInspection();
      await pumping;
      const run = await active.engine.store.read("deliveryRun", original.id);
      const tasks = (await inspect(active.engine.context)).tasks.filter(
        (t: any) =>
          t.record.kind === "sa.agentInput" && t.record.input.id === run.id,
      );
      expect(tasks).toHaveLength(1);
      expect(tasks[0].record.id).toBe(run.taskId);
      expect(run.executionAttempts ?? 1).toBe(1);
      const receipt = await (
        await active.harness.submission(
          original.submissionId,
          active.engine.context,
        )
      ).status(active.engine.context);
      await active.engine.transaction((tx: any) =>
        active.agents.settleInTransaction(tx, "deliveryRun", run.id, receipt, {
          taskId: original.taskId,
          attempt: original.attempt,
          requestId: original.requestId,
          submissionId: original.submissionId,
        }),
      );
      expect(await active.engine.store.read("deliveryRun", run.id)).toEqual(
        run,
      );
    } finally {
      releaseFirst();
      releaseRetry();
      releaseInspection();
      active.harness.inspect = inspect;
    }
    await active.agents.waitForIdle();
  });
  it("delivers an explicit ownership transition when a reported context input is attached before settlement", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "owner");
    await r.subscribe(a.id, "cli", "context-promotion", false, {
      commandId: "follow-promotion",
    });
    const source = await r.receive(event("context-promotion"));
    const candidate = (await r.listCandidates())[0];
    let entered!: () => void, release!: () => void;
    const reported = new Promise<void>((resolve) => (entered = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    let held = false;
    const seen: any[] = [];
    const response = async (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage("waiting");
      const input = JSON.parse(userText(ctx));
      if (input.triggerId !== source.triggerId)
        return fauxAssistantMessage("waiting");
      if (ctx.messages.at(-1).role === "user") {
        seen.push(input);
        return fauxAssistantMessage(
          fauxToolCall("report", {
            reportId: `report-${input.inputId}`,
            assignmentId: input.assignmentId,
            epoch: input.epoch,
            inputIds: [input.inputId],
            status: "waiting",
            content: "Consumed presented semantics",
          }),
          { stopReason: "toolUse" },
        );
      }
      if (!held) {
        held = true;
        entered();
        await gate;
      }
      return fauxAssistantMessage("waiting");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 30 }, () => response),
    );
    try {
      await reported;
      await active.decide(
        candidate.id,
        candidate.epoch,
        { kind: "attach", ticketId: a.id },
        { commandId: "attach-after-context-report" },
      );
      await active.agents.pump();
      const runs = (await active.list("deliveryRun")).filter(
        (d: any) => d.triggerId === source.triggerId,
      );
      expect(runs).toHaveLength(2);
      expect(
        runs.some(
          (d: any) =>
            JSON.parse(d.prompt).ownershipTransition === true &&
            JSON.parse(d.prompt).own === true,
        ),
      ).toBe(true);
    } finally {
      release();
    }
    await active.agents.waitForIdle();
    expect(seen.filter((i) => i.own === true)).toHaveLength(1);
    expect(
      await active.engine.store.read("disposition", source.triggerId),
    ).toMatchObject({ consumedTicketIds: [a.id] });
    expect(
      await active.engine.store.read("intake", source.triggerId),
    ).toMatchObject({ released: true });
  });
  it("lets the provider infer acceptance and discover coordination Tickets using only presented trusted authority", async () => {
    const { r, directory } = await setup();
    const existing = await accept(r, "existing coordination target");
    await r.updatePolicy(
      {
        version: 2,
        acceptMode: "auto",
        allowedActors: ["alice"],
        botActors: ["bot"],
        rules: [
          {
            id: "unpredictable-rule-713",
            enabled: true,
            instructions:
              "Maintain the project when a bot requests maintenance",
            capabilities: ["repo.read"],
          },
        ],
      },
      alice,
      { commandId: "discovery-policy" },
    );
    await r.receive(
      event("inferred-maintenance", {
        actor: { id: "bot", kind: "bot", trusted: true },
        body: "New maintenance request",
      }),
    );
    let discovered = false,
      workerAuthority = false;
    const response = async (ctx: any) => {
      const input = JSON.parse(userText(ctx));
      if (!isRouter(ctx)) {
        if (input.authority?.mandate?.basis === "inferred") {
          expect(input.authority.mandate.capabilities).toEqual(["repo.read"]);
          expect(input.authority.matchedRules[0].instructions).toContain(
            "Maintain the project",
          );
          expect(input.sourceId).toBe("cli");
          expect(input.threads).toEqual(["inferred-maintenance"]);
          expect(input.routingEpoch).toBe(1);
          workerAuthority = true;
        }
        return fauxAssistantMessage("waiting");
      }
      if (ctx.messages.at(-1).role !== "user" || !input.candidates.length)
        return fauxAssistantMessage("done");
      expect(input.authority.acceptMode).toBe("auto");
      const rule = input.authority.rules.find((rule: any) => rule.enabled);
      const target = input.tickets.items.find((ticket: any) =>
        ticket.goal.includes("coordination"),
      );
      expect(target).toBeDefined();
      discovered = true;
      const c = input.candidates[0];
      return fauxAssistantMessage(
        fauxToolCall("decide_batch", {
          decisions: [
            {
              evalId: c.id,
              epoch: c.epoch,
              outcome: c.body.startsWith("Attach")
                ? { kind: "attach", ticketId: target.id }
                : {
                    kind: "accept",
                    goal: "inferred maintenance",
                    ruleIds: [rule.id],
                    capabilities: rule.capabilities,
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
      Array.from({ length: 30 }, () => response),
    );
    await active.agents.waitForIdle();
    expect(discovered).toBe(true);
    expect(workerAuthority).toBe(true);
    expect(
      (await active.list("ticket")).some(
        (t: any) => t.goal === "inferred maintenance",
      ),
    ).toBe(true);
    const attached = await active.receive(
      event("discovered-attach", {
        body: "Attach this request to existing coordination target",
      }),
    );
    await active.agents.waitForIdle();
    expect(
      (await active.engine.store.read("disposition", attached.triggerId))
        .ownerTickets,
    ).toContain(existing.id);
  });
  it("presents the Trigger routing epoch separately from assignment epoch for native handback", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "persistent-owner");
    await r.subscribe(a.id, "cli", "epoch-source", false, {
      commandId: "follow-epoch",
    });
    const source = await r.receive(event("epoch-source"));
    let c = (await r.listCandidates())[0];
    await r.decide(
      c.id,
      c.epoch,
      { kind: "accept", goal: "temporary", ruleIds: [] },
      { commandId: "temporary" },
    );
    const b = (await r.list("ticket")).find((t: any) => t.goal === "temporary");
    await r.handback(b.id, source.triggerId, 1, "reroute", {
      commandId: "reroute",
    });
    c = (await r.listCandidates())[0];
    expect(c.epoch).toBe(2);
    await r.decide(
      c.id,
      c.epoch,
      { kind: "attach", ticketId: a.id },
      { commandId: "attach-epoch2" },
    );
    let handedBack = false;
    const response = async (ctx: any) => {
      if (isRouter(ctx) || ctx.messages.at(-1).role !== "user")
        return fauxAssistantMessage("waiting");
      const input = JSON.parse(userText(ctx));
      if (input.routingEpoch > input.epoch) {
        handedBack = true;
        return fauxAssistantMessage(
          fauxToolCall("handback", {
            triggerId: input.triggerId,
            epoch: input.routingEpoch,
            reason: "Presented routing epoch",
          }),
          { stopReason: "toolUse" },
        );
      }
      return fauxAssistantMessage("waiting");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 30 }, () => response),
    );
    await active.agents.waitForIdle();
    expect(handedBack).toBe(true);
    expect(
      (await active.engine.store.read("disposition", source.triggerId))
        .handbacks,
    ).toContainEqual({
      ticketId: a.id,
      epoch: 3,
      reason: "Presented routing epoch",
    });
  });
  it("retries terminal native cancel claims and marks bounded exhaustion as attention", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "cancel-retry");
    const { active } = await activate(r, directory);
    await active.agents.waitForIdle();
    const phase = active.agents.cancelTask.definition.phases.cancel;
    active.agents.cancelTask.definition.phases.cancel = async () => {
      throw new Error("Controlled terminal cancel fault");
    };
    try {
      await active.stopTicket(a.id, alice, { commandId: "cancel-with-fault" });
      for (let i = 0; i < 3; i++) {
        await active.agents.pump();
        const [intent] = await active.list("cancel");
        await active.harness.waitForTask(intent.taskId, active.engine.context);
      }
      await active.agents.pump();
      expect((await active.list("cancel"))[0].status).toBe("attention");
      expect((await active.readTicket(a.id)).attention).toMatch(
        /cancel.*3|3.*cancel/i,
      );
    } finally {
      active.agents.cancelTask.definition.phases.cancel = phase;
    }
  });

  it.each(["fault", "abort", "exhaustion"])(
    "recovers a terminal native redrive claim after %s without readmitting queued input",
    async (failure) => {
      const { r, directory } = await setup();
      const a = await accept(r, `redrive-${failure}`);
      let entered!: () => void, release!: () => void;
      const generationEntered = new Promise<void>(
          (resolve) => (entered = resolve),
        ),
        generationGate = new Promise<void>((resolve) => (release = resolve));
      let failed = false;
      const response = async (ctx: any) => {
        if (isRouter(ctx)) return fauxAssistantMessage("waiting");
        const input = JSON.parse(userText(ctx));
        if (ctx.messages.at(-1).role === "user")
          return fauxAssistantMessage(
            fauxToolCall("report", {
              reportId: `consume-${input.inputId}`,
              assignmentId: input.assignmentId,
              epoch: input.epoch,
              inputIds: [input.inputId],
              status: "waiting",
              content: "presented input consumed",
            }),
            { stopReason: "toolUse" },
          );
        if (!failed) {
          failed = true;
          entered();
          await generationGate;
          return fauxAssistantMessage("", {
            stopReason: "error",
            errorMessage: "Controlled invalid request",
          });
        }
        return fauxAssistantMessage("waiting");
      };
      const { active } = await activate(
        r,
        directory,
        Array.from({ length: 30 }, () => response),
      );
      let driveEntered!: () => void, releaseDrive!: () => void;
      const driveStarted = new Promise<void>(
          (resolve) => (driveEntered = resolve),
        ),
        driveGate = new Promise<void>((resolve) => (releaseDrive = resolve));
      const definition = active.agents.redriveTask.definition;
      const original = definition.phases.drive;
      definition.phases.drive = async (...args: any[]) => {
        if (failure === "abort") {
          driveEntered();
          await driveGate;
          return original(...args);
        }
        throw new Error("Controlled terminal redrive fault");
      };
      try {
        await generationEntered;
        await active.agents.noteWorker(
          active.agents.routerConversationId,
          a.id,
          "queued factual note",
          "queued-redrive",
        );
        await active.agents.pump();
        await until(async () =>
          (await active.list("deliveryRun")).some(
            (d: any) => !d.intentId && d.admitted,
          ),
        );
        const queued = (await active.list("deliveryRun")).find(
          (d: any) => !d.intentId,
        );
        release();
        await until(
          async () =>
            (await active.list("deliveryRun")).find((d: any) => d.intentId)
              .settled,
        );
        await active.agents.pump();
        await until(async () => (await active.list("redrive")).length > 0);
        let [claim] = await active.list("redrive");
        const firstTaskId = claim.taskId;
        if (failure === "abort") {
          await driveStarted;
          const aborting = active.harness.abortTask(
            claim.taskId,
            active.engine.context,
          );
          releaseDrive();
          await aborting;
        }
        await active.harness.waitForTask(claim.taskId, active.engine.context);
        if (failure === "exhaustion") {
          for (let i = 0; i < 2; i++) {
            await active.agents.pump();
            [claim] = await active.list("redrive");
            await active.harness.waitForTask(
              claim.taskId,
              active.engine.context,
            );
          }
        }
        definition.phases.drive = original;
        await active.agents.pump();
        await active.agents.waitForIdle();
        [claim] = await active.list("redrive");
        expect(claim.taskId).not.toBe(firstTaskId);
        expect(claim.status).toBe(
          failure === "exhaustion" ? "attention" : "completed",
        );
        expect(claim.attempts).toBe(failure === "exhaustion" ? 3 : 2);
        const run = await active.engine.store.read("deliveryRun", queued.id);
        expect(run.submissionId).toBe(queued.submissionId);
        expect(run.requestId).toBe(queued.requestId);
        expect(run.settled).toBe(true);
        if (failure === "exhaustion") expect(run.status).toBe("attention");
        else expect(run.consumed).toBe(true);
      } finally {
        release();
        releaseDrive();
        definition.phases.drive = original;
      }
    },
  );

  it("rechecks mutable intent semantics when context promotion races outside-commit prompt preparation", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "prepare-race");
    const { active } = await activate(r, directory);
    await active.agents.waitForIdle();
    await active.subscribe(a.id, "cli", "prepare-context", false, {
      commandId: "prepare-follow",
    });
    const source = await active.receive(event("prepare-context"));
    const [candidate] = await active.listCandidates();
    const spill = active.agents.artifacts.spill.bind(active.agents.artifacts);
    let entered!: () => void, release!: () => void;
    const prepared = new Promise<void>((resolve) => (entered = resolve)),
      gate = new Promise<void>((resolve) => (release = resolve));
    let held = false;
    active.agents.artifacts.spill = async (text: string) => {
      if (
        !held &&
        text.includes(source.triggerId) &&
        text.includes('"own":false')
      ) {
        held = true;
        entered();
        await gate;
      }
      return spill(text);
    };
    try {
      const pumping = active.agents.pump();
      await prepared;
      await active.decide(
        candidate.id,
        candidate.epoch,
        { kind: "attach", ticketId: a.id },
        { commandId: "promote-during-prepare" },
      );
      release();
      await pumping;
    } finally {
      release();
      active.agents.artifacts.spill = spill;
    }
    await active.agents.waitForIdle();
    const run = (await active.list("deliveryRun")).find(
      (d: any) => d.triggerId === source.triggerId,
    );
    expect(JSON.parse(run.prompt).own).toBe(true);
  });

  it("recovers an aborted cancel task and cancels the actual ownerless Worker generation", async () => {
    const { r, directory } = await setup();
    const a = await accept(r, "cancel-abort");
    let started!: () => void,
      releaseProvider!: () => void,
      providerSignal: AbortSignal | undefined;
    const providerStarted = new Promise<void>((resolve) => (started = resolve)),
      providerGate = new Promise<void>(
        (resolve) => (releaseProvider = resolve),
      );
    const { active } = await activate(r, directory, [
      async (_ctx: any, options: any) => {
        providerSignal = options.signal;
        started();
        await providerGate;
        return fauxAssistantMessage("interrupted");
      },
    ]);
    let cancelEntered!: () => void, releaseCancel!: () => void;
    const cancelStarted = new Promise<void>(
        (resolve) => (cancelEntered = resolve),
      ),
      cancelGate = new Promise<void>((resolve) => (releaseCancel = resolve));
    const definition = active.agents.cancelTask.definition,
      phase = definition.phases.cancel;
    definition.phases.cancel = async (...args: any[]) => {
      cancelEntered();
      await cancelGate;
      return phase(...args);
    };
    try {
      await providerStarted;
      await active.stopTicket(a.id, alice, {
        commandId: "stop-aborted-cancel",
      });
      await active.agents.pump();
      await cancelStarted;
      const [first] = await active.list("cancel");
      const aborting = active.harness.abortTask(
        first.taskId,
        active.engine.context,
      );
      releaseCancel();
      await aborting;
      await active.harness.waitForTask(first.taskId, active.engine.context);
      definition.phases.cancel = phase;
      await active.agents.pump();
      await until(async () => providerSignal?.aborted === true);
      releaseProvider();
      await active.agents.waitForIdle();
      const [recovered] = await active.list("cancel");
      expect(recovered).toMatchObject({ status: "completed", attempts: 2 });
      expect(recovered.taskId).not.toBe(first.taskId);
    } finally {
      releaseProvider();
      releaseCancel();
      definition.phases.cancel = phase;
    }
  });
  it("recalls spilled frozen Worker authority through native paged tools without granting Policy access", async () => {
    const { r, directory } = await setup();
    const instructions = "Original frozen operating instructions. ".repeat(
      1000,
    );
    await r.updatePolicy(
      {
        version: 2,
        acceptMode: "auto",
        allowedActors: ["alice"],
        botActors: ["bot"],
        rules: [
          {
            id: "large-rule",
            enabled: true,
            instructions,
            capabilities: ["repo.read"],
          },
        ],
      },
      alice,
      { commandId: "large-policy" },
    );
    await r.receive(
      event("large-authority", {
        actor: { id: "bot", kind: "bot", trusted: true },
      }),
    );
    const [candidate] = await r.listCandidates();
    await r.decide(
      candidate.id,
      candidate.epoch,
      { kind: "accept", goal: "large authority", ruleIds: ["large-rule"] },
      { commandId: "large-authority-ticket" },
    );
    let recovered = false;
    const response = async (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage("waiting");
      const input = JSON.parse(userText(ctx));
      const pages = ctx.messages
        .filter((m: any) => m.role === "toolResult" && m.toolName === "recall")
        .map((m: any) => JSON.parse(m.content[0].text));
      if (!pages.length)
        return fauxAssistantMessage(
          fauxToolCall("recall", { ref: input.authorityReference }),
          { stopReason: "toolUse" },
        );
      const last = pages.at(-1);
      expect(last.estimatedTokens).toBeLessThanOrEqual(8000);
      if (last.cursor)
        return fauxAssistantMessage(
          fauxToolCall("recall", {
            ref: input.authorityReference,
            cursor: last.cursor,
          }),
          { stopReason: "toolUse" },
        );
      const authority = JSON.parse(pages.map((p: any) => p.text).join(""));
      expect(authority.matchedRules[0].instructions).toBe(instructions);
      expect(authority.mandate.capabilities).toEqual(["repo.read"]);
      recovered = true;
      return fauxAssistantMessage("waiting");
    };
    const { active } = await activate(
      r,
      directory,
      Array.from({ length: 100 }, () => response),
    );
    await active.agents.waitForIdle();
    expect(recovered).toBe(true);
    const [run] = await active.list("deliveryRun");
    expect(run.prompt.length).toBeLessThan(8000);
    await expect(
      active.agents.recall(run.conversationId, "policy:2"),
    ).rejects.toThrow(/access/i);
  });
});
