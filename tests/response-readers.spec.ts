import { afterEach, expect, it, vi } from "vitest";
import { execFile } from "node:child_process";
import { getEventListeners } from "node:events";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import { requestJson } from "../src/daemon/protocol.js";
import { readResponseBytes } from "../src/http/response.js";
import { pollWatchdog } from "../src/watchdog/runner.js";
import { WatchdogLedger } from "../src/watchdog/core.js";
import { FeishuSource } from "../src/sources/feishu.js";
import { GitHubSource } from "../src/sources/github.js";
import { openSuperAgent } from "../src/runtime/open.js";

const execute = promisify(execFile);
const cleanup: (() => Promise<unknown> | void)[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of cleanup.splice(0).reverse()) await close();
});

for (const mode of ["deadline", "close"] as const) {
  it(`settles real related response readers after body/GC and ${mode}, before teardown`, async () => {
    const { stdout } = await execute(process.execPath, ["--expose-gc",
      fileURLToPath(new URL("./helpers/related-response-timeouts.mjs", import.meta.url)), mode,
    ], { timeout: mode === "deadline" ? 14500 : 5000 });
    const names = mode === "deadline" ? ["watchdog", "feishu", "github"] : ["feishu", "github"];
    expect(new Set(stdout.trim().split("\n"))).toEqual(new Set([
      ...names.flatMap((name) => ["HEADERS_RECEIVED", "BODY_READ", "PENDING_READ", "FORCED_GC", "APPLICATION_ABORT", "SETTLED", "CONNECTION_CLOSED"].map((marker) => `${name}:${marker}`)),
      "PASS",
    ]));
  }, mode === "deadline" ? 16500 : 7000);
}

async function setup() {
  const root = await mkdtemp(join(tmpdir(), "response-readers-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const runtime = await openSuperAgent({ directory: join(root, "runtime") });
  cleanup.push(() => runtime.close());
  const feishu = new FeishuSource({ verificationToken: "fake-verification", encryptKey: "fake-encryption", appId: "fake-app", chats: ["fake-chat"], ownSenderIds: ["fake-bot"], accessTokenProvider: async () => "fake-token" });
  await feishu.configure(runtime);
  cleanup.push(() => feishu.close());
  const github = new GitHubSource({ repositories: ["fake/repo"], webhookSecret: "fake-secret" });
  cleanup.push(() => github.close());
  const ledger = new WatchdogLedger(join(root, "watchdog"));
  return { runtime, feishu, github, ledger };
}
const message = { destination: { sourceId: "feishu", threadRef: "fake-chat" }, payload: { content: "fake body" } } as any;
const snapshot = '{"tickets":[],"jobs":[],"endpoints":[]}';
const reply = '{"code":0,"data":{"message_id":"fake-message"}}';
const limit = 1_048_576;

for (const name of ["endpoint", "watchdog", "feishu"] as const) {
  for (const extra of [0, 1]) {
    it(`${name} ${extra ? "rejects and cancels one MiB plus one" : "accepts exactly one MiB"}`, async () => {
      const f = await setup();
      let canceled = false;
      const text = name === "watchdog" ? snapshot : name === "feishu" ? reply : '{"ok":true}';
      const bytes = new TextEncoder().encode(text + " ".repeat(limit + extra - text.length));
      const body = new ReadableStream<Uint8Array>({
        start(controller) { controller.enqueue(bytes); if (!extra) controller.close(); },
        cancel() { canceled = true; return Promise.reject(new Error("cleanup failed")); },
      });
      const response = new Response(body);
      let signal!: AbortSignal;
      vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
        signal = init!.signal!;
        return response;
      });
      const pending = name === "endpoint" ? requestJson("http://127.0.0.1", "/", "token", {})
        : name === "watchdog" ? pollWatchdog({ url: "https://fake.invalid", token: "fake-token" }, {}, f.ledger)
        : f.feishu.send(message, "fake-key");
      if (extra) {
        const expected = name === "endpoint" ? { status: 413, message: "Response exceeds limit" }
          : { message: name === "watchdog" ? "Snapshot exceeds limit" : "Feishu response exceeds limit" };
        await expect(pending).rejects.toMatchObject({ name: "Error", ...expected });
        expect(canceled).toBe(true);
        expect(await f.feishu.queryReceipt("fake-key")).toEqual({ status: "unknown" });
      } else {
        await expect(pending).resolves.toEqual(name === "endpoint" ? { ok: true } : name === "watchdog" ? [] : { messageId: "fake-message" });
        if (name === "feishu") {
          expect(await f.feishu.queryReceipt("fake-key")).toEqual({ status: "found", receipt: { messageId: "fake-message" } });
          // A repeat uses its durable receipt instead of another external send.
          vi.mocked(fetch).mockRejectedValue(new Error("must use cached receipt"));
          await expect(f.feishu.send(message, "fake-key")).resolves.toEqual({ messageId: "fake-message" });
        }
      }
      expect(body.locked).toBe(false);
      expect(getEventListeners(signal, "abort")).toEqual([]);
    });
  }
}

for (const cleanupResult of ["reject", "pending"] as const) {
  it(`Watchdog rejects non-2xx before parsing despite ${cleanupResult} cancellation cleanup`, async () => {
    const f = await setup();
    const initial = await f.ledger.read();
    let canceled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new TextEncoder().encode("not JSON")); },
      cancel() {
        canceled = true;
        return cleanupResult === "reject" ? Promise.reject(new Error("cleanup failed")) : new Promise<void>(() => {});
      },
    });
    const response = new Response(body, { status: 403 });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response);
    const pending = pollWatchdog({ url: "https://fake.invalid", token: "fake-token" }, {}, f.ledger);
    await expect(Promise.race([pending, delay(200).then(() => { throw new Error("status rejection did not settle"); })])).rejects.toThrow("Management snapshot rejected: 403");
    expect(canceled).toBe(true);
    expect(body.locked).toBe(false);
    expect(await f.ledger.read()).toEqual(initial);
  });
}

for (const text of ["not JSON", '{"code":1}', '{"code":0,"data":{"message_id":"not valid!"}}']) {
  it(`Feishu preserves uncertain/no-receipt outcome for ${text}`, async () => {
    const f = await setup();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(text));
    await expect(f.feishu.send(message, "fake-key")).rejects.toBeInstanceOf(Error);
    expect(await f.feishu.queryReceipt("fake-key")).toEqual({ status: "unknown" });
    expect(await f.runtime.list("feishuReceipt")).toEqual([]);
  });
}

for (const kind of ["ordinary", "BOM", "over-one-MiB"] as const) {
  it(`GitHub preserves native JSON decoding with ${kind} responses and no byte cap`, async () => {
    const f = await setup();
    const text = JSON.stringify([{ id: 7, body: f.github.marker("fake-key"), html_url: "https://example.invalid/comment/7", padding: kind === "over-one-MiB" ? "x".repeat(limit) : "" }]);
    const response = new Response((kind === "BOM" ? "\uFEFF" : "") + text);
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response);
    await expect(f.github.queryReceipt("fake-key")).resolves.toEqual({ status: "found", receipt: { id: 7, url: "https://example.invalid/comment/7" } });
  });
}

it("GitHub queryReceipt accepts two leading UTF-8 BOMs like native Response.json", async () => {
  const f = await setup();
  const comments = [{ id: 7, body: f.github.marker("fake-key") }];
  const bytes = Buffer.concat([
    Buffer.from([0xef, 0xbb, 0xbf, 0xef, 0xbb, 0xbf]),
    Buffer.from(JSON.stringify(comments)),
  ]);
  await expect(new Response(bytes).json()).resolves.toEqual(comments);
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(bytes));
  await expect(f.github.queryReceipt("fake-key")).resolves.toEqual({
    status: "found", receipt: { id: 7 },
  });
});

it("GitHub preserves unknown on JSON parse failure rather than making absence authoritative", async () => {
  const f = await setup();
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("not JSON"));
  await expect(f.github.queryReceipt("fake-key")).resolves.toEqual({ status: "unknown" });
});

// Extraction controls: late-aborted responses and read failures retain their
// original reason while releasing the acquired reader and abort subscription.
for (const kind of ["already-aborted", "read-failed", "aborted-null-body"] as const) {
  it(`shared reader preserves the primary reason for ${kind}`, async () => {
    const controller = new AbortController();
    const reason = new Error("original reason");
    let canceledWith: unknown;
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        if (kind === "read-failed") stream.error(reason);
        else stream.enqueue(new Uint8Array([1]));
      },
      cancel(value) {
        canceledWith = value;
        return Promise.reject(new Error("cleanup failed"));
      },
    });
    if (kind !== "read-failed") controller.abort(reason);
    const response = new Response(kind === "aborted-null-body" ? null : body);
    await expect(readResponseBytes(response, controller.signal)).rejects.toBe(reason);
    if (kind === "already-aborted") expect(canceledWith).toBe(reason);
    expect(body.locked).toBe(false);
    expect(getEventListeners(controller.signal, "abort")).toEqual([]);
  });
}
