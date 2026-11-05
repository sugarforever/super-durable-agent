import { afterEach, describe, expect, it, vi } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { requestJson } from "../src/daemon/protocol.js";

const execute = promisify(execFile);
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("aborts a real open response after headers and GC and closes its connection", async () => {
  const { stdout } = await execute(process.execPath, [
    "--expose-gc",
    fileURLToPath(new URL("./helpers/response-timeout.mjs", import.meta.url)),
  ], { timeout: 4000 });
  expect(new Set(stdout.trim().split("\n"))).toEqual(new Set([
    "HEADERS_RECEIVED", "BODY_READ", "FORCED_GC", "ABORT_SIGNAL",
    "ABORT_REJECTION", "CONNECTION_CLOSED", "PASS",
  ]));
});

describe("response deadlines when fetch does not forward abort to its body", () => {
  for (const [name, prefix, cancelResult] of [
    ["incomplete JSON", '{"open":', "resolve"],
    ["complete JSON without EOF", '{"ok":true}', "resolve"],
    ["a rejecting cancellation", '{"open":', "reject"],
    ["a pending cancellation", '{"open":', "pending"],
  ] as const) {
    it(`cancels the active reader and rejects with AbortError for ${name}`, async () => {
      vi.useFakeTimers();
      let controller!: ReadableStreamDefaultController<Uint8Array>;
      let cancellationReason: unknown;
      let reading!: () => void;
      const readingStarted = new Promise<void>((resolve) => { reading = resolve; });
      const body = new ReadableStream<Uint8Array>({
        start(value) {
          controller = value;
          value.enqueue(new TextEncoder().encode(prefix));
        },
        pull() { reading(); },
        cancel(reason) {
          cancellationReason = reason;
          if (cancelResult === "reject") return Promise.reject(new Error("cleanup failed"));
          if (cancelResult === "pending") return new Promise<void>(() => {});
        },
      });
      // A real stream whose source deliberately ignores the request's signal.
      vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(body));
      const pending = requestJson("http://127.0.0.1", "/", "token", {}, 30)
        .then((value) => ({ value }), (error) => ({ error }));
      try {
        await readingStarted;
        await vi.advanceTimersByTimeAsync(30);
        const outcome = await Promise.race([
          pending,
          delay(200).then(() => ({ error: new Error("deadline did not settle") })),
        ]);
        expect(outcome).toMatchObject({ error: { name: "AbortError" } });
        expect(cancellationReason).toMatchObject({ name: "AbortError" });
        expect(body.locked).toBe(false);
        expect(vi.getTimerCount()).toBe(0);
      } finally {
        // Unblock unchanged production during RED without leaving a pending read.
        if (cancellationReason === undefined) controller.close();
        await pending;
      }
    });
  }
});

it("keeps the 413 response error when canceling an oversized open stream rejects", async () => {
  vi.useFakeTimers();
  let canceled = false;
  const body = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new Uint8Array(1_048_577)); },
    cancel() {
      canceled = true;
      return Promise.reject(new Error("cleanup failed"));
    },
  });
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(body));
  await expect(requestJson("http://127.0.0.1", "/", "token", {}))
    .rejects.toMatchObject({ status: 413 });
  expect(canceled).toBe(true);
  expect(body.locked).toBe(false);
  expect(vi.getTimerCount()).toBe(0);
});

for (const [name, text, status, expected] of [
  ["successful JSON", '{"ok":true}', 200, { ok: true }],
  ["HTTP errors", '{"error":"no access"}', 403, { status: 403, message: "no access" }],
  ["invalid JSON", '{"open":', 200, { message: "Invalid endpoint response" }],
] as const) {
  it(`releases the reader and clears the timer for ${name}`, async () => {
    vi.useFakeTimers();
    const response = new Response(text, { status });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response);
    const pending = requestJson("http://127.0.0.1", "/", "token", {});
    if (status === 200 && name === "successful JSON") await expect(pending).resolves.toEqual(expected);
    else await expect(pending).rejects.toMatchObject(expected);
    expect(response.body!.locked).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });
}
