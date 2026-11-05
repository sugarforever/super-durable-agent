import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pollWatchdog } from "../../dist/watchdog/runner.js";
import { WatchdogLedger } from "../../dist/watchdog/core.js";
import { FeishuSource } from "../../dist/sources/feishu.js";
import { GitHubSource } from "../../dist/sources/github.js";
import { openSuperAgent } from "../../dist/runtime/open.js";

const mode = process.argv[2];
assert.ok(["deadline", "close"].includes(mode));
assert.equal(typeof globalThis.gc, "function", "run with --expose-gc");
const names = mode === "deadline" ? ["watchdog", "feishu", "github"] : ["feishu", "github"];
const states = new Map(names.map((name) => [name, { markers: new Set(), requests: 0 }]));
const mark = (name, marker) => {
  states.get(name).markers.add(marker);
  console.log(`${name}:${marker}`);
};
const deferred = () => {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
};
for (const state of states.values()) {
  state.ready = deferred();
  state.closed = deferred();
}
// This bound is independent of the unchanged ten-second production deadlines.
const watchdog = setTimeout(() => {
  console.error("CHILD_WATCHDOG", JSON.stringify(Object.fromEntries([...states].map(([name, state]) =>
    [name, { markers: [...state.markers], outcome: state.outcome ?? "pending" }]))));
  // Do not end/destroy the stalled response to manufacture a settled result.
  process.exit(2);
}, mode === "deadline" ? 12500 : 3500);
const root = await mkdtemp(join(tmpdir(), "related-response-timeouts-"));
let runtime;
let feishu;
let github;
const prefixes = {
  watchdog: JSON.stringify({ tickets: [{ id: "fake-ticket", lifecycle: "open", revision: 1 }], jobs: [], endpoints: [] }),
  feishu: '{"code":0,"data":{"message_id":"fake-message"}}',
  github: '[{"id":7,"body":"<!-- super-agent:fake-key -->"}]',
};
const server = createServer((req, res) => {
  const name = req.url.startsWith("/taskGraph") ? "watchdog"
    : req.url.startsWith("/open-apis/") ? "feishu" : "github";
  const state = states.get(name);
  assert.ok(state, `unexpected request ${req.url}`);
  state.requests++;
  assert.equal(req.method, name === "feishu" ? "POST" : "GET");
  assert.equal(req.headers.authorization, `Bearer fake-${name}-token`);
  if (name === "github") assert.equal(new URL(req.url, "http://localhost").searchParams.get("page"), "1");
  req.resume();
  res.on("close", () => {
    assert.equal(res.writableEnded, false, "server must never finish the stalled body");
    mark(name, "CONNECTION_CLOSED");
    state.closed.resolve();
  });
  res.writeHead(200, { "content-type": "application/json" });
  // Complete, valid-looking JSON is still not a response until EOF.
  res.write(prefixes[name]);
});
const nativeFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  const name = new URL(url).pathname === "/taskGraph" ? "watchdog"
    : new URL(url).pathname.startsWith("/open-apis/") ? "feishu" : "github";
  const state = states.get(name);
  assert.equal(init.redirect, "error");
  init.signal.addEventListener("abort", () => mark(name, "APPLICATION_ABORT"), { once: true });
  const response = await nativeFetch(url, init);
  mark(name, "HEADERS_RECEIVED");
  const observe = (pending) => {
    let active = true;
    if (state.markers.has("BODY_READ") && !state.scheduled) {
      state.scheduled = true;
      mark(name, "PENDING_READ");
      setImmediate(() => {
        assert.ok(active, `${name}: subsequent read must still be pending before GC`);
        globalThis.gc();
        mark(name, "FORCED_GC");
        state.ready.resolve();
      });
    }
    return pending.then((value) => {
      active = false;
      if (!value.done && !state.markers.has("BODY_READ")) mark(name, "BODY_READ");
      return value;
    }, (error) => { active = false; throw error; });
  };
  const decorate = (target, method) => new Proxy(target, {
    get(target, property) {
      if (property === method) return (...args) => observe(target[method](...args));
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  // Instrument the actual native stream. A Response/body proxy misses native
  // response.json(), which is the original GitHub path this RED must exercise.
  const body = response.body;
  const getReader = body.getReader.bind(body);
  const iterator = body[Symbol.asyncIterator].bind(body);
  Object.defineProperty(body, "getReader", { value: (...args) => decorate(getReader(...args), "read") });
  Object.defineProperty(body, Symbol.asyncIterator, { value: (...args) => decorate(iterator(...args), "next") });
  return response;
};
try {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  runtime = await openSuperAgent({ directory: join(root, "runtime") });
  feishu = new FeishuSource({ verificationToken: "fake-verification", encryptKey: "fake-encryption", appId: "fake-app", chats: ["fake-chat"], ownSenderIds: ["fake-bot"], accessTokenProvider: async () => "fake-feishu-token", apiBase: base, allowInsecureLoopback: true });
  await feishu.configure(runtime);
  github = new GitHubSource({ repositories: ["fake/repo"], webhookSecret: "fake-webhook-secret", apiToken: "fake-github-token", apiBase: base });
  // Make the complete GitHub prefix look like a genuine found receipt, so EOF
  // accidentally manufactured by cancellation cannot satisfy unknown lookup.
  prefixes.github = JSON.stringify([{ id: 7, body: github.marker("fake-key") }]);
  const ledger = new WatchdogLedger(join(root, "watchdog"));
  const initialLedger = await ledger.read();
  await ledger.save(initialLedger);
  let evaluations = 0;
  let notifications = 0;
  const operations = {
    feishu: () => feishu.send({ destination: { sourceId: "feishu", threadRef: "fake-chat" }, payload: { content: "fake body" } }, "fake-key"),
    github: () => github.queryReceipt("fake-key"),
    watchdog: () => pollWatchdog({ url: base, token: "fake-watchdog-token", allowInsecureLoopback: true }, { thresholdMs: 0, maxCalls: 1, maxTokens: 100, maxCost: 1, callTokens: 100, callCost: 1 }, ledger,
      async () => { evaluations++; return { verdict: "stalled", reason: "must not evaluate" }; },
      async () => { notifications++; }),
  };
  const pending = names.map((name) => operations[name]().then((value) => {
    states.get(name).outcome = { value };
    mark(name, "SETTLED");
  }, (error) => {
    states.get(name).outcome = { error: { name: error.name, message: error.message } };
    mark(name, "SETTLED");
  }));
  if (mode === "close") {
    await Promise.all([...states.values()].map((state) => state.ready.promise));
    // A further task boundary separates GC from explicit adapter shutdown.
    await new Promise((resolve) => setImmediate(resolve));
    feishu.close();
    github.close();
  }
  await Promise.all(pending);
  for (const name of names) {
    const state = states.get(name);
    for (const marker of ["HEADERS_RECEIVED", "BODY_READ", "PENDING_READ", "FORCED_GC", "APPLICATION_ABORT", "SETTLED"])
      assert.ok(state.markers.has(marker), `${name}: missing ${marker}`);
    if (name === "github") assert.deepEqual(state.outcome, { value: { status: "unknown" } });
    else assert.equal(state.outcome.error?.name, name === "watchdog" ? "TimeoutError" : "AbortError");
    assert.equal(state.requests, 1, `${name}: no retry, action POST, or next page`);
    await state.closed.promise;
  }
  assert.equal(evaluations, 0);
  assert.equal(notifications, 0);
  assert.deepEqual(await ledger.read(), initialLedger);
  assert.deepEqual(await runtime.list("feishuReceipt"), []);
  assert.deepEqual(await feishu.queryReceipt("fake-key"), { status: "unknown" });
  // All outcomes and client-driven closures must precede any teardown.
  console.log("PASS");
} finally {
  clearTimeout(watchdog);
  globalThis.fetch = nativeFetch;
  feishu?.close();
  github?.close();
  server.closeAllConnections();
  server.close();
  await runtime?.close();
  await rm(root, { recursive: true, force: true });
}
