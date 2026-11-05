import assert from "node:assert/strict";
import { createServer } from "node:http";
import { requestJson } from "../../dist/daemon/protocol.js";

const markers = new Set();
const mark = (name) => {
  markers.add(name);
  console.log(name);
};
const watchdog = setTimeout(() => {
  console.error("WATCHDOG", [...markers].join(","));
  process.exit(2);
}, 2500);
let closed;
const connectionClosed = new Promise((resolve) => { closed = resolve; });
const server = createServer((req, res) => {
  req.resume();
  res.setHeader("content-type", "application/json");
  res.on("close", () => {
    assert.equal(res.writableEnded, false);
    mark("CONNECTION_CLOSED");
    closed();
  });
  res.write('{"open":');
});

// Keep native fetch and the real loopback body. Only observe reading so GC
// happens after headers/data, across a task boundary, with a read pending.
const nativeFetch = globalThis.fetch;
let scheduledGc = false;
const observeRead = async (pending) => {
  const result = await pending;
  if (!result.done && !scheduledGc) {
    scheduledGc = true;
    mark("BODY_READ");
    setImmediate(() => {
      globalThis.gc();
      mark("FORCED_GC");
    });
  }
  return result;
};
globalThis.fetch = async (...args) => {
  args[1].signal.addEventListener("abort", () => mark("ABORT_SIGNAL"), { once: true });
  const response = await nativeFetch(...args);
  mark("HEADERS_RECEIVED");
  const body = response.body;
  const observedBody = new Proxy(body, {
    get(target, property) {
      // Cover both async iteration and explicit readers without changing either.
      if (property === Symbol.asyncIterator) return (...args) => {
        const iterator = target[Symbol.asyncIterator](...args);
        return new Proxy(iterator, {
          get(target, property) {
            if (property === "next") return (...args) => observeRead(target.next(...args));
            const value = Reflect.get(target, property, target);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      };
      if (property === "getReader") return (...args) => {
        const reader = target.getReader(...args);
        return new Proxy(reader, {
          get(target, property) {
            if (property === "read") return (...args) => observeRead(target.read(...args));
            const value = Reflect.get(target, property, target);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return new Proxy(response, {
    get(target, property) {
      if (property === "body") return observedBody;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
};

try {
  assert.equal(typeof globalThis.gc, "function", "run with --expose-gc");
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  await assert.rejects(requestJson(url, "/stalled-body", "token", {}, 500), { name: "AbortError" });
  mark("ABORT_REJECTION");
  for (const name of ["HEADERS_RECEIVED", "BODY_READ", "FORCED_GC", "ABORT_SIGNAL"])
    assert.ok(markers.has(name), `missing setup marker ${name}`);
  await connectionClosed;
  await new Promise((resolve) => server.close(resolve));
  mark("PASS");
} finally {
  clearTimeout(watchdog);
  server.closeAllConnections();
  server.close();
}
