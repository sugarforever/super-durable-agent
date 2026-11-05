import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { IncomingMessage, ServerResponse } from "node:http";
import { canonicalJson, digest } from "../domain/model.js";
export const BODY_LIMIT = 1_048_576;
export const RESPONSE_LIMIT = 1_048_576;
export class ProtocolError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export function equalSecret(a: string, b: string): boolean {
  return timingSafeEqual(
    createHash("sha256").update(a).digest(),
    createHash("sha256").update(b).digest(),
  );
}
export function authenticated(req: IncomingMessage, token: string): boolean {
  return equalSecret(req.headers.authorization ?? "", `Bearer ${token}`);
}
export function secureUrl(value: string, allowInsecureLoopback = false): URL {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !(
      url.protocol === "https:" ||
      (allowInsecureLoopback &&
        url.protocol === "http:" &&
        ["127.0.0.1", "[::1]"].includes(url.hostname))
    )
  )
    throw new Error(
      "HTTPS/TLS required; explicit insecure loopback exception is test-only",
    );
  return url;
}
export async function readJson(req: IncomingMessage): Promise<any> {
  const length = Number(req.headers["content-length"]);
  if (Number.isFinite(length) && length > BODY_LIMIT)
    throw new ProtocolError(413, "Request exceeds 1 MiB");
  const parts: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const bytes = Buffer.from(chunk);
    size += bytes.length;
    if (size > BODY_LIMIT)
      throw new ProtocolError(413, "Request exceeds 1 MiB");
    parts.push(bytes);
  }
  try {
    const result = JSON.parse(Buffer.concat(parts).toString("utf8"));
    if (!result || typeof result !== "object" || Array.isArray(result))
      throw new Error();
    return result;
  } catch {
    throw new ProtocolError(400, "Expected JSON object");
  }
}
export function sendJson(res: ServerResponse, status: number, value: unknown) {
  if (res.destroyed) return;
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > RESPONSE_LIMIT) {
    res.writeHead(413, { "content-type": "application/json" });
    res.end('{"error":"Response exceeds limit"}');
    return;
  }
  res.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  res.end(text);
}
export async function requestJson(
  base: string,
  path: string,
  token: string,
  body: unknown,
  timeoutMs = 10000,
): Promise<any> {
  const serialized = JSON.stringify(body);
  if (Buffer.byteLength(serialized) > BODY_LIMIT)
    throw new ProtocolError(413, "Request exceeds 1 MiB");
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), timeoutMs);
  timer.unref();
  try {
    const response = await fetch(base.replace(/\/$/, "") + path, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: serialized,
      signal: abort.signal,
      redirect: "error",
    });
    const chunks: Uint8Array[] = [];
    let size = 0;
    for await (const chunk of response.body ?? []) {
      size += chunk.length;
      if (size > RESPONSE_LIMIT) {
        abort.abort();
        throw new ProtocolError(413, "Response exceeds limit");
      }
      chunks.push(chunk);
    }
    let result: any;
    try {
      result = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    } catch {
      throw new Error("Invalid endpoint response");
    }
    if (!response.ok)
      throw new ProtocolError(
        response.status,
        typeof result.error === "string"
          ? result.error
          : "Endpoint rejected request",
      );
    return result;
  } finally {
    clearTimeout(timer);
  }
}
/** Private host-owned records, independent of the primary Harness. Single controller process per directory. */
export class JsonLedger<T> {
  private readonly locks = new Map<string, Promise<unknown>>();
  constructor(readonly directory: string) {}
  path(id: string) {
    return join(this.directory, `${digest(id)}.json`);
  }
  async read(id: string): Promise<T | undefined> {
    try {
      const { checksum, value } = JSON.parse(
        await readFile(this.path(id), "utf8"),
      );
      if (checksum !== digest(value))
        throw new Error("Transport ledger integrity failure");
      return value;
    } catch (error: any) {
      if (error.code === "ENOENT") return;
      throw error;
    }
  }
  async write(id: string, value: T): Promise<void> {
    const target = this.path(id);
    await mkdir(dirname(target), { recursive: true, mode: 0o700 });
    const temporary = `${target}.${randomUUID()}.tmp`;
    const file = await open(temporary, "wx", 0o600);
    try {
      await file.writeFile(canonicalJson({ value, checksum: digest(value) }));
      await file.sync();
    } finally {
      await file.close();
    }
    try {
      await rename(temporary, target);
      const dir = await open(dirname(target), "r");
      try {
        await dir.sync();
      } finally {
        await dir.close();
      }
    } finally {
      await unlink(temporary).catch(() => {});
    }
  }
  async serial<R>(id: string, fn: () => Promise<R>): Promise<R> {
    const previous = this.locks.get(id) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(fn);
    this.locks.set(id, next);
    try {
      return await next;
    } finally {
      if (this.locks.get(id) === next) this.locks.delete(id);
    }
  }
}
