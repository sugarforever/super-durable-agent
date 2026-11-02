import { createServer as createHttpsServer } from "node:https";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createHash, timingSafeEqual } from "node:crypto";
import type { SuperAgentRuntime } from "../runtime/open.js";
import type {
  Actor,
  NormalizedEvent,
  Policy,
  Ticket,
} from "../domain/model.js";
import { FeishuError, type FeishuSource } from "../sources/feishu.js";
import type { RemoteEnvironmentBroker } from "../environments/remote.js";
import { GitHubError, type GitHubSource } from "../sources/github.js";

export interface ManagementPrincipal {
  token: string;
  role: "operator" | "viewer" | "watchdog";
  id: string;
  allowPause?: boolean;
}
export interface ManagementOptions {
  host?: string;
  port: number;
  token?: string;
  principals?: ManagementPrincipal[];
  github?: GitHubSource;
  feishu?: FeishuSource;
  remote?: RemoteEnvironmentBroker;
  tls?: { key: string | Buffer; cert: string | Buffer };
  allowInsecureLoopback?: boolean;
}
class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function readBody(request: IncomingMessage): Promise<Buffer> {
  const length = Number(request.headers["content-length"]);
  if (Number.isFinite(length) && length > 1_048_576)
    throw new HttpError(413, "Body exceeds 1 MiB");
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const b = Buffer.from(chunk);
    bytes += b.length;
    if (bytes > 1_048_576) throw new HttpError(413, "Body exceeds 1 MiB");
    chunks.push(b);
  }
  return Buffer.concat(chunks);
}
/** Conservative export scrubber. Only whitelisted domain projections are exported by routes. */
export function redact(
  value: unknown,
  secrets: readonly string[] = [],
): unknown {
  if (typeof value === "string") {
    let text = value
      .replace(/Bearer\s+[^\s"'<>]+/gi, "Bearer [redacted]")
      .replace(
        /\b(?:gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|sk-[A-Za-z0-9_-]+)\b/g,
        "[redacted]",
      );
    for (const secret of secrets)
      if (secret) text = text.split(secret).join("[redacted]");
    return text;
  }
  if (Array.isArray(value)) return value.map((v) => redact(v, secrets));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        /(?:token|secret|password|credential|api.?key|authorization)/i.test(
          key,
        ) && !/(?:Ref|Reference|Env)$/i.test(key)
          ? "[redacted]"
          : redact(item, secrets),
      ]),
    );
  return value;
}
export async function startManagementServer(
  runtime: SuperAgentRuntime,
  options: ManagementOptions,
) {
  if (options.token !== undefined && !options.token.trim())
    throw new Error("Management token must be nonempty");
  const principals: ManagementPrincipal[] = [
    ...(options.token
      ? [{ token: options.token, role: "operator" as const, id: "operator" }]
      : []),
    ...(options.principals ?? []),
  ];
  if (
    !principals.length ||
    principals.some(
      (p) =>
        !p.token?.trim() ||
        !p.id?.trim() ||
        !["operator", "viewer", "watchdog"].includes(p.role),
    ) ||
    new Set(principals.map((p) => p.token)).size !== principals.length
  )
    throw new Error("Explicit unique management token principals required");
  if (
    !Number.isSafeInteger(options.port) ||
    options.port < 0 ||
    options.port > 65535
  )
    throw new Error("Invalid management port");
  await options.remote?.initialize();
  const hashes = principals.map((p) =>
    createHash("sha256").update(p.token).digest(),
  );
  const authenticate = (header: string | undefined) => {
    const hash = createHash("sha256")
      .update(header?.startsWith("Bearer ") ? header.slice(7) : "")
      .digest();
    let found: ManagementPrincipal | undefined;
    for (let i = 0; i < hashes.length; i++)
      if (timingSafeEqual(hash, hashes[i])) found = principals[i];
    return found;
  };
  const secrets = principals.map((p) => p.token);
  const safeTickets = async () =>
    (await runtime.list<Ticket>("ticket")).map((t) => ({
      id: t.id,
      goal: t.goal,
      lifecycle: t.lifecycle,
      revision: t.revision,
      assignmentId: t.assignmentId,
      assignmentEpoch: t.assignmentEpoch,
      attention: t.attention,
      cancelPending: t.cancelPending,
      latestReport: t.latestReport,
    }));
  if (
    options.remote &&
    !options.tls &&
    !(
      options.allowInsecureLoopback === true &&
      ["127.0.0.1", "::1"].includes(options.host ?? "127.0.0.1")
    )
  )
    throw new Error(
      "Remote control listener requires TLS; insecure loopback is test-only",
    );
  const handler = async (req: IncomingMessage, res: ServerResponse) => {
    const send = (status: number, value: unknown) => {
      if (res.destroyed) return;
      res.statusCode = status;
      res.setHeader("content-type", "application/json");
      res.setHeader("cache-control", "no-store");
      if (status === 503) res.setHeader("retry-after", "2");
      res.end(JSON.stringify(redact(value, secrets)));
    };
    try {
      const path = new URL(req.url ?? "/", "http://localhost").pathname;
      if (options.remote && (await options.remote.handleControl(req, res)))
        return;
      if (path === "/webhooks/feishu") {
        if (req.method !== "POST")
          throw new HttpError(405, "Method not allowed");
        if (!options.feishu)
          throw new HttpError(404, "Feishu source not configured");
        const raw = await readBody(req);
        const headers = Object.fromEntries(
          Object.entries(req.headers).map(([k, v]) => [
            k,
            typeof v === "string" ? v : undefined,
          ]),
        );
        const result = await options.feishu.webhook(runtime, raw, headers);
        send(
          "disposition" in result && result.disposition === "backpressure"
            ? 503
            : 200,
          result,
        );
        return;
      }
      if (path === "/webhooks/github") {
        if (req.method !== "POST")
          throw new HttpError(405, "Method not allowed");
        if (!options.github)
          throw new HttpError(404, "GitHub source not configured");
        const raw = await readBody(req);
        const headers = Object.fromEntries(
          Object.entries(req.headers).map(([k, v]) => [
            k,
            typeof v === "string" ? v : undefined,
          ]),
        );
        const result = await options.github.webhook(runtime, raw, headers);
        send(result.disposition === "backpressure" ? 503 : 200, result);
        return;
      }
      const principal = authenticate(req.headers.authorization);
      if (!principal) throw new HttpError(401, "Authentication required");
      const ticketMatch =
        /^\/tickets\/([^/]+)(?:\/(stop|retry|pause|requestStatus))?$/.exec(
          path,
        );
      const readRoute =
        [
          "/health",
          "/status",
          "/tickets",
          "/attention",
          "/taskGraph",
          "/policy",
          "/sources",
        ].includes(path) || !!(ticketMatch && !ticketMatch[2]);
      const writeRoute =
        ["/policy", "/receive", "/reconcile"].includes(path) ||
        !!ticketMatch?.[2];
      if (req.method === "GET" && readRoute) {
        let value: unknown;
        if (path === "/tickets") value = await safeTickets();
        else if (path === "/attention")
          value = (await safeTickets()).filter(
            (t) => t.attention || t.cancelPending,
          );
        else if (path === "/policy")
          value =
            (await runtime.engine.store.read("policy", "current")) ?? null;
        else if (path === "/sources") value = await runtime.list("source");
        else if (path === "/taskGraph") {
          const native = await runtime.harness.inspect(runtime.engine.context);
          value = {
            tasks: native.tasks.map((t) => ({
              id: t.record.id,
              kind: t.record.kind,
              status: t.record.state.status,
            })),
            tickets: await safeTickets(),
            endpoints: options.remote ? [options.remote.diagnostics()] : [],
            jobs: (await runtime.list<any>("job")).map((j) => ({
              id: j.id,
              ticketId: j.ticketId,
              epoch: j.epoch,
              status: j.status,
            })),
          };
        } else if (ticketMatch) {
          value = (await safeTickets()).find(
            (t) => t.id === decodeURIComponent(ticketMatch[1]),
          );
          if (!value) throw new HttpError(404, "Unknown Ticket");
        } else
          value = {
            ok: true,
            tickets: (await safeTickets()).length,
            environmentConfigured:
              !!options.remote || !!runtime.agents?.jobs.broker,
            endpoints: options.remote ? [options.remote.diagnostics()] : [],
            pendingIntake: (await runtime.list<any>("intake")).filter(
              (i) => !i.released,
            ).length,
            outbox: (await runtime.list<any>("outbox")).map((m) => ({
              id: m.id,
              status: m.status,
              attempt: m.attempt,
              nextAttemptAt: m.nextAttemptAt,
            })),
          };
        send(200, value);
        return;
      }
      if (req.method !== "POST" || !writeRoute)
        throw new HttpError(
          readRoute || writeRoute ? 405 : 404,
          readRoute || writeRoute ? "Method not allowed" : "Unknown route",
        );
      const control = ticketMatch?.[2];
      const supervisory =
        path === "/reconcile" ||
        control === "requestStatus" ||
        (control === "pause" && principal.allowPause === true);
      if (
        principal.role !== "operator" &&
        !(principal.role === "watchdog" && supervisory)
      )
        throw new HttpError(403, "Role does not authorize this command");
      const raw = await readBody(req);
      let body: any;
      try {
        body = raw.length ? JSON.parse(raw.toString("utf8")) : {};
      } catch {
        throw new HttpError(400, "Invalid JSON body");
      }
      if (!body || typeof body !== "object" || Array.isArray(body))
        throw new HttpError(400, "Expected JSON object");
      const actor: Actor = {
        id: principal.id,
        kind: principal.role === "operator" ? "human" : "system",
        trusted: true,
      };
      let result: unknown = { ok: true };
      if (path === "/receive")
        result = await runtime.receive({ ...body, actor } as NormalizedEvent);
      else if (path === "/reconcile") await runtime.outbox.reconcile();
      else {
        if (typeof body.commandId !== "string" || !body.commandId.trim())
          throw new HttpError(400, "commandId required");
        const meta = {
          commandId: `management:${principal.id}:${body.commandId}`,
          ...(body.expectedRevision === undefined
            ? {}
            : { expectedRevision: body.expectedRevision }),
        };
        if (path === "/policy")
          await runtime.updatePolicy(body.policy as Policy, actor, meta);
        else {
          const id = decodeURIComponent(ticketMatch![1]);
          if (!(await runtime.readTicket(id)))
            throw new HttpError(404, "Unknown Ticket");
          if (control === "stop") await runtime.stopTicket(id, actor, meta);
          else if (control === "retry")
            await runtime.retryTicket(id, actor, meta);
          else if (control === "pause") {
            // Narrow host authority: a configured watchdog may revoke, never grant or reopen.
            await runtime.engine.command(
              meta.commandId,
              {
                operation: "supervisoryPause",
                ticketId: id,
                principal: principal.id,
              },
              (tx) =>
                runtime.engine.authorization.stopInTransaction(
                  tx,
                  id,
                  "Paused by authenticated supervisor",
                  meta.expectedRevision,
                ),
            );
            runtime.agents?.kick();
          } else
            result = { ticket: (await safeTickets()).find((t) => t.id === id) };
        }
      }
      send((result as any)?.disposition === "backpressure" ? 503 : 200, result);
    } catch (error) {
      if (
        error instanceof HttpError ||
        error instanceof GitHubError ||
        error instanceof FeishuError
      )
        send(error.status, { error: error.message });
      else
        send(400, {
          error: "Command rejected",
          detail: error instanceof Error ? error.message : "Invalid request",
        });
    }
  };
  const server = options.tls
    ? createHttpsServer(options.tls, handler)
    : createServer(handler);
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port, options.host ?? "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Invalid server address");
  let closing: Promise<void> | undefined;
  return {
    address: `${options.tls ? "https" : "http"}://${address.family === "IPv6" ? `[${address.address}]` : address.address}:${address.port}`,
    close: () =>
      (closing ??= new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
        server.closeIdleConnections();
      })),
  };
}
