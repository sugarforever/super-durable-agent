import {
  createServer as createHttpServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { realpath } from "node:fs/promises";
import { relative, isAbsolute, sep, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { canonicalJson } from "../domain/model.js";
import { LocalEnvironmentBroker } from "../environments/local.js";
import {
  operationDigest,
  type EndpointAction,
  type EndpointAuthorizer,
  type EndpointIdentity,
  type BrokerReceipt,
} from "../environments/types.js";
import type { RemoteCommand } from "../environments/remote.js";
import {
  authenticated,
  JsonLedger,
  ProtocolError,
  readJson,
  requestJson,
  secureUrl,
  sendJson,
} from "./protocol.js";
export interface RemoteDaemonOptions {
  endpointId: string;
  token: string;
  directory: string;
  repository: string;
  allowedRepositoryRoots: string[];
  capabilities: string[];
  trustedLocalShell?: boolean;
  authorize: EndpointAuthorizer;
}
/** The local broker's fsynced admission ledger is independent of the controller/Harness. */
export class RemoteDaemon {
  readonly broker: LocalEnvironmentBroker;
  private readonly fences: JsonLedger<{ epoch: number }>;
  private readyPromise?: Promise<void>;
  private instance?: string;
  private readonly metadata: JsonLedger<{ instanceId: string }>;
  constructor(readonly options: RemoteDaemonOptions) {
    if (
      !/^[\w.-]{1,128}$/.test(options.endpointId) ||
      !options.token?.trim() ||
      !options.directory?.trim() ||
      !options.repository?.trim() ||
      !Array.isArray(options.allowedRepositoryRoots) ||
      !options.allowedRepositoryRoots.length ||
      !Array.isArray(options.capabilities) ||
      options.capabilities.some(
        (c) => !["repo.read", "repo.write", "command.execute"].includes(c),
      ) ||
      typeof options.authorize !== "function"
    )
      throw new Error("Invalid daemon configuration");
    this.metadata = new JsonLedger(join(options.directory, "identity"));
    this.fences = new JsonLedger(join(options.directory, "epoch-fences"));
    this.broker = new LocalEnvironmentBroker({
      directory: join(options.directory, "endpoint-ledger"),
      repository: options.repository,
      trustedLocalShell: options.trustedLocalShell,
      authorize: (identity, action) => this.authorize(identity, action),
    });
  }
  private async ready() {
    await (this.readyPromise ??= (async () => {
      await this.metadata.serial("ledger-instance", async () => {
        const saved = await this.metadata.read("ledger-instance");
        this.instance = saved?.instanceId ?? randomUUID();
        if (!saved)
          await this.metadata.write("ledger-instance", {
            instanceId: this.instance,
          });
      });
      const repository = await realpath(this.options.repository);
      let allowed = false;
      for (const root of this.options.allowedRepositoryRoots) {
        const path = relative(await realpath(root), repository);
        if (path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path))
          allowed = true;
      }
      if (!allowed)
        throw new Error("Daemon repository is outside allowed roots");
    })());
  }
  async instanceId(): Promise<string> {
    await this.ready();
    if (
      (await this.metadata.read("ledger-instance"))?.instanceId !==
      this.instance
    )
      throw new ProtocolError(409, "Daemon ledger continuity mismatch");
    return this.instance!;
  }
  private async authorize(identity: EndpointIdentity, action: EndpointAction) {
    await this.ready();
    const fence = await this.fences.read(identity.ticketId);
    if (
      action !== "cancel" &&
      action !== "release" &&
      fence &&
      identity.epoch <= fence.epoch
    )
      throw new Error("Endpoint epoch is fenced/revoked");
    const grant = await this.options.authorize(identity, action);
    const latestFence = await this.fences.read(identity.ticketId);
    if (
      action !== "cancel" &&
      action !== "release" &&
      latestFence &&
      identity.epoch <= latestFence.epoch
    )
      throw new Error("Endpoint epoch is fenced/revoked");
    const { capabilities, revokeEpoch, ...bound } = grant;
    if (
      canonicalJson(bound) !== canonicalJson(identity) ||
      !Array.isArray(capabilities)
    )
      throw new Error("Stale controller grant identity");
    return {
      ...identity,
      ...(action === "cancel" && revokeEpoch === true
        ? { revokeEpoch: true as const }
        : {}),
      capabilities: capabilities.filter((c) =>
        this.options.capabilities.includes(c),
      ),
    };
  }
  async dispatch(action: string, body: any): Promise<any> {
    await this.instanceId();
    if (action === "query") {
      if (
        typeof body.operationId !== "string" ||
        body.operationId.length > 1024
      )
        throw new ProtocolError(400, "Invalid operationId");
      return { receipt: (await this.broker.query(body.operationId)) ?? null };
    }
    if (action === "prepare" || action === "execute") {
      if (
        Object.keys(body).some(
          (k) =>
            !["operationId", "payloadDigest", "identity", "payload"].includes(
              k,
            ),
        )
      )
        throw new ProtocolError(
          403,
          "Caller capability assertions are forbidden",
        );
      try {
        return action === "prepare"
          ? await this.broker.prepare(body)
          : await this.broker.execute(body);
      } catch (error) {
        throw new ProtocolError(
          403,
          error instanceof Error
            ? error.message
            : "Endpoint admission rejected",
        );
      }
    }
    if (action === "cancel") {
      const target = body.targetOperationId;
      const original = await this.broker.query(target);
      if (!original || original.identity.epoch !== body.epoch)
        throw new ProtocolError(409, "Unknown operation or epoch conflict");
      const grant = await this.authorize(original.identity, "cancel");
      // Only trusted Ticket cancellation authority revokes the whole epoch. stop_job cancels one operation.
      if (grant.revokeEpoch === true)
        await this.fences.serial(original.identity.ticketId, async () => {
          const previous = await this.fences.read(original.identity.ticketId);
          await this.fences.write(original.identity.ticketId, {
            epoch: Math.max(previous?.epoch ?? 0, body.epoch),
          });
        });
      const receipt = await this.broker.cancel(target, body.epoch);
      return {
        ...receipt,
        operationId: body.operationId,
        payloadDigest: operationDigest(original.identity, body),
      };
    }
    if (action === "release")
      return this.broker.release(body.environmentId, body.operationId);
    throw new ProtocolError(404, "Unknown endpoint action");
  }
  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    try {
      if (!authenticated(req, this.options.token))
        throw new ProtocolError(401, "Authentication required");
      if (req.method !== "POST") throw new ProtocolError(405, "POST required");
      const prefix = `/v1/endpoints/${this.options.endpointId}/`;
      const path = new URL(req.url ?? "/", "http://localhost").pathname;
      if (!path.startsWith(prefix))
        throw new ProtocolError(404, "Unknown endpoint");
      const body = await readJson(req);
      const expected = body._ledgerInstanceId;
      delete body._ledgerInstanceId;
      const instanceId = await this.instanceId();
      if (expected !== undefined && expected !== instanceId)
        throw new ProtocolError(409, "Daemon ledger continuity mismatch");
      sendJson(res, 200, {
        instanceId,
        value: await this.dispatch(path.slice(prefix.length), body),
      });
    } catch (error) {
      sendJson(res, error instanceof ProtocolError ? error.status : 403, {
        error:
          error instanceof Error ? error.message : "Endpoint request rejected",
      });
    }
  }
}
export interface DaemonServerOptions {
  host?: string;
  port: number;
  allowInsecureLoopback?: boolean;
  tls?: { key: string | Buffer; cert: string | Buffer };
}
export async function startDaemonServer(
  daemon: RemoteDaemon,
  options: DaemonServerOptions,
) {
  const host = options.host ?? "127.0.0.1";
  if (
    !options.tls &&
    !(
      options.allowInsecureLoopback === true &&
      ["127.0.0.1", "::1"].includes(host)
    )
  )
    throw new Error("Daemon TLS required; insecure loopback is test-only");
  const handler = (req: IncomingMessage, res: ServerResponse) => {
    void daemon.handle(req, res);
  };
  const server = options.tls
    ? createHttpsServer(options.tls, handler)
    : createHttpServer(handler);
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Invalid daemon listener");
  let closing: Promise<void> | undefined;
  return {
    address: `${options.tls ? "https" : "http"}://${address.family === "IPv6" ? `[${address.address}]` : address.address}:${address.port}`,
    close: () =>
      (closing ??= new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
        server.closeAllConnections();
      })),
  };
}
export interface PollingOptions {
  url: string;
  token: string;
  allowInsecureLoopback?: boolean;
  intervalMs?: number;
  timeoutMs?: number;
}
/** Outbound-only daemon connection. Repeated deliveries consult the daemon ledger; no command replay after process loss. */
export async function runDaemonPolling(
  daemon: RemoteDaemon,
  options: PollingOptions,
) {
  secureUrl(options.url, options.allowInsecureLoopback);
  if (
    (options.intervalMs !== undefined &&
      (!Number.isSafeInteger(options.intervalMs) ||
        options.intervalMs < 10 ||
        options.intervalMs > 60000)) ||
    (options.timeoutMs !== undefined &&
      (!Number.isSafeInteger(options.timeoutMs) ||
        options.timeoutMs < 10 ||
        options.timeoutMs > 60000))
  )
    throw new Error("Invalid daemon polling interval or timeout");
  const prefix = `/remote/${daemon.options.endpointId}/`;
  const instanceId = await daemon.instanceId();
  let closed = false;
  const active = new Map<string, Promise<void>>();
  const readyReceipts = new Map<
    string,
    { id: string; digest: string; receipt?: BrokerReceipt; error?: string }
  >();
  const tick = async () => {
    for (const [id, value] of readyReceipts) {
      await requestJson(
        options.url,
        prefix + "receipt",
        options.token,
        { ...value, instanceId },
        options.timeoutMs,
      );
      readyReceipts.delete(id);
    }
    const result = await requestJson(
      options.url,
      prefix + "poll",
      options.token,
      { instanceId },
      options.timeoutMs,
    );
    if (!Array.isArray(result.commands) || result.commands.length > 8)
      throw new Error("Invalid poll response");
    for (const command of result.commands as RemoteCommand[]) {
      if (active.has(command.id) || readyReceipts.has(command.id)) continue;
      const task = (async () => {
        try {
          const receipt =
            command.reconcileOnly && command.action === "execute"
              ? ((await daemon.broker.query(command.body.operationId)) ?? {
                  operationId: command.body.operationId,
                  identity: command.identity,
                  payloadDigest: command.body.payloadDigest,
                  status: "unknown" as const,
                  reason:
                    "Previously delivered operation has no daemon admission receipt; shell will not be replayed",
                })
              : await daemon.dispatch(command.action, command.body);
          readyReceipts.set(command.id, {
            id: command.id,
            digest: command.digest,
            receipt,
          });
        } catch {
          readyReceipts.set(command.id, {
            id: command.id,
            digest: command.digest,
            error: "Daemon admission rejected",
          });
        }
      })().finally(() => active.delete(command.id));
      active.set(command.id, task);
    }
  };
  const loop = (async () => {
    while (!closed) {
      try {
        await tick();
      } catch {
        /* Offline: retain receipts locally/in the daemon ledger and reconnect. */
      }
      if (!closed) await delay(options.intervalMs ?? 250);
    }
  })();
  return {
    close: async () => {
      closed = true;
      await loop;
      await Promise.allSettled(active.values());
    },
  };
}
