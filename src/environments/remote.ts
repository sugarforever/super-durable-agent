import { readdir } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { canonicalJson, digest } from "../domain/model.js";
import {
  authenticated,
  BODY_LIMIT,
  RESPONSE_LIMIT,
  JsonLedger,
  ProtocolError,
  readJson,
  requestJson,
  secureUrl,
  sendJson,
} from "../daemon/protocol.js";
import {
  UncertainEnvironmentError,
  operationDigest,
  type BrokerReceipt,
  type EndpointAction,
  type EndpointAuthorizer,
  type EndpointIdentity,
  type EnvironmentBroker,
  type ExecuteRequest,
  type ExecutionReceipt,
  type LifecycleReceipt,
  type PreparedReceipt,
  type PrepareRequest,
} from "./types.js";
export interface RemoteEnvironmentOptions {
  endpointId: string;
  token: string;
  directory: string;
  transport: "http" | "poll";
  url?: string;
  allowInsecureLoopback?: boolean;
  timeoutMs?: number;
  authorize: EndpointAuthorizer;
}
export interface RemoteCommand {
  id: string;
  action: EndpointAction;
  body: any;
  identity: EndpointIdentity;
  digest: string;
  /** A delivery may have reached the daemon: query only, never launch a missing shell. */
  reconcileOnly?: boolean;
}
interface Record {
  command: RemoteCommand;
  receipt?: BrokerReceipt;
  error?: string;
  delivered?: boolean;
}
export interface EndpointDiagnostics {
  endpointId: string;
  transport: "http" | "poll";
  state: "online" | "offline" | "unknown";
  lastSuccessfulContact?: number;
  reason?: string;
  queued: number;
  active: number;
}
export class RemoteEnvironmentBroker implements EnvironmentBroker {
  private readonly ledger: JsonLedger<Record>;
  private readonly inFlight = new Map<string, Promise<BrokerReceipt>>();
  private readonly records = new Map<string, Record>();
  private initialized?: Promise<void>;
  private lastContact?: number;
  private failed = false;
  private continuityLost = false;
  private instanceId?: string;
  private readonly metadata: JsonLedger<{ instanceId: string }>;
  constructor(readonly options: RemoteEnvironmentOptions) {
    if (
      !/^[\w.-]{1,128}$/.test(options.endpointId) ||
      !options.token?.trim() ||
      !options.directory?.trim() ||
      !["http", "poll"].includes(options.transport) ||
      typeof options.authorize !== "function"
    )
      throw new Error("Invalid remote endpoint configuration");
    if (options.transport === "http")
      secureUrl(options.url ?? "", options.allowInsecureLoopback);
    if (
      options.timeoutMs !== undefined &&
      (!Number.isSafeInteger(options.timeoutMs) ||
        options.timeoutMs < 10 ||
        options.timeoutMs > 60000)
    )
      throw new Error("Invalid remote timeout");
    this.metadata = new JsonLedger(
      join(options.directory, options.endpointId, "identity"),
    );
    this.ledger = new JsonLedger(join(options.directory, options.endpointId));
  }
  async initialize(): Promise<void> {
    await this.ready();
  }
  bindAuthority(authorize: EndpointAuthorizer): void {
    this.options.authorize = authorize;
  }
  private async ready() {
    await (this.initialized ??= (async () => {
      this.instanceId = (await this.metadata.read("daemon-instance"))
        ?.instanceId;
      let names: string[];
      try {
        names = await readdir(this.ledger.directory);
      } catch (e: any) {
        if (e.code === "ENOENT") return;
        throw e;
      }
      // Filenames are hashes; read checksummed values without exposing commands through diagnostics.
      const { readFile } = await import("node:fs/promises");
      for (const name of names.filter((n) => /^[a-f0-9]{64}\.json$/.test(n))) {
        const value = JSON.parse(
          await readFile(join(this.ledger.directory, name), "utf8"),
        ).value as Record;
        const checked = await this.ledger.read(value.command.id);
        if (!checked || digest(checked.command) !== digest(value.command))
          throw new Error("Transport record conflict");
        this.records.set(value.command.id, checked);
      }
      if (
        !this.instanceId &&
        [...this.records.values()].some(
          (record) => record.delivered || record.receipt,
        )
      )
        throw new Error("Transport ledger continuity identity is missing");
    })());
  }
  diagnostics(): EndpointDiagnostics {
    const stale =
      this.lastContact !== undefined && Date.now() - this.lastContact > 30000;
    return {
      endpointId: this.options.endpointId,
      transport: this.options.transport,
      state: this.continuityLost
        ? "unknown"
        : this.failed
          ? "offline"
          : this.lastContact === undefined || stale
            ? "unknown"
            : "online",
      ...(this.lastContact === undefined
        ? { reason: "No successful endpoint contact observed" }
        : { lastSuccessfulContact: this.lastContact }),
      ...(this.failed
        ? {
            reason:
              "Endpoint contact failed; operation outcomes require receipt query",
          }
        : {}),
      ...(stale
        ? {
            reason:
              "No recent successful endpoint contact; connectivity is unknown",
          }
        : {}),
      ...(this.continuityLost
        ? {
            reason:
              "Daemon ledger continuity mismatch; operator recovery required",
          }
        : {}),
      queued: [...this.records.values()].filter(
        (r) => !r.receipt && !r.error && !r.delivered,
      ).length,
      active: [...this.records.values()].filter(
        (r) => !r.receipt && !r.error && r.delivered,
      ).length,
    };
  }
  private contact() {
    this.lastContact = Date.now();
    this.failed = false;
  }
  private async persist(record: Record) {
    await this.ledger.write(record.command.id, record);
    this.records.set(record.command.id, record);
  }
  private path(action: string) {
    return `/v1/endpoints/${this.options.endpointId}/${action}`;
  }
  private async pin(instanceId: unknown) {
    if (typeof instanceId !== "string" || !instanceId)
      throw new Error("Missing daemon ledger identity");
    await this.metadata.serial("daemon-instance", async () => {
      if (this.instanceId && this.instanceId !== instanceId) {
        this.continuityLost = true;
        throw new ProtocolError(409, "Daemon ledger continuity mismatch");
      }
      if (!this.instanceId) {
        await this.metadata.write("daemon-instance", { instanceId });
        this.instanceId = instanceId;
      }
    });
  }
  private async http(action: string, body: any) {
    try {
      const result = await requestJson(
        this.options.url!,
        this.path(action),
        this.options.token,
        {
          ...body,
          ...(this.instanceId ? { _ledgerInstanceId: this.instanceId } : {}),
        },
        this.options.timeoutMs,
      );
      await this.pin(result.instanceId);
      this.contact();
      return result.value;
    } catch (error) {
      if (error instanceof ProtocolError && /continuity/.test(error.message))
        this.continuityLost = true;
      if (!(error instanceof ProtocolError)) this.failed = true;
      throw error;
    }
  }
  private expectedDigest(command: RemoteCommand): string {
    return (
      command.body.payloadDigest ??
      operationDigest(
        command.identity,
        command.action === "release"
          ? { kind: "release", environmentId: command.body.environmentId }
          : command.body,
      )
    );
  }
  private unknown(record: Record): ExecutionReceipt {
    const body = record.command.body;
    return {
      operationId: body.operationId,
      identity: record.command.identity,
      payloadDigest: this.expectedDigest(record.command),
      status: "unknown",
      reason:
        "Remote outcome is unconfirmed; query existing operation, never replay shell",
    };
  }
  private async invoke(
    action: EndpointAction,
    body: any,
    identity: EndpointIdentity,
  ): Promise<BrokerReceipt> {
    await this.ready();
    if (this.continuityLost)
      throw new Error("Daemon ledger continuity mismatch");
    await this.options.authorize(identity, action);
    if (this.options.transport === "http" && !this.instanceId)
      await this.http("query", { operationId: "transport-handshake" });
    const id = body.operationId;
    if (typeof id !== "string" || !id.trim())
      throw new Error("operationId required");
    const command: RemoteCommand = {
      id,
      action,
      body,
      identity,
      digest: digest({ action, body, identity }),
    };
    if (Buffer.byteLength(canonicalJson(command)) > BODY_LIMIT - 1024)
      throw new ProtocolError(
        413,
        "Remote operation envelope exceeds byte limit",
      );
    const existing: Record = await this.ledger.serial(
      id,
      async (): Promise<Record> => {
        const previous = await this.ledger.read(id);
        if (previous && previous.command.digest !== command.digest)
          throw new Error("Operation payload conflict");
        if (previous) return previous;
        const record = { command };
        await this.persist(record);
        return record;
      },
    );
    if (existing.receipt) {
      if(existing.receipt.status==='cancelled' && !existing.delivered)return existing.receipt;
      if (this.options.transport === "http") {
        const current = await this.query(id);
        if (this.continuityLost)
          throw new UncertainEnvironmentError(
            id,
            "Daemon ledger continuity mismatch",
          );
        return current ?? this.unknown(existing);
      }
      return existing.receipt;
    }
    if (existing.error) throw new Error(existing.error);
    const live = this.inFlight.get(id);
    if (live) return live;
    const pending = this.deliver(existing);
    this.inFlight.set(id, pending);
    try {
      return await pending;
    } finally {
      this.inFlight.delete(id);
    }
  }
  private async deliver(record: Record): Promise<BrokerReceipt> {
    const { command } = record;
    if (this.options.transport === "http") {
      // Once a request could have reached the daemon, only query is allowed after transport loss.
      if (record.delivered) {
        const prior = await this.query(command.body.operationId);
        if (
          command.action === "execute" ||
          (prior && ["prepared", "released"].includes(prior.status))
        )
          return prior ?? this.unknown(record);
        // Preparation/release/cancel are idempotent daemon operations. Shell execution is never redelivered.
      }
      const admitted=await this.ledger.serial(command.id,async()=>{
        const latest=this.records.get(command.id)!;
        if(latest.receipt)return false;
        await this.persist({...latest,delivered:true});return true;
      });
      if(!admitted)return this.records.get(command.id)!.receipt!;
      try {
        const receipt = (await this.http(
          command.action,
          command.body,
        )) as BrokerReceipt;
        this.validateReceipt(command, receipt);
        if (!["running", "cancelPending", "unknown"].includes(receipt.status))
          await this.persist({ ...record, delivered: true, receipt });
        return receipt;
      } catch (error) {
        if (error instanceof ProtocolError) {
          await this.persist({
            ...record,
            delivered: true,
            error: error.message,
          });
          throw error;
        }
        return this.unknown(record);
      }
    }
    const deadline = performance.now() + (this.options.timeoutMs ?? 10000);
    while (performance.now() < deadline) {
      const latest = this.records.get(command.id)!;
      if (latest.receipt) return latest.receipt;
      if (latest.error) throw new Error(latest.error);
      await delay(10);
    }
    return this.unknown(record);
  }
  private validateReceipt(command: RemoteCommand, receipt: BrokerReceipt) {
    if (
      !receipt ||
      receipt.operationId !== command.body.operationId ||
      canonicalJson(receipt.identity) !== canonicalJson(command.identity) ||
      receipt.payloadDigest !== this.expectedDigest(command)
    )
      throw new Error("Remote receipt identity conflict");
  }
  async prepare(request: PrepareRequest): Promise<PreparedReceipt> {
    if (
      request.payloadDigest !==
      operationDigest(request.identity, request.payload)
    )
      throw new Error("Operation digest conflict");
    let result: BrokerReceipt;
    try {
      result = await this.invoke("prepare", request, request.identity);
    } catch (error) {
      if (
        error instanceof ProtocolError ||
        error instanceof UncertainEnvironmentError
      )
        throw error;
      if (this.failed) throw new UncertainEnvironmentError(request.operationId);
      throw error;
    }
    if (result.status !== "prepared")
      throw new UncertainEnvironmentError(
        request.operationId,
        "Remote preparation outcome unknown; query existing preparation operation",
      );
    return result;
  }
  async execute(request: ExecuteRequest): Promise<ExecutionReceipt> {
    if (
      request.payloadDigest !==
      operationDigest(request.identity, request.payload)
    )
      throw new Error("Operation digest conflict");
    return this.invoke(
      "execute",
      request,
      request.identity,
    ) as Promise<ExecutionReceipt>;
  }
  async query(operationId: string): Promise<BrokerReceipt | undefined> {
    await this.ready();
    const record = this.records.get(operationId);
    if (this.continuityLost && record) return this.unknown(record);
    if (this.options.transport === "http") {
      try {
        const receipt = (await this.http("query", { operationId })).receipt as
          BrokerReceipt | undefined;
        if (receipt && record) {
          this.validateReceipt(record.command, receipt);
          if (!["running", "unknown", "cancelPending"].includes(receipt.status))
            await this.persist({ ...record, receipt });
        }
        return (
          receipt ??
          (record?.delivered
            ? (record.receipt ?? this.unknown(record))
            : undefined)
        );
      } catch (error) {
        if (this.continuityLost && record) return this.unknown(record);
        if (error instanceof ProtocolError) throw error;
        return record
          ? (record.receipt ?? this.unknown(record))
          : Promise.reject(new UncertainEnvironmentError(operationId));
      }
    }
    // Poll receipts are returned by the daemon's ledger after every reconnect.
    return record ? (record.receipt ?? this.unknown(record)) : undefined;
  }
  async cancel(operationId: string, epoch: number): Promise<ExecutionReceipt> {
    await this.ready();
    const original = this.records.get(operationId);
    if (!original || original.command.identity.epoch !== epoch)
      throw new Error("Unknown operation or epoch conflict");
    await this.options.authorize(original.command.identity,'cancel');
    await this.ledger.serial(operationId,async()=>{
      const latest=this.records.get(operationId)!;
      if(!latest.delivered&&!latest.receipt)await this.persist({...latest,receipt:{operationId,payloadDigest:latest.command.body.payloadDigest,identity:latest.command.identity,status:'cancelled',reason:'Cancelled before transport delivery'}});
    });
    const result = await this.invoke(
      "cancel",
      {
        operationId: `transport-cancel:${operationId}:${epoch}`,
        targetOperationId: operationId,
        epoch,
        target: original.command.body,
      },
      original.command.identity,
    );
    const local = this.records.get(operationId);
    if(local?.receipt?.status === "cancelled" && !local.delivered) return local.receipt as ExecutionReceipt;
    return {
      ...result,
      operationId,
      payloadDigest: original.command.body.payloadDigest,
    } as ExecutionReceipt;
  }
  async release(
    environmentId: string,
    operationId: string,
  ): Promise<LifecycleReceipt> {
    await this.ready();
    const prepared = [...this.records.values()].find(
      (r) =>
        r.receipt?.status === "prepared" &&
        r.receipt.environmentId === environmentId,
    );
    if (!prepared) throw new Error("Unknown prepared environment");
    return this.invoke(
      "release",
      { operationId, environmentId },
      prepared.command.identity,
    ) as Promise<LifecycleReceipt>;
  }
  /** Mount only on a TLS listener outside explicit loopback tests. Authentication precedes body parsing. */
  async handleControl(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<boolean> {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;
    const prefix = `/remote/${this.options.endpointId}/`;
    if (!path.startsWith(prefix)) return false;
    try {
      if (!authenticated(req, this.options.token))
        throw new ProtocolError(401, "Authentication required");
      if (req.method !== "POST") throw new ProtocolError(405, "POST required");
      await this.ready();
      const body = await readJson(req);
      const action = path.slice(prefix.length);
      if (action === "authorize") {
        if (!["prepare", "execute", "cancel", "release"].includes(body.action))
          throw new ProtocolError(400, "Invalid authority action");
        let grant;
        try {
          grant = await this.options.authorize(body.identity, body.action);
        } catch {
          throw new ProtocolError(403, "Stale or revoked endpoint authority");
        }
        this.contact();
        sendJson(res, 200, { grant });
        return true;
      }
      if (action === "poll" && this.options.transport === "poll") {
        await this.pin(body.instanceId);
        this.contact();
        const values = [...this.records.values()].filter(
          (r) => !r.receipt && !r.error,
        );
        values.sort(
          (a, b) =>
            Number(b.command.action === "cancel") -
            Number(a.command.action === "cancel"),
        );
        const records: Record[] = [];
        const commands: RemoteCommand[] = [];
        for (const record of values.slice(0, 8)) {
          const command = {
            ...record.command,
            reconcileOnly:
              record.command.action === "execute" && record.delivered === true,
          };
          if (
            Buffer.byteLength(
              JSON.stringify({ commands: [...commands, command] }),
            ) > RESPONSE_LIMIT
          )
            break;
          await this.ledger.serial(record.command.id,async()=>{
            const latest=this.records.get(record.command.id)!;
            if(latest.receipt||latest.error)return;
            commands.push(command);
            await this.persist({...latest,delivered:true});
          });
        }
        sendJson(res, 200, { commands });
        return true;
      }
      if (action === "receipt" && this.options.transport === "poll") {
        await this.pin(body.instanceId);
        const record = this.records.get(body.id);
        if (!record || record.command.digest !== body.digest)
          throw new ProtocolError(409, "Unknown delivery or digest conflict");
        if (body.error)
          await this.persist({
            ...record,
            delivered: true,
            error: "Daemon rejected operation",
          });
        else {
          this.validateReceipt(record.command, body.receipt);
          await this.persist({
            ...record,
            delivered: true,
            receipt: body.receipt,
          });
        }
        this.contact();
        sendJson(res, 200, { ok: true });
        return true;
      }
      throw new ProtocolError(404, "Unknown remote control route");
    } catch (error) {
      sendJson(res, error instanceof ProtocolError ? error.status : 400, {
        error:
          error instanceof ProtocolError
            ? error.message
            : "Remote control request rejected",
      });
      return true;
    }
  }
}
