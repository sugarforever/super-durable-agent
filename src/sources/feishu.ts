import { createHash, createDecipheriv } from "node:crypto";
import type { Tx } from "@earendil-works/pi-durable";
import {
  digest,
  scopedId,
  type JsonValue,
  type NormalizedEvent,
  type ReceiveBatchResult,
  type Source,
} from "../domain/model.js";
import type { SuperAgentRuntime } from "../runtime/open.js";
import {
  NoSendError,
  type SourceAdapter,
  type MessageDestination,
  type OutboundMessage,
  type ReceiptLookup,
} from "../services/outbox.js";
import { equalSecret, secureUrl } from "../daemon/protocol.js";
import { readResponseBytes } from "../http/response.js";
export class FeishuError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export interface AccessToken {
  token: string;
  expiresAt: number;
}
export type AccessTokenProvider = () => Promise<string>;
/** Trusted credentials stay outside model/persisted source state; coalesce refresh and honor provider expiry. */
export function cachedAccessTokenProvider(
  load: () => Promise<AccessToken>,
  now = () => Date.now(),
): AccessTokenProvider {
  let cached: AccessToken | undefined,
    refreshing: Promise<AccessToken> | undefined;
  return async () => {
    if (cached && cached.expiresAt - 30000 > now()) return cached.token;
    const pending = (refreshing ??= load());
    try {
      const value = await pending;
      if (
        !value.token?.trim() ||
        !Number.isFinite(value.expiresAt) ||
        value.expiresAt <= now()
      )
        throw new Error("Access token is unavailable or expired");
      cached = value;
      return value.token;
    } finally {
      if (refreshing === pending) refreshing = undefined;
    }
  };
}
export interface FeishuOptions {
  sourceId?: string;
  verificationToken: string;
  encryptKey: string;
  appId: string;
  chats: string[];
  trustedUsers?: string[];
  trustedBots?: string[];
  /** Verified bot open IDs in sender.sender_id, not header.app_id (the receiving app). */
  ownSenderIds?: string[];
  enabled?: boolean;
  listenedScopes?: string[];
  addressed?: boolean;
  accessTokenProvider?: AccessTokenProvider;
  apiBase?: string;
  allowInsecureLoopback?: boolean;
  replayWindowMs?: number;
  timeoutMs?: number;
}
interface Delivery {
  id: string;
  hash: string;
}
interface Nonce {
  id: string;
  expiresAt: number;
}
function object(value: any): Record<string, any> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new FeishuError(400, "Invalid Feishu payload");
  return value;
}
function identifier(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_.:-]{1,256}$/.test(value);
}
/** Ordinary event subscription protocol, intentionally excluding legacy card callback signatures. */
export class FeishuSource implements SourceAdapter {
  readonly sourceId: string;
  readonly enabled: boolean;
  private readonly chats: Set<string>;
  private readonly ownSenders: Set<string>;
  private runtime?: SuperAgentRuntime;
  private readonly base: string;
  private readonly requests = new Set<AbortController>();
  constructor(readonly options: FeishuOptions) {
    this.sourceId = options.sourceId ?? "feishu";
    this.enabled = options.enabled ?? true;
    if (
      !identifier(this.sourceId) ||
      !options.verificationToken?.trim() ||
      !options.encryptKey?.trim() ||
      !identifier(options.appId) ||
      !Array.isArray(options.chats) ||
      !options.chats.length ||
      options.chats.some((c) => !identifier(c))
    )
      throw new Error(
        "Feishu app, verification token, encrypt key and chat allowlist are required",
      );
    for (const ids of [
      options.trustedUsers,
      options.trustedBots,
      options.ownSenderIds,
    ])
      if (
        ids !== undefined &&
        (!Array.isArray(ids) || ids.some((id) => !identifier(id)))
      )
        throw new Error("Invalid Feishu sender IDs");
    if (
      (options.enabled !== undefined && typeof options.enabled !== "boolean") ||
      (options.addressed !== undefined &&
        typeof options.addressed !== "boolean") ||
      (options.listenedScopes !== undefined &&
        (!Array.isArray(options.listenedScopes) ||
          options.listenedScopes.some(
            (s) => typeof s !== "string" || !s.trim(),
          )))
    )
      throw new Error("Invalid Feishu source settings");
    if (
      options.replayWindowMs !== undefined &&
      (!Number.isSafeInteger(options.replayWindowMs) ||
        options.replayWindowMs < 1000 ||
        options.replayWindowMs > 300000)
    )
      throw new Error("Invalid Feishu replay window");
    if (
      options.timeoutMs !== undefined &&
      (!Number.isSafeInteger(options.timeoutMs) ||
        options.timeoutMs < 10 ||
        options.timeoutMs > 60000)
    )
      throw new Error("Invalid Feishu timeout");
    if (
      options.allowInsecureLoopback !== undefined &&
      typeof options.allowInsecureLoopback !== "boolean"
    )
      throw new Error("Invalid Feishu loopback flag");
    this.chats = new Set(options.chats);
    this.ownSenders = new Set(options.ownSenderIds ?? []);
    this.base = (options.apiBase ?? "https://open.feishu.cn").replace(
      /\/$/,
      "",
    );
    secureUrl(this.base, options.allowInsecureLoopback);
  }
  async configure(runtime: SuperAgentRuntime) {
    this.runtime = runtime;
    await runtime.engine.configureSource({
      id: this.sourceId,
      kind: "feishu",
      enabled: this.enabled,
      ...(this.options.listenedScopes
        ? { listenedScopes: this.options.listenedScopes }
        : {}),
    });
  }
  private destination(ref: string) {
    const match =
      /^([A-Za-z0-9_.:-]{1,256})(?:\/message\/([A-Za-z0-9_.:-]{1,256}))?$/.exec(
        ref,
      );
    return match && this.chats.has(match[1])
      ? { chat: match[1], message: match[2] }
      : undefined;
  }
  listened(destination: MessageDestination): boolean {
    return (
      this.enabled &&
      destination.sourceId === this.sourceId &&
      !!this.destination(destination.threadRef) &&
      (!this.options.listenedScopes ||
        this.options.listenedScopes.includes(destination.threadRef))
    );
  }
  private async source(runtime: SuperAgentRuntime, tx?: Tx, ref?: string) {
    const source = tx
      ? await runtime.engine.store.get<Source>(tx, "source", this.sourceId)
      : await runtime.engine.store.read<Source>("source", this.sourceId);
    if (!this.enabled || !source?.enabled)
      throw new FeishuError(403, "Feishu source disabled");
    if (
      ref &&
      (!this.listened({ sourceId: this.sourceId, threadRef: ref }) ||
        (source.listenedScopes && !source.listenedScopes.includes(ref)))
    )
      throw new FeishuError(403, "Feishu chat or scope is not listened to");
    return source;
  }
  private parse(raw: Buffer, headers: Record<string, string | undefined>) {
    if (raw.length > 1_048_576)
      throw new FeishuError(413, "Feishu event exceeds 1 MiB");
    const timestamp = headers["x-lark-request-timestamp"],
      nonce = headers["x-lark-request-nonce"];
    const millis = Number(timestamp) * 1000,
      now = Date.now(),
      window = this.options.replayWindowMs ?? 300000;
    if (
      !timestamp ||
      !/^\d{10,13}$/.test(timestamp) ||
      !Number.isFinite(millis) ||
      Math.abs(now - millis) > window
    )
      throw new FeishuError(401, "Invalid or stale Feishu timestamp");
    if (!nonce || nonce.length > 256)
      throw new FeishuError(401, "Missing Feishu nonce");
    const signature = createHash("sha256")
      .update(timestamp + nonce + this.options.encryptKey)
      .update(raw)
      .digest("hex");
    if (!equalSecret(signature, headers["x-lark-signature"] ?? ""))
      throw new FeishuError(401, "Invalid Feishu signature");
    let data: any;
    try {
      data = object(JSON.parse(raw.toString("utf8")));
      if ("encrypt" in data) {
        if (
          typeof data.encrypt !== "string" ||
          !/^[A-Za-z0-9+/]+={0,2}$/.test(data.encrypt)
        )
          throw new Error();
        const cipher = Buffer.from(data.encrypt, "base64");
        if (cipher.length < 32 || cipher.length % 16 !== 0) throw new Error();
        const decipher = createDecipheriv(
          "aes-256-cbc",
          createHash("sha256").update(this.options.encryptKey).digest(),
          cipher.subarray(0, 16),
        );
        data = object(
          JSON.parse(
            Buffer.concat([
              decipher.update(cipher.subarray(16)),
              decipher.final(),
            ]).toString("utf8"),
          ),
        );
      }
    } catch {
      throw new FeishuError(400, "Invalid Feishu event envelope");
    }
    const supplied =
      data.schema !== undefined || data.header !== undefined
        ? data.header?.token
        : data.token;
    if (
      typeof supplied !== "string" ||
      !equalSecret(supplied, this.options.verificationToken)
    )
      throw new FeishuError(401, "Invalid Feishu verification token");
    return {
      data,
      nonce: digest([timestamp, nonce]),
      expiresAt: millis + window,
    };
  }
  private normalized(data: any): {
    event: NormalizedEvent;
    own: boolean;
    eventId: string;
  } {
    const header = object(data.header),
      event = object(data.event),
      sender = object(event.sender),
      message = object(event.message);
    if (
      data.schema !== "2.0" ||
      header.event_type !== "im.message.receive_v1" ||
      header.app_id !== this.options.appId ||
      !identifier(header.event_id)
    )
      throw new FeishuError(400, "Unsupported Feishu event or app");
    if (!identifier(message.chat_id) || !this.chats.has(message.chat_id))
      throw new FeishuError(403, "Feishu chat is not allowed");
    if (
      !identifier(message.message_id) ||
      !["user", "bot"].includes(sender.sender_type) ||
      !identifier(sender.sender_id?.open_id) ||
      message.message_type !== "text"
    )
      throw new FeishuError(400, "Unsupported Feishu message/sender");
    let body: string;
    try {
      const content = JSON.parse(message.content);
      if (typeof content.text !== "string") throw new Error();
      body = content.text;
    } catch {
      throw new FeishuError(400, "Invalid Feishu text content");
    }
    const bot = sender.sender_type === "bot",
      id = sender.sender_id.open_id;
    const actor = {
      id: `${this.sourceId}:${id}`,
      kind: bot ? ("bot" as const) : ("human" as const),
      trusted:
        (bot ? this.options.trustedBots : this.options.trustedUsers)?.includes(
          id,
        ) === true,
    };
    const root = message.root_id ?? message.message_id;
    if (!identifier(root))
      throw new FeishuError(400, "Invalid Feishu root message");
    const threadRef = `${message.chat_id}/message/${root}`;
    const occurredAt = Number(message.update_time ?? message.create_time);
    if (!Number.isFinite(occurredAt) || occurredAt < 0)
      throw new FeishuError(400, "Invalid Feishu message timestamp");
    return {
      eventId: header.event_id,
      own: bot && this.ownSenders.has(id),
      event: {
        sourceId: this.sourceId,
        externalId: `${message.chat_id}:${message.message_id}`,
        revision: digest({ actor, body, occurredAt, threadRef }),
        actor,
        threads: [threadRef],
        body,
        receivedAt: Date.now(),
        occurredAt,
        addressed: this.options.addressed ?? false,
      },
    };
  }
  async webhook(
    runtime: SuperAgentRuntime,
    raw: Buffer,
    headers: Record<string, string | undefined>,
  ): Promise<ReceiveBatchResult | { challenge: string }> {
    const parsed = this.parse(raw, headers);
    await this.source(runtime);
    const nonceId = scopedId(runtime.engine.instanceId, "feishuNonce", [
      this.sourceId,
      parsed.nonce,
    ]);
    const recordNonce = async (tx: Tx) => {
      const stateId = scopedId(runtime.engine.instanceId, "feishuReplay", [
        this.sourceId,
      ]);
      const state = await runtime.engine.store.get<{ nonces: Nonce[] }>(
        tx,
        "feishuReplay",
        stateId,
      );
      const nonces = (state?.nonces ?? []).filter(
        (n) => n.expiresAt >= Date.now(),
      );
      if (nonces.some((n) => n.id === nonceId))
        throw new FeishuError(409, "Feishu request replay denied");
      if (nonces.length >= 4096)
        throw new FeishuError(503, "Feishu replay window capacity exceeded");
      nonces.push({ id: nonceId, expiresAt: parsed.expiresAt });
      await runtime.engine.store.put(tx, "feishuReplay", stateId, { nonces });
    };
    if (parsed.data.type === "url_verification") {
      if (
        typeof parsed.data.challenge !== "string" ||
        parsed.data.challenge.length > 4096
      )
        throw new FeishuError(400, "Invalid Feishu challenge");
      await runtime.engine.transaction(async (tx) => {
        await this.source(runtime, tx);
        await recordNonce(tx);
      });
      return { challenge: parsed.data.challenge };
    }
    const normalized = this.normalized(parsed.data);
    await this.source(runtime, undefined, normalized.event.threads[0]);
    const deliveryId = scopedId(runtime.engine.instanceId, "feishuDelivery", [
      this.sourceId,
      normalized.eventId,
    ]);
    const hash = digest({
      eventId: normalized.eventId,
      event: { ...normalized.event, receivedAt: 0 },
      own: normalized.own,
    });
    const source = await this.source(runtime);
    const result = await runtime.engine.receiveBatch(
      normalized.own ? [] : [normalized.event],
      { sourceId: this.sourceId, value: source.cursor ?? null },
      async (tx) => {
        await this.source(runtime, tx, normalized.event.threads[0]);
        await recordNonce(tx);
        const previous = await runtime.engine.store.get<Delivery>(
          tx,
          "feishuDelivery",
          deliveryId,
        );
        if (previous && previous.hash !== hash)
          throw new FeishuError(409, "Feishu event ID payload conflict");
        await runtime.engine.store.put(tx, "feishuDelivery", deliveryId, {
          id: deliveryId,
          hash,
        });
      },
      { preserveCursor: true },
    );
    runtime.agents?.kick();
    return result;
  }
  private receiptId(key: string) {
    if (!this.runtime) throw new Error("Feishu source is not configured");
    return scopedId(this.runtime.engine.instanceId, "feishuReceipt", [
      this.sourceId,
      key,
    ]);
  }
  async queryReceipt(key: string): Promise<ReceiptLookup> {
    if (!this.runtime) return { status: "unknown" };
    const stored = await this.runtime.engine.store.read<{ receipt: JsonValue }>(
      "feishuReceipt",
      this.receiptId(key),
    );
    return stored
      ? { status: "found", receipt: stored.receipt }
      : { status: "unknown" };
  }
  async send(
    message: Readonly<OutboundMessage>,
    key: string,
  ): Promise<JsonValue> {
    if (
      !this.runtime ||
      !this.options.accessTokenProvider ||
      !this.ownSenders.size ||
      !this.listened(message.destination)
    )
      throw new NoSendError(
        "Feishu destination/provider/own sender configuration unavailable",
      );
    const prior = await this.queryReceipt(key);
    if (prior.status === "found") return prior.receipt;
    const target = this.destination(message.destination.threadRef)!;
    let accessToken: string;
    try {
      accessToken = await this.options.accessTokenProvider();
      if (!accessToken?.trim()) throw new Error();
    } catch {
      throw new NoSendError("Feishu access token unavailable");
    }
    const payload = message.payload as any;
    const body = [
      typeof payload?.content === "string"
        ? payload.content
        : JSON.stringify(payload),
      ...(Array.isArray(payload?.questions)
        ? payload.questions.map((q: any) =>
            typeof q === "string" ? q : JSON.stringify(q),
          )
        : []),
    ]
      .join("\n")
      .slice(0, 12000);
    const abort = new AbortController();
    this.requests.add(abort);
    const timer = setTimeout(
      () => abort.abort(),
      this.options.timeoutMs ?? 10000,
    );
    timer.unref();
    try {
      const path = target.message
        ? `/open-apis/im/v1/messages/${encodeURIComponent(target.message)}/reply`
        : "/open-apis/im/v1/messages?receive_id_type=chat_id";
      const response = await fetch(this.base + path, {
        method: "POST",
        headers: {
          authorization: `Bearer ${accessToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ...(target.message
            ? { reply_in_thread: true }
            : { receive_id: target.chat }),
          msg_type: "text",
          content: JSON.stringify({ text: body }),
          uuid: digest([this.sourceId, key]).slice(0, 32),
        }),
        signal: abort.signal,
        redirect: "error",
      });
      const bytes = await readResponseBytes(response, abort.signal, {
        maxBytes: 1_048_576,
        error: () => {
          const error = new Error("Feishu response exceeds limit");
          abort.abort(error);
          return error;
        },
      });
      const data = JSON.parse(bytes.toString("utf8"));
      if (!response.ok || data.code !== 0 || !identifier(data.data?.message_id))
        throw new Error("Feishu send outcome requires receipt inspection");
      const receipt = { messageId: data.data.message_id };
      await this.runtime.engine.transaction((tx) =>
        this.runtime!.engine.store.put(
          tx,
          "feishuReceipt",
          this.receiptId(key),
          { id: this.receiptId(key), receipt },
        ),
      );
      return receipt;
    } finally {
      clearTimeout(timer);
      this.requests.delete(abort);
    }
  }
  close() {
    for (const request of this.requests) request.abort();
    this.requests.clear();
  }
}
