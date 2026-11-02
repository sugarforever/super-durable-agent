import { createHmac, timingSafeEqual } from "node:crypto";
import type {
  JsonValue,
  NormalizedEvent,
  ReceiveBatchResult,
} from "../domain/model.js";
import { digest, scopedId } from "../domain/model.js";
import type { SuperAgentRuntime } from "../runtime/open.js";
import {
  NoSendError,
  type SourceAdapter,
  type MessageDestination,
  type OutboundMessage,
  type ReceiptLookup,
} from "../services/outbox.js";

export class GitHubError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export interface GitHubOptions {
  sourceId?: string;
  repositories: string[];
  webhookSecret: string;
  apiToken?: string;
  apiBase?: string;
  enabled?: boolean;
  listenedScopes?: string[];
  addressed?: boolean;
  /** Verified host configuration for the account used by outbound automation. */
  ownLogins?: string[];
}
export interface GitHubPollEvent {
  event: string;
  payload: unknown;
}
type ObjectValue = Record<string, any>;
function object(value: unknown): ObjectValue {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new GitHubError(400, "Invalid GitHub payload");
  return value as ObjectValue;
}
function equal(a: string, b: string) {
  const x = Buffer.from(a),
    y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

/** Trusted host adapter. Credentials remain in this instance, never persisted in source records. */
export class GitHubSource implements SourceAdapter {
  readonly sourceId: string;
  readonly enabled: boolean;
  private readonly repositories: Set<string>;
  private readonly ownLogins: Set<string>;
  private readonly base: string;
  private readonly requests = new Set<AbortController>();
  constructor(private readonly options: GitHubOptions) {
    this.sourceId = options.sourceId ?? "github";
    this.enabled = options.enabled ?? true;
    if (
      typeof this.sourceId !== "string" ||
      !this.sourceId.trim() ||
      typeof options.webhookSecret !== "string" ||
      !options.webhookSecret.trim()
    )
      throw new Error("GitHub source and webhook secret are required");
    if (
      !Array.isArray(options.repositories) ||
      !options.repositories.length ||
      options.repositories.some((r) => !/^[\w.-]+\/[\w.-]+$/.test(r))
    )
      throw new Error("GitHub repositories allowlist required");
    if (
      options.listenedScopes !== undefined &&
      (!Array.isArray(options.listenedScopes) ||
        options.listenedScopes.some(
          (ref) => typeof ref !== "string" || !ref.trim(),
        ))
    )
      throw new Error("Invalid GitHub listened scopes");
    if (
      (options.enabled !== undefined && typeof options.enabled !== "boolean") ||
      (options.addressed !== undefined &&
        typeof options.addressed !== "boolean")
    )
      throw new Error("Invalid GitHub source flags");
    if (
      options.ownLogins !== undefined &&
      (!Array.isArray(options.ownLogins) ||
        options.ownLogins.some(
          (login) =>
            typeof login !== "string" ||
            !/^[a-z0-9][a-z0-9-]*(?:\[bot\])?$/i.test(login),
        ))
    )
      throw new Error("Invalid configured GitHub own logins");
    this.ownLogins = new Set(
      (options.ownLogins ?? []).map((login) => login.toLowerCase()),
    );
    this.repositories = new Set(
      options.repositories.map((r) => r.toLowerCase()),
    );
    this.base = (options.apiBase ?? "https://api.github.com").replace(
      /\/$/,
      "",
    );
    const url = new URL(this.base);
    if (
      url.protocol !== "https:" &&
      !(
        url.protocol === "http:" &&
        ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)
      )
    )
      throw new Error("GitHub API requires HTTPS or loopback");
  }
  async configure(runtime: SuperAgentRuntime) {
    await runtime.engine.configureSource({
      id: this.sourceId,
      kind: "github",
      enabled: this.enabled,
      repositoryAllowlist: [...this.repositories],
      ...(this.options.listenedScopes
        ? { listenedScopes: this.options.listenedScopes }
        : {}),
    });
  }
  listened(destination: MessageDestination) {
    const match = /^([\w.-]+\/[\w.-]+)#([1-9]\d*)$/.exec(destination.threadRef);
    return (
      this.enabled &&
      destination.sourceId === this.sourceId &&
      !!match &&
      this.repositories.has(match[1].toLowerCase()) &&
      (!this.options.listenedScopes ||
        this.options.listenedScopes.includes(destination.threadRef))
    );
  }
  marker(key: string) {
    const hash = digest([this.sourceId, key]);
    return `<!-- super-agent:${hash}:${createHmac("sha256", this.options.webhookSecret).update(hash).digest("hex")} -->`;
  }
  private family(payload: ObjectValue): string {
    const keys = [
      "issue",
      "pull_request",
      "comment",
      "review",
      "check_run",
      "check_suite",
    ].filter((key) => payload[key] !== undefined);
    const shape = keys.sort().join(",");
    const families: Record<string, string> = {
      issue: "issues",
      "comment,issue": "issue_comment",
      pull_request: "pull_request",
      "pull_request,review": "pull_request_review",
      "comment,pull_request": "pull_request_review_comment",
      check_run: "check_run",
      check_suite: "check_suite",
    };
    if (families[shape]) return families[shape];
    if (
      !keys.length &&
      typeof payload.sha === "string" &&
      typeof payload.state === "string" &&
      typeof payload.context === "string"
    )
      return "status";
    if (!keys.length && typeof payload.zen === "string" && payload.hook)
      return "ping";
    throw new GitHubError(
      422,
      "Unsupported or ambiguous GitHub payload family",
    );
  }
  private render(message: Readonly<OutboundMessage>, key: string): string {
    const payload = message.payload as any;
    const content =
      typeof payload === "string"
        ? payload
        : typeof payload?.content === "string"
          ? payload.content
          : JSON.stringify(payload);
    return `## Agent ${message.purpose}\n\n${content}\n\n${this.marker(key)}`;
  }
  private async ownEcho(
    runtime: SuperAgentRuntime,
    event: string,
    payload: unknown,
    normalized: NormalizedEvent | undefined,
  ): Promise<boolean> {
    if (event !== "issue_comment" || !normalized) return false;
    const value = object(payload),
      comment = value.comment;
    if (
      value.action !== "created" ||
      !Number.isSafeInteger(comment?.id) ||
      typeof comment.body !== "string" ||
      !comment.user?.login ||
      value.sender?.login !== comment.user.login
    )
      return false;
    const messages = await runtime.list<OutboundMessage>("outbox");
    return messages.some((message) => {
      const receipt = message.receipt as { id?: number } | undefined;
      return (
        message.destination.sourceId === this.sourceId &&
        normalized.threads.includes(message.destination.threadRef) &&
        (receipt?.id === comment.id ||
          (!receipt &&
            this.ownLogins.has(value.sender.login.toLowerCase()) &&
            ["pending", "queued", "sending", "retrying", "uncertain"].includes(
              message.status,
            ))) &&
        comment.body === this.render(message, message.idempotencyKey)
      );
    });
  }
  private normalize(
    event: string,
    input: unknown,
  ): NormalizedEvent | undefined {
    const p = object(input),
      repo = String(p.repository?.full_name ?? "").toLowerCase();
    if (!this.repositories.has(repo))
      throw new GitHubError(403, "GitHub repository is not allowed");
    if (this.family(p) !== event)
      throw new GitHubError(
        422,
        "GitHub event header does not match signed payload family",
      );
    const allowedActions: Record<string, string[]> = {
      issues: [
        "opened",
        "edited",
        "deleted",
        "transferred",
        "pinned",
        "unpinned",
        "closed",
        "reopened",
        "assigned",
        "unassigned",
        "labeled",
        "unlabeled",
        "locked",
        "unlocked",
        "milestoned",
        "demilestoned",
      ],
      issue_comment: ["created", "edited", "deleted"],
      pull_request: [
        "assigned",
        "unassigned",
        "labeled",
        "unlabeled",
        "opened",
        "edited",
        "closed",
        "reopened",
        "synchronize",
        "converted_to_draft",
        "ready_for_review",
        "locked",
        "unlocked",
        "review_requested",
        "review_request_removed",
        "auto_merge_enabled",
        "auto_merge_disabled",
        "enqueued",
        "dequeued",
        "milestoned",
        "demilestoned",
      ],
      pull_request_review: ["submitted", "edited", "dismissed"],
      pull_request_review_comment: ["created", "edited", "deleted"],
      check_run: ["created", "completed", "rerequested", "requested_action"],
      check_suite: ["completed", "requested", "rerequested"],
    };
    if (allowedActions[event] && !allowedActions[event].includes(p.action))
      throw new GitHubError(422, "Unsupported GitHub event action");
    let item: ObjectValue | undefined, number: number | undefined;
    if (event === "issues") {
      item = p.issue;
      number = p.issue?.number;
    } else if (event === "issue_comment") {
      item = p.comment;
      number = p.issue?.number;
    } else if (event === "pull_request") {
      item = p.pull_request;
      number = p.pull_request?.number ?? p.number;
    } else if (
      event === "pull_request_review" ||
      event === "pull_request_review_comment"
    ) {
      item = p.review ?? p.comment;
      number = p.pull_request?.number ?? p.number;
    } else if (event === "check_run" || event === "check_suite") {
      item = p[event];
      number = item?.pull_requests?.[0]?.number;
    } else if (event === "status") {
      item = p;
    } else if (event === "ping") return undefined;
    else throw new GitHubError(422, "Unsupported GitHub event");
    if (!item || (!item.id && !item.sha))
      throw new GitHubError(400, "Missing GitHub event entity");
    const sender = object(p.sender ?? item.user),
      login = sender.login;
    if (typeof login !== "string" || !login.trim())
      throw new GitHubError(400, "Missing GitHub sender");
    let body = [
      item.title,
      item.body,
      item.description,
      event === "status" ? item.state : undefined,
      item.status,
      item.conclusion,
    ]
      .filter((x) => typeof x === "string")
      .join("\n");
    const refs = number
      ? [`${repo}#${number}`]
      : [`${repo}@${item.head_sha ?? item.sha ?? item.id}`];
    if (
      (event === "check_run" || event === "check_suite") &&
      item.pull_requests?.length
    )
      for (const pr of item.pull_requests)
        if (Number.isSafeInteger(pr.number) && pr.number > 0)
          refs.push(`${repo}#${pr.number}`);
    const parsedTime = Date.parse(
      item.updated_at ??
        item.submitted_at ??
        item.completed_at ??
        item.created_at ??
        item.started_at ??
        item.head_commit?.timestamp ??
        p.created_at ??
        "",
    );
    const timestamp = Number.isFinite(parsedTime) ? parsedTime : 0;
    const automation = ["check_run", "check_suite", "status"].includes(event);
    if (automation)
      body = `GitHub ${event} triggered by github:${login.toLowerCase()}\n${body}`;
    const actor = {
      id: automation ? `github:ci:${event}` : `github:${login.toLowerCase()}`,
      kind: automation
        ? ("bot" as const)
        : login === "ghost"
          ? ("system" as const)
          : sender.type === "Bot" || login.endsWith("[bot]")
            ? ("bot" as const)
            : ("human" as const),
      trusted: !automation && sender.type === "User" && login !== "ghost",
    };
    const stable = {
      event,
      action: p.action ?? "",
      entity:
        event === "status"
          ? {
              sha: item.sha ?? "",
              context: item.context ?? "",
              state: item.state ?? "",
              target_url: item.target_url ?? "",
            }
          : item,
      id: String(item.id ?? item.sha),
      body,
      state: item.state ?? "",
      merged: item.merged === true,
      conclusion: item.conclusion ?? "",
      timestamp,
      actor,
      refs: [...new Set(refs)].sort(),
    };
    return {
      sourceId: this.sourceId,
      externalId: `${repo}:${event}:${item.id ?? item.sha}`,
      revision: digest(stable),
      actor,
      threads: stable.refs,
      body,
      receivedAt: Date.now(),
      occurredAt: timestamp,
      addressed: this.options.addressed ?? false,
    };
  }
  async webhook(
    runtime: SuperAgentRuntime,
    raw: Buffer,
    headers: Record<string, string | undefined>,
  ): Promise<ReceiveBatchResult> {
    if (raw.length > 1_048_576)
      throw new GitHubError(413, "Webhook exceeds 1 MiB");
    const expected =
      "sha256=" +
      createHmac("sha256", this.options.webhookSecret)
        .update(raw)
        .digest("hex");
    if (!equal(headers["x-hub-signature-256"] ?? "", expected))
      throw new GitHubError(401, "Invalid GitHub webhook signature");
    const delivery = headers["x-github-delivery"],
      event = headers["x-github-event"];
    if (!delivery?.trim() || !event?.trim())
      throw new GitHubError(400, "Missing GitHub delivery headers");
    let payload: unknown;
    try {
      payload = JSON.parse(raw.toString("utf8"));
    } catch {
      throw new GitHubError(400, "Invalid webhook JSON");
    }
    const factEvent = this.normalize(event, payload);
    const normalized = (await this.ownEcho(runtime, event, payload, factEvent))
      ? undefined
      : factEvent;
    const source = await runtime.engine.store.read<{ cursor?: JsonValue }>(
      "source",
      this.sourceId,
    );
    const hash = digest(payload),
      id = scopedId(runtime.engine.instanceId, "githubDelivery", [
        this.sourceId,
        delivery,
      ]);
    const result = await runtime.engine.receiveBatch(
      normalized ? [normalized] : [],
      { sourceId: this.sourceId, value: source?.cursor ?? null },
      async (tx, results) => {
        // The unsigned delivery header cannot create semantic work. Normalized identity is payload-bound.
        const existing = await runtime.engine.store.get<{
          hash: string;
          event: string;
        }>(tx, "githubDelivery", id);
        if (existing && (existing.hash !== hash || existing.event !== event))
          throw new GitHubError(409, "GitHub delivery payload conflict");
        if (
          normalized &&
          results.some(
            (r) => r.disposition === "stored" || r.disposition === "duplicate",
          )
        ) {
          if (existing || results.some((r) => r.disposition === "stored"))
            await runtime.engine.store.put(tx, "githubDelivery", id, {
              id,
              hash,
              event,
            });
          await this.recordFacts(runtime, tx, event, payload, normalized);
        } else if (factEvent)
          await this.recordFacts(runtime, tx, event, payload, factEvent);
      },
      { preserveCursor: true, filterDisabled: true },
    );
    await runtime.engine.transaction((tx) =>
      runtime.engine.lifecycle.completeInTransaction(tx),
    );
    runtime.agents?.kick();
    return result;
  }
  async receivePoll(
    runtime: SuperAgentRuntime,
    events: GitHubPollEvent[],
    cursor: JsonValue,
  ): Promise<ReceiveBatchResult> {
    const prepared = await Promise.all(
      events.map(async (event) => {
        const fact = this.normalize(event.event, event.payload);
        return {
          ...event,
          fact,
          normalized: (await this.ownEcho(
            runtime,
            event.event,
            event.payload,
            fact,
          ))
            ? undefined
            : fact,
        };
      }),
    );
    const normalized = prepared.flatMap((event) =>
      event.normalized ? [event.normalized] : [],
    );
    const result = await runtime.engine.receiveBatch(
      normalized,
      { sourceId: this.sourceId, value: cursor },
      async (tx, results) => {
        let index = 0;
        for (const event of prepared) {
          const n = event.normalized;
          if (!n) {
            if (event.fact)
              await this.recordFacts(
                runtime,
                tx,
                event.event,
                event.payload,
                event.fact,
              );
            continue;
          }
          const result = results[index++];
          if (
            result?.disposition === "stored" ||
            result?.disposition === "duplicate"
          )
            await this.recordFacts(runtime, tx, event.event, event.payload, n);
        }
      },
    );
    await runtime.engine.transaction((tx) =>
      runtime.engine.lifecycle.completeInTransaction(tx),
    );
    runtime.agents?.kick();
    return result;
  }
  private async recordFacts(
    runtime: SuperAgentRuntime,
    tx: import("@earendil-works/pi-durable").Tx,
    event: string,
    payload: unknown,
    normalized: NormalizedEvent,
  ) {
    if (event !== "pull_request" && event !== "issues") return;
    const source = await runtime.engine.store.get<
      import("../domain/model.js").Source
    >(tx, "source", this.sourceId);
    if (
      !source?.enabled ||
      (source.listenedScopes &&
        !source.listenedScopes.includes(normalized.threads[0]))
    )
      return;
    const p = object(payload),
      item = p.pull_request ?? p.issue;
    if (
      !["opened", "reopened", "closed"].includes(p.action) ||
      !["open", "closed"].includes(item?.state)
    )
      return;
    await runtime.engine.lifecycle.recordFact(tx, {
      sourceId: this.sourceId,
      threadRef: normalized.threads[0],
      state: item.state,
      occurredAt: normalized.occurredAt,
      revision: normalized.revision,
    });
  }
  /** Trusted host callback after confirmed PR creation, never a Worker assertion. */
  async trackPullRequest(
    runtime: SuperAgentRuntime,
    ticketId: string,
    threadRef: string,
    meta: import("../domain/model.js").CommandMeta,
  ) {
    if (!this.listened({ sourceId: this.sourceId, threadRef }))
      throw new Error("GitHub repository or destination is not allowed");
    await runtime.engine.trackThread(ticketId, this.sourceId, threadRef, meta);
  }
  private async request(path: string, init: RequestInit = {}) {
    const controller = new AbortController();
    this.requests.add(controller);
    const timer = setTimeout(() => controller.abort(), 10000);
    timer.unref();
    try {
      const response = await fetch(this.base + path, {
        ...init,
        redirect: "error",
        signal: controller.signal,
        headers: {
          accept: "application/vnd.github+json",
          "x-github-api-version": "2022-11-28",
          ...(this.options.apiToken
            ? { authorization: `Bearer ${this.options.apiToken}` }
            : {}),
          ...init.headers,
        },
      });
      const data: unknown = await response.json();
      return {
        ok: response.ok,
        status: response.status,
        data,
        headers: response.headers,
      };
    } finally {
      clearTimeout(timer);
      this.requests.delete(controller);
    }
  }
  async send(
    message: Readonly<OutboundMessage>,
    key: string,
  ): Promise<JsonValue> {
    if (!this.listened(message.destination))
      throw new NoSendError("GitHub destination unavailable");
    if (!this.ownLogins.size)
      throw new NoSendError("GitHub outbound requires a configured own login");
    const [repo, number] = message.destination.threadRef.split("#");
    const response = await this.request(
      `/repos/${repo}/issues/${number}/comments`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          body: this.render(message, key),
        }),
      },
    );
    if (!response.ok)
      throw new Error("GitHub comment outcome requires receipt inspection");
    const data = response.data as any;
    if (!Number.isSafeInteger(data.id))
      throw new Error("Invalid GitHub receipt");
    return {
      id: data.id,
      ...(typeof data.html_url === "string" ? { url: data.html_url } : {}),
    };
  }
  async queryReceipt(key: string): Promise<ReceiptLookup> {
    try {
      for (const repo of this.repositories)
        for (let page = 1; page <= 10; page++) {
          const response = await this.request(
            `/repos/${repo}/issues/comments?sort=created&direction=desc&per_page=100&page=${page}`,
          );
          if (!response.ok) return { status: "unknown" };
          const data = response.data;
          if (!Array.isArray(data)) return { status: "unknown" };
          const match = data.find(
            (c) =>
              typeof c.body === "string" && c.body.includes(this.marker(key)),
          );
          if (match && Number.isSafeInteger(match.id))
            return {
              status: "found",
              receipt: {
                id: match.id,
                ...(typeof match.html_url === "string"
                  ? { url: match.html_url }
                  : {}),
              },
            };
          if (data.length < 100) break;
        }
    } catch {
      /* GitHub list absence is never authoritative, including transport errors. */
    }
    return { status: "unknown" };
  }
  close() {
    for (const c of this.requests) c.abort();
    this.requests.clear();
  }
}
