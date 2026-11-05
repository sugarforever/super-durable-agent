import { mkdir, open } from "node:fs/promises";
import { join, resolve } from "node:path";
import fsExt from "fs-ext";
import { JsonLedger } from "../daemon/protocol.js";
import { digest } from "../domain/model.js";

export interface WatchdogSnapshot {
  tickets: {
    id: string;
    lifecycle: string;
    revision: number;
    attention?: string;
    latestReport?: unknown;
    artifactDigest?: string;
  }[];
  jobs: {
    id: string;
    ticketId: string;
    status: string;
    commandDigest?: string;
  }[];
  endpoints: { state: string; lastSuccessfulContact?: number }[];
  /** Only populated by hosts with actual metering. Absence means unknown. */
  metrics?: { cost: number; tokens: number };
}
export interface WatchdogConfig {
  thresholdMs?: number;
  intervalMs?: number;
  maxCalls?: number;
  maxTokens?: number;
  maxCost?: number;
  callTokens?: number;
  callCost?: number;
  allowPause?: boolean;
  allowSafeRetry?: boolean;
  costAnomalyThreshold?: number;
}
export interface WatchdogAction {
  id: string;
  ticketId: string;
  kind: "requestStatus" | "knownSafeRetry" | "pause" | "notify";
  classification: string;
  reason: string;
}
export type SemanticEvaluator = (
  candidate: { ticketId: string; reason: string; snapshot: WatchdogSnapshot },
  limits: { maxTokens: number; maxCost: number },
) => Promise<{
  verdict: "stalled" | "progressing" | "unknown";
  reason: string;
}>;
interface Observation {
  fingerprint: string;
  since: number;
  classification: string;
  notified?: string;
}
export interface WatchdogState {
  observations: Record<string, Observation>;
  budget: { calls: number; tokens: number; cost: number };
  actions: Record<
    string,
    { action: WatchdogAction; state: "claimed" | "done" | "unknown" }
  >;
}
const empty = (): WatchdogState => ({
  observations: {},
  budget: { calls: 0, tokens: 0, cost: 0 },
  actions: {},
});
/** The only writer opened by the supervisor; never points at the Harness database. */
export class WatchdogLedger {
  readonly directory: string;
  private readonly ledger: JsonLedger<WatchdogState>;
  constructor(directory: string) {
    this.directory = resolve(directory);
    this.ledger = new JsonLedger(this.directory);
  }
  async read(): Promise<WatchdogState> {
    return (await this.ledger.read("watchdog")) ?? empty();
  }
  async save(state: WatchdogState) {
    await this.ledger.write("watchdog", state);
  }
  async exclusive<T>(fn: () => Promise<T>, busy: T): Promise<T> {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    const file = await open(
      join(this.directory, ".watchdog.lock"),
      "a+",
      0o600,
    );
    try {
      try {
        fsExt.flockSync(file.fd, "exnb");
      } catch {
        return busy;
      }
      return await fn();
    } finally {
      await file.close();
    }
  }
}
function validate(config: WatchdogConfig) {
  for (const [key, value] of Object.entries(config)) {
    if (["allowPause", "allowSafeRetry"].includes(key)) {
      if (typeof value !== "boolean") throw Error(`Invalid ${key}`);
    } else if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value < 0
    )
      throw Error(`Invalid ${key}`);
  }
}
export async function watchdogTick(
  snapshot: WatchdogSnapshot,
  config: WatchdogConfig,
  ledger: WatchdogLedger,
  evaluate?: SemanticEvaluator,
  now = Date.now(),
): Promise<WatchdogAction[]> {
  validate(config);
  return ledger.exclusive(async () => {
    const state = await ledger.read(),
      actions: WatchdogAction[] = [];
    const threshold = config.thresholdMs ?? 900000;
    for (const ticket of snapshot.tickets) {
      const jobs = snapshot.jobs.filter((j) => j.ticketId === ticket.id);
      const report = ticket.latestReport as
        | { status?: string; waitingFor?: string; questions?: unknown[] }
        | undefined;
      const classification =
        ticket.lifecycle === "ended"
          ? "completed"
          : ticket.lifecycle === "stopped"
            ? "paused"
            : report?.waitingFor === "human" || !!report?.questions?.length
              ? "waitingForPerson"
              : snapshot.endpoints.some((e) => e.state !== "online")
                ? "endpointOffline"
                : jobs.some((j) =>
                      [
                        "pending",
                        "running",
                        "cancelPending",
                        "unknown",
                      ].includes(j.status),
                    )
                  ? "activeJob"
                  : report?.status === "ready"
                    ? "reviewable"
                    : report?.status === "waiting"
                      ? report.waitingFor === "endpoint"
                        ? "waitingForEndpoint"
                        : "waitingForInput"
                      : ticket.attention || report?.status === "failed"
                        ? "failure"
                        : "observing";
      const fingerprint = digest({
        ticket,
        jobs,
        endpoints: snapshot.endpoints.map((e) => e.state),
        metrics: snapshot.metrics ?? null,
      });
      let previous = state.observations[ticket.id];
      if (!previous || previous.fingerprint !== fingerprint) {
        previous = { fingerprint, since: now, classification };
        state.observations[ticket.id] = previous;
      }
      const immediate = ["completed", "failure", "paused"].includes(
        classification,
      );
      if (!immediate && now - previous.since < threshold) continue;
      const key = digest({ ticketId: ticket.id, fingerprint, classification });
      if (previous.notified === key) continue;
      let reason = classification,
        verdict = "unknown";
      if (classification === "observing") {
        const repeated = jobs
          .filter((j) => j.commandDigest)
          .some(
            (j, i, a) =>
              a.findIndex((x) => x.commandDigest === j.commandDigest) !== i,
          );
        const anomaly =
          snapshot.metrics !== undefined &&
          config.costAnomalyThreshold !== undefined &&
          snapshot.metrics.cost > config.costAnomalyThreshold;
        reason = anomaly
          ? "Observed cost anomaly"
          : repeated
            ? "Repeated command with no new artifact"
            : "No observed report or artifact change";
        const tokens = config.callTokens ?? 1024,
          cost = config.callCost ?? 0.05;
        if (!evaluate)
          reason += "; evaluator unavailable; semantic status unknown";
        else if (
          state.budget.calls + 1 > (config.maxCalls ?? 0) ||
          state.budget.tokens + tokens > (config.maxTokens ?? 0) ||
          state.budget.cost + cost > (config.maxCost ?? 0)
        )
          reason += "; semantic budget exhausted";
        else {
          // Reserve worst-case budget before the call. A crash/error never refunds an uncertain call.
          state.budget.calls++;
          state.budget.tokens += tokens;
          state.budget.cost += cost;
          previous.notified = key;
          await ledger.save(state);
          try {
            const result = await evaluate(
              { ticketId: ticket.id, reason, snapshot },
              { maxTokens: tokens, maxCost: cost },
            );
            if (
              !["stalled", "progressing", "unknown"].includes(result.verdict) ||
              typeof result.reason !== "string"
            )
              throw Error("Invalid semantic verdict");
            verdict = result.verdict;
            reason += `; ${result.verdict}: ${result.reason.slice(0, 2048)}`;
          } catch {
            reason += "; evaluator failed; semantic status unknown";
          }
        }
        const kind =
          verdict === "stalled" && config.allowPause
            ? "pause"
            : verdict === "stalled" && config.allowSafeRetry
              ? "knownSafeRetry"
              : "requestStatus";
        actions.push({
          id: `${key}:${kind}`,
          ticketId: ticket.id,
          kind,
          classification: "candidate",
          reason,
        });
      }
      actions.push({
        id: `${key}:notify`,
        ticketId: ticket.id,
        kind: "notify",
        classification,
        reason,
      });
      previous.notified = key;
    }
    for (const action of actions)
      state.actions[action.id] = { action, state: "claimed" };
    await ledger.save(state);
    return actions;
  }, []);
}
