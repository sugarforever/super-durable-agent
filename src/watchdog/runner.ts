import { secureUrl, requestJson, RESPONSE_LIMIT } from "../daemon/protocol.js";
import { readResponseBytes } from "../http/response.js";
import {
  watchdogTick,
  WatchdogLedger,
  type SemanticEvaluator,
  type WatchdogAction,
  type WatchdogConfig,
  type WatchdogSnapshot,
} from "./core.js";
export interface WatchdogConnection {
  url: string;
  token: string;
  allowInsecureLoopback?: boolean;
}
export async function pollWatchdog(
  connection: WatchdogConnection,
  config: WatchdogConfig,
  ledger: WatchdogLedger,
  evaluate?: SemanticEvaluator,
  notify: (action: WatchdogAction) => Promise<void> = async (a) => {
    process.stdout.write(JSON.stringify(a) + "\n");
  },
) {
  secureUrl(connection.url, connection.allowInsecureLoopback);
  const signal = AbortSignal.timeout(10000);
  const response = await fetch(
    connection.url.replace(/\/$/, "") + "/taskGraph",
    {
      headers: { authorization: `Bearer ${connection.token}` },
      signal,
      redirect: "error",
    },
  );
  if (!response.ok) {
    const error = Error(`Management snapshot rejected: ${response.status}`);
    // Reject status before parsing/draining, without waiting on body cleanup.
    void response.body?.cancel(error).catch(() => {});
    throw error;
  }
  const bytes = await readResponseBytes(response, signal, {
    maxBytes: RESPONSE_LIMIT,
    error: () => Error("Snapshot exceeds limit"),
  });
  const snapshot = JSON.parse(bytes.toString("utf8")) as WatchdogSnapshot;
  if (
    !Array.isArray(snapshot.tickets) ||
    !Array.isArray(snapshot.jobs) ||
    !Array.isArray(snapshot.endpoints)
  )
    throw Error("Invalid management snapshot");
  const actions = await watchdogTick(snapshot, config, ledger, evaluate);
  for (const action of actions) {
    let status: "done" | "unknown" = "done";
    try {
      if (action.kind === "notify") await notify(action);
      else
        await requestJson(
          connection.url,
          action.kind === "knownSafeRetry"
            ? "/reconcile"
            : `/tickets/${encodeURIComponent(action.ticketId)}/${action.kind}`,
          connection.token,
          { commandId: action.id },
        );
    } catch {
      status = "unknown";
    }
    // Claimed before effect: uncertain actions are retained for inspection, never automatically resent.
    await ledger.exclusive(async () => {
      const state = await ledger.read();
      if (state.actions[action.id]) state.actions[action.id].state = status;
      await ledger.save(state);
    }, undefined);
  }
  return actions;
}
