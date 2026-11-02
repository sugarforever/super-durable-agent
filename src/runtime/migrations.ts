import {
  AgentDoc,
  InboxDoc,
  LiveDoc,
  ProviderDoc,
  UsageDoc,
  type Storage,
  type Cursor,
  type RegistrySnapshot,
  type StoredDocument,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { canonicalJson, type RuntimeMetadata } from "../domain/model.js";
/** Public storage scans only; no pi table names or private SQLite mutations. */
export async function inspectDocuments(
  storage: Storage,
  visit: (document: StoredDocument) => void,
): Promise<void> {
  const scopes: any[] = [{ kind: "session" }];
  let cursor: Cursor | undefined;
  do {
    const page = await storage.scanConversations({}, 200, cursor, context);
    for (const record of page.items)
      scopes.push({ kind: "conversation", conversationId: record.id });
    cursor = page.next;
  } while (cursor);
  do {
    const page = await storage.scanTasks({}, 200, cursor, context);
    for (const record of page.items)
      scopes.push({ kind: "task", taskId: record.id });
    cursor = page.next;
  } while (cursor);
  for (const scope of scopes) {
    do {
      const page = await storage.scanDocuments(
        { scope, at: "current" },
        200,
        cursor,
        context,
      );
      for (const record of page.items) {
        const document = await storage.document(record.id, "current", context);
        if (!document) throw Error(`Missing Document: ${record.kind}`);
        visit(document);
      }
      cursor = page.next;
    } while (cursor);
  }
}
export async function preflightDocuments(storage: Storage): Promise<void> {
  const versions = new Map(
    [AgentDoc, InboxDoc, LiveDoc, ProviderDoc, UsageDoc].map((t) => [
      t.definition.kind,
      t.definition.version,
    ]),
  );
  await inspectDocuments(storage, (document) => {
    const expected = document.record.kind.startsWith("sa.")
      ? 1
      : versions.get(document.record.kind);
    if (expected === undefined || document.version !== expected)
      throw Error(
        `Unsupported Document definition/version: ${document.record.kind} v${document.version}`,
      );
  });
}
export function compatibleMigration(
  previous: RuntimeMetadata,
  snapshot: RegistrySnapshot,
): boolean {
  const names = new Set(snapshot.installed().map((e) => e.name));
  return (
    previous.extensions.every((name) => names.has(name)) &&
    Object.entries(previous.tasks).every(([name, version]) => {
      const def = snapshot.task(name)?.definition;
      return (
        !!def &&
        (def.version === version ||
          (def.version > version && typeof def.migrate === "function"))
      );
    })
  );
}
export async function preflightTasks(
  storage: Storage,
  snapshot: RegistrySnapshot,
  allowMigration: boolean,
): Promise<void> {
  let cursor: Cursor | undefined;
  do {
    const page = await storage.scanTasks({}, 200, cursor, context);
    for (const task of page.items) {
      if (
        task.state.status === "terminal" ||
        task.state.status === "completing"
      )
        continue;
      const def = snapshot.task(task.kind)?.definition;
      if (!def) throw Error(`Missing live Task definition: ${task.kind}`);
      if (task.version === def.version) continue;
      if (
        !allowMigration ||
        task.version > def.version ||
        typeof def.migrate !== "function"
      )
        throw Error(
          `Unsupported live Task migration: ${task.kind} v${task.version}`,
        );
      // Pure validation only. Native reservation performs/persists the real migration later.
      const migrated = def.migrate(
        structuredClone(task.input),
        structuredClone(task.state.checkpoint),
        task.version,
      );
      canonicalJson(migrated);
      if (
        !migrated?.checkpoint ||
        typeof migrated.checkpoint.phase !== "string" ||
        !(migrated.checkpoint.phase in def.phases)
      )
        throw Error(`Invalid migrated checkpoint: ${task.kind}`);
    }
    cursor = page.next;
  } while (cursor);
}
