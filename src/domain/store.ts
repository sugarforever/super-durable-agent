import type { Context, JsonValue } from '@earendil-works/chord';
import { defineDocFamily, type DocumentReader, type JsonObject, type SessionDocFamilyToken, type Tx } from '@earendil-works/pi-durable';
import { canonicalJson } from './model.js';

const indexes = defineDocFamily({
  kind: 'sa.index', version: 1, scope: 'session', family: true,
  initial: (_seed: string): { ids: string[] } => ({ ids: [] }),
});
const families = new Map<string, SessionDocFamilyToken<JsonObject, string>>();
function family(kind: string): SessionDocFamilyToken<JsonObject, string> {
  if (!/^[a-z][a-zA-Z0-9_-]*$/.test(kind) || kind === 'index') throw new Error(`Invalid record kind: ${kind}`);
  let token = families.get(kind);
  if (!token) {
    token = defineDocFamily({ kind: `sa.${kind}`, version: 1, scope: 'session', family: true,
      initial: (_seed: string): JsonObject => ({ present: false, value: null }) });
    families.set(kind, token);
  }
  return token;
}

/** Typed domain facade. Only this module crosses pi's structural JSON boundary. */
export class DomainStore {
  constructor(private readonly reader: DocumentReader, private readonly context: Context, readonly maxIndexEntries = 10_000) {}

  async get<T>(tx: Tx, kind: string, id: string): Promise<T | undefined> {
    const token = family(kind);
    // tx.doc creates missing members. Consult the transaction-local bounded index first,
    // so filtered events and missing reads cannot create unlimited unindexed documents.
    const index = await tx.doc(indexes, kind, kind);
    if (!index.ids.includes(id)) return undefined;
    const document = await tx.doc(token, id, id);
    return document.present ? JSON.parse(JSON.stringify(document.value)) as T : undefined;
  }

  async put<T>(tx: Tx, kind: string, id: string, value: T): Promise<void> {
    if (!id) throw new Error('Record id must be nonempty');
    const encoded = canonicalJson(value);
    const document = await tx.doc(family(kind), id, id);
    if (document.present && (kind === 'trigger' || kind === 'audit') && canonicalJson(document.value) !== encoded) {
      throw new Error(`Immutable ${kind} conflict: ${id}`);
    }
    const index = await tx.doc(indexes, kind, kind);
    if (!index.ids.includes(id)) {
      if (index.ids.length >= this.maxIndexEntries) throw new Error(`Index capacity exceeded for ${kind}`);
      index.ids.push(id);
    }
    document.present = true;
    document.value = JSON.parse(encoded) as JsonValue;
  }

  /** Enumerate the bounded index inside the same transaction as subsequent mutations. */
  async listInTransaction<T>(tx: Tx, kind: string): Promise<T[]> {
    family(kind);
    const index = await tx.doc(indexes, kind, kind);
    const values: T[] = [];
    for (const id of index.ids) {
      const value = await this.get<T>(tx, kind, id);
      if (value !== undefined) values.push(value);
    }
    return values;
  }

  async read<T>(kind: string, id: string): Promise<T | undefined> {
    const document = await this.reader.snapshot(family(kind), id, this.context);
    return document?.present ? JSON.parse(JSON.stringify(document.value)) as T : undefined;
  }

  async list<T>(kind: string): Promise<T[]> {
    family(kind);
    const index = await this.reader.snapshot(indexes, kind, this.context);
    if (!index) return [];
    const values = await Promise.all(index.ids.map((id) => this.read<T>(kind, id)));
    return values.filter((value) => value !== undefined) as T[];
  }
}
