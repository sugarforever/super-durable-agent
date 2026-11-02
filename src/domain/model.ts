import { createHash } from 'node:crypto';
export type { JsonValue } from '@earendil-works/chord';
import type { JsonValue } from '@earendil-works/chord';
import type { TaskId } from '@earendil-works/pi-durable';

export type ID = string;
export interface Actor { id: ID; kind: 'human' | 'bot' | 'system'; trusted: boolean }
export interface NormalizedEvent {
  sourceId: ID;
  externalId: ID;
  revision: string;
  actor: Actor;
  threads: string[];
  body: string;
  receivedAt: number;
  occurredAt: number;
  addressed?: boolean;
}
export interface Source {
  id: ID;
  kind: string;
  enabled: boolean;
  listenedScopes?: string[];
  cursor?: JsonValue;
}
export interface Trigger extends NormalizedEvent { id: ID; ordinal: number; digest: string }
export interface Thread { id: ID; sourceId: ID; externalRef: string; triggerIds: ID[]; lastOrdinal: number }
export interface IntakeIntent { id: ID; triggerId: ID; status: 'pending' | 'accepted'; taskId: TaskId<null> }
export interface AuditEvent { id: ID; kind: 'trigger.received'; triggerId: ID; occurredAt: number }
export interface RuntimeMetadata {
  schemaVersion: number;
  instanceId: ID;
  extensions: string[];
  tasks: Record<string, number>;
  maxPending: number;
  maxIndexEntries: number;
}
export type ReceiveResult =
  | { disposition: 'stored' | 'duplicate'; triggerId: ID }
  | { disposition: 'filtered'; sourceId: ID }
  | Backpressure;
export interface Backpressure { disposition: 'backpressure'; retryable: true }
export interface SourceCursor { sourceId: ID; value: JsonValue }
export type ReceiveBatchResult = { disposition: 'stored'; results: ReceiveResult[] } | Backpressure;

/** Deterministic, strict JSON encoding; refuses values persistence would silently change. */
export function canonicalJson(value: unknown): string {
  const active = new Set<object>();
  function encode(item: unknown): string {
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item);
    if (typeof item !== 'object' || item === null) throw new Error('Expected finite JSON value');
    if (active.has(item)) throw new Error('Cyclic JSON value');
    active.add(item);
    try {
      if (Array.isArray(item)) return '[' + Array.from(item, encode).join(',') + ']';
      if (Object.getPrototypeOf(item) !== Object.prototype && Object.getPrototypeOf(item) !== null) {
        throw new Error('Expected plain JSON object');
      }
      return '{' + Object.keys(item).sort().map((key) => JSON.stringify(key) + ':' + encode(Reflect.get(item, key))).join(',') + '}';
    } finally { active.delete(item); }
  }
  return encode(value);
}
export function digest(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}
export function scopedId(instanceId: ID, kind: string, parts: string[]): ID {
  return `${instanceId}:${kind}:${digest(parts)}`;
}
function nonempty(value: unknown, name: string): asserts value is string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${name} must be nonempty`);
}
export function normalizeEvent(event: NormalizedEvent): NormalizedEvent {
  if (!event || typeof event !== 'object') throw new Error('Expected normalized event');
  nonempty(event.sourceId, 'sourceId'); nonempty(event.externalId, 'externalId'); nonempty(event.revision, 'revision');
  if (!event.actor || !['human', 'bot', 'system'].includes(event.actor.kind) || typeof event.actor.trusted !== 'boolean') {
    throw new Error('Invalid actor');
  }
  nonempty(event.actor.id, 'actor.id');
  if (!Array.isArray(event.threads) || event.threads.length === 0) throw new Error('At least one thread is required');
  for (const thread of event.threads) nonempty(thread, 'thread');
  if (typeof event.body !== 'string') throw new Error('body must be a string');
  if (!Number.isFinite(event.receivedAt) || !Number.isFinite(event.occurredAt)) throw new Error('Event timestamps must be finite');
  if (event.addressed !== undefined && typeof event.addressed !== 'boolean') throw new Error('addressed must be boolean');
  return {
    sourceId: event.sourceId, externalId: event.externalId, revision: event.revision,
    actor: { id: event.actor.id, kind: event.actor.kind, trusted: event.actor.trusted },
    threads: [...new Set(event.threads)].sort(), body: event.body,
    receivedAt: event.receivedAt, occurredAt: event.occurredAt,
    ...(event.addressed === undefined ? {} : { addressed: event.addressed }),
  };
}
