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
export interface PreparedEvent extends NormalizedEvent { bodyArtifact?: string; /** Host-only digest of the original normalized payload before spill. */ originalDigest?: string }
export interface Trigger extends PreparedEvent { id: ID; ordinal: number; digest: string }
export interface Thread { id: ID; sourceId: ID; externalRef: string; triggerIds: ID[]; lastOrdinal: number }
export interface IntakeIntent { id: ID; triggerId: ID; status: 'pending' | 'accepted'; taskId: TaskId<null>; routed?: boolean; released?: boolean }
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

export interface Rule { id: ID; enabled: boolean; instructions: string; capabilities: string[]; communication?: { acknowledgements?: boolean; progress?: boolean } }
export interface Policy {
  version: number;
  acceptMode: 'off' | 'observe' | 'auto';
  allowedActors: string[];
  botActors: string[];
  /** Trusted upper bound for explicit work; defaults to repo.read and channel.reply. */
  explicitCapabilities?: string[];
  rules: Rule[];
}
export interface CommandMeta { commandId: string; expectedRevision?: number; /** Authenticated source input consumed by a stop command. */ triggerId?: ID }
export interface Mandate {
  id: ID; actor: Actor; basis: 'explicit' | 'inferred'; policyVersion: number;
  ruleIds: ID[]; capabilities: string[]; rulesSnapshot?: Rule[]; sourceId: ID; scope: string[]; withdrawnAt: number | null;
}
export interface Ticket {
  id: ID; goal: string; mandateId: ID; lifecycle: 'open' | 'ended' | 'stopped'; revision: number;
  assignmentId: ID; assignmentEpoch: number; goalArtifact?: string; workerConversationId?: import('@earendil-works/pi-durable').ConversationId;
  latestReport?: JsonValue; attention?: string; cancelPending?: boolean;
}
export interface Assignment { id: ID; ticketId: ID; epoch: number; status: 'pending' | 'running' | 'ended' | 'stopped'; capacityClass?: 'ordinary' | 'human' }
export interface Eval {
  id: ID; threadId: ID; threadIds?: ID[]; triggerIds: ID[]; policyVersion: number; epoch: number;
  status: 'pending' | 'decided' | 'invalid'; basis: 'explicit' | 'inferred';
  actor: Actor; sourceId: ID; decisionId?: ID;
}
export interface Handback { ticketId: ID; epoch: number; reason: string }
export interface Disposition {
  triggerId: ID; ownerTickets: ID[]; contextTickets: ID[]; handbacks: Handback[];
  acknowledged: boolean; consumedTicketIds?: ID[]; epoch: number; conflicts: number; decisionId?: ID;
}
export interface Subscription { ticketId: ID; threadId: ID; own: boolean; active: boolean }
export type DecisionOutcome =
  | { kind: 'accept'; goal: string; ruleIds: ID[]; capabilities?: string[] }
  | { kind: 'attach'; ticketId: ID; note?: string }
  | { kind: 'ignore'; reason: string; wouldAccept?: boolean }
  | { kind: 'stop'; ticketId: ID; reason: string };
export interface Decision { id: ID; evalId: ID; epoch: number; policyVersion: number; outcome: DecisionOutcome }
export interface DeliveryIntent {
  id: ID; ticketId: ID; triggerId: ID; epoch: number; own: boolean; resume: boolean;
  status: 'pending' | 'delivered' | 'invalid'; ownershipOf?: ID; mode?: 'followUp' | 'steer';
}
export interface CancelIntent { id: ID; ticketId: ID; assignmentId: ID; epoch: number; status: 'pending' | 'completed' | 'attention'; reason: string; cancelKey?: string; payloadDigest?: string; nativeConfirmed?: boolean; endpointConfirmed?: boolean; taskId?: TaskId<null>; attempts?: number }
export type ReportStatus = 'progress' | 'waiting' | 'ready' | 'blocked' | 'failed' | 'dropped' | 'attention';
export interface WorkerQuestion { id: ID; text: string; options?: string[] }
export interface Report {
  ticketId: ID; assignmentId: ID; epoch: number; reportId: ID; inputIds: ID[];
  status: ReportStatus; questions?: WorkerQuestion[]; waitingFor?: 'human' | 'endpoint' | 'input'; content: JsonValue;
}
