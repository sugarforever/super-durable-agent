import { digest } from '../domain/model.js';
export interface EndpointIdentity { ticketId: string; assignmentId: string; epoch: number; mandateId: string }
export interface EndpointGrant extends EndpointIdentity { capabilities: string[] }
export type EndpointAction = 'prepare' | 'execute' | 'cancel' | 'release';
/** Host/daemon authority lookup on every mutation. Never accept model-provided capability flags. */
export type EndpointAuthorizer = (identity: EndpointIdentity, action: EndpointAction) => Promise<EndpointGrant>;
export interface PreparePayload { kind: 'prepare'; baseCommit?: string }
export type ExecutionPayload = { environmentId: string; cwd?: string; sandbox?: boolean } & (
  { kind: 'read'; path: string } | { kind: 'write'; path: string; text: string } |
  { kind: 'edit'; path: string; oldText: string; newText: string } |
  { kind: 'shell'; command: string });
export interface OperationRequest<P> { operationId: string; payloadDigest: string; identity: EndpointIdentity; payload: P }
export type PrepareRequest = OperationRequest<PreparePayload>;
export type ExecuteRequest = OperationRequest<ExecutionPayload>;
export function operationDigest(identity: EndpointIdentity, payload: unknown): string { return digest({ identity, payload }); }
export interface ReceiptIdentity { operationId: string; payloadDigest: string; identity: EndpointIdentity }
export interface PreparedReceipt extends ReceiptIdentity {
  status: 'prepared'; environmentId: string; cwd: string; baseCommit: string;
  /** Present on new receipts; legacy receipts are verified through their environment hash and real git state. */
  repository?: string; gitCommonDirectory?: string;
}
export type ExecutionResult = { text: string; truncated: boolean } | { bytes: number } |
  { stdout: string; stderr: string; truncated: boolean; exitCode: number | null; signal: string | null };
export type ExecutionReceipt = ReceiptIdentity & (
  { status: 'running' | 'unknown' | 'cancelPending'; reason?: string } |
  { status: 'completed' | 'failed' | 'cancelled'; result?: ExecutionResult; reason?: string });
export type LifecycleReceipt = ReceiptIdentity & { status: 'released' | 'cancelled' | 'unknown'; reason?: string };
export type PreparationReceipt = PreparedReceipt | (Omit<PreparedReceipt, 'status'> & { status: 'unknown'; reason: string });
export type BrokerReceipt = PreparationReceipt | ExecutionReceipt | LifecycleReceipt;
export interface EnvironmentBroker {
  prepare(request: PrepareRequest): Promise<PreparedReceipt>;
  execute(request: ExecuteRequest): Promise<ExecutionReceipt>;
  query(operationId: string): Promise<BrokerReceipt | undefined>;
  cancel(operationId: string, epoch: number): Promise<ExecutionReceipt>;
  release(environmentId: string, operationId: string): Promise<LifecycleReceipt>;
}
