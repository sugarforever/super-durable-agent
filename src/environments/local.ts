import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { constants, realpathSync } from 'node:fs';
import { mkdir, readFile, realpath, lstat, open, rename, unlink, link, readdir } from 'node:fs/promises';
import { join, resolve, relative, dirname, isAbsolute, sep } from 'node:path';
import { randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { digest, canonicalJson } from '../domain/model.js';
import { operationDigest, type EndpointAuthorizer, type EndpointIdentity, type EnvironmentBroker, type PrepareRequest, type PreparedReceipt, type PreparationReceipt, type ExecuteRequest, type ExecutionReceipt, type BrokerReceipt, type LifecycleReceipt, type ReceiptIdentity, type EndpointAction } from './types.js';
const git = promisify(execFile);
const OUTPUT_LIMIT = 16384;
const FILE_LIMIT = 1024 * 1024;
const MARKER = '.super-agent-environment.json';
interface Ledger { request: PrepareRequest | ExecuteRequest | { operationId: string; payloadDigest: string; identity: EndpointIdentity; payload: unknown }; receipt: BrokerReceipt }
interface Active { request: ExecuteRequest; promise: Promise<ExecutionReceipt>; child?: ChildProcess; cancelling: boolean; termination?: Promise<boolean>; unknown?: () => void }
const active = new Map<string, Active>();
const locks = new Map<string, Promise<unknown>>();
const releasing = new Set<string>();
function boundedText(buffer: Buffer): string {
  let text = buffer.subarray(0, OUTPUT_LIMIT).toString("utf8");
  while (Buffer.byteLength(text) > OUTPUT_LIMIT) text = text.slice(0, -1);
  return text;
}
export interface LocalEnvironmentOptions {
  readonly directory: string; repository: string; authorize: EndpointAuthorizer;
  /** Grants the shell the OS account's filesystem/network authority. This is not a sandbox. */
  trustedLocalShell?: boolean;
}
/** Single-host endpoint. The ledger is separate from pi; an unreceipted launch is never replayed. */
export class LocalEnvironmentBroker implements EnvironmentBroker {
  readonly directory: string;
  private initialized?: Promise<void>;
  constructor(readonly options: LocalEnvironmentOptions) {
    let parent = resolve(options.directory); const tail: string[] = [];
    for (;;) { try { this.directory = join(realpathSync(parent), ...tail); break; }
      catch (error: any) { if (error.code !== 'ENOENT') throw error; tail.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); } }
  }
  private async ready(): Promise<void> { await (this.initialized ??= (async () => { await mkdir(this.directory, { recursive: true }); if (await realpath(this.directory) !== this.directory) throw new Error('Endpoint ledger path changed'); })()); }
  private file(id: string) { return join(this.directory, 'operations', `${digest(id)}.json`); }
  private async serial<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const prior = locks.get(key) ?? Promise.resolve();
    const result = prior.catch(() => {}).then(fn); locks.set(key, result);
    try { return await result; } finally { if (locks.get(key) === result) locks.delete(key); }
  }
  private async read(id: string): Promise<Ledger | undefined> {
    try {
      const { checksum, ...record } = JSON.parse(await readFile(this.file(id), 'utf8')) as Ledger & { checksum: string };
      if (checksum !== digest(record) || record.request.operationId !== id || record.receipt.operationId !== id || record.request.payloadDigest !== record.receipt.payloadDigest || canonicalJson(record.request.identity) !== canonicalJson(record.receipt.identity)) throw new Error('Corrupt endpoint ledger integrity');
      this.identity(record.request);
      return record;
    }
    catch (error: any) { if (error.code === 'ENOENT') return; throw error; }
  }
  private async atomic(path: string, value: unknown, exclusive = false): Promise<boolean> {
    await mkdir(dirname(path), { recursive: true });
    const temporary = `${path}.${randomUUID()}.tmp`;
    const handle = await open(temporary, 'wx', 0o600);
    const stored = value && typeof value === 'object' && 'request' in value && 'receipt' in value ? { ...value, checksum: digest(value) } : value;
    try { await handle.writeFile(canonicalJson(stored)); await handle.sync(); } finally { await handle.close(); }
    try {
      if (exclusive) { try { await link(temporary, path); } catch (error: any) { if (error.code === 'EEXIST') return false; throw error; } }
      else await rename(temporary, path);
      const directory = await open(dirname(path), 'r'); try { await directory.sync(); } finally { await directory.close(); }
      return true;
    } finally { await unlink(temporary).catch(() => {}); }
  }
  private identity(request: { operationId: string; identity: EndpointIdentity; payloadDigest: string; payload: unknown }): ReceiptIdentity {
    const i = request.identity;
    if (!request.operationId?.trim() || !i?.ticketId?.trim() || !i.assignmentId?.trim() || !i.mandateId?.trim() || !Number.isSafeInteger(i.epoch) || i.epoch < 1) throw new Error('Invalid operation identity');
    if (operationDigest(i, request.payload) !== request.payloadDigest) throw new Error('Operation payload digest conflict');
    return { operationId: request.operationId, payloadDigest: request.payloadDigest, identity: i };
  }
  private async authorize(identity: EndpointIdentity, action: EndpointAction, capability?: string): Promise<void> {
    const grant = await this.options.authorize(identity, action);
    const { capabilities, ...bound } = grant;
    if (canonicalJson(bound) !== canonicalJson(identity)) throw new Error('Stale endpoint grant');
    if (capability && !capabilities.includes(capability)) throw new Error(`Missing capability: ${capability}`);
  }
  private conflict(previous: Ledger, request: { payloadDigest: string; identity: EndpointIdentity }): void {
    if (previous.request.payloadDigest !== request.payloadDigest || canonicalJson(previous.request.identity) !== canonicalJson(request.identity)) throw new Error('Operation payload conflict');
  }
  async prepare(request: PrepareRequest): Promise<PreparedReceipt> {
    await this.ready();
    const identity = this.identity(request);
    await this.authorize(request.identity, 'prepare', 'repo.read');
    return this.serial(this.file(request.operationId), async () => {
      const repository = await realpath(this.options.repository);
      const gitCommonDirectory = await this.commonDirectory(repository);
      const environmentId = digest({ identity: request.identity, repository });
      let previous = await this.read(request.operationId);
      if (previous) {
        this.conflict(previous, request);
        if (previous.receipt.status === 'prepared') { await this.environment(previous.receipt.environmentId, request.identity); return previous.receipt; }
        const checkpoint = previous.receipt as PreparationReceipt;
        if (checkpoint.environmentId !== environmentId || (checkpoint.repository !== undefined && checkpoint.repository !== repository) || (checkpoint.gitCommonDirectory !== undefined && checkpoint.gitCommonDirectory !== gitCommonDirectory)) throw new Error('Prepared repository identity conflict');
        // Preparation can safely inspect its deterministic marker/git state below.
      }
      const cwd = join(this.directory, 'worktrees', environmentId);
      const admitted = previous?.receipt as PreparationReceipt | undefined;
      const baseCommit = admitted?.baseCommit ?? (await git('git', ['-C', repository, 'rev-parse', '--verify', `${request.payload.baseCommit ?? 'HEAD'}^{commit}`])).stdout.trim();
      const legacy = previous && admitted?.repository === undefined && admitted?.gitCommonDirectory === undefined;
      const receipt: PreparedReceipt = { ...identity, status: 'prepared', environmentId, cwd, baseCommit, ...(!legacy ? { repository, gitCommonDirectory } : {}) };
      if (!previous) {
        const won = await this.atomic(this.file(request.operationId), { request, receipt: { ...receipt, status: 'unknown', reason: 'Preparation admitted' } }, true);
        if (!won) { previous = (await this.read(request.operationId))!; this.conflict(previous, request); throw new Error('Concurrent preparation requires query/retry'); }
      }
      let exists = false;
      try { await lstat(cwd); exists = true; } catch (error: any) { if (error.code !== 'ENOENT') throw error; }
      if (exists) {
        let marker: unknown;
        try { marker = JSON.parse(await readFile(join(cwd, MARKER), 'utf8')); }
        catch (error: any) {
          if (error.code !== 'ENOENT' || !previous || admitted?.cwd !== cwd || admitted?.baseCommit !== baseCommit) throw new Error('Unknown worktree path; refusing overwrite');
          // Recover only our durably admitted git registration, never a coincidental directory.
          await this.verifyWorktree(repository, cwd, baseCommit);
          await this.atomic(join(cwd, MARKER), receipt, true); marker = receipt;
        }
        if (canonicalJson(marker) !== canonicalJson(receipt)) throw new Error('Unknown worktree marker conflict');
        await this.verifyWorktree(repository, cwd, baseCommit);
      } else {
        await mkdir(dirname(cwd), { recursive: true });
        await git('git', ['-C', repository, 'worktree', 'add', '--detach', cwd, baseCommit]);
        await this.atomic(join(cwd, MARKER), receipt, true);
      }
      await this.atomic(join(this.directory, 'environments', `${environmentId}.json`), receipt, true);
      await this.atomic(this.file(request.operationId), { request, receipt });
      return receipt;
    });
  }
  private async verifyWorktree(repository: string, cwd: string, baseCommit: string): Promise<void> {
    if ((await lstat(cwd)).isSymbolicLink() || await realpath(cwd) !== cwd) throw new Error('Worktree path escape');
    const listing = (await git('git', ['-C', repository, 'worktree', 'list', '--porcelain', '-z'])).stdout;
    const registration = listing.split('\0\0').map(block => block.split('\0')).find(fields => fields[0] === `worktree ${cwd}`);
    if (!registration?.includes(`HEAD ${baseCommit}`) || !registration.includes('detached')) throw new Error('Unknown or changed registered worktree');
    if (await this.commonDirectory(repository) !== await this.commonDirectory(cwd) || (await git('git', ['-C', cwd, 'rev-parse', 'HEAD'])).stdout.trim() !== baseCommit || await realpath((await git('git', ['-C', cwd, 'rev-parse', '--show-toplevel'])).stdout.trim()) !== cwd) throw new Error('Worktree repository/base conflict');
  }
  private async commonDirectory(repository: string): Promise<string> {
    return realpath(resolve(repository, (await git('git', ['-C', repository, 'rev-parse', '--git-common-dir'])).stdout.trim()));
  }
  private async environment(environmentId: string, identity: EndpointIdentity): Promise<PreparedReceipt> {
    if (!/^[a-f0-9]{64}$/.test(environmentId)) throw new Error('Unknown environment');
    const descriptor = JSON.parse(await readFile(join(this.directory, 'environments', `${environmentId}.json`), 'utf8')) as PreparedReceipt;
    const repository = await realpath(this.options.repository);
    const gitCommonDirectory = await this.commonDirectory(repository);
    if (descriptor.environmentId !== environmentId || digest({ identity, repository }) !== environmentId ||
      (descriptor.repository !== undefined && descriptor.repository !== repository) ||
      (descriptor.gitCommonDirectory !== undefined && descriptor.gitCommonDirectory !== gitCommonDirectory)) throw new Error('Prepared repository identity conflict');
    if (canonicalJson(descriptor.identity) !== canonicalJson(identity)) throw new Error('Unrelated environment identity');
    const cwd = await realpath(descriptor.cwd);
    if (cwd !== descriptor.cwd || cwd !== join(this.directory, 'worktrees', environmentId)) throw new Error('Environment path escape');
    const marker = JSON.parse(await readFile(join(cwd, MARKER), 'utf8'));
    if (canonicalJson(marker) !== canonicalJson(descriptor)) throw new Error('Environment marker conflict');
    // Normal reuse allows user commits/branches and uncommitted edits. Bind the
    // actual repository and top level, without requiring current HEAD == base.
    if (await this.commonDirectory(cwd) !== gitCommonDirectory ||
      await realpath((await git('git', ['-C', cwd, 'rev-parse', '--show-toplevel'])).stdout.trim()) !== cwd) throw new Error('Prepared repository identity conflict');
    const listing = (await git('git', ['-C', repository, 'worktree', 'list', '--porcelain', '-z'])).stdout;
    if (!listing.split('\0\0').some(block => block.split('\0')[0] === `worktree ${cwd}`)) throw new Error('Prepared repository registration conflict');
    return descriptor;
  }
  private async path(cwd: string, path: string): Promise<string> {
    if (typeof path !== 'string' || !path || isAbsolute(path) || path.split(/[\\/]/).some(x => x === '..' || x.toLowerCase() === '.git' || x.toLowerCase() === MARKER)) throw new Error('Path traversal or metadata access denied');
    const target = resolve(cwd, path);
    const inside = (p: string) => { const r = relative(cwd, p); return r !== '..' && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
    if (!inside(target)) throw new Error('Path escape denied');
    let parent = target;
    for (;;) {
      try {
        const stat = await lstat(parent);
        // Reject all symlink traversal. This also handles dangling symlinks safely.
        if (stat.isSymbolicLink()) throw new Error('Path symlink traversal denied');
        const canonical = await realpath(parent);
        if (!inside(canonical)) throw new Error('Path escape denied');
      } catch (error: any) { if (error.code !== 'ENOENT') throw error; }
      if (parent === cwd) break;
      parent = dirname(parent);
    }
    return target;
  }
  execute(request: ExecuteRequest): Promise<ExecutionReceipt> {
    const key = this.file(request.operationId);
    const prior = active.get(key);
    if (prior) {
      try { this.identity(request); this.conflict({ request: prior.request, receipt: {} as BrokerReceipt }, request); }
      catch (error) { return Promise.reject(error); }
      return this.authorize(request.identity, 'execute', request.payload.kind === 'shell' ? 'command.execute' : request.payload.kind === 'read' ? 'repo.read' : 'repo.write').then(() => prior.promise);
    }
    const state: Active = { request, cancelling: false, promise: undefined! };
    active.set(key, state);
    state.promise = this.executeOperation(request, state).finally(() => { if (active.get(key) === state) active.delete(key); });
    return state.promise;
  }
  private async executeOperation(request: ExecuteRequest, state: Active): Promise<ExecutionReceipt> {
    await this.ready();
    const identity = this.identity(request);
    const p = request.payload;
    if (!['shell', 'read', 'write', 'edit'].includes(p.kind)) throw new Error('Unknown execution kind');
    await this.authorize(request.identity, 'execute', p.kind === 'shell' ? 'command.execute' : p.kind === 'read' ? 'repo.read' : 'repo.write');
    if (p.sandbox !== undefined && p.sandbox !== false) throw new Error('Requested sandbox is unavailable');
    if (p.kind === 'shell') {
      if (this.options.trustedLocalShell !== true) throw new Error('trustedLocalShell must be explicitly enabled; shell has OS account authority');
      if (typeof p.command !== 'string' || !p.command.trim()) throw new Error('Command must be nonempty');
    }
    if (releasing.has(join(this.directory, p.environmentId))) throw new Error('Environment release is in progress');
    const environment = await this.environment(p.environmentId, request.identity);
    if (p.cwd !== undefined && await realpath(p.cwd) !== environment.cwd) throw new Error('cwd must be the assigned worktree');
    const target = p.kind === 'shell' ? undefined : await this.path(environment.cwd, p.path);
    if ((p.kind === 'write' && Buffer.byteLength(p.text) > FILE_LIMIT) || (p.kind === 'edit' && Buffer.byteLength(p.newText) > FILE_LIMIT)) throw new Error('Structured write exceeds byte limit');
    const key = this.file(request.operationId);
    let launch = false;
    const existing = await this.serial(key, async () => {
      const previous = await this.read(request.operationId);
      if (previous) { this.conflict(previous, request); return previous.receipt as ExecutionReceipt; }
      const receipt: ExecutionReceipt = { ...identity, status: 'running' };
      if (!(await this.atomic(key, { request, receipt }, true))) { const other = (await this.read(request.operationId))!; this.conflict(other, request); return other.receipt as ExecutionReceipt; }
      launch = true; return receipt;
    });
    if (!launch) {
      if (['running','cancelPending'].includes(existing.status)) {
        const receipt: ExecutionReceipt = { ...identity, status: 'unknown', reason: 'No durable completion receipt; execution will not be repeated' };
        await this.atomic(key, { request, receipt }); return receipt;
      }
      return existing;
    }
    return (async (): Promise<ExecutionReceipt> => {
      let receipt: ExecutionReceipt;
      try {
        // Fresh endpoint admission immediately before any external mutation/spawn.
        await this.authorize(request.identity, 'execute', p.kind === 'shell' ? 'command.execute' : p.kind === 'read' ? 'repo.read' : 'repo.write');
        if (state.cancelling) receipt = { ...identity, status: 'cancelled', reason: 'Cancelled before launch' };
        else if (p.kind === 'shell') receipt = await this.shell(request, environment.cwd, state);
        else if (p.kind === 'read') {
          const handle = await open(target!, constants.O_RDONLY | constants.O_NOFOLLOW);
          try {
            const data = Buffer.alloc(OUTPUT_LIMIT + 1); const { bytesRead } = await handle.read(data, 0, data.length, 0);
            receipt = { ...identity, status: 'completed', result: { text: boundedText(data.subarray(0, bytesRead)), truncated: bytesRead > OUTPUT_LIMIT } };
          } finally { await handle.close(); }
        } else {
          let text = p.kind === 'write' ? p.text : '';
          if (p.kind === 'edit') {
            const handle = await open(target!, constants.O_RDONLY | constants.O_NOFOLLOW);
            try {
              const stat = await handle.stat(); if (stat.size > FILE_LIMIT) throw new Error('Edit exceeds byte limit');
              const old = await handle.readFile('utf8');
              if (!p.oldText || !old.includes(p.oldText) || old.indexOf(p.oldText) !== old.lastIndexOf(p.oldText)) throw new Error('Edit requires exactly one oldText match');
              text = old.replace(p.oldText, () => p.newText);
            } finally { await handle.close(); }
          }
          if (Buffer.byteLength(text) > FILE_LIMIT) throw new Error('Structured write exceeds byte limit');
          // Validate again immediately before the atomic replacement. Never follow target symlinks.
          await this.path(environment.cwd, p.path);
          const temp = join(dirname(target!), `.sa-write-${randomUUID()}`);
          const handle = await open(temp, 'wx', 0o600);
          try { await handle.writeFile(text); await handle.sync(); } finally { await handle.close(); }
          try { await this.path(environment.cwd, p.path); await rename(temp, target!); }
          finally { await unlink(temp).catch(() => {}); }
          receipt = { ...identity, status: 'completed', result: { bytes: Buffer.byteLength(text) } };
        }
      } catch (error) { receipt = { ...identity, status: 'failed', reason: error instanceof Error ? error.message : String(error) }; }
      if (state.cancelling && state.child?.pid && !(await this.terminateGroup(state))) receipt = { ...identity, status: 'unknown', reason: 'Process group termination could not be established' };
      await this.serial(key, () => this.atomic(key, { request, receipt }));
      return receipt;
    })();
  }
  private shell(request: ExecuteRequest, cwd: string, state: Active): Promise<ExecutionReceipt> {
    const identity = this.identity(request);
    const p = request.payload; if (p.kind !== 'shell') throw new Error('Expected shell');
    return new Promise(resolveReceipt => {
      const child = spawn('/bin/sh', ['-c', p.command], { cwd, stdio: ['ignore', 'pipe', 'pipe'], detached: process.platform !== 'win32' }); state.child = child;
      state.unknown = () => {
        // An uncertain descendant may retain inherited pipe FDs indefinitely.
        // Return the payload-bound unknown receipt independently of close and
        // release only the host's observation resources, never claim cessation.
        child.stdout?.destroy(); child.stderr?.destroy(); child.unref();
        resolveReceipt({ ...identity, status: 'unknown', reason: 'Process group termination could not be established' });
      };
      const chunks = { stdout: Buffer.alloc(0), stderr: Buffer.alloc(0) }; let truncated = false;
      for (const name of ['stdout', 'stderr'] as const) child[name]!.on('data', (chunk: Buffer) => {
        const available = OUTPUT_LIMIT - chunks[name].length; if (chunk.length > available) truncated = true;
        if (available > 0) chunks[name] = Buffer.concat([chunks[name], chunk.subarray(0, available)]);
      });
      child.once('error', error => resolveReceipt({ ...identity, status: 'failed', reason: error.message }));
      child.once('close', (exitCode, signal) => {
        // A closed shell can leave live members of its original group behind.
        // Keep the endpoint handle until independent group supervision finishes.
        void (async () => {
          const confirmed = state.cancelling ? await this.terminateGroup(state) : undefined;
          resolveReceipt({ ...identity,
            status: state.cancelling ? (confirmed ? 'cancelled' : 'unknown') : exitCode === 0 ? 'completed' : 'failed',
            ...(state.cancelling && !confirmed ? { reason: 'Process group termination could not be established' } : {}),
            result: { stdout: boundedText(chunks.stdout), stderr: boundedText(chunks.stderr), truncated, exitCode, signal },
          });
        })();
      });
    });
  }
  private async groupState(pgid: number): Promise<'gone' | 'live' | 'unknown'> {
    try { process.kill(-pgid, 0); }
    catch (error: any) { return error.code === 'ESRCH' ? 'gone' : 'unknown'; }
    try {
      // kill(0) also sees orphan zombies on hosts whose PID 1 reaps slowly.
      // Zombies cannot execute effects; otherwise require an absent group or
      // a complete snapshot proving every remaining member is already a zombie.
      const listing = (await git('ps', ['-A', '-o', 'pgid=,stat='], { timeout: 200, maxBuffer: 4 * 1024 * 1024 })).stdout;
      const members = listing.split('\n').flatMap(line => {
        const match = /^\s*(\d+)\s+(\S+)/.exec(line);
        return match && Number(match[1]) === pgid ? [match[2]] : [];
      });
      if (members.length) return members.every(status => status.startsWith('Z')) ? 'gone' : 'live';
      // A missing ps row alone is not confirmation of group disappearance.
      try { process.kill(-pgid, 0); return 'unknown'; }
      catch (error: any) { return error.code === 'ESRCH' ? 'gone' : 'unknown'; }
    } catch { return 'unknown'; }
  }
  private terminateGroup(state: Active): Promise<boolean> {
    if (state.termination) return state.termination;
    state.termination = (async () => {
      const pgid = state.child?.pid;
      if (!pgid) return false;
      if (process.platform === 'win32') { state.child!.kill('SIGTERM'); return false; }
      const signal = (name: NodeJS.Signals): 'sent' | 'gone' | 'unknown' => {
        try { process.kill(-pgid, name); return 'sent'; }
        catch (error: any) { return error.code === 'ESRCH' ? 'gone' : 'unknown'; }
      };
      const wait = async (): Promise<'gone' | 'live' | 'unknown'> => {
        const deadline = performance.now() + 500;
        for (;;) {
          const current = await this.groupState(pgid);
          if (current === 'gone' || performance.now() >= deadline) return current;
          await delay(20);
        }
      };
      const term = signal('SIGTERM');
      if (term !== 'sent') return term === 'gone';
      const afterTerm = await wait();
      if (afterTerm === 'gone') return true;
      // Escalation survives shell close. Never schedule a signal after losing
      // the short-lived group's identity or after this bounded supervision.
      if (afterTerm === 'unknown') return false;
      const kill = signal('SIGKILL');
      if (kill !== 'sent') return kill === 'gone';
      return await wait() === 'gone';
    })().then(confirmed => { if (!confirmed) state.unknown?.(); return confirmed; });
    return state.termination;
  }
  async query(operationId: string): Promise<BrokerReceipt | undefined> {
    await this.ready();
    return this.serial(this.file(operationId), async () => {
      const record = await this.read(operationId);
      if (!record) { const pending = active.get(this.file(operationId)); return pending ? { ...this.identity(pending.request), status: 'running' as const } : undefined; }
      if (['running', 'cancelPending'].includes(record.receipt.status) && !active.has(this.file(operationId))) {
        const receipt = { ...record.receipt, status: 'unknown' as const, reason: 'Endpoint has no live process handle or durable completion receipt; execution will not be repeated' };
        await this.atomic(this.file(operationId), { ...record, receipt }); return receipt;
      }
      return record.receipt;
    });
  }
  async cancel(operationId: string, epoch: number): Promise<ExecutionReceipt> {
    await this.ready();
    const live = active.get(this.file(operationId));
    const record = await this.read(operationId) ?? (live ? { request: live.request, receipt: { ...this.identity(live.request), status: 'running' as const } } : undefined);
    if (!record || record.request.identity.epoch !== epoch) throw new Error('Cancellation operation epoch conflict');
    await this.authorize(record.request.identity, 'cancel');
    const cancellationId = `cancel:${operationId}:${epoch}`;
    const payload = { kind: 'cancel', operationId, epoch };
    const cancelRequest = { operationId: cancellationId, identity: record.request.identity, payload, payloadDigest: operationDigest(record.request.identity, payload) };
    const previous = await this.read(cancellationId); if (previous) this.conflict(previous, cancelRequest);
    else await this.atomic(this.file(cancellationId), { request: cancelRequest, receipt: { ...this.identity(cancelRequest), status: 'unknown' } }, true);
    const state = active.get(this.file(operationId));
    if (state) {
      state.cancelling = true;
      if (state.child?.pid) void this.terminateGroup(state);
    }
    let receipt = state ? await state.promise : (await this.query(operationId)) as ExecutionReceipt;
    if (state?.termination && !(await state.termination)) {
      receipt = { ...this.identity(state.request), status: 'unknown', reason: 'Process group termination could not be established' };
      await this.serial(this.file(operationId), () => this.atomic(this.file(operationId), { request: state.request, receipt }));
    }
    await this.atomic(this.file(cancellationId), { request: cancelRequest, receipt: { ...this.identity(cancelRequest), status: ['completed','failed','cancelled'].includes(receipt.status) ? 'cancelled' : 'unknown' } });
    return receipt;
  }
  async release(environmentId: string, operationId: string): Promise<LifecycleReceipt> {
    await this.ready();
    if (!/^[a-f0-9]{64}$/.test(environmentId)) throw new Error('Unknown environment');
    const descriptor = JSON.parse(await readFile(join(this.directory, 'environments', `${environmentId}.json`), 'utf8')) as PreparedReceipt;
    await this.authorize(descriptor.identity, 'release');
    const payload = { kind: 'release', environmentId };
    const request = { operationId, identity: descriptor.identity, payload, payloadDigest: operationDigest(descriptor.identity, payload) };
    const releaseKey = join(this.directory, environmentId);
    if (releasing.has(releaseKey)) throw new Error('Environment release is in progress');
    releasing.add(releaseKey);
    try { return await this.serial(this.file(operationId), async () => {
      const previous = await this.read(operationId); if (previous) { this.conflict(previous, request); return previous.receipt as LifecycleReceipt; }
      if ([...active.values()].some(x => x.request.payload.environmentId === environmentId)) throw new Error('Environment has active operations');
      for (const name of await readdir(join(this.directory, 'operations'))) {
        if (!name.endsWith('.json')) continue;
        const saved = JSON.parse(await readFile(join(this.directory, 'operations', name), 'utf8')) as Ledger;
        const record = await this.read(saved.request.operationId);
        if (record && ['shell','read','write','edit'].includes((record.request.payload as { kind: string }).kind) && (record.request.payload as { environmentId?: string }).environmentId === environmentId && ['running','cancelPending','unknown'].includes(record.receipt.status)) throw new Error('Environment has running or unknown operations');
      }
      await this.environment(environmentId, descriptor.identity);
      const receipt: LifecycleReceipt = { ...this.identity(request), status: 'unknown' };
      if (!(await this.atomic(this.file(operationId), { request, receipt }, true))) throw new Error('Concurrent release');
      // Remove our marker only. Git refuses to remove dirty worktrees; never force user changes away.
      await unlink(join(descriptor.cwd, MARKER));
      try { await git('git', ['-C', this.options.repository, 'worktree', 'remove', descriptor.cwd]); }
      catch (error) { await this.atomic(join(descriptor.cwd, MARKER), descriptor, true); throw error; }
      const result: LifecycleReceipt = { ...receipt, status: 'released' };
      await this.atomic(this.file(operationId), { request, receipt: result }); return result;
    }); } finally { releasing.delete(releaseKey); }
  }
}
