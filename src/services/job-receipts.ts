import type { TaskId } from '@earendil-works/pi-durable';
import type { ExecutionReceipt } from '../environments/types.js';
import type { Jobs, Job } from './jobs.js';

const terminal = (job: Job) => ['completed', 'failed', 'cancelled'].includes(job.status);
/** Finite native lookup claims. Due times survive restart; no lookup can launch work. */
export class JobReceipts {
  private closed = false;
  private timer?: ReturnType<typeof setTimeout>;
  private reconciling?: Promise<void>;
  constructor(private readonly jobs: Jobs) {}

  reconcile(): Promise<void> {
    if (this.closed || !this.jobs.broker) return Promise.resolve();
    return this.reconciling ??= this.schedule().finally(() => { this.reconciling = undefined; });
  }
  private async schedule(): Promise<void> {
    const { engine } = this.jobs;
    const deadlines = await engine.transaction(async tx => {
      const eligible: Job[] = [];
      let active = 0;
      // Read native Task state before creating any new table rows.
      for (const job of await engine.store.listInTransaction<Job>(tx, 'job')) {
        if (!job.payloadDigest || terminal(job)) continue;
        const execution = await tx.task(job.taskId);
        const query = job.receiptTaskId ? await tx.task(job.receiptTaskId) : undefined;
        if (query && query.state.status !== 'terminal') { active++; continue; }
        if (execution && execution.state.status !== 'terminal') continue;
        eligible.push(job);
      }
      const deadlines: number[] = [];
      for (const job of eligible) {
        const due = job.receiptQueryAt ?? 0;
        if (due > engine.scheduler.clock.now() || active >= 8) { deadlines.push(due); continue; }
        const taskId = await tx.createTask(this.jobs.task, { jobId: job.id, queryOnly: true },
          { ownership: { kind: 'conversation' }, background: true });
        await engine.store.put(tx, 'job', job.id, { ...job, receiptTaskId: taskId });
        active++;
      }
      return deadlines;
    });
    if (this.timer) clearTimeout(this.timer);
    if (this.closed || !deadlines.length) return;
    // Capacity-blocked Jobs are reconsidered by completion, with a bounded fallback.
    const delay = Math.max(1000, Math.min(60_000, Math.min(...deadlines) - engine.scheduler.clock.now()));
    this.timer = setTimeout(() => { this.timer = undefined; void this.reconcile().catch(() => {
      // Persisted claims remain inspectable and startup/explicit pump can retry.
    }); }, delay);
    this.timer.unref();
  }
  async run(id: string, taskId: TaskId<null>): Promise<void> {
    const { engine, broker } = this.jobs;
    if (!broker || this.closed) return;
    const job = await engine.transaction(async tx => {
      const current = await engine.store.get<Job>(tx, 'job', id);
      if (!current || current.receiptTaskId !== taskId || !current.payloadDigest || terminal(current)) return;
      const attempts = (current.receiptQueryAttempts ?? 0) + 1;
      const delay = Math.min(60_000, 2000 * 2 ** Math.min(5, attempts - 1));
      await engine.store.put(tx, 'job', id, { ...current, receiptQueryAttempts: attempts, receiptQueryAt: engine.scheduler.clock.now() + delay });
      return current;
    });
    if (!job) return;
    try {
      const receipt = await broker.query(job.operationId);
      // Absence, running and uncertainty never authorize prepare/execute/cancel.
      if (receipt && ['completed', 'failed', 'cancelled', 'unknown'].includes(receipt.status))
        await this.jobs.finish(id, receipt as ExecutionReceipt, taskId);
    } catch {
      // Query failure or conflicting receipt preserves accounting and durable backoff.
    }
  }
  close(): void {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
  }
}
