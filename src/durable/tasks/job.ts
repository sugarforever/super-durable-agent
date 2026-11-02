import { defineTask } from '@earendil-works/pi-durable';
import type { Jobs } from '../../services/jobs.js';
export function createJobTask(jobs: Jobs) {
  return defineTask<{ jobId: string; queryOnly?: boolean }, { phase: 'execute' }, null>({
    name: 'sa.job', version: 1, initial: () => ({ phase: 'execute' }),
    phases: { execute: async (task, runtime, context) => {
      if (task.input.queryOnly) await jobs.receipts.run(task.input.jobId, runtime.taskId);
      else await jobs.run(task.input.jobId, runtime.taskId, runtime.signal);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'completed', result: null } }), context);
      await jobs.receipts.reconcile();
    } },
    abort: async (task, runtime, context) => {
      if (!task.input.queryOnly) await jobs.cancelExecution(task.input.jobId);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'aborted' } }), context);
      await jobs.receipts.reconcile();
    },
  });
}
