import { defineTask } from '@earendil-works/pi-durable';
import type { Jobs } from '../../services/jobs.js';
export function createJobTask(jobs: Jobs) {
  return defineTask<{ jobId: string }, { phase: 'execute' }, null>({
    name: 'sa.job', version: 1, initial: () => ({ phase: 'execute' }),
    phases: { execute: async (task, runtime, context) => {
      await jobs.run(task.input.jobId, runtime.taskId, runtime.signal);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'completed', result: null } }), context);
    } },
    abort: async (task, runtime, context) => {
      await jobs.cancelExecution(task.input.jobId);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'aborted' } }), context);
    },
  });
}
