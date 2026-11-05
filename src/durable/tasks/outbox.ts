import { defineTask } from '@earendil-works/pi-durable';
import type { Outbox } from '../../services/outbox.js';
export function createOutboxTask(outbox: Outbox) {
  return defineTask<{ messageId: string }, { phase: 'send' }, null>({
    name: 'sa.outbox', version: 1, initial: () => ({ phase: 'send' }),
    phases: { send: async (task, runtime, context) => {
      await outbox.run(task.input.messageId, runtime.taskId);
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'completed', result: null } }), context);
      await outbox.completed();
    } },
    abort: async (_task, runtime, context) => {
      await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'aborted' } }), context);
    },
  });
}
