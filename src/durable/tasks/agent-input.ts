import {
  defineTask,
  type ConversationId,
  type SubmissionId,
  type TaskId,
} from "@earendil-works/pi-durable";
import type { JsonValue } from "@earendil-works/chord";
import type { AgentCoordinator } from "../../agents/coordinator.js";
export interface AgentRun {
  id: string;
  conversationId: ConversationId;
  requestId: string;
  prompt: string;
  promptDigest: string;
  mode: "followUp" | "steer";
  attempt: number;
  taskId?: TaskId<null>;
  submissionId?: SubmissionId;
  status: "pending" | "submitted" | "settled" | "attention" | "invalid";
  admitted: boolean;
  placed: boolean;
  settled: boolean;
  consumed: boolean;
  ticketId?: string;
  triggerId?: string;
  inputId?: string;
  epoch?: number;
  intentId?: string;
  evalIds?: string[];
  noticeIds?: string[];
  estimatedTokens?: number;
  promptArtifact?: string;
  settlement?: JsonValue;
  redrives?: number;
  executionAttempts?: number;
}
export function createAgentInputTask(coordinator: AgentCoordinator) {
  return defineTask<
    { kind: "deliveryRun" | "routerBatch"; id: string },
    { phase: "submit" } | { phase: "wait" },
    null
  >({
    name: "sa.agentInput",
    version: 1,
    initial: () => ({ phase: "submit" }),
    phases: {
      submit: async (task, runtime, context) => {
        const run = await coordinator.engine.store.read<AgentRun>(
          task.input.kind,
          task.input.id,
        );
        if (
          !run ||
          run.status === "invalid" ||
          !(await coordinator.isCurrent(run))
        ) {
          await runtime.commit(async (tx) => {
            if (run)
              await coordinator.engine.store.put(tx, task.input.kind, run.id, {
                ...run,
                status: "invalid",
                settled: true,
              });
            return {
              status: "terminal",
              outcome: { status: "completed", result: null },
            };
          }, context);
          return;
        }
        const conversation = await runtime.conversation(
          run.conversationId,
          context,
        );
        if (!conversation) throw new Error("Missing agent conversation");
        coordinator.validatePrompt(run);
        const submission = await conversation.submit(
          {
            type: "input",
            content: run.prompt,
            requestId: run.requestId,
            whenBusy: run.mode,
          },
          context,
        );
        if (!(await coordinator.isCurrent(run)))
          await conversation.abort(context, { background: true });
        const receipt = await submission.status(context);
        await runtime.commit(async (tx) => {
          const current = (await coordinator.engine.store.get<AgentRun>(
            tx,
            task.input.kind,
            run.id,
          ))!;
          await coordinator.engine.store.put(tx, task.input.kind, run.id, {
            ...current,
            status: current.status === "invalid" ? "invalid" : "submitted",
            admitted: true,
            placed: receipt.status !== "queued",
            submissionId: submission.id,
          });
          return { status: "running", checkpoint: { phase: "wait" } };
        }, context);
      },
      wait: async (task, runtime, context) => {
        const run = (await coordinator.engine.store.read<AgentRun>(
          task.input.kind,
          task.input.id,
        ))!;
        const submission = await coordinator.engine.harness.submission(
          run.submissionId!,
          context,
        );
        if (!submission) throw new Error("Missing durable agent submission");
        const receipt = await submission.wait(context);
        await runtime.commit(async (tx) => {
          await coordinator.settleInTransaction(
            tx,
            task.input.kind,
            run.id,
            receipt,
          );
          return {
            status: "terminal",
            outcome: { status: "completed", result: null },
          };
        }, context);
        coordinator.kick();
      },
    },
    abort: async (_task, runtime, context) => {
      await runtime.commit(
        () => ({ status: "terminal", outcome: { status: "aborted" } }),
        context,
      );
    },
  });
}
export function createCancelTask(coordinator: AgentCoordinator) {
  return defineTask<
    { intentId: string; conversationId: ConversationId },
    { phase: "cancel" },
    null
  >({
    name: "sa.cancel",
    version: 1,
    initial: () => ({ phase: "cancel" }),
    phases: {
      cancel: async (task, runtime, context) => {
        const conversation = await runtime.conversation(
          task.input.conversationId,
          context,
        );
        await conversation?.abort(context, { background: true });
        await runtime.commit(async (tx) => {
          const intent = await coordinator.engine.store.get<
            Record<string, JsonValue>
          >(tx, "cancel", task.input.intentId);
          if (intent)
            await coordinator.engine.store.put(
              tx,
              "cancel",
              task.input.intentId,
              { ...intent, status: "completed" },
            );
          return {
            status: "terminal",
            outcome: { status: "completed", result: null },
          };
        }, context);
      },
    },
    abort: async (_task, runtime, context) => {
      await runtime.commit(
        () => ({ status: "terminal", outcome: { status: "aborted" } }),
        context,
      );
    },
  });
}

export function createRedriveTask(coordinator: AgentCoordinator) {
  return defineTask<
    {
      id: string;
      kind: "deliveryRun" | "routerBatch";
      runId: string;
      conversationId: ConversationId;
      exhausted: boolean;
    },
    { phase: "drive" },
    null
  >({
    name: "sa.redrive",
    version: 1,
    initial: () => ({ phase: "drive" }),
    phases: {
      drive: async (task, runtime, context) => {
        const run = await coordinator.engine.store.read<AgentRun>(
          task.input.kind,
          task.input.runId,
        );
        if (run?.submissionId) {
          const submission = await coordinator.engine.harness.submission(
            run.submissionId,
            context,
          );
          if ((await submission?.status(context))?.status === "queued") {
            if (task.input.exhausted) await submission!.abort(context);
            else {
              const conversation =
                await coordinator.engine.harness.conversation(
                  task.input.conversationId,
                  context,
                );
              await conversation!.submit(
                {
                  type: "write",
                  requestId: task.input.id,
                  entry: { kind: "sa.redrive", data: { runId: run.id } },
                },
                context,
              );
            }
          }
        }
        await runtime.commit(async (tx) => {
          await coordinator.engine.store.put(tx, "redrive", task.input.id, {
            id: task.input.id,
            status: "completed",
            taskId: runtime.taskId,
          });
          return {
            status: "terminal",
            outcome: { status: "completed", result: null },
          };
        }, context);
      },
    },
    abort: async (_task, runtime, context) => {
      await runtime.commit(
        () => ({ status: "terminal", outcome: { status: "aborted" } }),
        context,
      );
    },
  });
}
