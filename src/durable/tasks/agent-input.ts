import {
  defineTask,
  type ConversationId,
  type SubmissionId,
  type TaskId,
} from "@earendil-works/pi-durable";
import type { JsonValue } from "@earendil-works/chord";
import type { AgentCoordinator } from "../../agents/coordinator.js";
export interface InputClaim {
  taskId: TaskId<null>;
  attempt: number;
  requestId: string;
  submissionId?: SubmissionId;
}
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
  own?: boolean;
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
        const claim: InputClaim | undefined = run
          ? {
              taskId: runtime.taskId,
              attempt: run.attempt,
              requestId: run.requestId,
            }
          : undefined;
        if (run && !coordinator.matchesClaim(run, claim!)) {
          await runtime.commit(
            () => ({
              status: "terminal",
              outcome: { status: "completed", result: null },
            }),
            context,
          );
          return;
        }
        if (
          !run ||
          run.status === "invalid" ||
          !(await coordinator.isCurrent(run))
        ) {
          await runtime.commit(async (tx) => {
            const current = run
              ? await coordinator.engine.store.get<AgentRun>(
                  tx,
                  task.input.kind,
                  run.id,
                )
              : undefined;
            if (current && coordinator.matchesClaim(current, claim!))
              await coordinator.engine.store.put(tx, task.input.kind, run!.id, {
                ...current,
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
          if (!coordinator.matchesClaim(current, claim!))
            return {
              status: "terminal",
              outcome: { status: "completed", result: null },
            };
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
        const claim: InputClaim = {
          taskId: runtime.taskId,
          attempt: run.attempt,
          requestId: run.requestId,
          submissionId: run.submissionId,
        };
        if (!coordinator.matchesClaim(run, claim)) {
          await runtime.commit(
            () => ({
              status: "terminal",
              outcome: { status: "completed", result: null },
            }),
            context,
          );
          return;
        }
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
            claim,
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
    { intentId: string; conversationId?: ConversationId },
    { phase: "cancel" },
    null
  >({
    name: "sa.cancel",
    version: 1,
    initial: () => ({ phase: "cancel" }),
    phases: {
      cancel: async (task, runtime, context) => {
        const claim = await coordinator.engine.store.read<import("../../domain/model.js").CancelIntent>("cancel", task.input.intentId);
        if (
          !claim ||
          claim.taskId !== runtime.taskId ||
          claim.status !== "pending"
        ) {
          await runtime.commit(
            () => ({
              status: "terminal",
              outcome: { status: "completed", result: null },
            }),
            context,
          );
          return;
        }
        const receipt = await coordinator.cancellation.cancel(claim, task.input.conversationId, context, runtime);
        await runtime.commit(async (tx) => {
          await coordinator.cancellation.receipt(tx, task.input.intentId, receipt, runtime.taskId);
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

export interface RedriveInput {
  id: string;
  kind: "deliveryRun" | "routerBatch";
  runId: string;
  conversationId: ConversationId;
  exhausted: boolean;
  requestId?: string;
  submissionId?: SubmissionId;
}
export interface RedriveClaim {
  id: string;
  status: "pending" | "completed" | "attention";
  taskId?: TaskId<null>;
  attempts?: number;
  input?: RedriveInput;
}
export function createRedriveTask(coordinator: AgentCoordinator) {
  const drive = async (
    task: { input: RedriveInput },
    runtime: import("@earendil-works/pi-durable").TaskRuntime<
      RedriveInput,
      { phase: "drive" } | { phase: "withdraw" },
      null,
      object
    >,
    context: import("@earendil-works/chord").Context,
  ) => {
    const claim = await coordinator.engine.store.read<RedriveClaim>(
      "redrive",
      task.input.id,
    );
    if (!claim || claim.taskId !== runtime.taskId) {
      await runtime.commit(
        () => ({
          status: "terminal",
          outcome: { status: "completed", result: null },
        }),
        context,
      );
      return;
    }
    const run = await coordinator.engine.store.read<AgentRun>(
      task.input.kind,
      task.input.runId,
    );
    if (
      run?.submissionId &&
      (!task.input.requestId || task.input.requestId === run.requestId) &&
      (!task.input.submissionId || task.input.submissionId === run.submissionId)
    ) {
      const submission = await coordinator.engine.harness.submission(
        run.submissionId,
        context,
      );
      if ((await submission?.status(context))?.status === "queued") {
        if (task.input.exhausted || !(await coordinator.isCurrent(run))) await submission!.abort(context);
        else {
          const conversation = await coordinator.engine.harness.conversation(
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
      const current = await coordinator.engine.store.get<RedriveClaim>(
        tx,
        "redrive",
        task.input.id,
      );
      if (current?.taskId === runtime.taskId)
        await coordinator.engine.store.put(tx, "redrive", task.input.id, {
          ...current,
          status: current.status === "attention" ? "attention" : "completed",
        });
      return {
        status: "terminal",
        outcome: { status: "completed", result: null },
      };
    }, context);
    coordinator.kick();
  };
  return defineTask<
    RedriveInput,
    { phase: "drive" } | { phase: "withdraw" },
    null
  >({
    name: "sa.redrive",
    version: 1,
    initial: (input) => ({ phase: input.exhausted ? "withdraw" : "drive" }),
    phases: { drive, withdraw: drive },
    abort: async (_task, runtime, context) => {
      await runtime.commit(
        () => ({ status: "terminal", outcome: { status: "aborted" } }),
        context,
      );
    },
  });
}
