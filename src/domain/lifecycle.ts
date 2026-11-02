import type { Tx } from "@earendil-works/pi-durable";
import type { Engine } from "./engine.js";
import type { AgentRun } from "../durable/tasks/agent-input.js";
import {
  scopedId,
  type Assignment,
  type CommandMeta,
  type DeliveryIntent,
  type Disposition,
  type Mandate,
  type Source,
  type Ticket,
} from "./model.js";
import { nativeTasks } from "../runtime/reconcile.js";
export interface TrackedThread {
  id: string;
  ticketId: string;
  sourceId: string;
  threadRef: string;
}
export interface SourceThreadState {
  sourceId: string;
  threadRef: string;
  state: "open" | "closed";
  occurredAt: number;
  revision: string;
}
/** Source facts join intake commits; completion rechecks settled state in an Engine transaction. */
export class TicketLifecycle {
  constructor(private readonly engine: Engine) {}
  async trackThread(
    ticketId: string,
    sourceId: string,
    threadRef: string,
    meta: CommandMeta,
  ) {
    await this.engine.command(
      meta.commandId,
      {
        operation: "trackThread",
        ticketId,
        sourceId,
        threadRef,
        meta: { ...meta },
      },
      async (tx) => {
        const { store } = this.engine;
        const ticket = await store.get<Ticket>(tx, "ticket", ticketId);
        if (!ticket || ticket.lifecycle !== "open")
          throw new Error("Tracking requires an open Ticket");
        if (
          meta.expectedRevision !== undefined &&
          meta.expectedRevision !== ticket.revision
        )
          throw new Error("Ticket revision conflict");
        const mandate = await store.get<Mandate>(
          tx,
          "mandate",
          ticket.mandateId,
        );
        const source = await store.get<Source>(tx, "source", sourceId);
        if (
          !threadRef.trim() ||
          !mandate ||
          mandate.withdrawnAt !== null ||
          mandate.sourceId !== sourceId ||
          !source?.enabled ||
          (source.listenedScopes && !source.listenedScopes.includes(threadRef))
        )
          throw new Error("Tracked Thread source denied");
        if (source.kind === "github") {
          const repository = /^([\w.-]+\/[\w.-]+)#[1-9]\d*$/
            .exec(threadRef)?.[1]
            ?.toLowerCase();
          if (!repository || !source.repositoryAllowlist?.includes(repository))
            throw new Error("Tracked GitHub repository is not allowed");
        }
        const policy = await this.engine.authorization.policyInTransaction(tx);
        const allowed = await this.engine.authorization.revalidateMandate(
          tx,
          mandate,
          {
            id: "binding",
            sourceId,
            externalId: "binding",
            revision: "1",
            actor: mandate.actor,
            threads: mandate.scope,
            body: "",
            receivedAt: 0,
            occurredAt: 0,
            ordinal: 0,
            digest: "",
          },
          policy,
        );
        if (!allowed) throw new Error("Tracked Thread authority revoked");
        const capabilities =
          mandate.basis === "explicit"
            ? (policy.explicitCapabilities ?? ["repo.read", "channel.reply"])
            : policy.rules
                .filter((r) => r.enabled && mandate.ruleIds.includes(r.id))
                .flatMap((r) => r.capabilities);
        if (!mandate.scope.includes(threadRef)) {
          if (
            !mandate.capabilities.includes("pr.create") ||
            !capabilities.includes("pr.create")
          )
            throw new Error(
              "Created PR binding requires current pr.create capability",
            );
          const authority =
            mandate.basis === "explicit"
              ? mandate.actor.kind === "human" &&
                mandate.actor.trusted &&
                policy.allowedActors.includes(mandate.actor.id)
              : policy.acceptMode === "auto" &&
                mandate.ruleIds.every((id) =>
                  policy.rules.some((r) => r.id === id && r.enabled),
                );
          if (!authority) throw new Error("Created PR authority revoked");
          await store.put(tx, "mandate", mandate.id, {
            ...mandate,
            scope: [...mandate.scope, threadRef],
          });
        }
        await this.engine.authorization.subscribeInTransaction(
          tx,
          ticketId,
          sourceId,
          threadRef,
          true,
          meta.expectedRevision,
        );
        const id = scopedId(this.engine.instanceId, "trackedThread", [
          ticketId,
          sourceId,
          threadRef,
        ]);
        await store.put(tx, "trackedThread", id, {
          id,
          ticketId,
          sourceId,
          threadRef,
        } satisfies TrackedThread);
      },
    );
  }
  async recordFact(tx: Tx, fact: SourceThreadState) {
    const key = `${fact.sourceId}:${fact.threadRef}`;
    const previous = await this.engine.store.get<SourceThreadState>(
      tx,
      "sourceThreadState",
      key,
    );
    if (
      previous &&
      (previous.occurredAt > fact.occurredAt ||
        (previous.occurredAt === fact.occurredAt &&
          previous.revision !== fact.revision &&
          fact.state === "closed"))
    )
      return;
    await this.engine.store.put(tx, "sourceThreadState", key, fact);
  }
  async completeInTransaction(tx: Tx) {
    const { store } = this.engine;
    const tracked = await store.listInTransaction<TrackedThread>(
      tx,
      "trackedThread",
    );
    if (!tracked.length) return;
    const runs = await store.listInTransaction<AgentRun>(tx, "deliveryRun");
    const deliveries = await store.listInTransaction<DeliveryIntent>(
      tx,
      "delivery",
    );
    const dispositions = await store.listInTransaction<Disposition>(
      tx,
      "disposition",
    );
    const notes = await store.listInTransaction<{
      id: string;
      ticketId: string;
      status: string;
    }>(tx, "workerNote");
    const jobs = await store.listInTransaction<{
      ticketId: string;
      status: string;
    }>(tx, "job");
    const tasks = await nativeTasks(tx);
    for (const id of new Set(tracked.map((t) => t.ticketId))) {
      const ticket = await store.get<Ticket>(tx, "ticket", id),
        report = ticket?.latestReport as any;
      if (
        !ticket ||
        ticket.lifecycle !== "open" ||
        ticket.cancelPending ||
        ticket.attention ||
        report?.status !== "ready" ||
        report.epoch !== ticket.assignmentEpoch ||
        report.assignmentId !== ticket.assignmentId
      )
        continue;
      const required = tracked.filter((t) => t.ticketId === id);
      let closed = true;
      for (const thread of required) {
        const fact = await store.get<SourceThreadState>(
          tx,
          "sourceThreadState",
          `${thread.sourceId}:${thread.threadRef}`,
        );
        if (fact?.state !== "closed") {
          closed = false;
          break;
        }
      }
      if (!closed) continue;
      if (
        dispositions.some(
          (d) =>
            d.ownerTickets.includes(id) &&
            !d.consumedTicketIds?.includes(id) &&
            !d.handbacks.some((h) => h.ticketId === id && h.epoch === d.epoch),
        )
      )
        continue;
      if (
        deliveries.some(
          (d) =>
            d.ticketId === id &&
            d.epoch === ticket.assignmentEpoch &&
            d.status === "pending",
        )
      )
        continue;
      if (
        runs.some(
          (r) =>
            r.ticketId === id &&
            r.epoch === ticket.assignmentEpoch &&
            (!r.settled || !r.consumed) &&
            r.status !== "invalid",
        )
      )
        continue;
      if (
        notes.some(
          (n) =>
            n.ticketId === id &&
            n.status === "pending" &&
            !runs.some((r) => r.id === n.id && r.consumed),
        )
      )
        continue;
      if (
        jobs.some(
          (j) =>
            j.ticketId === id &&
            !["completed", "failed", "cancelled"].includes(j.status),
        )
      )
        continue;
      if (tasks.some((t) => t.conversationId === ticket.workerConversationId))
        continue;
      await store.put(tx, "ticket", id, {
        ...ticket,
        lifecycle: "ended",
        revision: ticket.revision + 1,
      });
      const assignment = await store.get<Assignment>(
        tx,
        "assignment",
        ticket.assignmentId,
      );
      if (assignment)
        await store.put(tx, "assignment", assignment.id, {
          ...assignment,
          status: "ended",
        });
    }
  }
}
