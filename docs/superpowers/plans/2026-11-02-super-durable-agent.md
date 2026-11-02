# Super Durable Agent Implementation Plan

> Execute one numbered requirement per PR. The architecture is binding; each issue supplies acceptance criteria. Use test-first implementation, a scoped independent review, and the checkpoints below. Do not merge PRs automatically.

**Goal:** Implement the persistent Super-agent described by [the architecture](../../architecture.md), with one issue and one stacked PR per requirement.

**Architecture:** Domain state and execution intents share a Harness commit. A long-lived Router coordinates independent ownerless Workers. Background Tasks handle delivery and external effects; Ticket identity outlives Task execution.

**Stack:** Node.js >=22.19.0, TypeScript, pi-durable/pi-ai/chord 1.1.0, SQLite, Vitest, TypeBox.

## Global constraints

- One active writer per trust-boundary Session; no external IO in commit callbacks.
- Use public pi APIs and explicit role extension/tool allowlists.
- Ticket, Assignment, Conversation, and Task have distinct identities and lifetimes.
- Bind replay keys to canonical payloads; unknown external effects remain visible.
- Revocation fences queued and future effects; late receipts may reconcile accounting only.
- No unrestricted-shell sandbox claims; local shell is trusted and explicitly enabled.
- All documentation, issue bodies, PR descriptions, and checkpoint evidence are in English.
- Every PR targets its predecessor; human approval is required for merging.

## Review focus

Protect multi-owner handback, admission-before-receipt crashes, stale reports and queued sends after stop, unknown provider effects, migration failures, and false stall detection while awaiting people/endpoints. Each owning task adds behavioral regression tests. See [review corrections](../../design-review.md).

## Task 1: English architecture and executable design checkpoints

**Issue:** [#1](https://github.com/sugarforever/super-durable-agent/issues/1). **Branch:** `codex/01-design-baseline`. **Base:** `main`.

**Files:** `docs/architecture.md`, `docs/research/evidence.md`, `docs/design-review.md`, `docs/checkpoints.md`, `scripts/check-docs.mjs`, `.github/workflows/checks.yml`.

**Outcome:** English-only architecture, evidence and implementation plan; resolve the independent review findings; exact pi 1.1.0 dependency pins; CI and explicit human approval gates.

**Interfaces:** Produces the English architecture, traceability, and CI baseline.

**Acceptance:** Documentation links and language audit; pi contract probes; review traceability with no unsupported production claims.

- [x] Characterize the pinned public pi APIs and audit the English design baseline; no runtime feature readiness is inferred.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 2: Persistent domain kernel and single-writer runtime

**Issue:** [#2](https://github.com/sugarforever/super-durable-agent/issues/2). **Branch:** `codex/02-domain-kernel`. **Base:** `codex/01-design-baseline`.

**Files:** `src/domain/model.ts`, `src/domain/store.ts`, `src/domain/engine.ts`, `src/runtime/open.ts`, `src/cli.ts`, `tests/kernel.spec.ts`.

**Outcome:** Typed Session-scoped documents, authenticated command ledger with payload binding, event identity, atomic intake, bounded indexes, durable task intents, singleton lock, minimal CLI, SQLite reopen and schema/version preflight.

**Interfaces:** Produces openSuperAgent, SuperAgentRuntime, Engine transaction/command/store, receive/receiveBatch, read/list, close, and the minimal CLI.

**Acceptance:** Duplicate versus edited event; conflicting idempotency keys rejected; rollback of documents and tasks; no second writer; CLI round trip and SQLite reopen.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 3: Authorization and subscription ownership

**Issue:** [#3](https://github.com/sugarforever/super-durable-agent/issues/3). **Branch:** `codex/03-policy-ownership`. **Base:** `codex/02-domain-kernel`.

**Files:** `src/domain/policy.ts`, `src/domain/intake.ts`, `tests/policy.spec.ts`, `tests/ownership.spec.ts`.

**Outcome:** Policy off/observe/auto, explicit versus inferred admission, Mandates and capability upper bounds, accept/attach/ignore/stop, own/context-only subscriptions, multi-owner handback and policy epochs.

**Interfaces:** Consumes the public interfaces introduced by parts 2–2 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Authorization matrix; no new Eval for owned human feedback; bot evaluation independent of fanout; last-owner handback; revocation and stale-decision rejection.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 4: Router and Worker conversations with durable delivery

**Issue:** [#4](https://github.com/sugarforever/super-durable-agent/issues/4). **Branch:** `codex/04-agent-conversations`. **Base:** `codex/03-policy-ownership`.

**Files:** `src/agents/router.ts`, `src/agents/worker.ts`, `src/durable/tasks/delivery.ts`, `tests/conversations.spec.ts`.

**Outcome:** Explicit role extension/tool allowlists, one long-lived Router, bounded batching and recall, ownerless Workers, transactional configure, payload-bound Delivery requestIds, busy steer/followUp semantics, structured reports and failure recovery.

**Interfaces:** Consumes the public interfaces introduced by parts 2–3 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Controlled provider runs real pi generation; isolated Worker history; Router cancellation independence; crash after input admission; placed versus application-consumed evidence; no implicit ready.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 5: Capacity scheduling and cancellation fencing

**Issue:** [#5](https://github.com/sugarforever/super-durable-agent/issues/5). **Branch:** `codex/05-scheduling-cancellation`. **Base:** `codex/04-agent-conversations`.

**Files:** `src/runtime/scheduler.ts`, `src/runtime/reconcile.ts`, `tests/scheduling.spec.ts`.

**Outcome:** Persistent three-normal-plus-one-human reservations, preparation accounting, aging, Drive, precise stopping and cancel intents, startup reconciliation and assignmentEpoch fencing.

**Interfaces:** Consumes the public interfaces introduced by parts 2–4 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** No oversubscription after restart; ordinary work cannot consume human reserve; no duplicate capacity for steer; old report cannot revive Ticket; cancellation receipts reconcile accounting without new progress.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 6: Local environments and asynchronous jobs

**Issue:** [#6](https://github.com/sugarforever/super-durable-agent/issues/6). **Branch:** `codex/06-environments-jobs`. **Base:** `codex/05-scheduling-cancellation`.

**Files:** `src/environments/local.ts`, `src/services/jobs.ts`, `tests/jobs.spec.ts`.

**Outcome:** EnvironmentBroker, idempotent worktree preparation, durable operation ledger, start/get/stop Job tools, completion triggers and command boundary enforcement.

**Interfaces:** Consumes the public interfaces introduced by parts 2–5 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Real temporary repository and subprocess execution; payload-bound operation IDs; long job does not block feedback; cancellation and unknown-outcome recovery; explicit trusted-local-shell boundary.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 7: Transactional Outbox and external effect recovery

**Issue:** [#7](https://github.com/sugarforever/super-durable-agent/issues/7). **Branch:** `codex/07-outbox`. **Base:** `codex/06-environments-jobs`.

**Files:** `src/services/outbox.ts`, `tests/outbox.spec.ts`.

**Outcome:** Fenced outbound messages with Ticket, Mandate, epoch and payload digest; receipt lookup, bounded exponential retry, source availability/listening policy and uncertain outcomes.

**Interfaces:** Consumes the public interfaces introduced by parts 2–6 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Stop between enqueue and send prevents send; old messages remain fenced after reopen; found/absent/unknown receipt cases; 2s-to-60s backoff and eight-failure attention.

- [x] Add behavioral tests first and confirm the intended missing behavior fails.
- [x] Implement the scoped feature and required integration through existing public boundaries.
- [x] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [x] Obtain an independent scoped review; fix important findings and record limitations.
- [x] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 8: CLI, GitHub integration and management API

**Issue:** [#8](https://github.com/sugarforever/super-durable-agent/issues/8). **Branch:** `codex/08-github-management`. **Base:** `codex/07-outbox`.

**Files:** `src/sources/github.ts`, `src/api/server.ts`, `src/cli.ts`, `tests/integrations.spec.ts`.

**Outcome:** Runnable service/CLI, authenticated management reads and controls, verified GitHub webhooks, polling cursors, reply-loop suppression and ready-versus-closed Ticket lifecycle.

**Interfaces:** Consumes the public interfaces introduced by parts 2–7 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Webhook signature failure never admits events; cursor commits atomically; API actor cannot be forged in body; ready PR stays open; merge with unread followup does not end work.

- [ ] Add behavioral tests first and confirm the intended missing behavior fails.
- [ ] Implement the scoped feature and required integration through existing public boundaries.
- [ ] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [ ] Obtain an independent scoped review; fix important findings and record limitations.
- [ ] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 9: Remote endpoint daemon and Feishu integration

**Issue:** [#9](https://github.com/sugarforever/super-durable-agent/issues/9). **Branch:** `codex/09-remote-feishu`. **Base:** `codex/08-github-management`.

**Files:** `src/environments/remote.ts`, `src/daemon/server.ts`, `src/sources/feishu.ts`, `tests/remote.spec.ts`.

**Outcome:** Authenticated outbound endpoint protocol, minimal execution receipt ledger, capability/epoch fencing, reconnect query rather than blind restart; authenticated Feishu intake and outbound adapter.

**Interfaces:** Consumes the public interfaces introduced by parts 2–8 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Offline/reconnect retains operation identity; duplicate remote execution rejected; endpoint revocation rejects new jobs; webhook/event authentication; Feishu receipt uncertainty visible.

- [ ] Add behavioral tests first and confirm the intended missing behavior fails.
- [ ] Implement the scoped feature and required integration through existing public boundaries.
- [ ] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [ ] Obtain an independent scoped review; fix important findings and record limitations.
- [ ] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Task 10: Independent Watchdog and operational recovery

**Issue:** [#10](https://github.com/sugarforever/super-durable-agent/issues/10). **Branch:** `codex/10-watchdog-operations`. **Base:** `codex/09-remote-feishu`.

**Files:** `src/watchdog/index.ts`, `docs/operations.md`, `tests/watchdog.spec.ts`, `tests/recovery.spec.ts`.

**Outcome:** Independent supervisor storage and budget, health and semantic-progress classification, quiet unchanged monitoring, constrained actions, backup/restore and migration procedures; full-stack acceptance.

**Interfaces:** Consumes the public interfaces introduced by parts 2–9 and extends the same Engine/Harness; exact exports and integration examples must be recorded with this PR. No parallel store or scheduler is permitted.

**Acceptance:** Waiting for people/endpoints/long jobs is not a stall; repeated no-progress becomes a candidate; no repeated unchanged notification; fail-closed migrations and restore; final offline E2E and documented live-account limitations.

- [ ] Add behavioral tests first and confirm the intended missing behavior fails.
- [ ] Implement the scoped feature and required integration through existing public boundaries.
- [ ] Run targeted tests, the full suite, typecheck/build where introduced, documentation checks, and pi probes.
- [ ] Obtain an independent scoped review; fix important findings and record limitations.
- [ ] Commit/push only this requirement on its branch; open its linked incremental PR; update checkpoint evidence.

## Completion rules

`Implemented` means code and its automated checkpoint pass, not that live-account effectiveness is proven. Controlled-provider tests are labeled as such. Real-model/live GitHub/Feishu/BYOM trials require configured isolated credentials/endpoints; report any unexecuted live trials separately. Never substitute planned test names for executed evidence.

The durable execution ledger under `.superpowers/` tracks active agents, commits, reviews, and rulings across chat compaction. Public implementation/approval status lives in [checkpoints](../../checkpoints.md).
