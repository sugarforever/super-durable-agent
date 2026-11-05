# Implementation checkpoints and PR stack

Each row corresponds to one requirement issue and one incremental PR. A PR targets the preceding branch; review and merge from the bottom upward. Approval and merge are separate from implementation status. No auto-merge is enabled.

| Part | Requirement | Issue | PR | Base | Checkpoint status |
| --- | --- | --- | --- | --- | --- |
| 1 | English architecture and executable design checkpoints | [#1](https://github.com/sugarforever/super-durable-agent/issues/1) | [#11](https://github.com/sugarforever/super-durable-agent/pull/11) | `main` | Implemented; user approval pending |
| 2 | Persistent domain kernel and single-writer runtime | [#2](https://github.com/sugarforever/super-durable-agent/issues/2) | [#12](https://github.com/sugarforever/super-durable-agent/pull/12) | `codex/01-design-baseline` | Implemented; user approval pending |
| 3 | Authorization and subscription ownership | [#3](https://github.com/sugarforever/super-durable-agent/issues/3) | [#13](https://github.com/sugarforever/super-durable-agent/pull/13) | `codex/02-domain-kernel` | Implemented; user approval pending |
| 4 | Router and Worker conversations with durable delivery | [#4](https://github.com/sugarforever/super-durable-agent/issues/4) | [#14](https://github.com/sugarforever/super-durable-agent/pull/14) | `codex/03-policy-ownership` | Implemented; user approval pending |
| 5 | Capacity scheduling and cancellation fencing | [#5](https://github.com/sugarforever/super-durable-agent/issues/5) | [#15](https://github.com/sugarforever/super-durable-agent/pull/15) | `codex/04-agent-conversations` | Implemented; user approval pending |
| 6 | Local environments and asynchronous jobs | [#6](https://github.com/sugarforever/super-durable-agent/issues/6) | [#16](https://github.com/sugarforever/super-durable-agent/pull/16) | `codex/05-scheduling-cancellation` | Implemented; user approval pending |
| 7 | Transactional Outbox and external effect recovery | [#7](https://github.com/sugarforever/super-durable-agent/issues/7) | [#17](https://github.com/sugarforever/super-durable-agent/pull/17) | `codex/06-environments-jobs` | Implemented; user approval pending |
| 8 | CLI, GitHub integration and management API | [#8](https://github.com/sugarforever/super-durable-agent/issues/8) | [#18](https://github.com/sugarforever/super-durable-agent/pull/18) | `codex/07-outbox` | Implemented; user approval pending |
| 9 | Remote endpoint daemon and Feishu integration | [#9](https://github.com/sugarforever/super-durable-agent/issues/9) | [#19](https://github.com/sugarforever/super-durable-agent/pull/19) | `codex/08-github-management` | Implemented; user approval pending |
| 10 | Independent Watchdog and operational recovery | [#10](https://github.com/sugarforever/super-durable-agent/issues/10) | [#20](https://github.com/sugarforever/super-durable-agent/pull/20) | `codex/09-remote-feishu` | Implemented; user approval pending |

## Required evidence per checkpoint

1. Tests for the requirement and relevant failure windows.
2. Full suite, typecheck/build after their introduction, pi contract probes, and documentation checks.
3. Independent review with important findings resolved and a reference to any design changes.
4. Exact tested commit, command outcomes, and unexecuted live trials.
5. User approval remains pending until the user approves the matching PR.

## Baseline evidence

The initial seven pi characterization probes pass on Node.js v26.8.1. They cover atomic rollback, passive-write deduplication, background/ownerless cancellation boundaries, and checkpoint recovery after SIGKILL. They do not exercise model input or establish product readiness. See [evidence](research/evidence.md) and [review corrections](design-review.md).

## Review protocol

Use the PR diff against its immediate base, not main, for incremental review. CI still runs the complete accumulated suite. If a lower PR changes, propagate the correction upward and rerun affected checks before requesting approval. Do not merge, close requirement issues, or enable auto-merge on the user’s behalf. After an approved predecessor merges, retarget/rebase the next PR as necessary and reverify.

## Part 1 evidence

Implementation commit `530303b` establishes the reviewed design baseline. `npm run verify:pi` passed all seven characterization probes; `node scripts/check-docs.mjs` passed six English documents and local links. Independent scoped review passed specification and quality checks with no important findings. This checkpoint records documentation/probe readiness, not runtime implementation. PR #11 remains unmerged and requires user approval.

## Part 2 evidence

Implementation commit `bbb2542`. 19 integration tests pass, including real SQLite reopen, atomic rollback/cursor admission, payload conflicts, CLI subprocesses, second-writer refusal, and SIGKILL lock/checkpoint recovery. Typecheck, build, and seven pi probes pass. Local validation used Node26/macOS; the native addon requires install scripts and a compiler. Routing and models are introduced in later PRs. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Design invariant traceability

| ID | Invariant | Owning checkpoint and regression area |
| --- | --- | --- |
| D1 | One writer; domain state, index, and Task intent commit together | Part 2: lock, rollback, cursor and reopen tests |
| D2 | Ticket outlives execution; Worker does not inherit Router cancellation | Part 4: ownerless generation and input delivery |
| D3 | Input text cannot grant authority; proactive acceptance stays within Policy/Mandate | Part 3: admission matrix, capability ceilings and stale Policy |
| D4 | Own feedback, context-only input, bot events, and multi-owner handback differ | Part 3: subscription and ownership scenarios |
| D5 | Reused replay key must bind the same canonical payload | Parts 2, 4, 6, 7: command, delivery, Job and Outbox conflicts |
| D6 | Stop fences progress and queued sends; old receipts reconcile accounting only | Parts 5–7: cancellation and stale-effect races |
| D7 | Capacity counts preparation/generation/Jobs and retains human intervention | Parts 5–6: reservations, restart and human wakeups |
| D8 | Long external work does not block the Worker's input loop | Part 6: Job start/feedback/complete sequence |
| D9 | Unknown non-idempotent effects are not blindly retried | Parts 6–9: command/send/reconnect receipt windows |
| D10 | Incompatible state fails closed before effects; restore validates integrity | Parts 2, 4, 10: preflight, migration and offline restore |
| D11 | External identity is verified and cannot be forged in body text | Parts 8–9: GitHub, API, daemon and Feishu authentication |
| D12 | Supervision is independent, budgeted and quiet when unchanged | Part 10: wait classifications, budget and notification deduplication |

## Part 3 evidence

Implementation commit `242759c`. 57 real SQLite tests pass (19 kernel, 17 policy, 21 ownership), plus typecheck/build, seven pi probes, and English-document checks. Scoped review and one correction round resolved all four important findings: per-Thread Eval serialization, Policy reevaluation, unread-owner release, and same-Ticket resumption. No model or external sender execution is claimed by this part. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 4 evidence

Implementation commit `f61dacd`. 92 tests pass across five files, including 34 native conversation cases and interrupted/concurrent artifact publication. Typecheck/build, seven pi probes, and seven English-document checks pass. One scoped correction round resolved all six findings: retry claim fencing, ownership transition delivery, trusted authority discovery, atomic artifacts, upgrade replay, and terminal auxiliary recovery. CI passes Node22.19 and24. No real provider or external-account trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 5 evidence

Implementation commit `996c7c9`. 111 tests across seven files pass, plus typecheck/build, seven pi probes and English-document checks. Scoped correction review resolved cancellation monotonicity, endpoint-only retry fencing, and pre-admission reservation recovery, with native barrier and SIGKILL regressions. No real broker, provider, or external-account trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 6 evidence

Implementation commit `98472cc`. 135 tests across eight files pass, plus typecheck/build, seven pi probes and English-document checks. Scoped correction review cleared process-group cancellation supervision, bounded unknown outcomes with inherited pipes, and canonical repository checks for retained worktree replay. Real git/subprocess/native crash tests cover the local broker. No real remote device, provider, or account trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 7 evidence

Implementation commit `48aaeeb`. 159 tests across nine files pass, plus typecheck/build, seven pi probes and English-document checks. Independent scoped review cleared the overdue retry/disabled-source wakeup fix; native tests verify automatic progress without manual reconciliation. Controlled HTTP/SIGKILL recovery observes one POST. No live provider/account trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 8 evidence

Implementation commit `7475ff8`. 198 tests across ten files pass, plus typecheck/build, seven pi probes and eight English-document checks. Independent scoped correction review cleared all five findings: final answer/lifecycle races, unsigned event-family substitution, copied markers, CI human classification, and recent receipt lookup. Native tests cover early echo ordering and model-free receipt/startup completion. No live account or hosted-model trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 9 evidence

Implementation commit `775da5b`. 237 tests across thirteen files pass, plus typecheck/build, seven pi probes and nine English-document checks. Scoped correction review resolved operation-only cancellation versus Ticket revocation and concurrent Feishu disable acknowledgment; deterministic CI barriers retain recovery assertions. The separately identified queued-operation cancellation gap was addressed and verified in [Part 10 / PR #20](https://github.com/sugarforever/super-durable-agent/pull/20). No live device/account/model trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.

## Part 10 evidence

Implementation commit `c46afb6`. 276 tests across fifteen files pass, plus typecheck/build, seven pi probes and ten English-document checks. Independent scoped and whole-stack reviews resolved all five final findings; three independent final-head probes pass. Controlled native E2E covers two Workers, queued/long Job cancellation, restart, Reports/Outbox, PR closure and preserved Git changes across resume/retry. Backups require original local endpoint dependencies; remote/custom restored clones remain inspection-only. No live provider/account/device/TLS trial was executed. Independent scoped specification/quality review completed with important findings resolved. User approval and merge remain pending.
