# Implementation checkpoints and PR stack

Each row corresponds to one requirement issue and one incremental PR. A PR targets the preceding branch; review and merge from the bottom upward. Approval and merge are separate from implementation status. No auto-merge is enabled.

| Part | Requirement | Issue | PR | Base | Checkpoint status |
| --- | --- | --- | --- | --- | --- |
| 1 | English architecture and executable design checkpoints | [#1](https://github.com/sugarforever/super-durable-agent/issues/1) | [#11](https://github.com/sugarforever/super-durable-agent/pull/11) | `main` | Implemented; user approval pending |
| 2 | Persistent domain kernel and single-writer runtime | [#2](https://github.com/sugarforever/super-durable-agent/issues/2) | [#12](https://github.com/sugarforever/super-durable-agent/pull/12) | `codex/01-design-baseline` | Implemented; user approval pending |
| 3 | Authorization and subscription ownership | [#3](https://github.com/sugarforever/super-durable-agent/issues/3) | Pending | `codex/02-domain-kernel` | Planned |
| 4 | Router and Worker conversations with durable delivery | [#4](https://github.com/sugarforever/super-durable-agent/issues/4) | Pending | `codex/03-policy-ownership` | Planned |
| 5 | Capacity scheduling and cancellation fencing | [#5](https://github.com/sugarforever/super-durable-agent/issues/5) | Pending | `codex/04-agent-conversations` | Planned |
| 6 | Local environments and asynchronous jobs | [#6](https://github.com/sugarforever/super-durable-agent/issues/6) | Pending | `codex/05-scheduling-cancellation` | Planned |
| 7 | Transactional Outbox and external effect recovery | [#7](https://github.com/sugarforever/super-durable-agent/issues/7) | Pending | `codex/06-environments-jobs` | Planned |
| 8 | CLI, GitHub integration and management API | [#8](https://github.com/sugarforever/super-durable-agent/issues/8) | Pending | `codex/07-outbox` | Planned |
| 9 | Remote endpoint daemon and Feishu integration | [#9](https://github.com/sugarforever/super-durable-agent/issues/9) | Pending | `codex/08-github-management` | Planned |
| 10 | Independent Watchdog and operational recovery | [#10](https://github.com/sugarforever/super-durable-agent/issues/10) | Pending | `codex/09-remote-feishu` | Planned |

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
