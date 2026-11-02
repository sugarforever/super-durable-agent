# Implementation checkpoints and PR stack

Each row corresponds to one requirement issue and one incremental PR. A PR targets the preceding branch; review and merge from the bottom upward. Approval and merge are separate from implementation status. No auto-merge is enabled.

| Part | Requirement | Issue | PR | Base | Checkpoint status |
| --- | --- | --- | --- | --- | --- |
| 1 | English architecture and executable design checkpoints | [#1](https://github.com/sugarforever/super-durable-agent/issues/1) | Pending | `main` | Planned |
| 2 | Persistent domain kernel and single-writer runtime | [#2](https://github.com/sugarforever/super-durable-agent/issues/2) | Pending | `codex/01-design-baseline` | Planned |
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
