# Architecture review

Reviewed 2026-11-02 against the original design, the pinned published prototype, pi durable 1.1.0 public declarations/implementation, and the initial seven probes. This review separates specification corrections from implementation evidence.

| Finding | Resolution in the English architecture | Implementation gate |
| --- | --- | --- |
| Queued replies could escape Ticket stop | Outbox binds Ticket/Mandate/epoch; stop invalidates unsent entries; sender revalidates immediately before send; in-flight results remain auditable | PR7 stale queued reply after stop and reopen; in-flight uncertain receipt |
| Blanket rejection of old Job writes prevented cancellation accounting | Reject stale progress but permit receipt-only reconciliation without resuming work or generating feedback | PR5/6 late cancellation/completion races |
| Shell restrictions implied a sandbox that a worktree cannot provide | Trusted-local-shell is explicit and opt-in; structured path guards do not confine subprocesses; requested unavailable sandbox fails closed | PR6 path/symlink tests and default untrusted-shell refusal |
| Earlier plan required a CLI before introducing it | Minimal CLI is part of the kernel; every PR states an executable local checkpoint | PR2 CLI receive/show/reopen and every later checkpoint |
| Stable keys did not bind payloads | Canonical digest ledger for commands, deliveries, operations, and sends; conflicting reuse rejects | PR2 command conflict; PR4/6/7 effect conflicts |
| Recovery claims lacked incompatible-version gates | Preflight before scheduling, public transactional configure, missing-definition diagnostics and migration/restore procedures | PR2 startup hook; PR10 migration and restore acceptance |
| Passive-write probes were presented too close to input semantics | Evidence explicitly limits the seven probes; real controlled-provider tests precede Worker input claims; consumed is application evidence | PR4 busy input, generation cancellation, admission crash and report consumption |

The design preserves the original two-layer task organization and event ownership while adapting execution to pi's ownership/cancellation/commit mechanisms. It adds explicit payload identity, sender fencing, and execution-accounting rules needed for this implementation. These are project decisions, not claims that the original prototype already implements the same interfaces.

Before each PR opens for approval, its checkpoint must record executable evidence, a scoped specification/code review, resolved important findings, and known limitations. At stack completion, run a whole-stack review and recovery scenario. Approvals and merges remain with the user.

## Corrections verified during implementation

| Checkpoint | Review correction | Regression evidence |
| --- | --- | --- |
| 3 | Serialize overlapping Thread evaluations; requeue Policy-invalidated inputs; preserve unread owners on termination; revalidate same-Ticket resumption | [Policy tests](../tests/policy.spec.ts), [ownership tests](../tests/ownership.spec.ts) |
| 4 | Fence native task attempts and settlement; represent frozen context-to-owner promotion as a distinct delivery; provide authoritative Policy/Mandate discovery | [Conversation tests](../tests/conversations.spec.ts) |
| 4 | Publish complete artifacts atomically and deduplicate the logical source payload across spill upgrades | [Artifact tests](../tests/artifacts.spec.ts), [conversation tests](../tests/conversations.spec.ts) |
| 4 | Recover terminal auxiliary claims with bounded attempts and explicit attention | [Conversation tests](../tests/conversations.spec.ts) |
| 5 | Make cancellation confirmation monotonic, skip confirmed native abort on endpoint retries, and reconstruct pre-admission Control work before resume | [Cancellation tests](../tests/cancellation.spec.ts), [scheduling tests](../tests/scheduling.spec.ts) |
| 6 | Supervise the observed process group beyond shell closure, bound unknown outcomes despite inherited pipes, and bind retained worktree replay to canonical repository identity | [Job tests](../tests/jobs.spec.ts) |
| 7 | Wake overdue retry/disabled-source intents after terminal completion without spinning on live claims | [Outbox tests](../tests/outbox.spec.ts) |
| 8 | Validate signed-body event families; preserve copied-marker human input; classify CI as automation; recover recent receipts; wait for final outbound obligations before natural completion | [Integration tests](../tests/integrations.spec.ts) |
| 9 | Separate operation-only cancellation from Ticket-wide epoch revocation; acknowledge Feishu intake using its durable acceptance result despite concurrent source disable | [Remote tests](../tests/remote.spec.ts), [Feishu tests](../tests/feishu.spec.ts) |

These corrections retain the original organization while making its state transitions explicit at pi's transactional and invocation boundaries. Controlled-provider tests exercise native execution; they do not establish real-model quality or live external-service behavior.
