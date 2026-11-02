# Runtime API

The [checkpoint table](checkpoints.md) identifies which features exist in each PR. Compile with `npm run build`; the runtime entry point is `dist/runtime/open.js`. All state changes use the same Engine/Harness.

## Open, inspect, and close

`openSuperAgent({ directory, models?, model?, instanceId?, maxPending?, maxIndexEntries?, clock?, cancellationHandler?, environment?, outbox?, preflight? })` acquires the local single-writer lock, opens SQLite, validates recovery metadata, and returns the runtime. Default bounds are 10,000 pending inputs and 10,000 entries per record-kind index. These are bounded first-release indexes, not unlimited historical retention.

The runtime exposes `engine`, `harness`, `control`, `artifactRoot`, `receive`, `receiveBatch`, `readTicket`, `list`, and `close`. The domain facade also exposes Policy, decision, subscription, handback, stop, and end operations. Use `close()` in a finally block.

Without `model`, domain operations need no provider credentials. If existing agent work needs a provider, a model-free reopen leaves scheduling paused. This is an inspection/recovery mode, not a guarantee against a trusted host explicitly calling low-level Harness methods that enable progress.

With `models` and a `model: { provider, modelId }`, `runtime.agents` is an AgentCoordinator. Register the intended provider in the pi-ai Models collection before opening. Models and credentials are host configuration, not message content.

## Domain commands

Normalized events contain sourceId, externalId, revision, actor, threads, body, receivedAt, occurredAt, and optional addressed. Event identity binds meaningful content, ignoring transport receipt time. Reusing an external key for changed content fails; edits use a new revision.

Mutations take a `CommandMeta` with a stable commandId and optional expectedRevision. The key binds the canonical command payload. A different payload using the same key fails. A stop can include triggerId to identify the authenticated stop message so it is not accidentally treated as fresh work.

The public Engine is a trusted application boundary. Network adapters must derive Actor identity from authentication. Model tools use the stored conversation/Ticket binding and never accept a caller-created operator Actor.

Policy contains version, acceptMode, allowedActors, botActors, rules, and optional explicitCapabilities. Explicit requests default to `repo.read` and `channel.reply`; other capabilities require trusted configuration. Inferred acceptance requires enabled matching rules. Tool arguments cannot increase the grant.

## Coordinator and delivery

- `pump()` durably claims pending work and creates finite background Tasks; it returns before model completion.
- `deliverPending()` invokes the same pump and does not promise input placement.
- `waitForIdle()` explicitly joins background input/redrive/cancellation work and ordinary generation, including newly promoted work.
- `ensureWorker(ticketId)` atomically creates/configures/binds one ownerless Worker under a current reservation, or returns the existing numeric ConversationId. Pump obtains the reservation first.
- `submitDelivery(id, exactPrompt?)` is a trusted-host replay helper for an already-admitted submission; a changed prompt or digest conflicts. Use pump for first admission so reservations and cancellation fences apply.
- `noteWorker(routerConversationId, ticketId, text, id, mode?)` creates factual context without authority. Normal notes use followUp; trusted host corrections may select steer.
- `decideBatch(routerConversationId, decisions, callId?)` returns independent success/error outcomes for each item.

Native admission, placement, application consumption, and settlement are distinct. Delivery records freeze the exact prompt, digest, requestId, mode, and input ID before submit. After a crash at admission, repeat the same requestId. Intentional bounded retries after terminal failure have separate persisted attempt identities. A durable redrive boundary can recover stranded native inbox items without readmitting the original queued input. Recovery reads native task identities on the transaction line; input admission and settlement compare task, attempt, request, and submission identities. Cancel and redrive claims also retry terminal/missing native tasks within three attempts, then record attention. Exhausted redrives withdraw queued input through a finite native cleanup task.

A context input's ownership semantics freeze with its prompt. Attaching that Trigger after freezing creates a separate ownership-transition input referencing the earlier input; consuming the earlier context cannot acknowledge the new ownership. A promotion racing outside-commit prompt preparation is re-drafted before the claim commits.

Router batch context includes bounded trusted Policy/rule summaries and a Ticket index, with authorized references for full paged discovery. Workers receive their frozen Mandate, source/scope/capability ceiling, matched-rule instructions, and source metadata. Delivery prompts distinguish `routingEpoch` for handback from the Assignment `epoch` for Reports. New Mandates persist matched-rule snapshots; legacy grants without recoverable historical rules explicitly expose that the old instructions are unavailable rather than substituting current authority.

## Structured reports

`agents.report(boundWorkerConversationId, report)` validates the live Ticket, bound Worker, assignmentId/epoch, known input IDs, native placement, and payload-bound reportId. Input consumption is per owner: one Worker's report cannot consume another Worker's unread feedback.

```typescript
const report = {
  reportId: 'stable-report-id',
  assignmentId: currentAssignmentId,
  epoch: currentAssignmentEpoch,
  inputIds: [deliveryRun.inputId],
  status: 'waiting',
  content: { summary: 'Input handled; waiting for the next event.' },
};
await runtime.agents.report(workerConversationId, report);
```

This checkpoint accepts only waiting, ready, and attention. The full progress/waiting/ready/blocked/failed/dropped and typed question lifecycle remains required in the following scheduler/Report checkpoint before Outbox or lifecycle consumers depend on it. This subset does not release Job reservations. Ready is a structured reviewable result, not automatic PR acceptance or Ticket completion. Plain final prose without a report requires attention and leaves input consumption outstanding.

## Artifact recall

`agents.recall(conversationId, ref, cursor?)` returns text, estimatedTokens, and an optional next cursor. References are Trigger, delivery-run, note, or Report receipt IDs, or `ticket:<id>`. Trusted context routes are `trigger_context:<triggerId>`, `ticket_context:<ticketId>`, `worker_context:<ticketId>`, Router-only `policy:<version>`, and the Router-only `tickets` index. Full frozen Worker authority may itself spill and is recovered through its role-checked context reference. Naked content digests and source-supplied pointers do not grant access. Every page rechecks the role and Ticket scope.

Artifacts spill above 16 KiB UTF-8, retaining a bounded excerpt and SHA-256 reference. Complete unique temporary files are atomically renamed into digest paths; existing digest objects are verified and truncated legacy objects are repaired before a reference can commit. Event deduplication hashes the original normalized payload before spill, preserving duplicate replay of pre-spill full-body Triggers. Recall pages are limited to 2,000 UTF-16 units / 8,000 UTF-8 bytes without splitting surrogate pairs. The byte-based token estimate is conservative, not an exact tokenizer measurement. Include artifactRoot in backups; SQLite alone cannot restore spilled context.

## Execution authority

Fine-grained capabilities apply to structured gateways. Enabling a trusted local shell with command.execute grants the OS account's command, filesystem, network, and credential reach; it cannot enforce a separate no-merge/no-network guarantee by checking a tool name. Use an isolated execution identity/sandbox when those guarantees are required. The worktree itself is not that boundary.

## Capacity, stop, and retry

The shared Engine scheduler reserves three ordinary slots and one slot for a human waking an existing Worker. New work uses ordinary capacity. Preparation counts immediately; active generation and pending/running/unknown Jobs retain capacity. Existing active steering shares its reservation. Human wakes have priority, with ordinary FIFO admission after three bypasses when an ordinary slot is available. Startup reconstructs live native admission and generation before dispatch resumes.

`stopTicket(ticketId, actor, meta)` revokes authority before cancellation effects. Cancellation targets that Ticket's epoch, Worker, and registered background work. Confirmed native cancellation is a persisted checkpoint; endpoint-only retries do not repeat Worker abort. Confirmation never downgrades to unknown. Unknown endpoint outcomes retain or quarantine capacity and require receipt reconciliation. Cancellation does not undo external effects already performed.

`retryTicket(ticketId, actor, meta)` requires an authenticated allowed human or operator, revalidates current Policy, and creates a new Assignment/epoch. It preserves Worker context and requeues unread obligations. Old cancellation must be confirmed before the reused conversation can execute the new assignment. Pumping alone never resumes stopped work.

Trusted hosts can configure a `cancellationHandler` for epoch-specific endpoint cancellation, returning the same cancelKey and payloadDigest with confirmed or unknown outcome. Job accounting uses the shared pending/running/cancelPending/unknown/completed/cancelled/failed states. A terminal receipt updates accounting after stop without creating new progress or feedback.

## Environments and Jobs

Configure a local broker with `environment: { repository: "/absolute/repository", directory: "/absolute/broker-state", trustedLocalShell: false }`, or inject `environment: { broker }` implementing EnvironmentBroker. The broker ledger is separate from pi state and must be retained for recovery. Model-enabled runtimes expose it through `agents.jobs`; execution tools fail closed when no broker is configured.

Worker tools start_job, get_job, stop_job, read_file, write_file, and edit_file bind to the conversation’s Ticket and current Assignment. A Job start returns its ID before subprocess completion. Background native Job tasks record admission, query the broker on recovery, and deliver a deduplicated result only for the still-current epoch. Feedback can reach the Worker while the Job runs. Stale completion receipts update accounting without creating fresh Worker input.

Broker prepare, execute, query, cancel, and release operations bind stable IDs to canonical payload digests and Ticket/Assignment/Mandate identity. Local preparation records an immutable base commit and uses a detached per-Ticket worktree. Repeated preparation preserves changes; unknown paths are never reset or overwritten. Structured operations reject traversal, repository metadata access, and symlink escapes. An unavailable requested sandbox is an error. These checks assume no hostile concurrent filesystem writer; they are not an OS sandbox.

Shell Jobs require command.execute and explicit trustedLocalShell configuration. Output is bounded. If a process dies after command admission without a trustworthy completion receipt, recovery records unknown and quarantines its reservation; it does not execute the command again. Cancel confirms process termination where observable, without claiming external effects were undone.

## Transactional Outbox

Configure trusted sender adapters through `outbox: { adapters, random? }` when opening the runtime. Each SourceAdapter supplies sourceId, enabled, listened(destination), send(message, key), and queryReceipt(key). Receipt lookup returns found with a receipt, authoritative absent, or unknown. Provider credentials stay in adapter configuration.

`runtime.outbox.enqueueMessage(tx, message)` participates in the caller's Engine transaction. Message authority includes ticketId, mandateId, assignmentEpoch, purpose, a sourceId/threadRef destination, payload, and idempotencyKey. A reused key binds all of these. Destinations must remain in the Mandate's source/scope, with channel.reply and current Policy authorization; no implicit administrator bypass exists.

Structured ready Reports enqueue answers, questions enqueue questions, and blocked/failed/dropped/attention outcomes enqueue attention in the same transaction as consumption. Routine progress is quiet by default. Explicit rule communication flags can permit acknowledgements/progress; silence is rechecked before sending. Without an adapter, message intent remains visibly pending.

Native sender Tasks are finite. Durable nextAttemptAt and attempt count survive restart; reconciliation dispatches due work, while a host timer only wakes it. Known no-send failures or authoritative absence can retry the same immutable key, with jittered backoff from 2 seconds to a 60-second cap and at most eight attempts. Generic network errors are uncertain until receipt lookup establishes otherwise. Stop fences queued sends; an already-started send may still complete and its receipt remains auditable.

`runtime.outbox.inspectReceipt(id)` only queries and records a found receipt, including after stop. Absent/unknown never triggers another effect or fresh Ticket progress. Retain unresolved uncertain messages for inspection; do not replace their keys to force a retry.
