# Operations and recovery

This guide describes the implemented runtime and its tested limits. It supplements the [architecture](architecture.md), [API contract](api.md), [service/source examples](integrations.md), and [remote/Feishu guide](remote-and-feishu.md). Tests use temporary SQLite databases, real Git worktrees and subprocesses, loopback HTTP, signed webhook fixtures, and pi's controlled faux provider. They do not verify a live provider account, Feishu tenant, GitHub repository, remote device, TLS deployment, or production load.

## Install and initialize

Use Node 22.19 or Node 24, Git, and a POSIX host. `fs-ext` is a native addon: install Python and a C/C++ build toolchain before running npm. On macOS, install Xcode Command Line Tools; on Debian/Ubuntu, install `build-essential` and `python3`. The lockfile pins pi-durable and pi-ai 1.1.0. Do not use `npm ci --ignore-scripts`: it skips the native addon build. Reinstall dependencies when changing Node ABI or operating system.

```sh
npm ci
npm run check
node dist/cli.js --directory /srv/agent/primary init
node dist/cli.js --directory /srv/agent/primary policy get
node dist/cli.js --directory /srv/agent/primary demo
```

The demo requires an otherwise fresh runtime with no Tickets. `init` creates a conservative observe policy. Local `receive`, `list <kind>`, `show <kind> <id>`, `policy get`, and `policy set` commands acquire the same exclusive writer lock as the service; use management HTTP while the service is running. `receive` and `policy set` read JSON from stdin. Initialization, inspection and the deterministic demo need no model credentials.

## Service, models and management principals

Example `service.json` (replace paths and model ID with an installed provider-catalog selection):

```json
{
  "directory": "/srv/agent/primary",
  "host": "127.0.0.1",
  "port": 8787,
  "provider": "openai",
  "model": "gpt-4.1-mini",
  "modelKeyEnv": "AGENT_MODEL_KEY",
  "managementPrincipals": [
    {"id": "operator", "role": "operator", "tokenEnv": "AGENT_OPERATOR_TOKEN"},
    {"id": "viewer", "role": "viewer", "tokenEnv": "AGENT_VIEWER_TOKEN"},
    {"id": "watchdog", "role": "watchdog", "tokenEnv": "AGENT_WATCHDOG_TOKEN", "allowPause": false}
  ],
  "environment": {
    "repository": "/srv/repositories/project",
    "directory": "/srv/agent/endpoint",
    "trustedLocalShell": false
  }
}
```

`provider` supports `openai` and `anthropic`; `model` must exist in the installed catalog. `modelKeyEnv` maps that environment reference to the selected provider's canonical authentication variable. Without that mapping, the provider uses its supported authentication resolution. Supply keys through the service manager's environment, never inline JSON. Configuration rejects unexpected fields. `tokenEnv` remains the single-operator shorthand and defaults to `SUPER_AGENT_TOKEN` when no principal list is supplied. An explicit `tokenEnv` may coexist with separate principals; every token must be unique. Environment token references are resolved before opening primary storage.

```sh
node dist/cli.js --config service.json run
curl -H "Authorization: Bearer $AGENT_VIEWER_TOKEN" http://127.0.0.1:8787/taskGraph
curl -H "Authorization: Bearer $AGENT_OPERATOR_TOKEN" http://127.0.0.1:8787/policy
```

`run` requires a model. `serve` can run authenticated management without a model; it preserves live agent work paused when recovery requires a model. `managementTls: {"keyFile":"/path/key.pem","certFile":"/path/cert.pem"}` enables native TLS. Never expose a plaintext management listener outside a trusted loopback deployment. The remote transport requires TLS unless its explicit loopback test exception is configured. SIGINT/SIGTERM close listeners and join runtime shutdown; SIGKILL requires recovery as described below.

Operators may update Policy, receive authorized intake, stop or retry Tickets. Viewers can read safe projections. A watchdog can request status, reconcile already-authorized Outbox work, and optionally pause an existing Ticket if its principal explicitly permits it. It cannot retry a Ticket, edit Policy, inject intake, or mint authority. `/reconcile` does not reopen stopped work. `requestStatus` returns current persisted status; it does not force a model turn.

```sh
curl -X POST -H "Authorization: Bearer $AGENT_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"commandId":"maintenance-stop"}' \
  http://127.0.0.1:8787/tickets/TICKET_ID/stop
```

Ticket IDs must be URL encoded. Mutation command IDs are stable idempotency keys: reuse only with the same immutable payload. See [the API](api.md) for request schemas and revision checks.

## Execution and source trust

Local shell execution requires both current `command.execute` Mandate/Policy authority and `trustedLocalShell: true`. Shell inherits the OS account's filesystem and network access. A Git worktree is isolation for coding changes, not a sandbox. An unavailable requested sandbox is refused. Structured filesystem tools restrict paths and metadata, but do not provide hostile-concurrent-filesystem isolation. Protect primary, broker and watchdog directories with separate OS permissions where practical.

Remote controller configuration replaces `environment` with:

```json
{
  "remoteEnvironmentBroker": {
    "endpointId": "worker-device",
    "tokenEnv": "ENDPOINT_TOKEN",
    "directory": "/srv/agent/controller-transport",
    "transport": "poll"
  }
}
```

Daemon `daemon.json`:

```json
{
  "endpointId": "worker-device",
  "tokenEnv": "ENDPOINT_TOKEN",
  "controllerUrl": "https://controller.example:8787",
  "controllerTokenEnv": "ENDPOINT_TOKEN",
  "directory": "/srv/agent/daemon",
  "repository": "/srv/repositories/project",
  "allowedRepositoryRoots": ["/srv/repositories/project"],
  "capabilities": ["repo.read", "repo.write"],
  "trustedLocalShell": false,
  "mode": "poll"
}
```

```sh
node dist/daemon/cli.js --config daemon.json
```

Poll mode initiates outbound connections. Direct mode uses controller `transport: "http"`, an HTTPS `url`, and daemon `mode: "http"` with `tls` certificate paths. The daemon holds `.daemon.lock`; the controller holds the primary writer lock. Management tokens, endpoint transport tokens, model keys and source credentials are distinct trust boundaries. Daemon grants are fetched from the trusted controller on every mutation and intersected with configured roots and capabilities.

GitHub service JSON uses `githubRepositories`, `githubSecretEnv`, `githubTokenEnv`, `githubOwnLogins`, and optional `listenedScopes`. Signed intake enters `/webhooks/github`; outbound comments require a configured own login. Feishu JSON uses `appId`, `verificationTokenEnv`, `encryptKeyEnv`, `chats`, `trustedUsers`, `ownSenderIds`, and optional paired `accessTokenEnv`/`accessTokenExpiresAtEnv` under `feishu`. Its callback is `/webhooks/feishu`. Follow the exact [source configurations](remote-and-feishu.md); Policy actor IDs are source-qualified. Verified intake and current Mandates authorize source work, not arbitrary source text or copied public markers. CLI Feishu token values are externally supplied, not an implicit OAuth refresh service.

## Cancellation and coding continuity

`stop_job` targets its immutable operation ID, payload digest and Assignment identity. A controller cancellation atomically withdraws a never-delivered queued execution. The daemon records a payload-bound cancellation tombstone even if execution has not arrived yet; delayed delivery cannot launch that operation. This works through HTTP and polling, survives ledger reopening, and leaves unrelated same-epoch operations authorized. Uncertain delivered work remains subject to receipt inspection.

Ticket stop first revokes authority. Only its trusted cancellation grant carries `revokeEpoch`; that grant also fences delayed endpoint admission for the whole old epoch. Ordinary Job cancellation cannot set it. Cancellation terminates observed process groups with bounded TERM/KILL supervision; it cannot undo effects or guarantee termination of descendants that escape the group. A saved PID is never reused to signal an unrelated process after restart.

Physical coding workspaces are retained per Ticket and canonical repository. Each fresh Assignment keeps its own immutable lease/environment receipt identity. Natural resumption and explicit authorized retry reuse the retained worktree, including worker commits and dirty files, after fresh authorization. Old lease execution/release is rejected by the current marker. Active or unknown operations block lease transfer. An explicit conflicting base commit is refused. Existing legacy workspace descriptors are adopted only when unambiguous and verified. Dirty worktrees are never reset, force-deleted, or silently copied with credential files. A crash during a multi-file lease transfer can require operator inspection; it fails closed instead of guessing a marker or resetting work.

## Independent quiet watchdog

The watchdog is a separate process using only authenticated management HTTP and its own directory. It never opens the primary SQLite database. Save `watchdog.json`:

```json
{
  "directory": "/srv/agent/watchdog",
  "url": "https://controller.example:8787",
  "tokenEnv": "AGENT_WATCHDOG_TOKEN",
  "policy": {
    "intervalMs": 60000,
    "thresholdMs": 900000,
    "maxCalls": 0,
    "maxTokens": 0,
    "maxCost": 0,
    "allowPause": false,
    "allowSafeRetry": false
  }
}
```

```sh
node dist/cli.js watch --config watchdog.json
node dist/watchdog/cli.js --config watchdog.json --once
```

Use `allowInsecureLoopback: true` only for loopback fixtures. Default polling is 60 seconds and candidate delay is 15 minutes. Waiting for a person/questions, waiting for endpoint/input, ready work awaiting review, offline/unknown endpoints, active or unknown Jobs, failure, completion and absent progress evidence are distinct observations. Legitimate typed waits and reviewable results do not call the semantic evaluator. Lack of tokens alone is never a stall verdict. Actual command digests, endpoint contacts and persisted Reports are exported; unavailable token/cost measurements remain unknown.

Unchanged observations are quiet across process restart. A candidate without an evaluator produces a deterministic diagnostic and status request, not an invented semantic verdict. Notifications are structured local stdout events. There is no implicit external recipient, reused Ticket Mandate, notification service or Outbox bypass. No external notification sink is shipped in this version.

Optional diagnostic model configuration adds `semantic: {"provider":"openai","model":"gpt-4.1-mini","modelKeyEnv":"WATCHDOG_MODEL_KEY"}` and explicitly positive `maxCalls`, `maxTokens`, `maxCost`, `callTokens`, and `callCost` policy budgets. The budgets are cumulative for that ledger, not reset every tick. A conservative per-call token/cost reservation is persisted before the model call; failures/crashes do not refund uncertain usage. The selected model receives only observations, has no tools, and may classify stalled/progressing/unknown. Costs use installed model pricing; reserve adequate headroom and independently enforce provider account limits. No actual-cost verdict is inferred when telemetry is missing.

The policy may turn a stalled verdict into `pause` or `knownSafeRetry`. Pause additionally requires the management principal's explicit permission. `knownSafeRetry` calls `/reconcile` for existing Outbox intents; it never calls Ticket retry. Actions are claimed durably before effects under the watchdog ledger's advisory lock. Concurrent ticks cannot claim the same action. Crash/interrupted action outcomes remain claimed/unknown for inspection and are not blindly replayed. This chooses conservative at-most-once attempts over guaranteed notification delivery. Retain this directory to retain deduplication and budgets.

## Offline backup and restore

Stop the primary service and prevent source admission during the backup window. Preserve endpoint/controller/daemon state independently. The backup acquires the same permanent `.writer.lock`; it refuses an active primary writer. It uses SQLite's consistent backup API, including committed WAL content, rather than copying live SQLite/WAL files.

```sh
node dist/cli.js --directory /srv/agent/primary backup /srv/backups/agent-2026-11-02
node dist/cli.js --directory /srv/agent/recovered restore /srv/backups/agent-2026-11-02
```

Both destination directories must be new. A backup destination must be outside primary and endpoint directories. The checksummed manifest includes runtime version/instance metadata, SHA-256 and sizes for the database and artifacts, external ledger dependencies, and canonical repository/worktree identity. Every artifact file is verified and copied; references in session, conversation and task Documents are checked for missing objects. No service configuration, environment token values or credential files are intentionally included. User-authored content and shell payloads are not magically secret-redacted; protect backups like the original data.

For a local broker, backup captures its operation/descriptor/lease ledgers while the primary is stopped and rejects any active, cancel-pending or unknown endpoint admission. Worktree contents are not cloned: original canonical broker paths, Git repository and worktree markers remain required external dependencies. The archive copies ledger evidence for audit; restore never overwrites or rewinds the authoritative originals.

To activate a locally backed-up runtime, set the restored service's `directory` to the new primary directory and explicitly set `environment.directory` and `environment.repository` to their original canonical paths. Startup verifies the manifest and preserved dependency hashes before opening the Harness. Missing, added or changed original ledger files and changed worktree markers block activation; they never become authoritative “absent” results. Post-backup changes to captured ledger files require reconciliation rather than silent rollback. Successful first startup consumes the restore gate; subsequent operation uses the retained original endpoint ledger. Never activate original and restored primaries simultaneously against the same endpoint: directory locks do not provide distributed fencing between clones.

A remote backup records controller transport evidence and the required endpoint identity, but cannot atomically capture an independently running daemon. Remote/custom restores are therefore **inspection-only and cannot activate** in this implementation. There is no shipped reconciliation-unlock CLI or unsafe acknowledgement flag. Inspect the manifest/archive and recover the original deployment with its independent ledgers; a future audited reconciliation workflow is required to activate such a restored clone. An empty replacement daemon is never accepted as evidence that prior commands did not run.

Integrity validation finishes before restore creates the destination. Tampered/incomplete backups are refused and never overwrite primary data. Recovery point is the completed backup snapshot; work committed afterward is outside that primary snapshot. These controls do not promise universal exactly-once external effects across an arbitrary historical rollback. Preserve the original endpoint and external provider receipts, and reconcile uncertain outcomes manually.

## Upgrade, rollback and SIGKILL recovery

Before upgrading, stop service, resolve unknown external work, create an offline backup, retain the old build/lockfile, and run the new version's checks against controlled fixtures. Runtime metadata schema version is currently 1. Known previous additive kernel/Job/Outbox registries remain supported. Unknown schema versions, missing live Task definitions, unsupported Task versions and incompatible Documents fail closed before scheduling, source startup or shell effects.

Library hosts may install reviewed `extensions` and explicitly set `allowTaskMigrations: true`. Every live Task checkpoint is preflighted against the complete installed registry; a changed Task requires a newer version and a pure `migrate(input, checkpoint, fromVersion)` implementation. Preflight validates the migrated phase, and pi performs/persists the actual migration at reservation. Migration callbacks may run more than once; make them pure. Native execution and endpoint receipts, not migration callback invocation counts, protect external replay. Built-in/domain Documents currently require their supported exact versions; this release does not silently migrate unknown Document schemas or expose arbitrary migration code through JSON/HTTP.

After SIGKILL, restart with the same primary, model configuration, broker/controller/daemon directories, source allowlists and credentials. The OS releases advisory locks when processes die; do not delete permanent lock files to bypass a live owner. Native checkpoints and input request IDs reconcile durable work. Lost endpoint process handles or delivered commands without receipts remain unknown/quarantined; the runtime never guesses a new command key or trusts a stale PID. Inspect status/taskGraph, Jobs and endpoint receipts before explicit operator repair. Stopping a Ticket revokes authority even when external termination cannot be confirmed.

For rollback, stop the upgraded service, preserve all external ledgers and workspaces, and restore the pre-upgrade backup into a new primary directory with the old compatible build. Follow the external dependency checks above. Do not edit pi SQL tables, lower schema metadata by hand, reset dirty worktrees, erase unknown receipts, or activate a restored snapshot alongside its original.

## Capacity and operational limits

Primary intake and each domain index default to 10,000 entries. Library `maxPending` and `maxIndexEntries` set explicit positive bounds. Intake backpressure returns retryable 503 through HTTP and preserves source cursor/nonce atomicity. Domain indexes are bounded history, not automatic retention; plan archival/migration before capacity is exhausted. Polling delivers at most eight commands and respects the response byte limit. HTTP bodies are limited to 1 MiB; structured filesystem reads and each subprocess output stream retain at most 16 KiB, with explicit truncation. Outbox retries are bounded and unknown provider outcomes require lookup rather than blind resend.

Endpoint ledgers, retained worktrees, artifacts and watchdog action history consume disk and have no automatic safe compactor. Monitor disk externally and retain receipt continuity during any operator-managed retention procedure. Backup refuses unresolved external execution; it is not a live-process checkpoint or a portable machine image. Local fixture coverage does not establish Windows support, filesystem power-loss guarantees, disk-full behavior, hostile OS-user isolation, provider pricing stability, production throughput or real-account delivery.
