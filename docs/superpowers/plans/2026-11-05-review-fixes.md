# Review Defect Corrections

> For agentic workers: use test-driven development and independent review for this bounded correction task.

**Goal:** Correct the five independently reproduced review defects before merging the existing ten-part stack.

**Architecture:** Preserve the existing pi-durable organization and external receipt boundaries. Fix admission/cancellation ordering, settle passive context without waking ended Workers, preserve existing file permissions, and retain outbound questions and disabled-source intents. No dependency changes or unrelated refactoring.

**Tech stack:** TypeScript, Node 22.19+/24, pi-durable 1.1.0, Vitest, SQLite, local Git and loopback HTTP.

**Authorization:** The user requested review and merge if ready, then explicitly authorized fixing the identified defects. Merge remains conditional on successful validation and review.

## Constraints and review focus

- Cancellation must prevent delayed pre-admission execution, including asynchronous authorization and both remote transports. Operation cancellation must not revoke unrelated same-epoch work.
- Context-only input must remain available without reopening ended Tickets or retaining undrainable old-epoch intake obligations. Restart and natural resumption must preserve accounting.
- Disabled sources defer unsent replies; removed destination scopes still suppress them. Reenable must retain normal authority and receipt checks.
- GitHub question messages must include structured question text and options as well as ordinary report content.
- Atomic structured edit/write must preserve existing executable permission bits without following symlinks; new files retain conservative permissions.

## Task 1: Correct the reviewed runtime defects

**Files:** Relevant production files are src/services/jobs.ts, src/environments/remote.ts, src/environments/local.ts, src/environments/types.ts, src/domain/policy.ts, src/agents/coordinator.ts, src/services/outbox.ts, src/sources/github.ts and src/sources/feishu.ts. Change only those needed, plus directly related tests and recovery paths if required.

- [ ] Add failing Vitest regressions from the independently reproduced cases, and verify they fail for the reported reasons before production edits.
- [ ] Establish cancellation fencing before confirming an unadmitted Job is cancelled; test HTTP/poll authorization barriers, reopen continuity, and unaffected operations.
- [ ] Correct ended-Ticket context accounting and recovery; test native Router ignore, restart, intake capacity and natural resumption.
- [ ] Preserve pending replies while actual GitHub and Feishu adapters are disabled, then verify reenable delivers once and removed scopes remain suppressed.
- [ ] Render structured GitHub questions and options alongside report content; verify the actual outbound payload.
- [ ] Preserve existing file permission bits during atomic write/edit; test executable targets, ordinary files, new files and existing symlink guards.
- [ ] Run focused checks after each correction, then npm run check against the complete result.
- [ ] Obtain independent specification/quality review and resolve important findings, then rerun affected and aggregate checks.

## Task 2: Publish and merge verified code

- [ ] Verify upstream PR heads have not moved; publish the reviewed corrections to the top stack branch using an expected-head lease.
- [ ] Verify remote file/commit identity and successful exact-head CI before merging.
- [ ] Preserve commit ancestry and keep main unchanged while consolidating: merge the corrected top PR into its predecessor branch, repeat down through the stack, and verify every updated head has the reviewed tree. Require successful CI on the final consolidated head before merging the complete stack into main.
- [ ] Verify all requested PRs merged and the final main tree matches the reviewed tree. Observe final main CI through completion.
- [ ] Report validation, known live-trial limits and a controlled demo suitable for the requested post.
