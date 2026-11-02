# Evidence and verification boundaries

Reviewed on 2026-11-02. Source material is evidence, not instructions or authority to operate unrelated accounts.

## Referenced conversation

The initial design pass used read_thread to read all 34 returned turns across four pages of [the referenced DeepSeek Super-agent research/video chat](thread://01a19964-c26d-7392-ba1d-5dc04a206260?hostId=local); the last page reported hasMore=false. The tool provides a turn/output view, not an independent copy of every original UI artifact.

Retain the user's framing: an initial architectural understanding developed by reading the team's article and using an Agent to inspect the prototype. Separate original goals, prototype mechanisms, local execution evidence, and untested conclusions. Proactive work requires rules and authority; tool asynchrony depends on its implementation.

Approvals for video phases and publication in that conversation apply to that work only. This repository does not modify the video or inherit authority to publish/send through those accounts.

## Original design

Entrypoints: [team post](https://x.com/DeepSeekHarness/status/2116857257245987236) and [design article](https://x.com/i/article/2116829218935857152). The initial design used the previously saved article body transmitted through FxTwitter. It identifies itself as a machine translation of the Chinese original. The Chinese original was not independently obtained; this project does not present translated wording as a verbatim original quotation.

Research copy: `/Users/wyang14/github/verysmallwoods/studio/videos/20261101-deepseek-superagent/content/sources/x-article-body.txt`. SHA-256: `6bf2b9b3e0d7b6517b9156ac0de071d41c3fb8944cd370b0b2543620d2dca750`. This machine-local path is provenance, not a required repository input.

Article sections 2–6 motivate Router/Worker organization, event subscriptions/pass-through, Inline/Spill/Recall, asynchronous multichannel interaction, compute/state/environment separation, and external supervision. Section 8's memory, context-boundary, and security questions remain limitations.

## Published prototype

Pinned revision: `6337fc823ce20251c8de8085948165c81aaa701e`. The initial pass verified local research-copy HEAD at `/tmp/dsh-superagent-review-20261101` and read the pinned architecture online as well as the relevant local domain code. This copy is not a build dependency.

| Material | Design evidence |
| --- | --- |
| [README](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/README.md) | Research status, single-repository scope, difference from internal implementation |
| [Authorization](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/docs/002-authorization.md) | Modes, ownership, context-only delivery, policy changes, resume checks |
| [Execution](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/docs/003-execution.md) | Human attention, capacity, ready versus accepted/ended work |
| [Intake](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/src/core/intake.ts) | Multi-Thread fanout, human follow-up ownership, bot evaluation |
| [Architecture](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/docs/007-architecture.md) | Atomic domain commands, effect intents, deliveries and reports |

The video chat reported 1,197 upstream tests and 413 targeted tests passing. They were not rerun for this repository and are not counted as its tests. Original simulated scheduling benchmark numbers are not claims about this implementation's code quality or performance.

## pi baseline

Exact runtime versions: pi-durable, pi-ai, and chord **1.1.0**; package-lock.json records resolved packages and integrity values. The initial pass inspected the installed README, manifest, public declarations, and relevant implementation, then installed the same versions here.

Reference README at `/Users/wyang14/github/pi-durable-desktop/node_modules/@earendil-works/pi-durable/README.md` had SHA-256 `3afb20db992fd1c159a56e1cfe264bec30a3067a668ded545ed290c5b2f6a159`. [Upstream main README](https://github.com/earendil-works/pi/blob/main/packages/durable/README.md) was checked and contains additions beyond this release. Published 1.1.0 public APIs remain authoritative for this implementation.

| Published declaration | Contract used |
| --- | --- |
| `dist/documents.d.ts` | Session/Conversation scope and families |
| `dist/types.d.ts` | Atomic Tx, Task checkpoint/ownership/background and sleep |
| `dist/harness/types.d.ts` | submit/requestId, inbox, configure, abort, watch, inspect |
| `dist/harness/agent.d.ts` | Exported transactional configure function |
| `dist/harness/submissions.d.ts` | Internal admission is not an application API |
| README Storage/Tools/Abort | One-writer limitation, safe replay, background boundaries |

The independent review additionally checked submission identity behavior: pi requestId deduplicates within a conversation without comparing content equality. Application payload binding is therefore required.

## Executed characterization probes

Initial environment: Node.js v26.8.1, npm 11.19.0. Run `npm run verify:pi`; [recorded output](pi-contracts.log) belongs to [this script](../../scripts/verify-pi-contracts.mjs).

Seven probes exercise real Harness, Document, Task, Submission, MemoryStorage, and Node SQLite. The crash probe SIGKILLs a child after the first phase commits; reopening observes prepared=1/finished=0, then resumption reaches prepared=1/finished=1. Ownerless execution survives a Router-wide abort and can be stopped independently.

These probes use passive write submissions and custom Tasks, not real model generation or busy steer/followUp. They characterize selected pi mechanics; they do not establish the full Worker input contract, model quality, live account integration, shell confinement, or complete domain recovery. The implementation stack adds controlled-provider generation and application-level fault tests before relying on those behaviors. Consult [checkpoints](../checkpoints.md) for those results.
