# Super Durable Agent

A pi-durable implementation of the DeepSeek Super-agent organization: a long-lived Router, independent Workers, subscription pass-through, scoped Mandates, asynchronous Jobs, durable recovery, and an external Watchdog.

Development is organized as ten stacked pull requests, each linked to one requirement issue and an executable approval checkpoint. See [implementation status and checkpoints](docs/checkpoints.md) before relying on a capability.

- [Architecture](docs/architecture.md): boundaries, state, event flow, authority, pi integration, and recovery.
- [Evidence](docs/research/evidence.md): original design, pinned prototype, and verified pi API baseline.
- [Implementation plan](docs/superpowers/plans/2026-11-02-super-durable-agent.md): issue/PR split and acceptance tests.
- [Design review](docs/design-review.md): findings and required corrections.
- [pi contract probes](scripts/verify-pi-contracts.mjs): real Harness and SQLite, without model calls.

```sh
npm ci --ignore-scripts
npm run verify:pi
```

Use Node.js 22.19.0 or newer. pi-durable, pi-ai, and chord are pinned to 1.1.0. Contract probes characterize runtime mechanics; they do not establish real-model task quality or live GitHub/Feishu/endpoint integration. All repository documentation is maintained in English.
