# Service and GitHub integration

Build with `npm ci && npm run build`. Use a fresh directory for the credential-free native demonstration:

```sh
node dist/cli.js --directory .data/demo demo
```

The demonstration uses real pi conversations, tools, Tasks and SQLite with a controlled faux provider. It reports two distinct Tickets and same-Ticket feedback. It is wiring evidence, not a real-model quality benchmark.

## Service configuration

Set the environment variables referenced by the configuration outside the command line. A service requires an explicit management token. Provider activation requires an explicit supported provider and an ID from its installed catalog; omission leaves agent execution inactive.

```json
{
  "directory": ".super-durable-agent",
  "host": "127.0.0.1",
  "port": 8787,
  "tokenEnv": "SUPER_AGENT_TOKEN",
  "provider": "openai",
  "model": "<ID from the installed provider catalog>",
  "modelKeyEnv": "MY_MODEL_KEY",
  "environment": {
    "repository": "/absolute/repository",
    "trustedLocalShell": false
  },
  "githubRepositories": ["owner/repository"],
  "githubSecretEnv": "GITHUB_WEBHOOK_SECRET",
  "githubTokenEnv": "GITHUB_TOKEN",
  "listenedScopes": ["owner/repository#123"]
}
```

```sh
node dist/cli.js --config service.json run
```

`serve` permits model-free intake/inspection; `run` requires provider/model selection. Supported provider factories are the pinned pi-ai OpenAI and Anthropic providers. Secrets remain environment references in configuration. The environment broker is optional; shell is disabled unless separately enabled and authorized by Policy. CLI options override JSON options; unknown configuration keys reject. SIGINT/SIGTERM closes source activity, HTTP, pumps, Harness and storage.

The default management listener is loopback. Deploy a protected HTTPS proxy/tunnel for external access and configure GitHub to POST supported events to `/webhooks/github`. Deployment and live accounts are not set up by the automated tests.

## Management roles

The host API `startManagementServer(runtime, options)` accepts a shorthand operator token or configured principals with token, id, and role. Roles derive from server configuration; request bodies cannot claim an Actor or authority. The CLI token shorthand is an operator credential. Use the host API to configure separate viewer and Watchdog credentials.

| Route | Method | Role |
| --- | --- | --- |
| `/health`, `/status`, `/tickets`, `/tickets/:id`, `/attention`, `/taskGraph`, `/sources`, `/policy` | GET | Any authenticated principal |
| `/receive`, `/policy`, `/tickets/:id/stop`, `/tickets/:id/retry` | POST | Operator |
| `/tickets/:id/requestStatus`, `/reconcile` | POST | Operator or Watchdog |
| `/tickets/:id/pause` | POST | Operator; Watchdog only with explicit allowPause |
| `/webhooks/github` | POST | Valid GitHub webhook signature |

Controls take stable commandId and optional expectedRevision; Policy updates additionally take policy. Receive takes a normalized event, replacing its Actor with the authenticated principal. Status requests are factual reads. Reconcile continues existing authorized Outbox intents; it cannot create a new Mandate or reopen stopped work. Pause withdraws authority through the stop fence. Watchdog cannot edit Policy, inject events, or call Ticket retry. Viewer has no control routes.

JSON bodies are limited to 1 MiB. Authentication precedes management body parsing. Backpressure returns 503 and a retry delay, without acknowledging intake. Safe exports omit native checkpoints, transcripts, broker commands and adapter configuration, with additional credential scrubbing. Avoid putting credentials in source messages or Report text; arbitrary human text is not universally classifiable as secret.

## GitHub protocol and lifecycle

GitHub intake verifies HMAC-SHA256 over exact raw request bytes before JSON parsing, restricts repositories/listened Threads, and qualifies external identities as github:login. An external account named operator does not become the local operator. Semantic event identity prevents a changed unsigned delivery header from creating duplicate work. Authenticated own-send markers suppress echoed intake while preserving independent closure facts. See [GitHub signature documentation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries) and [event schemas](https://docs.github.com/en/webhooks/webhook-events-and-payloads).

Supported families include Issues, issue comments, PRs, reviews/review comments, check runs/suites, and statuses. A trusted receivePoll hook atomically commits collector events and cursor; the service has no autonomous polling collector. Webhooks are the executable intake transport in this checkpoint.

The sender posts Issue/PR comments through Outbox. Receipt lookup scans bounded repository-comment pages for an authenticated key marker. Missing markers remain unknown; no invented idempotency header or authoritative absence is assumed. See [GitHub comment API](https://docs.github.com/en/rest/issues/comments).

A ready Report leaves an open tracked PR Ticket open. Authenticated closure/merge facts permit ending only after all required tracked Threads close and no unread input, newer execution, or active/unknown Job remains. Human own-Thread feedback can resume the same naturally ended Ticket after Policy revalidation; bots and explicit stop cannot do so.

Trusted trackPullRequest/Engine.trackThread callbacks bind a confirmed created PR, with original/current pr.create and source/repository checks for scope expansion. This adapter sends comments and tracks lifecycle. PR creation can use explicitly enabled trusted execution with host tooling; there is no structured PR creation or merge tool. A Worker claim cannot establish that a PR merged.

Automated integration uses local HTTP fixtures, native controlled-provider execution, and CLI subprocesses. Live webhook reachability, hosted-model quality and real account behavior remain separate unexecuted trials.
