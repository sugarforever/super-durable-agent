# Remote execution and Feishu

The remote transport reuses the local EnvironmentBroker contract and daemon-side operation ledger. It does not copy conversation history to the endpoint. All examples require explicit Policy grants and host configuration; automated tests use local fixtures, without real devices or accounts.

## Controller and daemon

Configure one remoteEnvironmentBroker instead of the local environment setting in service JSON. Poll transport lets the daemon initiate connections outbound to the controller, supporting NAT-friendly deployment.

```json
{
  "directory": "/absolute/controller-state",
  "host": "127.0.0.1",
  "port": 8787,
  "tokenEnv": "SUPER_AGENT_TOKEN",
  "managementTls": {
    "keyFile": "/absolute/controller-key.pem",
    "certFile": "/absolute/controller-cert.pem"
  },
  "remoteEnvironmentBroker": {
    "endpointId": "laptop",
    "tokenEnv": "LAPTOP_ENDPOINT_TOKEN",
    "directory": "/absolute/controller-transport-state",
    "transport": "poll"
  }
}
```

Add provider/model settings from the [service guide](integrations.md) for agent execution. The CLI configures sources and the controller listener before activating recovered Tasks. Library callers can use openSuperAgent with startPaused, bind services, then call the idempotent activate method. Model-free recovery preserves its inspection restrictions.

A corresponding daemon configuration:

```json
{
  "endpointId": "laptop",
  "tokenEnv": "LAPTOP_ENDPOINT_TOKEN",
  "controllerUrl": "https://controller.example:8787",
  "controllerTokenEnv": "LAPTOP_ENDPOINT_TOKEN",
  "directory": "/absolute/daemon-state",
  "repository": "/absolute/repository",
  "allowedRepositoryRoots": ["/absolute/repository"],
  "capabilities": ["repo.read", "repo.write"],
  "trustedLocalShell": false,
  "mode": "poll"
}
```

```sh
node dist/cli.js --config controller.json serve
node dist/daemon/cli.js --config daemon.json
```

Use distinct management and endpoint credentials. TLS certificates and credentials are deployment prerequisites; the examples do not provision them. In direct HTTP mode, the controller uses transport http with an HTTPS daemon URL; daemon mode http exposes a TLS listener configured with tls.keyFile/certFile. Insecure loopback requires an explicit test-only option. URLs cannot carry credential material.

Every mutation obtains a fresh controller grant and intersects it with daemon capabilities, repository roots and shell opt-in. Execute payloads cannot supply their own authority. Cancellation/revocation epochs fence future admissions; old receipts remain queryable for accounting. Shell requires command.execute and explicit trustedLocalShell at the relevant hosts and retains the broad OS authority described in the [API guide](api.md).

Stable operation IDs bind canonical payloads. The controller transport records and daemon ledger are independent recovery state. Ledger identity detects a replacement endpoint; an empty replacement is not proof an old command never ran. Unknown commands are not relaunched under a new key. Preserve these directories and reconcile endpoint state when restoring the primary runtime; do not erase them to force retries.

Preparation and transport can be uncertain before a shell operation has a receipt. Such Jobs remain unknown/quarantined, with a preparation operation reference for inspection. A missing execute record does not erase uncertainty about preparation or transport delivery. Process cancellation cannot undo prior external effects.

Authenticated management health/taskGraph include safe endpoint identity, transport, contact/availability and queue/activity diagnostics. Missing telemetry is unknown. Diagnostics exclude tokens and raw command payloads.

## Feishu source

Add a feishu object to service JSON:

```json
{
  "feishu": {
    "appId": "cli_your_app_id",
    "verificationTokenEnv": "FEISHU_VERIFICATION_TOKEN",
    "encryptKeyEnv": "FEISHU_ENCRYPT_KEY",
    "chats": ["oc_your_chat_id"],
    "trustedUsers": ["ou_allowed_person"],
    "ownSenderIds": ["ou_your_bot_open_id"],
    "accessTokenEnv": "FEISHU_ACCESS_TOKEN",
    "accessTokenExpiresAtEnv": "FEISHU_ACCESS_TOKEN_EXPIRES_AT"
  }
}
```

The access-token expiry reference contains an absolute epoch time in milliseconds. The CLI reads a supplied token through the trusted cached provider; it does not obtain or refresh an application token through an implicit OAuth flow. Restart the service with refreshed token/expiry environment values before expiry; the process environment does not update itself. Library hosts can provide a refresh-capable cached token loader. Tokens and encryption keys never enter source records or prompts.

Configure Feishu's callback as `/webhooks/feishu`. The source verifies signature over exact raw bytes, a bounded timestamp/replay window, recipient app ID and verification token; encrypted challenges use the documented AES envelope. The event signature uses SHA-256 with its prescribed concatenation, not a guessed HMAC scheme. See [Feishu event documentation](https://open.feishu.cn/document/server-docs/event-subscription-guide/event-subscription-configure-/encrypt-key-encryption-configuration-case) and the [official SDK](https://github.com/larksuite/node-sdk).

Only configured chats and listened scopes admit messages. Current text-message events retain revisions and qualify actors as sourceId:openId, for example feishu:ou_allowed_person. Configure corresponding Policy actors/rules separately; a source trusted-user list alone grants no Mandate. Message Threads retain the chat and root-message reference. Unsupported payloads fail closed.

Own-message suppression checks verified sender open ID and bot classification. The recipient header.app_id is not sender proof, and a copied public marker cannot suppress a human's input. Sender IDs are trusted configuration, never message text.

Outbound replies pass through the transactional Outbox and current Ticket scope/capabilities. The adapter records known platform receipts durably. A lost response without a known receipt remains unknown; no universal provider idempotency or authoritative absence is claimed. Local HTTP tests cover uncertainty without sending to real recipients.
