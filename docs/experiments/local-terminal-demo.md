# Local terminal experiment: the interrupted CSV exporter

This experiment uses internal Tickets and local files. GitHub, Feishu, and a public tunnel are unnecessary. Run the faux-provider starter to check runtime wiring. Use the separate CSV experiment to evaluate a real model’s reasoning and tool use.

## Quick start: no model key required

Start in the repository root with a POSIX shell, Node 22.19+ and installed dependencies. Run each command to completion before entering the next.

```sh
npm run build
export DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/super-agent-demo.XXXXXX")"
node dist/cli.js --directory "$DEMO_DIR" demo > "$DEMO_DIR/result.json"
node -e 'const fs=require("node:fs");const r=JSON.parse(fs.readFileSync(process.env.DEMO_DIR+"/result.json"));console.log(JSON.stringify({provider:r.demo,tickets:r.tickets.length,feedbackSameTicket:r.feedbackSameTicket,routerDecisions:r.routerDecisions,consumedInputs:r.consumedInputs,reports:r.tickets.map(t=>t.latestReport.status)},null,2))'
node dist/cli.js --directory "$DEMO_DIR" list ticket
```

Expected result: native-faux provider, two Tickets, feedbackSameTicket true, and ready Reports. The demo starts real native Conversations/Tasks and SQLite, then closes. It does not execute the CSV experiment, use a paid model, or prove model quality. Keep `DEMO_DIR` to inspect the database later.

## CSV experiment: real model required

### Prepare

Use three terminals: service, operator, and monitor. Create a disposable committed Git repository with a tiny Node project and a test command. Keep primary state and broker state in persistent directories outside that repository. Configure the service using [Operations](../operations.md), with:

- An OpenAI or Anthropic model from the installed catalog, with its API key supplied through an environment variable.
- Separate management operator and viewer credentials; the operator ID included in the Policy’s allowed actors.
- Local environment repository and broker directory; `trustedLocalShell` enabled only for the disposable execution environment.
- No GitHub or Feishu configuration.

Start `node dist/cli.js --config service.json run` in terminal 1. Wait for the listening message before sending input. Model calls require internet access. Do not display credentials while recording, and keep shell tracing disabled.

Use `http://127.0.0.1:8787` as the service URL in terminals 2 and 3. In terminal 2, send an authenticated `POST /policy` with:

- A unique `commandId` and a Policy version greater than the current version.
- `acceptMode: "observe"` and the configured operator in `allowedActors`.
- Explicit capabilities `repo.read`, `repo.write`, and `command.execute`.

Omit `channel.reply`: this experiment inspects Reports through the API and has no local sender adapter. See [the API](../api.md) for the Policy fields and authorization rules.

Submit requests to authenticated POST /receive. Each JSON event needs sourceId `cli`, a unique externalId, revision `1`, threads, body, receivedAt/occurredAt in epoch milliseconds, and addressed true. The API supplies the authenticated Actor; do not put credentials in the event. Use the same Thread string for follow-up and a different one for independent work.

### Act 1: Start the exporter

Send this request on Thread `local:csv`:

> Build a CSV exporter for an array of objects. Support commas, quotes, and embedded newlines. Add tests and run them through start_job. Use export.csv as the output filename. Report changed files, test results, and your Job ID. Do not make network calls or create a PR.

Wait until /tickets shows an accepted Ticket. The model may ask a clarification; answer on the same Thread before continuing. Record its Ticket ID.

### Act 2: Send feedback and start independent work

While the exporter is working, send another event on `local:csv`:

> Change the output filename to report.csv. Add Unicode and empty-input coverage. Preserve the earlier CSV escaping requirements.

Send a separate request on Thread `local:project-map`:

> Inspect this project and create docs/project-map.md describing entry points and how to run tests. Do not modify application code. Report your findings.

Observe two Tickets. The filename correction must belong to the original exporter Ticket, not a third Ticket. Inspect actual files in each Ticket's broker worktree; changes are not expected in the original checkout or automatically shared between Tickets.

### Act 3: A long Job and controller restart

On `local:csv`, request:

> Start one background Job that appends started to e2e-progress.txt, waits 90 seconds, then appends finished. Return the Job ID immediately and await its result. Never launch a replacement if the outcome is uncertain.

Wait for `/taskGraph` to show the Job and confirm the `started` line in `e2e-progress.txt` in that Ticket's worktree. Record the Job ID and file contents. Terminate the primary controller with SIGKILL using its verified PID, then restart the exact same configuration and directories.

Local subprocess recovery may conservatively become unknown after losing process supervision. In that state, the operation must not be relaunched automatically, and its capacity reservation and attention warning must remain visible. Do not erase the broker ledger to make it run. To demonstrate reliable late-completion lookup across controller death, run the optional remote daemon separately and leave it alive; query-only reconciliation then observes its authoritative completion receipt without another command execution.

### Act 4: Explicit stop

Start a second long Job after resolving the prior outcome. Before it finishes, POST /tickets/URL_ENCODED_TICKET_ID/stop with an operator credential and a new commandId. Observe stopped lifecycle and cancelled or explicitly unknown execution accounting. Restarting must not resume the stopped Ticket. An already written started line is not rolled back. `POST /tickets/URL_ENCODED_TICKET_ID/retry` is a separate authorized action that creates a new Assignment and epoch.

### Monitor and evaluate

In terminal 3, repeatedly GET these endpoints with the viewer credential. Use a two-second interval and stop with Ctrl-C after the final state is observed.

| Surface | Evidence |
| --- | --- |
| /tickets and /tickets/:id | Goal, lifecycle, Assignment epoch, latest Report, attention |
| /taskGraph | Native Task kinds/statuses, Jobs, environment IDs, receipt status |
| /attention | Uncertain execution or intervention requirements |
| /status | Intake backlog and outbound state |
| Broker worktree files and git diff | Actual code, filename, tests, and side effects |
| Separate optional Watchdog stdout | Meaningful diagnostics; quiet unchanged observations |

There is no management token-stream or full-transcript view. Local ready Reports may leave Tickets open awaiting further input or explicit lifecycle handling; ready does not mean automatically accepted/ended. Record waiting and unknown states as observed.

Check these results:

- Two independent Tickets exist; exporter feedback reaches the original Ticket.
- The exporter uses `report.csv` and passes the requested tests.
- Starting a Job returns its ID before the command finishes.
- State and worktree changes survive restart. Uncertain commands are not repeated automatically.
- Stopping a Ticket prevents automatic resumption.

Record deviations as model-quality, routing, execution, or recovery issues. If recording a video, retain the raw terminal recording. Edited footage must preserve event order and disclose faux-provider use.
