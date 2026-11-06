# Local terminal experiment: the interrupted CSV exporter

This experiment uses internal Tickets and local files. GitHub, Feishu, and a public tunnel are unnecessary. It separates a repeatable controlled-provider starter from a real-model experiment: the starter verifies wiring; the real-model run also evaluates reasoning and tool use.

## Starter run and recording runbook

Start in the repository root with a POSIX shell, Node 22.19+ and installed dependencies. Use a 120-column, 32-row terminal. The captured shell uses `demo[exit-status] > ` as its prompt. No model key is required. Run each command to completion before entering the next; each successful command returns to the shell prompt.

```sh
npm run build
export DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/super-agent-demo.XXXXXX")"
node dist/cli.js --directory "$DEMO_DIR" demo > "$DEMO_DIR/result.json"
node -e 'const fs=require("node:fs");const r=JSON.parse(fs.readFileSync(process.env.DEMO_DIR+"/result.json"));console.log(JSON.stringify({provider:r.demo,tickets:r.tickets.length,feedbackSameTicket:r.feedbackSameTicket,routerDecisions:r.routerDecisions,consumedInputs:r.consumedInputs,reports:r.tickets.map(t=>t.latestReport.status)},null,2))'
node dist/cli.js --directory "$DEMO_DIR" list ticket
```

Expected result: native-faux provider, two Tickets, feedbackSameTicket true, and ready Reports. The demo starts real native Conversations/Tasks and SQLite, then closes. It does not execute the CSV experiment, use a paid model, or prove model quality. The final shell prompt and persisted Ticket inspection are the end of this recording. Keep DEMO_DIR to inspect the database later.

## Full real-model experiment

### Prepare

Use three terminals: service, operator, and monitor. Create a disposable committed Git repository with a tiny Node project and a test command. Keep primary state and broker state in persistent directories outside that repository. Configure the service using [Operations](../operations.md), with:

- An installed OpenAI/Anthropic model and an environment-referenced API key.
- Separate management operator and viewer credentials; a configured operator ID of `operator` for the example Policy below.
- Local environment repository and broker directory; trustedLocalShell enabled only for the disposable execution environment.
- No GitHub or Feishu configuration.

Start `node dist/cli.js --config service.json run` in terminal 1. Wait for the listening message before sending input. Model calls require internet access. Do not display credentials while recording, and keep shell tracing disabled.

In terminal 2, set BASE to `http://127.0.0.1:8787`. Install a Policy granting the configured operator explicit `repo.read`, `repo.write`, and `command.execute` with acceptMode observe. Use authenticated POST /policy with a unique commandId and a monotonically increasing Policy version. Leave channel.reply out for this local experiment: inspect structured Reports in the terminal instead of creating outbound intent without a local sender adapter. See [the API](../api.md) for authority semantics.

Submit requests to authenticated POST /receive. Each JSON event needs sourceId `cli`, a unique externalId, revision `1`, threads, body, receivedAt/occurredAt in epoch milliseconds, and addressed true. The API supplies the authenticated Actor; do not put credentials in the event. Use the same Thread string for follow-up and a different one for independent work.

### Act 1: Start the exporter

Send this request on Thread `local:csv`:

> Build a CSV exporter for an array of objects. Support commas, quotes, and embedded newlines. Add tests and run them through start_job. Use export.csv as the output filename. Report changed files, test results, and your Job ID. Do not make network calls or create a PR.

Wait until /tickets shows an accepted Ticket. The model may ask a clarification; answer on the same Thread before continuing. Record its Ticket ID.

### Act 2: Steer and introduce independent work

While the exporter is working, send another event on `local:csv`:

> Change the output filename to report.csv. Add Unicode and empty-input coverage. Preserve the earlier CSV escaping requirements.

Send a separate request on Thread `local:project-map`:

> Inspect this project and create docs/project-map.md describing entry points and how to run tests. Do not modify application code. Report your findings.

Observe two Tickets. The filename correction must belong to the original exporter Ticket, not a third Ticket. Inspect actual files in each Ticket's broker worktree; changes are not expected in the original checkout or automatically shared between Tickets.

### Act 3: A long Job and controller restart

On `local:csv`, request:

> Start one background Job that appends started to e2e-progress.txt, waits 90 seconds, then appends finished. Return the Job ID immediately and await its result. Never launch a replacement if the outcome is uncertain.

Wait for /taskGraph to show the Job and confirm the started file in that Ticket's worktree. Record the Job ID and file contents. Terminate the primary controller with SIGKILL using its verified PID, then restart the exact same configuration and directories.

Local subprocess recovery may conservatively become unknown after losing process supervision. That is a valid safety result, not a failed demo: the operation must not be blindly relaunched, and its reservation/attention must remain visible. Do not erase the broker ledger to make it run. To demonstrate reliable late-completion lookup across controller death, run the optional remote daemon separately and leave it alive; query-only reconciliation then observes its authoritative completion receipt without another command execution.

### Act 4: Explicit stop

Start a second long Job after resolving the prior outcome. Before it finishes, POST /tickets/URL_ENCODED_TICKET_ID/stop with an operator credential and a new commandId. Observe stopped lifecycle and cancelled or explicitly unknown execution accounting. Restarting must not resume the stopped Ticket. An already written started line is not rolled back. POST /retry is a separate authorized action creating a fresh Assignment/epoch.

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

There is no management token-stream or full-transcript view. Local ready Reports may leave Tickets open awaiting further input or explicit lifecycle handling; ready does not mean automatically accepted/ended. A legitimate waiting or unknown state must be explained, not edited away for the recording.

Success means two independent Tickets, same-Ticket steering, correct files/tests, nonblocking Job admission, persisted state after restart, no blind repeat of an uncertain command, preserved worktree changes, and a stop that prevents automatic resumption. Record observed deviations separately as model-quality, routing, execution, or recovery issues. For video, capture actual terminal bytes/timing and retain the raw recording; any later edited replay must preserve their causal order and disclose controlled-provider use.
