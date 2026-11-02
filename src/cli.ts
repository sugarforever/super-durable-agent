#!/usr/bin/env node
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { readFile } from "node:fs/promises";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  openSuperAgent,
  type SuperAgentRuntime,
  type OpenOptions,
} from "./runtime/open.js";
import type {
  NormalizedEvent,
  RuntimeMetadata,
  Policy,
  Ticket,
} from "./domain/model.js";
import { startManagementServer, redact } from "./api/server.js";
import { GitHubSource } from "./sources/github.js";

const help = `Super Durable Agent

Usage: super-durable-agent [options] <command>

Commands:
  init                 Initialize a durable directory and conservative policy.
  receive              Receive one normalized event JSON from standard input.
  list <kind>          List persisted records.
  show <kind> <id>     Show one persisted record.
  policy get           Read current policy.
  policy set           Apply policy JSON from standard input as local operator.
  serve / run          Start authenticated HTTP service; run requires a model.
  demo                 Run deterministic native Router/Worker/feedback demo.

Options:
  --directory PATH / --data-dir PATH (or SUPER_AGENT_DIRECTORY)
  --config PATH        JSON config containing options, credential references,
                       and optional local environment settings.
  --provider NAME      openai or anthropic (registers only the selected provider).
  --model MODEL_ID     Explicit model ID from the selected provider.
  --model-key-env NAME Optional provider API key environment reference.
  --host HOST          Default 127.0.0.1.
  --port PORT          Default 8787; 0 chooses a free port.
  --token-env NAME     Management operator token environment reference.
  --github-repo OWNER/REPO  Repeatable GitHub repository allowlist.
  --github-secret-env NAME  GitHub webhook secret environment reference.
  --github-token-env NAME   GitHub API token environment reference.
  --github-own-login LOGIN  Repeatable verified outbound account login; required to send.
  --listened-scope REF  Repeatable listened GitHub thread refs (owner/repo#number).

Init, receive, list, show, policy and demo need no model credentials.
Service closes gracefully on SIGINT or SIGTERM. Secrets are never printed.
`;
const operator = { id: "operator", kind: "human" as const, trusted: true };
interface Config {
  directory?: string;
  provider?: "openai" | "anthropic";
  model?: string;
  modelKeyEnv?: string;
  host?: string;
  port?: number;
  tokenEnv?: string;
  githubRepositories?: string[];
  githubSecretEnv?: string;
  githubTokenEnv?: string;
  githubOwnLogins?: string[];
  listenedScopes?: string[];
  environment?: {
    repository: string;
    directory?: string;
    trustedLocalShell?: boolean;
  };
}
const allowedConfig = new Set([
  "directory",
  "provider",
  "model",
  "modelKeyEnv",
  "host",
  "port",
  "tokenEnv",
  "githubRepositories",
  "githubSecretEnv",
  "githubTokenEnv",
  "githubOwnLogins",
  "listenedScopes",
  "environment",
]);
function localEnvironment(input: unknown): OpenOptions["environment"] {
  if (input === undefined) return undefined;
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw new Error("Invalid local environment config");
  const value = input as Record<string, unknown>;
  if (
    Object.keys(value).some(
      (key) => !["repository", "directory", "trustedLocalShell"].includes(key),
    ) ||
    typeof value.repository !== "string" ||
    !value.repository.trim() ||
    (value.directory !== undefined &&
      (typeof value.directory !== "string" || !value.directory.trim())) ||
    (value.trustedLocalShell !== undefined &&
      typeof value.trustedLocalShell !== "boolean")
  )
    throw new Error("Invalid local environment config");
  return {
    repository: value.repository,
    ...(value.directory === undefined
      ? {}
      : { directory: value.directory as string }),
    ...(value.trustedLocalShell === undefined
      ? {}
      : { trustedLocalShell: value.trustedLocalShell as boolean }),
  };
}
function reference(name: unknown): string {
  if (typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
    throw new Error("Expected an environment variable reference");
  return name;
}
function secret(name: string | undefined, required = false) {
  if (!name) {
    if (required)
      throw new Error("Required credential environment reference missing");
    return undefined;
  }
  const value = process.env[reference(name)];
  if (!value?.trim() && required)
    throw new Error(`Required environment reference is unset: ${name}`);
  return value;
}
async function stdinJson() {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of process.stdin) {
    const data = Buffer.from(chunk);
    bytes += data.length;
    if (bytes > 1048576) throw new Error("Input exceeds the 1 MiB limit");
    chunks.push(data);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
export async function configuredModels(
  config: Pick<Config, "provider" | "model" | "modelKeyEnv">,
): Promise<Pick<OpenOptions, "models" | "model">> {
  if (!config.provider && !config.model && !config.modelKeyEnv) return {};
  if (
    !config.provider ||
    !config.model?.trim() ||
    !["openai", "anthropic"].includes(config.provider)
  )
    throw new Error("Explicit supported provider and model required");
  const keyRef = config.modelKeyEnv ? reference(config.modelKeyEnv) : undefined;
  const canonical =
    config.provider === "openai" ? "OPENAI_API_KEY" : "ANTHROPIC_API_KEY";
  const models = createModels(
    keyRef
      ? {
          authContext: {
            env: async (name) =>
              name === canonical ? process.env[keyRef] : undefined,
            fileExists: async () => false,
          },
        }
      : {},
  );
  const provider =
    config.provider === "openai"
      ? (
          await import("@earendil-works/pi-ai/providers/openai")
        ).openaiProvider()
      : (
          await import("@earendil-works/pi-ai/providers/anthropic")
        ).anthropicProvider();
  models.setProvider(provider);
  if (!models.getModel(provider.id, config.model))
    throw new Error("Selected model is not in the installed provider catalog");
  return { models, model: { provider: provider.id, modelId: config.model } };
}
async function initialize(runtime: SuperAgentRuntime) {
  if (!(await runtime.engine.store.read("policy", "current")))
    await runtime.updatePolicy(
      {
        version: 1,
        acceptMode: "observe",
        allowedActors: ["operator"],
        botActors: [],
        rules: [],
      },
      operator,
      { commandId: "cli:init:policy" },
    );
}
async function demo(directory: string) {
  const { fauxProvider, fauxAssistantMessage, fauxToolCall } = await import(
    "@earendil-works/pi-ai/providers/faux"
  );
  const faux = fauxProvider(),
    models = createModels();
  models.setProvider(faux.provider);
  const response = async (ctx: any) => {
    if (ctx.messages.at(-1)?.role !== "user")
      return fauxAssistantMessage("Structured work recorded.");
    const user = ctx.messages.filter((m: any) => m.role === "user").at(-1);
    let input: any;
    try {
      input = JSON.parse(
        typeof user.content === "string" ? user.content : user.content[0].text,
      );
    } catch {
      return fauxAssistantMessage("Awaiting input.");
    }
    const router = ctx.messages.some(
      (m: any) =>
        m.role === "system" &&
        m.toolsAdded?.some((t: any) => t.name === "list_candidates"),
    );
    if (router)
      return input.candidates?.length
        ? fauxAssistantMessage(
            fauxToolCall("decide_batch", {
              decisions: input.candidates.map((c: any) => ({
                evalId: c.id,
                epoch: c.epoch,
                outcome: {
                  kind: "accept",
                  goal: `Demo task ${c.id}`,
                  ruleIds: [],
                },
              })),
            }),
            { stopReason: "toolUse" },
          )
        : fauxAssistantMessage("No new tasks.");
    return input.inputId
      ? fauxAssistantMessage(
          fauxToolCall("report", {
            reportId: `demo:${input.inputId}`,
            assignmentId: input.assignmentId,
            epoch: input.epoch,
            inputIds: [input.inputId],
            status: "ready",
            content: input.body?.includes("filename")
              ? "Applied filename feedback."
              : "Reviewable deterministic demo result.",
          }),
          { stopReason: "toolUse" },
        )
      : fauxAssistantMessage("No source input.");
  };
  faux.setResponses(Array.from({ length: 200 }, () => response));
  const runtime = await openSuperAgent({
    directory,
    models,
    model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
  });
  try {
    if ((await runtime.list("ticket")).length)
      throw new Error("Demo requires a fresh directory");
    await runtime.updatePolicy(
      {
        version: 1,
        acceptMode: "auto",
        allowedActors: ["operator"],
        botActors: [],
        rules: [],
      },
      operator,
      { commandId: "demo:policy" },
    );
    const event = (
      externalId: string,
      thread: string,
      body: string,
    ): NormalizedEvent => ({
      sourceId: "cli",
      externalId,
      revision: "1",
      actor: operator,
      threads: [thread],
      body,
      receivedAt: 1000,
      occurredAt: 900,
      addressed: true,
    });
    await runtime.receive(event("demo-request", "export", "Fix export"));
    await runtime.agents!.waitForIdle();
    const [first] = await runtime.list<Ticket>("ticket");
    if (!first) throw new Error("Demo Router did not accept initial request");
    const feedback = await runtime.receive(
      event("demo-feedback", "export", "Use the filename report.csv"),
    );
    await runtime.agents!.waitForIdle();
    const distinct = await runtime.receive(
      event("demo-distinct", "other", "Investigate a separate task"),
    );
    await runtime.agents!.waitForIdle();
    const tickets = await runtime.list<Ticket>("ticket");
    const disposition =
      "triggerId" in feedback
        ? await runtime.engine.store.read<any>(
            "disposition",
            feedback.triggerId,
          )
        : undefined;
    if (
      tickets.length !== 2 ||
      !tickets.every((t) => (t.latestReport as any)?.status === "ready") ||
      !disposition?.ownerTickets.includes(first.id)
    )
      throw new Error("Demo structured evidence did not settle");
    return {
      demo: "native-faux",
      feedbackSameTicket: disposition.ownerTickets.includes(first.id),
      distinctTaskStored: "triggerId" in distinct,
      tickets: tickets.map((t) => ({
        id: t.id,
        lifecycle: t.lifecycle,
        latestReport: t.latestReport,
      })),
      routerDecisions: (await runtime.list("decision")).length,
      consumedInputs: (await runtime.list<any>("deliveryRun")).filter(
        (d) => d.consumed,
      ).length,
    };
  } finally {
    await runtime.close();
  }
}
export async function main(args = process.argv.slice(2)): Promise<void> {
  const positional: string[] = [];
  let config: Config = {
    directory: process.env.SUPER_AGENT_DIRECTORY ?? ".super-durable-agent",
  };
  const overrides: Config = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--help" || arg === "-h") {
      process.stdout.write(help);
      return;
    }
    if (!arg.startsWith("-")) {
      positional.push(arg);
      continue;
    }
    const value = args[++i];
    if (!value || value.startsWith("--"))
      throw new Error(`${arg} requires a value`);
    if (arg === "--config") {
      const data = JSON.parse(await readFile(value, "utf8"));
      if (
        !data ||
        typeof data !== "object" ||
        Array.isArray(data) ||
        Object.keys(data).some((k) => !allowedConfig.has(k))
      )
        throw new Error(
          "Invalid service config; use environment references for credentials",
        );
      config = { ...config, ...data };
      continue;
    }
    const names: Record<string, keyof Config> = {
      "--directory": "directory",
      "--data-dir": "directory",
      "--provider": "provider",
      "--model": "model",
      "--model-key-env": "modelKeyEnv",
      "--host": "host",
      "--port": "port",
      "--token-env": "tokenEnv",
      "--github-secret-env": "githubSecretEnv",
      "--github-token-env": "githubTokenEnv",
    };
    if (
      arg === "--github-repo" ||
      arg === "--listened-scope" ||
      arg === "--github-own-login"
    ) {
      const key =
        arg === "--github-repo"
          ? "githubRepositories"
          : arg === "--github-own-login"
            ? "githubOwnLogins"
            : "listenedScopes";
      (overrides[key] ??= []).push(value);
    } else if (names[arg])
      Object.assign(overrides, {
        [names[arg]]: arg === "--port" ? Number(value) : value,
      });
    else throw new Error(`Unknown option: ${arg}`);
  }
  config = { ...config, ...overrides };
  const [command, kind, id] = positional;
  if (!command) {
    process.stdout.write(help);
    return;
  }
  const expected = ["init", "receive", "serve", "run", "demo"].includes(command)
    ? 1
    : command === "list" || command === "policy"
      ? 2
      : command === "show"
        ? 3
        : undefined;
  if (expected === undefined) throw new Error(`Unknown command: ${command}`);
  if (positional.length !== expected)
    throw new Error(`Invalid arguments for ${command}. Use --help.`);
  if (command === "policy" && !["get", "set"].includes(kind))
    throw new Error("Use policy get or policy set");
  if (typeof config.directory !== "string" || !config.directory.trim())
    throw new Error("Directory required");
  if (command === "demo") {
    process.stdout.write(
      JSON.stringify(await demo(config.directory), null, 2) + "\n",
    );
    return;
  }
  const service = command === "serve" || command === "run";
  if (service) {
    if (
      config.port !== undefined &&
      (!Number.isSafeInteger(config.port) ||
        config.port < 0 ||
        config.port > 65535)
    )
      throw new Error("Invalid management port");
    if (
      config.host !== undefined &&
      (typeof config.host !== "string" || !config.host.trim())
    )
      throw new Error("Invalid management host");
    for (const name of [
      config.tokenEnv,
      config.githubSecretEnv,
      config.githubTokenEnv,
      config.modelKeyEnv,
    ])
      if (name !== undefined) reference(name);
  }
  const environment = service
    ? localEnvironment(config.environment)
    : undefined;
  const modelOptions = service ? await configuredModels(config) : {};
  if (command === "run" && !modelOptions.model)
    throw new Error("run requires --provider and --model");
  const token = service
    ? secret(config.tokenEnv ?? "SUPER_AGENT_TOKEN", true)
    : undefined;
  const github =
    service && config.githubRepositories?.length
      ? new GitHubSource({
          repositories: config.githubRepositories,
          webhookSecret: secret(
            config.githubSecretEnv ?? "GITHUB_WEBHOOK_SECRET",
            true,
          )!,
          apiToken: secret(config.githubTokenEnv ?? "GITHUB_TOKEN"),
          listenedScopes: config.listenedScopes,
          ownLogins: config.githubOwnLogins,
        })
      : undefined;
  const event = command === "receive" ? await stdinJson() : undefined;
  const policy =
    command === "policy" && kind === "set"
      ? ((await stdinJson()) as Policy)
      : undefined;
  const runtime = await openSuperAgent({
    directory: config.directory,
    ...modelOptions,
    ...(environment ? { environment } : {}),
    ...(github ? { outbox: { adapters: [github] } } : {}),
  });
  try {
    let result: unknown;
    switch (command) {
      case "init":
        await initialize(runtime);
        result = {
          directory: resolve(config.directory),
          instanceId: (await runtime.engine.store.read<RuntimeMetadata>(
            "metadata",
            "runtime",
          ))!.instanceId,
        };
        break;
      case "receive":
        result = await runtime.receive(event);
        if ((result as any).disposition === "backpressure")
          process.exitCode = 1;
        break;
      case "list":
        result = await runtime.list(kind);
        break;
      case "show":
        result = await runtime.engine.store.read(kind, id);
        if (result === undefined)
          throw new Error(`Record not found: ${kind}/${id}`);
        break;
      case "policy":
        if (kind === "set") {
          await runtime.updatePolicy(policy!, operator, {
            commandId: `cli:policy:${policy!.version}`,
          });
          result = { ok: true, version: policy!.version };
        } else
          result =
            (await runtime.engine.store.read("policy", "current")) ?? null;
        break;
      case "serve":
      case "run": {
        await initialize(runtime);
        await github?.configure(runtime);
        const server = await startManagementServer(runtime, {
          host: config.host,
          port: config.port ?? 8787,
          token,
          github,
        });
        process.stdout.write(
          JSON.stringify({
            listening: server.address,
            model: modelOptions.model ?? null,
            tokenRef: `env:${config.tokenEnv ?? "SUPER_AGENT_TOKEN"}`,
            source: github?.sourceId ?? null,
          }) + "\n",
        );
        try {
          await new Promise<void>((resolve) => {
            const shutdown = () => {
              process.off("SIGINT", shutdown);
              process.off("SIGTERM", shutdown);
              resolve();
            };
            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
          });
        } finally {
          github?.close();
          await server.close();
        }
        return;
      }
    }
    process.stdout.write(JSON.stringify(redact(result), null, 2) + "\n");
  } finally {
    github?.close();
    await runtime.close();
  }
}
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
)
  main().catch((error: unknown) => {
    process.stderr.write(
      (error instanceof Error ? error.message : String(error)) + "\n",
    );
    process.exitCode = 1;
  });
