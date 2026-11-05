import { openSuperAgent } from "../../dist/runtime/open.js";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
} from "@earendil-works/pi-ai/providers/faux";
const [directory, mode] = process.argv.slice(2);
const alice = { id: "alice", kind: "human", trusted: true };
const r = await openSuperAgent({ directory });
await r.updatePolicy(
  {
    version: 1,
    acceptMode: "auto",
    allowedActors: ["alice"],
    botActors: [],
    rules: [],
  },
  alice,
  { commandId: "policy" },
);
await r.receive({
  sourceId: "cli",
  externalId: "one",
  revision: "1",
  actor: alice,
  threads: ["one"],
  body: "Durable crash input",
  receivedAt: 1,
  occurredAt: 1,
  addressed: true,
});
const [c] = await r.listCandidates();
await r.decide(
  c.id,
  c.epoch,
  { kind: "accept", goal: "crash", ruleIds: [] },
  { commandId: "accept" },
);
await r.close();
const faux = fauxProvider(),
  models = createModels();
models.setProvider(faux.provider);
faux.setResponses([
  async () => {
    if (mode === "provider") process.kill(process.pid, "SIGKILL");
    return fauxAssistantMessage("Done");
  },
]);
const active = await openSuperAgent({
  directory,
  models,
  model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
  preflight: async (engine) => {
    if (mode === "admission")
      engine.harness.subscribeCommits((publication) => {
        if (
          publication.changes.some(
            (c) =>
              c.type === "submission" &&
              c.value.type === "input" &&
              c.value.requestId?.startsWith("sa:"),
          )
        )
          process.kill(process.pid, "SIGKILL");
      });
  },
});
await active.agents.waitForIdle();
throw new Error("Crash fixture survived");
