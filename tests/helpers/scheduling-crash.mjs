import { openSuperAgent } from '../../dist/runtime/open.js';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider } from '@earendil-works/pi-ai/providers/faux';
const directory = process.argv[2], mode = process.argv[3], alice = { id: 'alice', kind: 'human', trusted: true };
const r = await openSuperAgent({ directory });
await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
for (let i = 0; i < (mode === 'preadmission' ? 1 : 4); i++) {
  const id = `crash-${i}`;
  await r.receive({ sourceId: 'cli', externalId: id, revision: '1', actor: alice, threads: [id], body: id, receivedAt: 1, occurredAt: 1, addressed: true });
  const candidate = (await r.listCandidates())[0];
  await r.decide(candidate.id, candidate.epoch, { kind: 'accept', goal: id, ruleIds: [] }, { commandId: id });
}
await r.close();
const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
let started = 0;
faux.setResponses(Array.from({ length: 10 }, () => async () => {
  if (++started === 3) process.kill(process.pid, 'SIGKILL');
  await new Promise(() => {});
}));
const active = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, preflight: async engine => {
  if (mode === 'preadmission') engine.harness.subscribeCommits(publication => {
    // Session publication occurs after the native Control task and frozen input
    // are durably committed, before that task can admit a Worker submission.
    if (publication.changes.some(change => change.type === 'task' && change.value.kind === 'sa.agentInput')) process.kill(process.pid, 'SIGKILL');
  });
} });
await active.agents.waitForIdle();
throw new Error('Crash fixture survived');
