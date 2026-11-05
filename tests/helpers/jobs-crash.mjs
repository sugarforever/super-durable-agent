import { openSuperAgent } from '../../dist/runtime/open.js';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { LocalEnvironmentBroker } from '../../dist/environments/local.js';
import { join } from 'node:path';
import { readFile } from 'node:fs/promises';
const [directory, repository, counter, mode] = process.argv.slice(2);
const alice = { id: 'alice', kind: 'human', trusted: true };
let runtime = await openSuperAgent({ directory });
await runtime.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], explicitCapabilities: ['repo.read','command.execute'], rules: [] }, alice, { commandId: 'policy' });
await runtime.receive({ sourceId: 'cli', externalId: 'job', revision: '1', actor: alice, threads: ['work'], body: 'work', receivedAt: 1, occurredAt: 1, addressed: true });
const [candidate] = await runtime.listCandidates();
await runtime.decide(candidate.id, 1, { kind: 'accept', goal: 'work', ruleIds: [] }, { commandId: 'accept' });
const [ticket] = await runtime.list('ticket'); await runtime.close();
const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
let first = true;
faux.setResponses(Array.from({ length: 30 }, () => () => {
  if (!first) return fauxAssistantMessage('Job started');
  first = false;
  const script = `require('node:fs').appendFileSync(${JSON.stringify(counter)}, 'x'); setTimeout(() => {}, 500)`;
  const command = `node -e '${script.replaceAll("'", "'\\''")}'`;
  return fauxAssistantMessage(fauxToolCall('start_job', { assignmentId: ticket.assignmentId, epoch: 1, command }), { stopReason: 'toolUse' });
}));
const endpoint = mode === 'preendpoint' ? new LocalEnvironmentBroker({ directory: join(directory, 'environments'), repository, trustedLocalShell: true, authorize: async identity => ({ ...identity, capabilities: ['repo.read','command.execute'] }) }) : undefined;
if (endpoint) endpoint.execute = async () => { process.kill(process.pid, 'SIGKILL'); await new Promise(() => {}); };
runtime = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, environment: endpoint ? { broker: endpoint } : { repository, trustedLocalShell: true } });
for (;;) {
  try { if (await readFile(counter, 'utf8') === 'x') process.kill(process.pid, 'SIGKILL'); } catch {}
  await new Promise(resolve => setTimeout(resolve, 1));
}
