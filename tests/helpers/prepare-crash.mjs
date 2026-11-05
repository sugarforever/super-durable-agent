import { LocalEnvironmentBroker } from '../../dist/environments/local.js';
import { operationDigest } from '../../dist/environments/types.js';
const [directory, repository] = process.argv.slice(2);
const identity = { ticketId: 'ticket', assignmentId: 'assignment', epoch: 1, mandateId: 'mandate' };
const broker = new LocalEnvironmentBroker({ directory, repository, authorize: async () => ({ ...identity, capabilities: ['repo.read'] }) });
const atomic = broker.atomic.bind(broker);
broker.atomic = async (path, ...args) => {
  if (path.endsWith('.super-agent-environment.json')) process.kill(process.pid, 'SIGKILL');
  return atomic(path, ...args);
};
const payload = { kind: 'prepare' };
await broker.prepare({ operationId: 'prepare', identity, payload, payloadDigest: operationDigest(identity, payload) });
throw new Error('Expected SIGKILL before marker publication');
