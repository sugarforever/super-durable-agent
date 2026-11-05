import { openSuperAgent } from '../../dist/runtime/open.js';
const [directory, url] = process.argv.slice(2);
const adapter = { sourceId: 'cli', enabled: true, listened: () => true,
  async queryReceipt(key) { return (await fetch(url + '/' + encodeURIComponent(key))).json(); },
  async send(message, key) {
    await (await fetch(url, { method: 'POST', body: JSON.stringify({ key, payload: message.payload }) })).json();
    process.kill(process.pid, 'SIGKILL'); await new Promise(() => {});
  },
};
const r = await openSuperAgent({ directory, outbox: { adapters: [adapter] } });
const actor = { id: 'alice', kind: 'human', trusted: true };
await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, actor, { commandId: 'policy' });
await r.receive({ sourceId: 'cli', externalId: 'crash', revision: '1', actor, threads: ['thread'], body: 'reply', receivedAt: Date.now(), occurredAt: Date.now(), addressed: true });
const [candidate] = await r.listCandidates();
await r.decide(candidate.id, candidate.epoch, { kind: 'accept', goal: 'reply', ruleIds: [] }, { commandId: 'accept' });
const [ticket] = await r.list('ticket');
await r.engine.transaction(tx => r.outbox.enqueueMessage(tx, { ticketId: ticket.id, mandateId: ticket.mandateId, assignmentEpoch: ticket.assignmentEpoch, destination: { sourceId: 'cli', threadRef: 'thread' }, purpose: 'answer', payload: { text: 'one real HTTP send' }, idempotencyKey: 'crash-send' }));
await new Promise(() => {});
