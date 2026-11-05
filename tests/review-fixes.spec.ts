import { afterEach, expect, it, vi } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { openSuperAgent } from '../src/runtime/open.js';
import { GitHubSource } from '../src/sources/github.js';
import { FeishuSource } from '../src/sources/feishu.js';
import { accept, alice, event, isRouter, until } from './helpers/scheduling.js';
const cleanup: (() => Promise<unknown>)[] = [];
afterEach(async () => { vi.unstubAllGlobals(); for (const close of cleanup.splice(0).reverse()) await close(); });

for (const recoverLegacy of [false, true]) it(`retains ended-Ticket context without intake leakage, including native ignore and natural resume (legacy=${recoverLegacy})`, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'sa-passive-context-'));
  cleanup.push(() => rm(directory, { recursive: true, force: true }));
  let r = await openSuperAgent({ directory, maxPending: 2 });
  cleanup.push(() => r.close());
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  const ticket = await accept(r, 'owned');
  const initial = (await r.list<any>('trigger'))[0];
  await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, ticket.id, initial.id, 1));
  await r.subscribe(ticket.id, 'cli', 'context', false, { commandId: 'sub' });
  await r.endTicket(ticket.id, 1, { commandId: 'end' });
  const received = await r.receive(event('context-one', ['context']));
  if (recoverLegacy) await r.engine.transaction(async tx => {
    const delivery = (await r.engine.store.listInTransaction<any>(tx, 'delivery')).find(d => d.triggerId === received.triggerId)!;
    await r.engine.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'pending' });
  });
  await r.close();
  const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
  const workerInputs: any[] = [];
  faux.setResponses(Array.from({ length: 100 }, () => (ctx: any) => {
    if (ctx.messages.at(-1).role !== 'user') return fauxAssistantMessage('Settled');
    const content = ctx.messages.filter((m: any) => m.role === 'user').at(-1).content;
    const input = JSON.parse(typeof content === 'string' ? content : content[0].text);
    if (isRouter(ctx)) return input.candidates?.length ? fauxAssistantMessage(fauxToolCall('decide_batch', { decisions: input.candidates.map((c: any) => ({ evalId: c.id, epoch: c.epoch, outcome: { kind: 'ignore', reason: 'Context only' } })) }), { stopReason: 'toolUse' }) : fauxAssistantMessage('No decision');
    workerInputs.push(input);
    return fauxAssistantMessage(fauxToolCall('report', { reportId: `consumed-${input.inputId}`, assignmentId: input.assignmentId, epoch: input.epoch, inputIds: [input.inputId], status: 'waiting', content: 'Consumed' }), { stopReason: 'toolUse' });
  }));
  const options = { directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id } };
  r = await openSuperAgent(options); await r.agents!.waitForIdle();
  expect(await r.listCandidates()).toHaveLength(0);
  expect(workerInputs).toHaveLength(0);
  expect(await r.readTicket(ticket.id)).toMatchObject({ lifecycle: 'ended', assignmentEpoch: 1 });
  expect((await r.engine.store.read<any>('state', 'intake'))!.count).toBe(0);
  expect((await r.list<any>('delivery')).filter(d => d.triggerId === received.triggerId && d.status === 'pending')).toHaveLength(0);
  expect((await r.list<any>('disposition')).find(d => d.triggerId === received.triggerId)?.contextTickets).toContain(ticket.id);
  await r.close(); r = await openSuperAgent(options); await r.agents!.waitForIdle();
  expect((await r.receive(event('context-two', ['context']))).disposition).toBe('stored'); await r.agents!.waitForIdle();
  expect((await r.receive(event('resume-owned', ['owned']))).disposition).toBe('stored'); await r.agents!.waitForIdle();
  expect(await r.readTicket(ticket.id)).toMatchObject({ lifecycle: 'open', assignmentEpoch: 2 });
  expect(workerInputs).toHaveLength(1); expect(workerInputs[0].epoch).toBe(2);
  expect((await r.engine.store.read<any>('state', 'intake'))!.count).toBe(0);
  expect((await r.list<any>('delivery')).filter(d => d.status === 'pending')).toHaveLength(0);
  const recalled = await r.agents!.recall((await r.readTicket<any>(ticket.id)).workerConversationId, received.triggerId!);
  expect(JSON.stringify(recalled)).toContain('context-one');
});

function adapter(kind: 'github' | 'feishu', enabled: boolean, listenedScopes?: string[]) {
  return kind === 'github'
    ? new GitHubSource({ repositories: ['acme/repo'], webhookSecret: 'secret', enabled, ownLogins: ['agent'], listenedScopes })
    : new FeishuSource({ verificationToken: 'secret', encryptKey: 'secret', appId: 'app', chats: ['chat'], enabled, ownSenderIds: ['bot'], accessTokenProvider: async () => 'token', listenedScopes });
}
for (const kind of ['github', 'feishu'] as const) for (const removed of [false, true]) it(`${kind}: disabled reopen defers pending replies; reenable ${removed ? 'suppresses removed scopes' : 'sends once'}`, async () => {
  const posts: any[] = [];
  vi.stubGlobal('fetch', async (_url: any, init: any) => {
    if (init?.method === 'POST') { posts.push(JSON.parse(init.body)); return new Response(JSON.stringify(kind === 'github' ? { id: 1 } : { code: 0, data: { message_id: 'receipt' } }), { status: 201 }); }
    return new Response('[]', { status: 200 });
  });
  const directory = await mkdtemp(join(tmpdir(), 'sa-real-adapter-disabled-'));
  cleanup.push(() => rm(directory, { recursive: true, force: true }));
  let r = await openSuperAgent({ directory }); cleanup.push(() => r.close());
  const ref = kind === 'github' ? 'acme/repo#1' : 'chat/message/msg';
  await r.engine.configureSource({ id: kind, kind, enabled: true });
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  await r.receive({ ...event('reply'), sourceId: kind, threads: [ref] });
  const [candidate] = await r.listCandidates(); await r.decide(candidate.id, candidate.epoch, { kind: 'accept', goal: 'Reply', ruleIds: [] }, { commandId: 'accept' });
  const [ticket] = await r.list<any>('ticket');
  await r.engine.transaction(tx => r.outbox.enqueueMessage(tx, { ticketId: ticket.id, mandateId: ticket.mandateId, assignmentEpoch: 1, purpose: 'answer', destination: { sourceId: kind, threadRef: ref }, payload: { content: 'Result' }, idempotencyKey: 'reply' }));
  await r.close();
  const disabled = adapter(kind, false); cleanup.push(async () => disabled.close());
  r = await openSuperAgent({ directory, startPaused: true, outbox: { adapters: [disabled] } }); await disabled.configure(r); await r.activate();
  await until(async () => ['pending', 'dropped'].includes((await r.list<any>('outbox'))[0]?.status) && !!(await r.list<any>('outbox'))[0]?.error);
  expect((await r.list<any>('outbox'))[0]).toMatchObject({ status: 'pending', attempt: 0, error: 'Source is disabled' }); expect(posts).toHaveLength(0);
  await r.close();
  const enabled = adapter(kind, true, removed ? [] : undefined); cleanup.push(async () => enabled.close());
  r = await openSuperAgent({ directory, startPaused: true, outbox: { adapters: [enabled] } }); await enabled.configure(r);
  await r.engine.transaction(async tx => { const [m] = await r.engine.store.listInTransaction<any>(tx, 'outbox'); await r.engine.store.put(tx, 'outbox', m.id, { ...m, nextAttemptAt: 0 }); });
  await r.outbox.reconcile(); await r.activate(); await until(async () => (await r.list<any>('outbox'))[0]?.status === (removed ? 'dropped' : 'sent'));
  expect(posts).toHaveLength(removed ? 0 : 1); await r.outbox.reconcile(); expect(posts).toHaveLength(removed ? 0 : 1);
});

it('renders GitHub question text and every option alongside report content in the actual comment POST', async () => {
  const posts: any[] = [];
  vi.stubGlobal('fetch', async (_url: any, init: any) => { posts.push(JSON.parse(init.body)); return new Response('{"id":1}', { status: 201 }); });
  const source = adapter('github', true); cleanup.push(async () => source.close());
  await source.send({ purpose: 'question', destination: { sourceId: 'github', threadRef: 'acme/repo#1' }, payload: { status: 'waiting', content: 'I need one decision.', questions: [{ id: 'branch', text: 'Which branch should I use?', options: ['main', 'release'] }, { id: 'confirm', text: 'Should I continue?' }] } } as any, 'question-key');
  for (const text of ['I need one decision.', 'Which branch should I use?', 'main', 'release', 'Should I continue?']) expect(posts[0].body).toContain(text);
  expect(posts[0].body).toContain('<!-- super-agent:');
});

it('repairs persisted old-epoch passive context after natural resumption, even without an agent model', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'sa-old-passive-context-'));
  cleanup.push(() => rm(directory, { recursive: true, force: true }));
  let r = await openSuperAgent({ directory, maxPending: 2 }); cleanup.push(() => r.close());
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  const ticket = await accept(r, 'owned');
  const initial = (await r.list<any>('trigger'))[0];
  await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, ticket.id, initial.id, 1));
  await r.subscribe(ticket.id, 'cli', 'context', false, { commandId: 'sub' });
  await r.endTicket(ticket.id, 1, { commandId: 'end' });
  const passive = await r.receive(event('old-context', ['context']));
  const [candidate] = await r.listCandidates();
  await r.decide(candidate.id, candidate.epoch, { kind: 'ignore', reason: 'Passive' }, { commandId: 'ignore' });
  await r.engine.transaction(async tx => {
    const delivery = (await r.engine.store.listInTransaction<any>(tx, 'delivery')).find(d => d.triggerId === passive.triggerId)!;
    const intake = (await r.engine.store.get<any>(tx, 'intake', passive.triggerId!))!;
    const state = (await r.engine.store.get<any>(tx, 'state', 'intake'))!;
    await r.engine.store.put(tx, 'delivery', delivery.id, { ...delivery, status: 'pending' });
    await r.engine.store.put(tx, 'intake', intake.id, { ...intake, released: false });
    await r.engine.store.put(tx, 'state', 'intake', { ...state, count: 1 });
  });
  const resume = await r.receive(event('resume', ['owned']));
  expect(await r.readTicket(ticket.id)).toMatchObject({ lifecycle: 'open', assignmentEpoch: 2 });
  await r.close(); r = await openSuperAgent({ directory });
  expect((await r.list<any>('delivery')).find(d => d.triggerId === passive.triggerId)).toMatchObject({ epoch: 1, own: false, status: 'retained' });
  expect((await r.engine.store.read<any>('state', 'intake'))!.count).toBe(1);
  await r.engine.transaction(tx => r.engine.authorization.acknowledgeInputInTransaction(tx, ticket.id, resume.triggerId!, 2));
  expect((await r.engine.store.read<any>('state', 'intake'))!.count).toBe(0);
  expect((await r.receive(event('new-after-repair'))).disposition).toBe('stored');
});

for (const knownReceipt of [true, false]) it(`suppresses legacy GitHub question poll echoes without new Triggers or Worker deliveries (known receipt=${knownReceipt})`, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'sa-legacy-question-echo-'));
  cleanup.push(() => rm(directory, { recursive: true, force: true }));
  const r = await openSuperAgent({ directory }); cleanup.push(() => r.close());
  const source = new GitHubSource({ repositories: ['acme/repo'], webhookSecret: 'secret', ownLogins: ['agent-user'] });
  cleanup.push(async () => source.close()); await source.configure(r);
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  await r.receive({ ...event('initial'), sourceId: 'github', threads: ['acme/repo#1'] });
  const [candidate] = await r.listCandidates(); await r.decide(candidate.id, candidate.epoch, { kind: 'accept', goal: 'Reply', ruleIds: [] }, { commandId: 'accept' });
  const [ticket] = await r.list<any>('ticket');
  await r.engine.transaction(async tx => {
    const message = await r.outbox.enqueueMessage(tx, { ticketId: ticket.id, mandateId: ticket.mandateId, assignmentEpoch: 1, purpose: 'question', destination: { sourceId: 'github', threadRef: 'acme/repo#1' }, payload: { content: 'I need one decision.', questions: [{ id: 'branch', text: 'Which branch?', options: ['main', 'release'] }] }, idempotencyKey: 'legacy-question' });
    if (knownReceipt) await r.engine.store.put(tx, 'outbox', message.id, { ...message, status: 'sent', receipt: { id: 42 } });
  });
  const legacyBody = `## Agent question\n\nI need one decision.\n\n${source.marker('legacy-question')}`;
  const own = { login: 'agent-user', type: 'User' }, foreign = { login: 'alice', type: 'User' };
  const commentEvent = (patch: any = {}) => ({ event: 'issue_comment', payload: {
    action: patch.action ?? 'created', repository: { full_name: 'acme/repo' }, sender: patch.sender ?? own,
    issue: { id: patch.issueNumber ?? 1, number: patch.issueNumber ?? 1, title: 'Work' },
    comment: { id: patch.id ?? 42, user: patch.user ?? patch.sender ?? own, body: patch.body ?? legacyBody, created_at: '2026-01-01T00:01:00Z', ...(patch.action === 'edited' ? { updated_at: '2026-01-01T00:02:00Z' } : {}) },
  } });
  const triggers = await r.list('trigger'), deliveries = await r.list('delivery');
  expect((await source.receivePoll(r, [commentEvent()], { page: 1 })).results).toEqual([]);
  expect(await r.list('trigger')).toEqual(triggers); expect(await r.list('delivery')).toEqual(deliveries);
  const controls = [
    { id: 43, sender: foreign },
    { id: 44, body: `Human feedback with copied marker ${source.marker('legacy-question')}` },
    { id: 42, action: 'edited' },
    { id: 45, issueNumber: 2 },
    { id: 46, sender: foreign, user: own },
    ...(knownReceipt ? [{ id: 47 }] : []),
  ];
  for (const [index, control] of controls.entries()) {
    expect((await source.receivePoll(r, [commentEvent(control)], { page: index + 2 })).results).toMatchObject([{ disposition: 'stored' }]);
    expect(await r.list('trigger')).toHaveLength(triggers.length + index + 1);
  }
  expect((await r.list<any>('delivery')).some(delivery => !deliveries.some((prior: any) => prior.id === delivery.id) && delivery.ticketId === ticket.id && delivery.own && delivery.status === 'pending')).toBe(true);
});
