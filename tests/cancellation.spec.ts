import { afterEach, describe, expect, it } from 'vitest';
import { fixture, until, alice, event } from './helpers/scheduling.js';
const fixtures: Awaited<ReturnType<typeof fixture>>[] = [];
afterEach(async () => { for (const f of fixtures.splice(0)) await f.close(); });
describe('Ticket cancellation fences domain authority', () => {
  it('persists revocation and invalidation before abort; only its own native Worker is cancelled', async () => {
    const f = await fixture(2); fixtures.push(f);
    await until(async () => f.calls.length === 2);
    const [a, b] = await f.r.list<any>('ticket');
    const [run] = (await f.r.list<any>('deliveryRun')).filter(x => x.ticketId === a.id);
    const worker = await f.r.harness.conversation(a.workerConversationId, f.r.engine.context);
    const backgroundId = await worker!.commit(async tx => {
      const id = 'worker-background-input';
      const taskId = await tx.createTask(f.r.agents!.inputTask, { kind: 'deliveryRun', id }, { ownership: { kind: 'conversation' }, background: true });
      const background = { ...run, id, inputId: id, requestId: 'sa:worker-background:1', taskId, status: 'pending', admitted: false, placed: false, settled: false, consumed: false };
      delete background.submissionId; delete background.intentId;
      await f.r.engine.store.put(tx, 'deliveryRun', id, background);
      return taskId;
    }, f.r.engine.context);
    await until(async () => (await f.r.engine.store.read<any>('deliveryRun', 'worker-background-input'))?.admitted);
    await f.r.engine.transaction(async tx => { await f.r.engine.store.put(tx, 'outbox', 'pending-message', { id: 'pending-message', ticketId: a.id, status: 'pending' }); });
    await f.r.stopTicket(a.id, alice, { commandId: 'stop-a' });
    const stopped = await f.r.readTicket<any>(a.id);
    expect(stopped).toMatchObject({ lifecycle: 'stopped', assignmentEpoch: a.assignmentEpoch + 1, cancelPending: true });
    expect((await f.r.list<any>('outbox'))[0].status).toBe('invalid');
    await expect(f.r.agents!.report(a.workerConversationId, { reportId: 'late', assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [run.id], status: 'ready', content: {} })).rejects.toThrow('Stale');
    expect(await f.r.list('report')).toHaveLength(0);
    await f.r.agents!.pump();
    await until(async () => f.calls[0].signal.aborted);
    expect(f.calls[1].signal.aborted).toBe(false);
    expect((await f.r.list<any>('cancel'))[0]).toMatchObject({ status: 'pending' });
    f.release(); await f.r.agents!.waitForIdle();
    expect((await f.r.harness.getTask(backgroundId, f.r.engine.context))?.state).toMatchObject({ status: 'terminal', outcome: { status: 'aborted' } });
    expect((await f.r.readTicket<any>(b.id)).lifecycle).toBe('open');
    expect((await f.r.readTicket<any>(a.id)).cancelPending).toBe(false);
    expect((await f.r.list<any>('cancel'))[0]).toMatchObject({ status: 'completed', nativeConfirmed: true, endpointConfirmed: true });
    expect((await f.r.list<any>('reservation')).filter(x => x.ticketId === a.id && x.status !== 'released')).toHaveLength(0);
  });
  it('requires an authenticated explicit retry and keeps stopped work stopped across pump', async () => {
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0];
    await expect(f.r.stopTicket(a.id, { ...alice, trusted: 'true' as any }, { commandId: 'forged-stop' })).rejects.toThrow('trusted');
    await f.r.stopTicket(a.id, alice, { commandId: 'stop' });
    f.release(); await f.r.agents!.waitForIdle(); await f.r.agents!.pump();
    expect((await f.r.readTicket<any>(a.id)).lifecycle).toBe('stopped');
    expect(f.calls).toHaveLength(1);
    await expect(f.r.retryTicket(a.id, { ...alice, trusted: false }, { commandId: 'forged-retry' })).rejects.toThrow('trusted');
    await f.r.retryTicket(a.id, alice, { commandId: 'retry' });
    await f.r.retryTicket(a.id, alice, { commandId: 'retry' });
    await f.r.agents!.waitForIdle();
    const retried = await f.r.readTicket<any>(a.id);
    expect(retried).toMatchObject({ lifecycle: 'open', assignmentEpoch: a.assignmentEpoch + 2, workerConversationId: a.workerConversationId });
    expect(retried.assignmentId).not.toBe(a.assignmentId);
    expect((await f.r.list<any>('deliveryRun')).filter(x => x.epoch === retried.assignmentEpoch)).toHaveLength(1);
  });
  it('supports all structured outcome kinds and questions remain waiting for a person', async () => {
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0];
    const run = (await f.r.list<any>('deliveryRun'))[0];
    await until(async () => !!(await f.r.engine.transaction(tx => tx.submissionByRequest(run.conversationId, run.requestId)))?.entry);
    for (const status of ['progress','waiting','ready','blocked','failed','dropped'] as const) {
      await f.r.agents!.report(a.workerConversationId, { reportId: status, assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [run.id], status, content: { summary: status } });
      expect((await f.r.readTicket<any>(a.id)).lifecycle).toBe('open');
      expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(1);
    }
    await f.r.agents!.report(a.workerConversationId, { reportId: 'question', assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [], status: 'ready', questions: [{ id: 'choice', text: 'Which branch?' }], content: {} });
    expect((await f.r.readTicket<any>(a.id)).latestReport).toMatchObject({ status: 'waiting', waitingFor: 'human', questions: [{ id: 'choice', text: 'Which branch?' }] });
    f.release(); await f.r.agents!.waitForIdle();
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(0);
  });
});

describe('endpoint cancellation accounting', () => {
  it('quarantines unknown outcomes across restart, and old-epoch receipt releases accounting only', async () => {
    const { trackExecution, reconcile, recordExecutionReceipt } = await import('../src/runtime/reconcile.js');
    const { openSuperAgent } = await import('../src/runtime/open.js');
    let requests: any[] = [];
    const handler = async (request: any) => { requests.push(request); return { ...request, outcome: 'unknown' as const }; };
    const f = await fixture(1, { cancellationHandler: handler }); fixtures.push(f);
    await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0];
    const job = { id: 'remote-job', ticketId: a.id, assignmentId: a.assignmentId, epoch: a.assignmentEpoch, status: 'running' as const };
    await f.r.engine.transaction(tx => trackExecution(f.r.engine, tx, job));
    await f.r.stopTicket(a.id, alice, { commandId: 'stop-remote' });
    f.release(); await f.r.agents!.waitForIdle();
    expect(requests).toHaveLength(3);
    expect(new Set(requests.map(x => x.cancelKey))).toHaveLength(1);
    expect(new Set(requests.map(x => x.payloadDigest))).toHaveLength(1);
    expect((await f.r.list<any>('reservation'))[0].status).toBe('quarantined');
    await f.r.close();
    f.r = await openSuperAgent({ directory: f.directory, models: f.models, model: f.model, cancellationHandler: handler });
    expect((await f.r.list<any>('reservation'))[0].status).toBe('quarantined');
    await f.r.retryTicket(a.id, alice, { commandId: 'explicit-retry' });
    await f.r.agents!.pump();
    const before = await f.r.readTicket<any>(a.id);
    expect(before.assignmentEpoch).toBe(a.assignmentEpoch + 2);
    expect(f.calls).toHaveLength(1);
    const cancel = (await f.r.list<any>('cancel'))[0];
    await expect(f.r.engine.transaction(tx => f.r.agents!.cancellation.receipt(tx, cancel.id, { ...requests[0], payloadDigest: 'wrong', outcome: 'confirmed' }))).rejects.toThrow('identity');
    await f.r.engine.transaction(tx => f.r.agents!.cancellation.receipt(tx, cancel.id, { ...requests[0], outcome: 'confirmed' }));
    await reconcile(f.r.engine);
    expect(await f.r.readTicket<any>(a.id)).toMatchObject({ assignmentId: before.assignmentId, assignmentEpoch: before.assignmentEpoch, lifecycle: 'open' });
    expect(await f.r.list('report')).toHaveLength(0);
    expect(await f.r.list('outbox')).toHaveLength(0);
    expect((await f.r.list<any>('reservation')).find(x => x.epoch === a.assignmentEpoch).status).toBe('released');
    // A late Job completion is the same accounting-only path and cannot consume inputs.
    await f.r.engine.transaction(tx => recordExecutionReceipt(f.r.engine, tx, { ...job, status: 'completed' }));
    await f.r.agents!.waitForIdle();
    expect(f.calls).toHaveLength(2);
  });
  it('ready and failed Reports retain capacity for a live Job after native generation settles', async () => {
    const { trackExecution, reconcile, recordExecutionReceipt } = await import('../src/runtime/reconcile.js');
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0], run = (await f.r.list<any>('deliveryRun'))[0];
    const job = { id: 'long-job', ticketId: a.id, assignmentId: a.assignmentId, epoch: a.assignmentEpoch, status: 'running' as const };
    await f.r.engine.transaction(tx => trackExecution(f.r.engine, tx, job));
    await f.r.agents!.report(a.workerConversationId, { reportId: 'job-ready', assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [run.id], status: 'ready', content: {} });
    f.release(); await f.r.agents!.waitForIdle();
    await f.r.agents!.report(a.workerConversationId, { reportId: 'job-failed', assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [], status: 'failed', content: {} });
    await reconcile(f.r.engine);
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(1);
    expect((await f.r.readTicket<any>(a.id)).lifecycle).toBe('open');
    await expect(f.r.engine.transaction(tx => recordExecutionReceipt(f.r.engine, tx, { ...job, epoch: 999, status: 'completed' }))).rejects.toThrow('identity');
    await f.r.engine.transaction(tx => recordExecutionReceipt(f.r.engine, tx, { ...job, status: 'completed' }));
    await reconcile(f.r.engine);
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(0);
  });
});

describe('structured Report consumption remains per owner', () => {
  it('questions and blocked reports preserve an unread sibling owner', async () => {
    const f = await fixture(2); fixtures.push(f); await until(async () => f.calls.length === 2);
    const received = await f.r.receive(event('shared-feedback', ['ticket-0', 'ticket-1']));
    await f.r.agents!.pump(); f.release(); await f.r.agents!.waitForIdle();
    const tickets = await f.r.list<any>('ticket');
    const run = (await f.r.list<any>('deliveryRun')).find(x => x.ticketId === tickets[0].id && x.triggerId === (received as any).triggerId);
    await f.r.agents!.report(tickets[0].workerConversationId, { reportId: 'question-unread', assignmentId: tickets[0].assignmentId, epoch: tickets[0].assignmentEpoch, inputIds: [run.id], status: 'blocked', questions: [{ id: 'clarify', text: 'Please clarify acceptance.' }], content: {} });
    expect(await f.r.engine.store.read('disposition', (received as any).triggerId)).toMatchObject({ acknowledged: false, consumedTicketIds: [tickets[0].id] });
    expect((await f.r.readTicket<any>(tickets[0].id)).latestReport).toMatchObject({ status: 'waiting', waitingFor: 'human' });
    expect((await f.r.list<any>('deliveryRun')).find(x => x.ticketId === tickets[1].id && x.triggerId === (received as any).triggerId).consumed).toBe(false);
  });
});

describe('retry while execution is live', () => {
  it('fences the old assignment, cancels it, then admits the retained input with fresh authority', async () => {
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0];
    await f.r.retryTicket(a.id, alice, { commandId: 'retry-live' });
    const next = await f.r.readTicket<any>(a.id);
    expect(next.assignmentEpoch).toBe(a.assignmentEpoch + 1);
    await f.r.agents!.pump();
    await until(async () => f.calls[0].signal.aborted);
    expect(f.calls).toHaveLength(1);
    let releaseNext!: () => void;
    const nextGate = new Promise<void>(resolve => releaseNext = resolve);
    const { fauxAssistantMessage } = await import('@earendil-works/pi-ai/providers/faux');
    f.faux.setResponses([async (ctx: any, options: any) => { f.calls.push({ ctx, signal: options.signal }); await nextGate; return fauxAssistantMessage('New assignment'); }]);
    try {
      f.release();
      await until(async () => { await f.r.agents!.pump(); return f.calls.length === 2; });
      const current = await f.r.readTicket<any>(a.id);
      expect(current).toMatchObject({ lifecycle: 'open', assignmentEpoch: next.assignmentEpoch, workerConversationId: a.workerConversationId, cancelPending: false });
      const authority = await f.r.engine.store.read<any>('workerContext', a.id);
      expect(authority.mandateId).toBe(current.mandateId);
      expect(authority.summary.mandate.withdrawnAt).toBe(null);
      const cancel = (await f.r.list<any>('cancel'))[0];
      const request = f.r.agents!.cancellation.request(cancel);
      await f.r.engine.transaction(tx => f.r.agents!.cancellation.receipt(tx, cancel.id, { ...request, outcome: 'confirmed' }));
      const { reconcile } = await import('../src/runtime/reconcile.js');
      await reconcile(f.r.engine);
      expect((await f.r.list<any>('reservation')).find(x => x.epoch === a.assignmentEpoch).status).toBe('released');
      expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toMatchObject([{ epoch: next.assignmentEpoch, status: 'active' }]);
      expect(f.calls[1].signal.aborted).toBe(false);
    } finally { releaseNext(); await f.r.agents!.waitForIdle(); }
  });
});

describe('native Report tool outcome schema', () => {
  it('executes progress with explicit questions through pi and preserves reviewable lifecycle', async () => {
    const { fauxAssistantMessage, fauxToolCall } = await import('@earendil-works/pi-ai/providers/faux');
    const { accept } = await import('./helpers/scheduling.js');
    const f = await fixture(0); fixtures.push(f);
    f.faux.setResponses([
      async (ctx: any) => {
        const user = ctx.messages.filter((m: any) => m.role === 'user').at(-1);
        const input = JSON.parse(typeof user.content === 'string' ? user.content : user.content[0].text);
        return fauxAssistantMessage(fauxToolCall('report', { reportId: 'native-question', assignmentId: input.assignmentId, epoch: input.epoch, inputIds: [input.inputId], status: 'progress', questions: [{ id: 'branch', text: 'Which branch?', options: ['main', 'release'] }], content: { summary: 'Need branch selection.' } }), { stopReason: 'toolUse' });
      },
      fauxAssistantMessage('Waiting for your choice.'),
    ]);
    const a = await accept(f.r, 'native-question');
    await f.r.agents!.waitForIdle();
    expect((await f.r.readTicket<any>(a.id))).toMatchObject({ lifecycle: 'open', latestReport: { status: 'waiting', waitingFor: 'human' } });
    expect((await f.r.list<any>('deliveryRun'))[0].consumed).toBe(true);
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(0);
  });
});

describe('revocation during Report preparation', () => {
  it('rejects a Report prepared before stop but committed after revocation', async () => {
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0], run = (await f.r.list<any>('deliveryRun'))[0];
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => enter = resolve), gate = new Promise<void>(resolve => release = resolve);
    const original = f.r.agents!.artifacts.spill.bind(f.r.agents!.artifacts);
    f.r.agents!.artifacts.spill = async (text: string) => { if (text.includes('racing-report')) { enter(); await gate; } return original(text); };
    const reporting = f.r.agents!.report(a.workerConversationId, { reportId: 'racing', assignmentId: a.assignmentId, epoch: a.assignmentEpoch, inputIds: [run.id], status: 'ready', content: { summary: 'racing-report' } });
    const rejection = expect(reporting).rejects.toThrow('Stale');
    try { await entered; await f.r.stopTicket(a.id, alice, { commandId: 'stop-before-report-commit' }); }
    finally { release(); }
    await rejection;
    expect(await f.r.list('report')).toHaveLength(0);
    expect(await f.r.list('outbox')).toHaveLength(0);
    f.release(); await f.r.agents!.waitForIdle();
  });
});

describe('monotonic cancellation checkpoints', () => {
  it('does not reopen a repaired cancellation when a delayed native task returns unknown during the next generation', async () => {
    let handlerEntered!: () => void, releaseHandler!: () => void;
    const entered = new Promise<void>(resolve => handlerEntered = resolve), handlerGate = new Promise<void>(resolve => releaseHandler = resolve);
    let request: any;
    const f = await fixture(1, { cancellationHandler: async current => { request = current; handlerEntered(); await handlerGate; return { ...current, outcome: 'unknown' }; } }); fixtures.push(f);
    let releaseNext!: () => void;
    const nextGate = new Promise<void>(resolve => releaseNext = resolve);
    try {
      await until(async () => f.calls.length === 1);
      const a = (await f.r.list<any>('ticket'))[0];
      await f.r.stopTicket(a.id, alice, { commandId: 'delayed-stop' }); await f.r.agents!.pump(); f.release();
      await entered;
      const cancel = (await f.r.list<any>('cancel'))[0];
      expect(cancel.nativeConfirmed).toBe(true);
      await f.r.engine.transaction(tx => f.r.agents!.cancellation.receipt(tx, cancel.id, { ...request, outcome: 'confirmed' }));
      const { fauxAssistantMessage } = await import('@earendil-works/pi-ai/providers/faux');
      f.faux.setResponses([async (ctx: any, options: any) => { f.calls.push({ ctx, signal: options.signal }); await nextGate; return fauxAssistantMessage('new generation'); }]);
      await f.r.retryTicket(a.id, alice, { commandId: 'delayed-retry' });
      await until(async () => { await f.r.agents!.pump(); return f.calls.length === 2; });
      const next = await f.r.readTicket<any>(a.id);
      expect(next.workerConversationId).toBe(a.workerConversationId);
      releaseHandler(); await f.r.harness.waitForTask(cancel.taskId, f.r.engine.context);
      expect(await f.r.engine.store.read('cancel', cancel.id)).toMatchObject({ status: 'completed', nativeConfirmed: true, endpointConfirmed: true });
      await f.r.agents!.pump();
      expect(f.calls[1].signal.aborted).toBe(false);
      expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toMatchObject([{ epoch: next.assignmentEpoch, status: 'active' }]);
    } finally { releaseHandler(); releaseNext(); f.release(); await f.r.agents!.waitForIdle(); }
  });
  it('uses confirmed native abort as a checkpoint while an endpoint retry is suspended and repaired', async () => {
    let endpointEntered!: () => void, forbiddenAbort!: () => void, releaseEndpoint!: () => void, releaseAbort!: () => void, releaseNext!: () => void;
    const endpoint = new Promise<'endpoint'>(resolve => endpointEntered = () => resolve('endpoint'));
    const abort = new Promise<'native-abort'>(resolve => forbiddenAbort = () => resolve('native-abort'));
    const endpointGate = new Promise<void>(resolve => releaseEndpoint = resolve), abortGate = new Promise<void>(resolve => releaseAbort = resolve), nextGate = new Promise<void>(resolve => releaseNext = resolve);
    let attempts = 0, request: any;
    const f = await fixture(1, { cancellationHandler: async current => { request = current; if (++attempts > 1) { endpointEntered(); await endpointGate; } return { ...current, outcome: 'unknown' }; } }); fixtures.push(f);
    const cancellation = f.r.agents!.cancellation, original = cancellation.cancel.bind(cancellation);
    // Wrap the actual invocation-bound native handle only to hold an attempted
    // repeat abort. No native task/state/abort receipt is fabricated.
    cancellation.cancel = async (intent, conversationId, context, runtime) => original(intent, conversationId, context, {
      signal: runtime.signal,
      waitForTask: runtime.waitForTask.bind(runtime),
      conversation: async (id, ctx) => {
        const handle = await runtime.conversation(id, ctx);
        if (!handle || !intent.nativeConfirmed) return handle;
        return { ...handle, submit: handle.submit.bind(handle), waitForIdle: handle.waitForIdle.bind(handle), abort: async (ctx, options) => { forbiddenAbort(); await abortGate; return handle.abort(ctx, options); } };
      },
    });
    try {
      await until(async () => f.calls.length === 1);
      const a = (await f.r.list<any>('ticket'))[0];
      await f.r.stopTicket(a.id, alice, { commandId: 'checkpoint-stop' }); f.release();
      await until(async () => { await f.r.agents!.pump(); return attempts >= 1; });
      await f.r.agents!.pump();
      expect(await Promise.race([endpoint, abort])).toBe('endpoint');
      const cancel = (await f.r.list<any>('cancel'))[0];
      expect(cancel.attempts).toBe(2); expect(cancel.nativeConfirmed).toBe(true);
      await f.r.engine.transaction(tx => cancellation.receipt(tx, cancel.id, { ...request, outcome: 'confirmed' }));
      const { fauxAssistantMessage } = await import('@earendil-works/pi-ai/providers/faux');
      f.faux.setResponses([async (ctx: any, options: any) => { f.calls.push({ ctx, signal: options.signal }); await nextGate; return fauxAssistantMessage('new assignment'); }]);
      await f.r.retryTicket(a.id, alice, { commandId: 'checkpoint-retry' });
      await until(async () => { await f.r.agents!.pump(); return f.calls.length === 2; });
      releaseAbort(); releaseEndpoint(); await f.r.harness.waitForTask(cancel.taskId, f.r.engine.context);
      await f.r.agents!.pump();
      expect(f.calls[1].signal.aborted).toBe(false);
      expect(await f.r.engine.store.read('cancel', cancel.id)).toMatchObject({ status: 'completed', nativeConfirmed: true, endpointConfirmed: true });
    } finally { releaseAbort(); releaseEndpoint(); releaseNext(); f.release(); await f.r.agents!.waitForIdle(); }
  });
});
