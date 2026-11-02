import { afterEach, describe, expect, it } from 'vitest';
import { fauxAssistantMessage } from '@earendil-works/pi-ai/providers/faux';
import { fixture, accept, until, event, isRouter, withoutSqliteExperimentalWarning } from './helpers/scheduling.js';
const fixtures: Awaited<ReturnType<typeof fixture>>[] = [];
afterEach(async () => { for (const f of fixtures.splice(0)) await f.close(); });
describe('durable domain capacity', () => {
  it('admits exactly three new Workers; concurrent pumps preserve the queued fourth', async () => {
    const f = await fixture(4); fixtures.push(f);
    await until(async () => f.calls.length === 3);
    await Promise.all(Array.from({ length: 8 }, () => f.r.agents!.pump()));
    expect(f.calls).toHaveLength(3);
    expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(3);
    expect((await f.r.list<any>('ticket')).filter(x => x.workerConversationId !== undefined)).toHaveLength(3);
    expect((await f.r.list<any>('scheduleRequest')).filter(x => x.status === 'queued')).toHaveLength(1);
    f.release(); await f.r.agents!.waitForIdle();
    expect(f.calls).toHaveLength(4);
  });
  it('reserves before outside-transaction Worker preparation begins', async () => {
    const f = await fixture(0); fixtures.push(f);
    const original = f.r.agents!.ensureWorker.bind(f.r.agents);
    let observed = false;
    f.r.agents!.ensureWorker = async (id: string) => {
      observed = true;
      expect((await f.r.list<any>('reservation')).some(x => x.ticketId === id && x.status !== 'released')).toBe(true);
      return original(id);
    };
    await accept(f.r, 'preparing'); await f.r.agents!.pump();
    expect(observed).toBe(true);
    f.release(); await f.r.agents!.waitForIdle();
  });
  it('human existing-task wake uses the fourth slot and active steer adds none', async () => {
    const f = await fixture(1); fixtures.push(f);
    f.release(); await f.r.agents!.waitForIdle();
    const idle = (await f.r.list<any>('ticket'))[0];
    let release!: () => void; const gate = new Promise<void>(resolve => release = resolve);
    let calls = 0;
    f.faux.setResponses(Array.from({ length: 30 }, () => async (ctx: any) => { if (isRouter(ctx)) return fauxAssistantMessage('No decisions'); calls++; await gate; return fauxAssistantMessage('waiting'); }));
    try {
      for (let i = 0; i < 3; i++) await accept(f.r, `ordinary-${i}`);
      await f.r.agents!.pump(); await until(async () => calls === 3);
      await f.r.receive(event('human-wake', ['ticket-0'])); await f.r.agents!.pump();
      await until(async () => calls === 4);
      expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released').map(x => x.capacityClass).sort()).toEqual(['human', 'ordinary', 'ordinary', 'ordinary']);
      await f.r.agents!.noteWorker(f.r.agents!.routerConversationId, idle.id, 'Correction', 'steer-correction', 'steer');
      await f.r.agents!.pump();
      expect((await f.r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(4);
    } finally { release(); await f.r.agents!.waitForIdle(); }
  });
});

describe('reconciliation and fair admission', () => {
  it('keeps all preparation slots reserved while Worker creation is suspended', async () => {
    const f = await fixture(0); fixtures.push(f);
    for (let i = 0; i < 4; i++) await accept(f.r, `prepared-${i}`);
    const original = f.r.agents!.ensureWorker.bind(f.r.agents);
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => enter = resolve), gate = new Promise<void>(resolve => release = resolve);
    f.r.agents!.ensureWorker = async (id: string) => { enter(); await gate; return original(id); };
    const pump = f.r.agents!.pump();
    try {
      await entered;
      const { reconcile } = await import('../src/runtime/reconcile.js');
      await reconcile(f.r.engine);
      expect((await f.r.list<any>('reservation')).filter(x => x.status === 'preparing')).toHaveLength(3);
      expect((await f.r.list<any>('scheduleRequest')).filter(x => x.status === 'queued')).toHaveLength(1);
      expect(f.calls).toHaveLength(0);
    } finally { release(); await pump; f.release(); await f.r.agents!.waitForIdle(); }
  });
  it('prioritizes existing human wakes with FIFO ordinary admission after three bypasses', async () => {
    const f = await fixture(0); fixtures.push(f);
    // Warm up five real ownerless contexts without retaining live work.
    for (let i = 0; i < 5; i++) { await accept(f.r, `human-${i}`); f.release(); await f.r.agents!.waitForIdle(); }
    const tickets = await f.r.list<any>('ticket');
    const releases = new Map<string, () => void>(), order: string[] = [];
    f.faux.setResponses(Array.from({ length: 100 }, () => async (ctx: any) => {
      if (isRouter(ctx)) return fauxAssistantMessage('No decisions');
      const text = ctx.messages.filter((x: any) => x.role === 'user').at(-1).content;
      const input = JSON.parse(typeof text === 'string' ? text : text[0].text);
      order.push(input.ticketId);
      await new Promise<void>(resolve => releases.set(input.ticketId, resolve));
      return fauxAssistantMessage('Waiting');
    }));
    try {
      for (const t of tickets) await f.r.receive(event(`wake-${t.goal}`, [t.goal]));
      const ordinary = await accept(f.r, 'aged-ordinary');
      await f.r.agents!.pump();
      await until(async () => order.length === 4);
      expect(order.slice(0, 3)).toEqual(tickets.slice(0, 3).map(x => x.id));
      expect(order[3]).toBe(tickets[3].id); // The human-only slot cannot admit new work.
      releases.get(tickets[0].id)!();
      await until(async () => { await f.r.agents!.pump(); return order.length === 5; });
      expect(order[4]).toBe(ordinary.id);
      expect((await f.r.list<any>('scheduleRequest')).find(x => x.ticketId === tickets[4].id)?.status).toBe('queued');
    } finally {
      // Subsequent wakes can be admitted as the initial four settle.
      f.faux.setResponses(Array.from({ length: 50 }, () => fauxAssistantMessage('Waiting')));
      for (const release of releases.values()) release();
      await f.r.agents!.waitForIdle();
    }
  });
});

describe('restart reservations', () => {
  it('reconstructs live native generations before dispatch after SIGKILL', async () => {
    const { mkdtemp, rm } = await import('node:fs/promises');
    const { tmpdir } = await import('node:os'); const { join } = await import('node:path');
    const { spawn } = await import('node:child_process'); const { once } = await import('node:events');
    const { createModels } = await import('@earendil-works/pi-ai/models');
    const { fauxProvider } = await import('@earendil-works/pi-ai/providers/faux');
    const { openSuperAgent } = await import('../src/runtime/open.js');
    const directory = await mkdtemp(join(tmpdir(), 'sa-capacity-restart-'));
    let r: Awaited<ReturnType<typeof openSuperAgent>> | undefined, release!: () => void;
    const gate = new Promise<void>(resolve => release = resolve);
    const child = spawn(process.execPath, ['tests/helpers/scheduling-crash.mjs', directory], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; child.stderr.on('data', v => stderr += v);
    try {
      const [code, signal] = await once(child, 'exit');
      expect({ code, signal, stderr: withoutSqliteExperimentalWarning(stderr) }).toEqual({ code: null, signal: 'SIGKILL', stderr: '' });
      const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
      let started = 0;
      faux.setResponses(Array.from({ length: 30 }, () => async () => { started++; await gate; return fauxAssistantMessage('Recovered'); }));
      r = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, preflight: async engine => {
        // Simulate upgrade from live pi work without application accounting. All
        // native tasks/submissions remain real and are recovered by Harness.
        await engine.transaction(async tx => {
          for (const reservation of await engine.store.listInTransaction<any>(tx, 'reservation'))
            await engine.store.put(tx, 'reservation', reservation.id, { ...reservation, status: 'released' });
        });
      } });
      await until(async () => started === 3);
      await r.agents!.pump();
      expect(started).toBe(3);
      expect((await r.list<any>('reservation')).filter(x => x.status !== 'released')).toHaveLength(3);
      expect((await r.list<any>('ticket')).filter(x => x.workerConversationId !== undefined)).toHaveLength(3);
      release(); await r.agents!.waitForIdle();
      expect(started).toBe(4);
    } finally { release(); await r?.close(); await rm(directory, { recursive: true, force: true }); }
  });
});

describe('trusted delivery replay helper', () => {
  it('cannot first-admit while the durable native task is still before admission', async () => {
    const f = await fixture(1); fixtures.push(f); await until(async () => f.calls.length === 1);
    const a = (await f.r.list<any>('ticket'))[0];
    const original = f.r.agents!.isCurrent.bind(f.r.agents);
    let enter!: () => void, release!: () => void, first = true;
    const entered = new Promise<void>(resolve => enter = resolve), gate = new Promise<void>(resolve => release = resolve);
    f.r.agents!.isCurrent = async run => {
      if (!run.intentId && run.ticketId === a.id && first) { first = false; enter(); await gate; }
      return original(run);
    };
    await f.r.agents!.noteWorker(f.r.agents!.routerConversationId, a.id, 'Replay must wait for native admission', 'replay-only');
    await f.r.agents!.pump(); await entered;
    const run = (await f.r.list<any>('deliveryRun')).find(x => !x.intentId);
    try {
      await expect(f.r.agents!.submitDelivery(run.id)).rejects.toThrow('pump');
      expect(await f.r.engine.transaction(tx => tx.submissionByRequest(run.conversationId, run.requestId))).toBeUndefined();
    } finally { release(); f.release(); await f.r.agents!.waitForIdle(); }
    expect(await f.r.agents!.submitDelivery(run.id)).toBe((await f.r.engine.store.read<any>('deliveryRun', run.id)).submissionId);
  });
});


describe('pre-admission native recovery', () => {
  it('reconstructs a missing reservation for the live Control input before resume after SIGKILL', async () => {
    const { mkdtemp, rm } = await import('node:fs/promises');
    const { tmpdir } = await import('node:os'); const { join } = await import('node:path');
    const { spawn } = await import('node:child_process'); const { once } = await import('node:events');
    const { createModels } = await import('@earendil-works/pi-ai/models');
    const { fauxProvider, fauxToolCall } = await import('@earendil-works/pi-ai/providers/faux');
    const { openSuperAgent } = await import('../src/runtime/open.js');
    const directory = await mkdtemp(join(tmpdir(), 'sa-admission-reservation-'));
    const child = spawn(process.execPath, ['tests/helpers/scheduling-crash.mjs', directory, 'preadmission'], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = '', runtime: Awaited<ReturnType<typeof openSuperAgent>> | undefined;
    let original: any, worker: any, reports = 0;
    child.stderr.on('data', value => stderr += value);
    try {
      const [code, signal] = await once(child, 'exit');
      expect({ code, signal, stderr: withoutSqliteExperimentalWarning(stderr) }).toEqual({ code: null, signal: 'SIGKILL', stderr: '' });
      const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
      faux.setResponses([
        async (ctx: any) => {
          reports++;
          const user = ctx.messages.filter((m: any) => m.role === 'user').at(-1);
          const input = JSON.parse(typeof user.content === 'string' ? user.content : user.content[0].text);
          return fauxAssistantMessage(fauxToolCall('report', { reportId: 'recovered-admission', assignmentId: input.assignmentId, epoch: input.epoch, inputIds: [input.inputId], status: 'ready', content: {} }), { stopReason: 'toolUse' });
        },
        fauxAssistantMessage('Recovered once'),
      ]);
      runtime = await openSuperAgent({ directory, models, model: { provider: faux.getModel().provider, modelId: faux.getModel().id }, preflight: async engine => {
        original = (await engine.store.list<any>('deliveryRun'))[0];
        expect(original.status).toBe('pending'); expect(original.admitted).toBe(false);
        expect(await engine.transaction(tx => tx.submissionByRequest(original.conversationId, original.requestId))).toBeUndefined();
        const controlTask = await engine.harness.getTask(original.taskId, engine.context);
        expect(controlTask).toMatchObject({ kind: 'sa.agentInput', conversationId: engine.control.id });
        expect(controlTask?.state.status).not.toBe('terminal');
        worker = (await engine.store.list<any>('ticket'))[0];
        expect((await engine.transaction(tx => tx.scanTasks({ conversationId: worker.workerConversationId, kind: 'pi.generation' }, 10))).items).toHaveLength(0);
        await engine.transaction(async tx => {
          for (const reservation of await engine.store.listInTransaction<any>(tx, 'reservation'))
            await engine.store.put(tx, 'reservation', reservation.id, { ...reservation, status: 'released' });
        });
      } });
      await runtime.agents!.waitForIdle();
      expect(reports).toBe(1);
      const runs = await runtime.list<any>('deliveryRun');
      expect(runs).toHaveLength(1);
      expect(runs[0]).toMatchObject({ id: original.id, requestId: original.requestId, promptDigest: original.promptDigest, taskId: original.taskId, consumed: true, settled: true });
      // pi uses one generation for the tool call and another for the final reply.
      expect((await runtime.engine.transaction(tx => tx.scanTasks({ conversationId: worker.workerConversationId, kind: 'pi.generation' }, 10))).items).toHaveLength(2);
      const native = await runtime.engine.transaction(tx => tx.submissionByRequest(original.conversationId, original.requestId));
      expect(native?.id).toBe(runs[0].submissionId); expect(native?.entry).toBeDefined();
      expect((await runtime.readTicket<any>(worker.id)).latestReport.status).toBe('ready');
    } finally { await runtime?.close(); await rm(directory, { recursive: true, force: true }); }
  });
});
