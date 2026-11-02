import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider, fauxAssistantMessage } from '@earendil-works/pi-ai/providers/faux';
import { openSuperAgent, type OpenOptions } from '../../src/runtime/open.js';
export const alice = { id: 'alice', kind: 'human' as const, trusted: true };
export const event = (id: string, threads = [id]) => ({ sourceId: 'cli', externalId: id, revision: '1', actor: alice, threads, body: id, receivedAt: 1000, occurredAt: 900, addressed: true });
export async function accept(r: any, id: string) {
  const received = await r.receive(event(id));
  const c = (await r.listCandidates()).find((c: any) => c.triggerIds.includes(received.triggerId));
  await r.decide(c.id, c.epoch, { kind: 'accept', goal: id, ruleIds: [] }, { commandId: `accept-${id}` });
  return (await r.list('ticket')).find((t: any) => t.goal === id);
}
export async function until(fn: () => Promise<boolean>) {
  for (let i = 0; i < 1000; i++) { if (await fn()) return; await new Promise(resolve => setTimeout(resolve, 1)); }
  throw new Error('Native condition did not settle');
}
export const isRouter = (ctx: any) => ctx.messages.some((m: any) => m.role === 'system' && m.toolsAdded?.some((t: any) => t.name === 'list_candidates'));
export async function fixture(count: number, options: Pick<OpenOptions, 'cancellationHandler' | 'clock'> = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'sa-scheduler-'));
  let r = await openSuperAgent({ directory });
  await r.updatePolicy({ version: 1, acceptMode: 'auto', allowedActors: ['alice'], botActors: [], rules: [] }, alice, { commandId: 'policy' });
  const tickets = [];
  for (let i = 0; i < count; i++) tickets.push(await accept(r, `ticket-${i}`));
  await r.close();
  const faux = fauxProvider(), models = createModels(); models.setProvider(faux.provider);
  const model = { provider: faux.getModel().provider, modelId: faux.getModel().id };
  let release!: () => void;
  const gate = new Promise<void>(resolve => release = resolve);
  const calls: any[] = [];
  const response = async (ctx: any, options: any) => { if (isRouter(ctx)) return fauxAssistantMessage('No new decisions.'); calls.push({ ctx, signal: options.signal }); await gate; return fauxAssistantMessage('No structured acknowledgement.'); };
  faux.setResponses(Array.from({ length: 100 }, () => response));
  r = await openSuperAgent({ directory, models, model, ...options });
  return { r, directory, tickets, calls, faux, models, model, release, async close() { release(); await this.r.close(); await rm(directory, { recursive: true, force: true }); } };
}

/** Node 22/24 SQLite emits this known warning. Preserve every other stderr line. */
export function withoutSqliteExperimentalWarning(stderr: string): string {
  return stderr
    .replace(/^\(node:\d+\) ExperimentalWarning: SQLite is an experimental feature and might change at any time\r?\n/gm, '')
    .replace(/^\(Use `node --trace-warnings \.\.\.` to show where the warning was created\)\r?\n/gm, '');
}
