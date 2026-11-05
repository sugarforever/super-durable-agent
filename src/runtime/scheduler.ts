import type { Tx } from '@earendil-works/pi-durable';
import type { Engine } from '../domain/engine.js';
import type { Assignment, Ticket, CancelIntent } from '../domain/model.js';
export interface Clock { now(): number }
export type CapacityClass = 'ordinary' | 'human';
export interface Reservation {
  id: string; ticketId: string; assignmentId: string; epoch: number;
  capacityClass: CapacityClass; status: 'preparing' | 'active' | 'quarantined' | 'released'; reservedAt: number;
}
export interface ScheduleRequest {
  id: string; ticketId: string; epoch: number; human: boolean; existing: boolean;
  ordinal: number; queuedAt: number; status: 'queued' | 'admitted' | 'invalid';
}
interface QueueState { sequence: number; humanBypass: number }
/** Session commits are the sole admission authority. Native pi concurrency is independent. */
export class Scheduler {
  constructor(readonly engine: Engine, readonly clock: Clock = { now: () => Date.now() }) {}
  key(ticketId: string, epoch: number): string { return `${ticketId}:${epoch}`; }
  async enqueue(tx: Tx, ticket: Ticket, human: boolean): Promise<void> {
    const { store } = this.engine, id = this.key(ticket.id, ticket.assignmentEpoch);
    const prior = await store.get<ScheduleRequest>(tx, 'scheduleRequest', id);
    const reservation = await store.get<Reservation>(tx, 'reservation', id);
    if (reservation && reservation.status !== 'released') return;
    if (prior?.status === 'queued') {
      if (human && !prior.human) await store.put(tx, 'scheduleRequest', id, { ...prior, human });
      return;
    }
    const state = await store.get<QueueState>(tx, 'scheduler', 'queue') ?? { sequence: 0, humanBypass: 0 };
    await store.put(tx, 'scheduler', 'queue', { ...state, sequence: state.sequence + 1 });
    await store.put(tx, 'scheduleRequest', id, { id, ticketId: ticket.id, epoch: ticket.assignmentEpoch, human, existing: ticket.workerConversationId !== undefined, ordinal: state.sequence + 1, queuedAt: this.clock.now(), status: 'queued' } satisfies ScheduleRequest);
  }
  async reserve(tx: Tx, request: ScheduleRequest, capacityClass: CapacityClass): Promise<Reservation | undefined> {
    const { store } = this.engine;
    const ticket = await store.get<Ticket>(tx, 'ticket', request.ticketId);
    if (!ticket || ticket.lifecycle !== 'open' || ticket.assignmentEpoch !== request.epoch) return;
    const prior = await store.get<Reservation>(tx, 'reservation', request.id);
    if (prior && prior.status !== 'released') return prior;
    if (capacityClass === 'human' && !(request.human && request.existing)) return;
    const active = (await store.listInTransaction<Reservation>(tx, 'reservation')).filter(x => x.status !== 'released');
    if (active.filter(x => x.capacityClass === capacityClass).length >= (capacityClass === 'ordinary' ? 3 : 1)) return;
    // Unknown cancellation on the same conversation cannot overlap a new epoch.
    if (active.some(x => x.ticketId === ticket.id && x.epoch !== request.epoch)) return;
    if ((await store.listInTransaction<CancelIntent>(tx, 'cancel')).some(x => x.ticketId === ticket.id && x.status !== 'completed')) return;
    const reservation: Reservation = { id: request.id, ticketId: ticket.id, assignmentId: ticket.assignmentId, epoch: request.epoch, capacityClass, status: 'preparing', reservedAt: this.clock.now() };
    await store.put(tx, 'reservation', request.id, reservation);
    await store.put(tx, 'scheduleRequest', request.id, { ...request, status: 'admitted' });
    const assignment = await store.get<Assignment>(tx, 'assignment', ticket.assignmentId);
    if (assignment) await store.put(tx, 'assignment', assignment.id, { ...assignment, capacityClass, status: 'running' });
    return reservation;
  }
  async dispatch(tx: Tx): Promise<void> {
    const { store } = this.engine;
    const queued = (await store.listInTransaction<ScheduleRequest>(tx, 'scheduleRequest')).filter(x => x.status === 'queued').sort((a,b) => a.ordinal - b.ordinal);
    let state = await store.get<QueueState>(tx, 'scheduler', 'queue') ?? { sequence: 0, humanBypass: 0 };
    while (queued.length) {
      const ordinary = queued.filter(x => !(x.human && x.existing));
      const human = queued.filter(x => x.human && x.existing);
      const preferOrdinary = ordinary.length > 0 && state.humanBypass >= 3;
      const candidates = preferOrdinary ? [...ordinary, ...human] : [...human, ...ordinary];
      let selected: ScheduleRequest | undefined;
      for (const request of candidates) {
        const ticket = await store.get<Ticket>(tx, 'ticket', request.ticketId);
        if (!ticket || ticket.lifecycle !== 'open' || ticket.assignmentEpoch !== request.epoch) {
          await store.put(tx, 'scheduleRequest', request.id, { ...request, status: 'invalid' });
          queued.splice(queued.indexOf(request), 1); selected = request; break;
        }
        const reservation = await this.reserve(tx, request, 'ordinary') ?? (request.human && request.existing ? await this.reserve(tx, request, 'human') : undefined);
        if (!reservation) continue;
        state = { ...state, humanBypass: request.human && request.existing ? state.humanBypass + 1 : 0 };
        await store.put(tx, 'scheduler', 'queue', state);
        queued.splice(queued.indexOf(request), 1); selected = request; break;
      }
      if (!selected) break;
    }
  }
  async holds(tx: Tx, ticketId: string, epoch: number): Promise<boolean> {
    const value = await this.engine.store.get<Reservation>(tx, 'reservation', this.key(ticketId, epoch));
    if ((await this.engine.store.listInTransaction<CancelIntent>(tx, 'cancel')).some(x => x.ticketId === ticketId && x.status !== 'completed')) return false;
    return !!value && value.status !== 'released' && value.status !== 'quarantined';
  }
}
