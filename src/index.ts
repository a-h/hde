import {
  Record,
  StateRecord,
  InboundRecord,
  OutboundRecord,
  isStateRecord,
  isInboundRecord,
  isOutboundRecord,
  newStateRecord,
  newInboundRecord,
  newOutboundRecord,
} from "./db";
import { Processor, Event } from "./processor";

export interface GetOutput<T> {
  record: Record;
  item: T;
}

export interface ChangeOutput<T> {
  seq: number;
  item: T;
  pastOutboundEvents: Array<Event<any>>;
  newOutboundEvents: Array<Event<any>>;
}

// DB is the database access required by Facet<T>. Use EventDB.
export interface DB {
  getState(id: string): Promise<Record>;
  getRecords(id: string): Promise<Array<Record>>;
  putState(
    state: StateRecord,
    previousSeq: number,
    newInboundEvents: Array<InboundRecord>,
    newOutboundEvents: Array<OutboundRecord>,
  ): Promise<void>;
}

// recordsOutput is the return type of the records method.
interface RecordsOutput {
  state: StateRecord | null;
  inboundEvents: Array<InboundRecord>;
  outboundEvents: Array<OutboundRecord>;
}

// A Facet is a type of record stored in a DynamoDB table. It's constructed of a
// "state" record that contains a view of the up-to-date item, multiple inbound
// event records that result in a changes to the item, and outbound event records that
// are used to send messages asynchronously using DynamoDB Streams. This allows messages
// to be queued for delivery at the same time as the transaction is comitted, removing
// the risk of an item being updated, but a message not being sent (e.g. because SQS
// was temporarily unavailable).
export class Facet<T> {
  name: string;
  db: DB;
  processor: Processor<T>;
  constructor(name: string, db: DB, processor: Processor<T>) {
    this.name = name;
    this.db = db;
    this.processor = processor;
  }
  async get(id: string): Promise<GetOutput<T> | null> {
    const state = await this.db.getState(id);
    return state
      ? ({
          record: state,
          item: JSON.parse(state._itm) as T,
        } as GetOutput<T>)
      : null;
  }
  private async records(id: string): Promise<RecordsOutput> {
    const records = await this.db.getRecords(id);
    const result = {
      inboundEvents: new Array<InboundRecord>(),
      outboundEvents: new Array<OutboundRecord>(),
    } as RecordsOutput;
    if (records) {
      records.forEach((r) => {
        if (isInboundRecord(r)) {
          result.inboundEvents.push(r);
          return;
        }
        if (isOutboundRecord(r)) {
          result.outboundEvents.push(r);
          return;
        }
        if (isStateRecord(r)) {
          result.state = r as StateRecord;
        }
      });
    }
    result.inboundEvents = sortRecords(result.inboundEvents);
    return result;
  }
  // append new event(s) to an item. This method executes two database commands,
  // one to retrieve the current state value, and one to put the updated state back.
  async append(id: string, ...newInboundEvents: Array<Event<any>>): Promise<ChangeOutput<T>> {
    const stateRecord = await this.get(id);
    const state = stateRecord ? stateRecord.item : null;
    const seq = stateRecord ? stateRecord.record._seq : 0;
    return this.appendTo(id, state, seq, ...newInboundEvents);
  }
  // appendTo appends new events to an item that has already been retrieved from the
  // database. This method executes a single database command to update the state
  // record.
  async appendTo(id: string, state: T | null, seq: number, ...newInboundEvents: Array<Event<any>>) {
    return this.calculate(id, state, seq, new Array<InboundRecord>(), ...newInboundEvents);
  }
  // recalculate all the state by reading all previous records in the facet item and
  // processing each inbound event record. This method may execute multiple Query operations
  // and a single put operation.
  async recalculate(id: string, ...newInboundEvents: Array<Event<any>>): Promise<ChangeOutput<T>> {
    // Get the records.
    const records = await this.records(id);
    const seq = records.state ? records.state._seq : 0;
    return this.calculate(id, null, seq, records.inboundEvents, ...newInboundEvents);
  }
  // calculate the state.
  private async calculate(
    id: string,
    state: T | null,
    seq: number,
    pastInboundEvents: Array<InboundRecord>,
    ...newInboundEvents: Array<Event<any>>
  ): Promise<ChangeOutput<T>> {
    const pastEvents = pastInboundEvents.map((e) => new Event<any>(e._typ, JSON.parse(e._itm)));
    const newInboundEventsSequence = newInboundEvents.map((e) => new Event(e.type, e.event));

    // Process the events.
    const processingResult = this.processor.process(state, pastEvents, newInboundEventsSequence);

    // Create new records.
    const now = new Date();
    const hr = newStateRecord(
      this.name,
      id,
      seq + newInboundEvents.length,
      processingResult.state,
      now,
    );
    const newInboundRecords = newInboundEvents.map((e, i) =>
      newInboundRecord(this.name, id, seq + 1 + i, e.type, e.event, now),
    );
    const newOutboundRecords = processingResult.newOutboundEvents.map((e, i) =>
      newOutboundRecord(this.name, id, seq + newInboundEvents.length, i, e.type, e.event, now),
    );

    // Write the new records to the database.
    await this.db.putState(hr, seq, newInboundRecords, newOutboundRecords);
    return {
      seq: hr._seq,
      item: processingResult.state,
      pastOutboundEvents: processingResult.pastOutboundEvents,
      newOutboundEvents: processingResult.newOutboundEvents,
    } as ChangeOutput<T>;
  }
}

// sortRecords sorts event records by their sequence number ascending.
const sortRecords = (eventRecords: Array<Record>): Array<Record> =>
  eventRecords.sort((a, b) => {
    if (a._seq < b._seq) {
      return -1;
    }
    if (a._seq === b._seq) {
      return 0;
    }
    return 1;
  });
