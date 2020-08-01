import {
  Record,
  HeadRecord,
  DataRecord,
  EventRecord,
  isDataRecord,
  isEventRecord,
  isHeadRecord,
  newDataRecord,
  newHeadRecord,
  newEventRecord,
} from "./records";

export interface GetOutput<T> {
  record: Record;
  item: T;
}

export interface ChangeOutput<T> {
  id: string;
  seq: number;
  item: T;
  events: Array<any>;
}

export interface HeadUpdaterInput<THead, TCurrent> {
  // head value of the facet.
  head: THead;
  // headSeq is the sequence number of the current head value.
  headSeq: number;
  // current data that is modifying the head.
  current: TCurrent;
  currentSeq: number;
  // all allows access to all of the records, new and old.
  all: Array<DataRecord>;
  // current index within the sorted records.
  index: number;
  // publish an event. This should be idempotent, i.e. only call this if the
  // currentSeq > headSeq to avoid sending out duplicate messages on recalculation
  // of the head.
  publish: (name: string, event: any) => void;
}

// HeadUpdater<THead, TCurrent> defines a function used to update head based on the current type.
export type HeadUpdater<THead, TCurrent> = (
  input: HeadUpdaterInput<THead, TCurrent>
) => THead;

// Data that makes up the facet item. Reading through all the data, and applying the rules creates
// a materialised view. This view is the "HEAD" record stored in the database.
export class Data<T> {
  typeName: string;
  data: T;
  constructor(typeName: string, data: T) {
    this.typeName = typeName;
    this.data = data;
  }
}

export type RecordName = string;
export type RecordType = any;
// EmptyFacet constructs the default value of a facet item. For example, if the facet is a 
// bank account, perhaps the starting balance would be zero, and an overdraft of 1000 would
// be set.
export type EmptyFacet<T> = () => T;

// DB is the database access required by Facet<T>. Use EventDB.
export interface DB {
  getHead(id: string): Promise<Record>;
  getRecords(id: string): Promise<Array<Record>>;
  putHead(
    head: HeadRecord,
    data: Array<DataRecord>,
    events: Array<EventRecord>
  ): Promise<void>;
}

// recordsOutput is the return type of the records method.
interface RecordsOutput {
  head: HeadRecord | null;
  data: Array<DataRecord>;
  events: Array<EventRecord>;
}

// A Facet is a type of record stored in a DynamoDB table. It's constructed of a 
// "head" record that contains a view of the up-to-date item, multiple "data" records 
// (usually events) that result in a changes to the item, and "event" records that
// are used to send messages asynchronously using DynamoDB Streams. This allows messages
// to be queued for delivery at the same time as the transaction is comitted, removing
// the risk of an item being updated, but a message not being sent (e.g. because SQS
// was temporarily unavailable).
export class Facet<T> {
  name: string;
  rules: Map<RecordName, HeadUpdater<T, RecordType>>;
  initial: EmptyFacet<T>;
  db: DB;
  constructor(
    db: DB,
    name: string,
    rules: Map<RecordName, HeadUpdater<T, RecordType>>,
    initial: EmptyFacet<T> = () => ({} as T)
  ) {
    this.name = name;
    this.rules = rules;
    this.initial = initial;
    this.db = db;
  }
  async get(id: string): Promise<GetOutput<T> | null> {
    const head = await this.db.getHead(id);
    return head
      ? ({
          record: head,
          item: JSON.parse(head._itm) as T,
        } as GetOutput<T>)
      : null;
  }
  private async records(id: string): Promise<RecordsOutput> {
    const records = await this.db.getRecords(id);
    const result = {
      data: new Array<DataRecord>(),
      events: new Array<EventRecord>(),
    } as RecordsOutput;
    if (records) {
      records.forEach((r) => {
        if (isDataRecord(r)) {
          result.data.push(r);
          return;
        }
        if (isEventRecord(r)) {
          result.events.push(r);
          return;
        }
        if (isHeadRecord(r)) {
          result.head = r as HeadRecord;
        }
      });
    }
    result.data = sortData(result.data);
    return result;
  }
  // append new data to an item. This method executes two database commands, 
  // one to retrieve the current head value, and one to put the updated head back.
  async append(id: string, ...newData: Array<Data<any>>): Promise<ChangeOutput<T>> {
    const headRecord = await this.get(id);
    const head = headRecord ? headRecord.item : this.initial();
    const seq = headRecord ? headRecord.record._seq : 1;
    return this.appendTo(id, head, seq, ...newData);
  }
  // appendTo appends new data to an item that has already been retrieved from the
  // database. This method executes a single database command to update the head
  // record.
  async appendTo(id: string, head: T, seq: number, ...newData: Array<Data<any>>) {
    return this.calculate(id, head, seq, new Array<DataRecord>(), ...newData);
  }
  // recalculate all the state by reading all previous records in the facet item and
  // processing each data record. This method may execute multiple Query operations
  // and a single put operation.
  async recalculate(id: string, ...newData: Array<Data<any>>): Promise<ChangeOutput<T>> {
    // Get the records.
    const records = await this.records(id);
    const seq = records.head ? records.head._seq : 0;
    const head = this.initial();
    return this.calculate(id, head, seq, records.data, ...newData);
  }
  // calculate the head.
  private async calculate(id: string, head: T, seq: number, currentData: Array<DataRecord>, ...newData: Array<Data<any>>): Promise<ChangeOutput<T>> {
    const newDataRecords = newData.map((typeNameToData) =>
      newDataRecord(
        this.name,
        id,
        seq + 1,
        typeNameToData.typeName,
        typeNameToData.data
      )
    );
    const newEvents = new Array<EventRecord>();
    const facetName = this.name;
    const rules = this.rules;
    const data = [...currentData, ...newDataRecords];
    data.forEach((curr, idx) => {
      const updater = rules.get(curr._typ);
      if (updater) {
        head = updater({
          head: head,
          headSeq: seq,
          current: JSON.parse(curr._itm),
          currentSeq: curr._seq,
          all: data,
          index: idx,
          publish: (eventName: string, event: any) =>
            newEvents.push(
              newEventRecord(facetName, id, seq+1, eventName, event)
            ),
        } as HeadUpdaterInput<T, any>);
      }
    });
    // Write the head transaction back.
    await this.db.putHead(
      newHeadRecord(this.name, id, seq+1, head),
      newDataRecords,
      newEvents
    );
    return {
      id: id,
      seq: seq+1,
      item: head,
      events: newEvents,
    } as ChangeOutput<T>;
  }
}

// sortData sorts data records by their sequence number ascending, then
// by their timestamp field value, then by their range key.
const sortData = (data: Array<Record>): Array<Record> =>
  data.sort((a, b) => {
    const bySeq = cmp(a._seq, b._seq);
    if(bySeq === 0) {
      const byTimestamp = cmp(a._ts, b._ts);
      if(byTimestamp === 0) {
        return cmp(a._rng, b._rng);
      }
      return byTimestamp;
    }
    return bySeq;
  });

const cmp = (a: string| number, b: string|number): number => {
  if(a < b) {
    return -1;
  }
  if (a === b) {
    return 0;
  }
  return 1;
};
