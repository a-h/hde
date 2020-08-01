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

interface RecordsOutput {
  head: HeadRecord | null;
  data: Array<DataRecord>;
  events: Array<EventRecord>;
}

export interface PutOutput<T> {
  id: string;
  seq: number;
  head: T;
  events: Array<any>;
}

export interface HeadUpdaterInput<THead, TCurrent> {
  // head is the head that will replace the current head. At sequence 0, the
  // head is the initial value. To view the current head value stored in the
  // database, see storedHead.
  head: THead;
  // stored is the current value of the head that is stored in the database.
  stored: THead | null;
  // storedSeq is the sequence  number of the current stored head value.
  storedSeq: number;
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
export type EmptyFacet<T> = () => T;

export interface DB {
  getHead(id: string): Promise<Record>;
  getRecords(id: string): Promise<Array<Record>>;
  putHead(
    head: HeadRecord,
    data: Array<DataRecord>,
    events: Array<EventRecord>
  ): Promise<void>;
}

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
  async put(id: string, ...newData: Array<Data<any>>) {
    // Get the records.
    const records = await this.records(id);
    const seq = records.head ? records.head._seq + 1 : 1;
    const newDataRecords = newData.map((typeNameToData) =>
      newDataRecord(
        this.name,
        id,
        seq,
        typeNameToData.typeName,
        typeNameToData.data
      )
    );
    const newEvents = new Array<EventRecord>();
    const facetName = this.name;
    const rules = this.rules;
    const data = [...records.data, ...newDataRecords];
    let head = this.initial();
    data.forEach((curr, idx) => {
      const updater = rules.get(curr._typ);
      if (updater) {
        head = updater({
          head: head,
          stored: records.head ? (JSON.parse(records.head._itm) as T) : null,
          storedSeq: records.head?._seq,
          current: JSON.parse(curr._itm),
          currentSeq: curr._seq,
          all: data,
          index: idx,
          publish: (eventName: string, event: any) =>
            newEvents.push(
              newEventRecord(facetName, id, seq, eventName, event)
            ),
        } as HeadUpdaterInput<T, any>);
      }
    });
    // Write the head transaction back.
    await this.db.putHead(
      newHeadRecord(this.name, id, seq, head),
      newDataRecords,
      newEvents
    );
    return {
      id: id,
      seq: seq,
      head: head,
      events: newEvents,
    } as PutOutput<T>;
  }
}

const sortData = (data: Array<Record>): Array<Record> =>
  data.sort((a, b) => {
    if (a._seq < b._seq) {
      return -1;
    }
    if (a._seq === b._seq) {
      if (a._ts < b._ts) {
        return -1;
      }
      if (a._ts === b._ts) {
        return 0;
      }
      return 1;
    }
    return 1;
  });
