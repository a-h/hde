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
  EventDB,
} from "./records";
import { DocumentClient } from "aws-sdk/clients/dynamodb";

export interface GetOutput<T> {
  record: Record;
  item: T;
}

export interface RecordsOutput {
  head: HeadRecord;
  data: Array<DataRecord>;
  events: Array<EventRecord>;
}

export interface HeadUpdaterInput<THead, TCurrent> {
  head: THead;
  headSeq: number;
  current: TCurrent;
  currentSeq: number;
  all: Array<DataRecord>;
  index: number;
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

interface DB {
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
  emptyFacet: EmptyFacet<T>;
  db: DB;
  constructor(
    client: DocumentClient,
    table: string,
    name: string,
    rules: Map<RecordName, HeadUpdater<T, RecordType>>,
    emptyFacet: EmptyFacet<T> = () => ({} as T)
  ) {
    this.name = name;
    this.rules = rules;
    this.emptyFacet = emptyFacet;
    this.db = new EventDB(client, table, name);
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
    return result;
  }
  async put(id: string, ...newData: Array<Data<any>>) {
    // Get the records.
    const records = await this.records(id);
    // Apply the updates to the head.
    const data = records.data.sort((a, b) => {
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
    let head = records.head?._itm
      ? (JSON.parse(records.head._itm) as T)
      : this.emptyFacet();
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
    const rules = this.rules;
    [...data, ...newDataRecords].forEach((curr, idx) => {
      const updater = rules.get(curr._typ);
      if (updater) {
        head = updater({
          head: head,
          headSeq: records.head._seq,
          current: JSON.parse(curr._itm),
          currentSeq: curr._seq,
          all: data,
          index: idx,
        } as HeadUpdaterInput<T, any>);
      }
    });
    // Write the head transaction back.
    await this.db.putHead(
      newHeadRecord(this.name, id, seq, head),
      newDataRecords,
      []
    );
  }
}
