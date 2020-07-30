import {
  Record,
  getHead,
  getRecords,
  HeadRecord,
  DataRecord,
  EventRecord,
  isDataRecord,
  isEventRecord,
  isHeadRecord,
  putHead,
  newDataRecord,
  newHeadRecord,
} from ".";
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

// dataSinceHead returns all data from the data records that has happened since the current head record.
export const dataSinceHead = (head: HeadRecord, data: Array<DataRecord>) =>
  data.filter((d) => d._seq > head._seq);

// HeadUpdater<THead, TCurrent> defines a function used to update head based on the current type.
export type HeadUpdater<THead, TCurrent> = (
  head: THead,
  headSeq: number,
  current: TCurrent,
  currentSeq: number,
  all: Array<DataRecord>,
  index: number
) => THead;

// TypeNameToHeadUpdater is a map of type names to a function used to create a new head record.
export interface TypeNameToHeadUpdater<HeadType> {
  [key: string]: HeadUpdater<HeadType, any>;
}

export class TypeNameToData<T> {
  name: string;
  data: T;
  constructor(name: string, data: T) {
    this.name = name;
    this.data = data;
  }
}

export class Facet<T> {
  client: DocumentClient;
  table: string;
  name: string;
  constructor(client: DocumentClient, table: string, name: string) {
    this.client = client;
    this.table = table;
    this.name = name;
  }
  get = async (id: string): Promise<GetOutput<T> | null> => {
    const head = await getHead(this.client, this.table, this.name, id);
    return head
      ? ({
          record: head,
          item: JSON.parse(head._itm) as T,
        } as GetOutput<T>)
      : null;
  };
  records = async (id: string): Promise<RecordsOutput> => {
    const records = await getRecords(this.client, this.name, this.table, id);
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
  };
  update = async (
    id: string,
    newData: Array<TypeNameToData<any>>,
    headUpdater: TypeNameToHeadUpdater<T>
  ) => {
    // Get the records.
    const records = await this.records(id);
    // Apply the updates to the head.
    const data = records.data.sort((a, b) => {
      if (a._seq < b._seq) {
        return -1;
      }
      if (a._seq == b._seq) {
        return 0;
      }
      return 1;
    });
    let head = JSON.parse(records.head._itm) as T;
    const seq = records.head ? records.head._seq + 1 : 1;
    const newRecords = newData.map((typeNameToData) =>
      newDataRecord(
        this.name,
        id,
        seq,
        typeNameToData.name,
        typeNameToData.data
      )
    );
    [...data, ...newRecords].forEach((curr, idx) => {
      const updater = headUpdater[curr._typ];
      if (updater) {
        head = updater(
          head,
          records.head._seq,
          JSON.parse(curr._itm),
          curr._seq,
          data,
          idx
        );
      }
    });
    // Write the head transaction back.
    await putHead(
      this.client,
      this.table,
      this.name,
      newHeadRecord(this.name, id, seq, records.head._typ, head),
      newRecords
    );
  };
}
