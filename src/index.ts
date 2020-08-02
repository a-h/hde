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
} from "./db";
import { Processor, SequenceData } from "./processor";

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

// DB is the database access required by Facet<T>. Use EventDB.
export interface DB {
  getHead(id: string): Promise<Record>;
  getRecords(id: string): Promise<Array<Record>>;
  putHead(
    head: HeadRecord,
    previousSeq: number,
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
  db: DB;
  processor: Processor<T>;
  constructor(name: string, db: DB, processor: Processor<T>) {
    this.name = name;
    this.db = db;
    this.processor = processor;
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
  async append(
    id: string,
    ...newData: Array<Data<any>>
  ): Promise<ChangeOutput<T>> {
    const headRecord = await this.get(id);
    const head = headRecord ? headRecord.item : null;
    const seq = headRecord ? headRecord.record._seq : 1;
    return this.appendTo(id, head, seq, ...newData);
  }
  // appendTo appends new data to an item that has already been retrieved from the
  // database. This method executes a single database command to update the head
  // record.
  async appendTo(
    id: string,
    head: T | null,
    seq: number,
    ...newData: Array<Data<any>>
  ) {
    return this.calculate(id, head, seq, new Array<DataRecord>(), ...newData);
  }
  // recalculate all the state by reading all previous records in the facet item and
  // processing each data record. This method may execute multiple Query operations
  // and a single put operation.
  async recalculate(
    id: string,
    ...newData: Array<Data<any>>
  ): Promise<ChangeOutput<T>> {
    // Get the records.
    const records = await this.records(id);
    const seq = records.head ? records.head._seq : 0;
    return this.calculate(id, null, seq, records.data, ...newData);
  }
  // calculate the head.
  private async calculate(
    id: string,
    head: T | null,
    seq: number,
    currentData: Array<DataRecord>,
    ...newData: Array<Data<any>>
  ): Promise<ChangeOutput<T>> {
    // Get the records ready for reprocessing.
    const headSequence = new SequenceData<T>(seq, this.name, head);
    const existingDataSequence = currentData.map(
      (d, i): SequenceData<any> =>
        new SequenceData<any>(i + 1, d._typ, JSON.parse(d._itm))
    );
    const newDataSequence = newData.map(
      (d, i) => new SequenceData(seq + 1 + i, d.typeName, d.data)
    );

    // Process the data.
    const processingResult = this.processor.process(
      headSequence,
      existingDataSequence,
      newDataSequence
    );

    // Create new records.
    const now = new Date();
    const hr = newHeadRecord(
      this.name,
      id,
      processingResult.head.seq,
      processingResult.head.data,
      now
    );
    const newDataRecords = newData.map((d, i) =>
      newDataRecord(this.name, id, seq + 1 + i, d.typeName, d.data, now)
    );
    const newEventRecords = processingResult.newEvents.map((e) =>
      newEventRecord(this.name, id, e.seq, e.typeName, e.data, now)
    );

    // Write the new records to the database.
    await this.db.putHead(hr, seq, newDataRecords, newEventRecords);
    return {
      id: id,
      seq: processingResult.head.seq,
      item: processingResult.head.data,
      events: processingResult.newEvents,
    } as ChangeOutput<T>;
  }
}

// sortData sorts data records by their sequence number ascending.
const sortData = (data: Array<Record>): Array<Record> =>
  data.sort((a, b) => {
    if (a._seq < b._seq) {
      return -1;
    }
    if (a._seq === b._seq) {
      return 0;
    }
    return 1;
  });
