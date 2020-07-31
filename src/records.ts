import { DocumentClient } from "aws-sdk/clients/dynamodb";

// A record is written to DynamoDB.
export interface Record {
  // Identifier of the record group.
  _id: string;
  // Event sort key.
  _rng: string;
  // Facet of the event.
  _facet: string;
  // Type of the event.
  _typ: string;
  // Timestamp of the record.
  _ts: number;
  // ISO date.
  _date: string;
  // Item data.
  _itm: string;
  // Sequence of the record.
  _seq: number;
}

// A HeadRecord represents the current state of an item.
export interface HeadRecord extends Record {}
// DataRecords represent all of the events assocated with an item.
export interface DataRecord extends Record {}
// EventRecords are the events send to external systems due to item changes.
export interface EventRecord extends Record {}

const facetId = (facet: string, id: string) => `${facet}/${id}`;

const newRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  rng: string,
  typeName: string,
  item: T
): Record =>
  ({
    _facet: facet,
    _id: facetId(facet, id),
    _seq: seq,
    _rng: rng,
    _typ: typeName,
    _ts: new Date().getTime(),
    _date: new Date().toISOString(),
    _itm: JSON.stringify(item),
  } as Record);

const isFacet = (facet: string, r: Record) => r._facet === facet;

// Create a new head record to represent the state of an item.
// facet: the name of the DynamoDB facet.
export const newHeadRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  item: T
): HeadRecord => newRecord(facet, id, seq, "HEAD", facet, item);

export const isHeadRecord = (r: HeadRecord) => r._rng === "HEAD";

const dataRecordRangeKey = (typeName: string) =>
  `DATA/${typeName}/${new Date().toISOString()}/${Math.round(
    Math.random() * 1000
  ).toString(16)}`;

export const newDataRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  typeName: string,
  item: T
): DataRecord =>
  newRecord(facet, id, seq, dataRecordRangeKey(typeName), typeName, item);

export const isDataRecord = (r: DataRecord) => r._rng.startsWith("DATA");

const eventRecordRangeKey = (typeName: string) => `EVENT/${typeName}`;

export const newEventRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  typeName: string,
  item: T
): EventRecord =>
  newRecord(facet, id, seq, eventRecordRangeKey(typeName), typeName, item);

export const isEventRecord = (r: EventRecord) => r._rng.startsWith("EVENT");

const createPut = (
  table: string,
  r: Record
): DocumentClient.TransactWriteItem => ({
  Put: {
    TableName: table,
    Item: r,
    ConditionExpression: "attribute_not_exists(#_id)",
    ExpressionAttributeNames: {
      "#_id": "_id",
    },
  },
});

const createPutHead = (
  table: string,
  r: Record
): DocumentClient.TransactWriteItem => ({
  Put: {
    TableName: table,
    Item: r,
    ConditionExpression: "attribute_not_exists(#_id) OR #_seq = :_seq",
    ExpressionAttributeNames: {
      "#_id": "_id",
      "#_seq": "_seq",
    },
    ExpressionAttributeValues: {
      ":_seq": r._seq - 1, // Only write if the sequence is a single increment.
    },
  },
});

export class EventDB {
  client: DocumentClient;
  table: string;
  facet: string;
  constructor(client: DocumentClient, table: string, facet: string) {
    this.client = client;
    this.table = table;
    this.facet = facet;
  }
  async getHead(id: string): Promise<Record> {
    const params = {
      TableName: this.table,
      Key: {
        _id: facetId(this.facet, id),
        _rng: "HEAD",
      },
      ConsistentRead: true,
    } as DocumentClient.GetItemInput;
    const result = await this.client.get(params).promise();
    return result.Item as Record;
  }
  async putHead(
    head: HeadRecord,
    data: Array<DataRecord> = [],
    events: Array<EventRecord> = []
  ) {
    if (!isHeadRecord(head)) {
      throw Error("putHead: invalid head record");
    }
    if (!isFacet(this.facet, head)) {
      throw Error(
        `putHead: head record has mismatched facet. Expected: "${this.facet}", got: "${head._facet}"`
      );
    }
    if (data.some((d) => !isDataRecord(d))) {
      throw Error("putHead: invalid data record");
    }
    if (data.some((d) => !isFacet(this.facet, d))) {
      throw Error("putHead: invalid facet for data record");
    }
    if (events.some((e) => !isEventRecord(e))) {
      throw Error("putHead: invalid events record");
    }
    if (events.some((e) => !isFacet(this.facet, e))) {
      throw Error("putHead: invalid facet for event record");
    }
    const eventCount = events?.length + data?.length + 1;
    if (eventCount > 25) {
      throw Error(
        `putHead: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write ${eventCount}.`
      );
    }
    const transactItems = [
      ...data.map((d) => createPut(this.table, d)),
      ...events.map((e) => createPut(this.table, e)),
      createPutHead(this.table, head),
    ] as DocumentClient.TransactWriteItemList;
    const params = {
      TransactItems: transactItems,
    } as DocumentClient.TransactWriteItemsInput;
    await this.client.transactWrite(params).promise();
  }
  // getRecords returns all records grouped under the ID.
  async getRecords(id: string): Promise<Array<Record>> {
    const params = {
      TableName: this.table,
      KeyConditionExpression: "#_id = :_id",
      ExpressionAttributeNames: {
        "#_id": "_id",
      },
      ExpressionAttributeValues: {
        ":_id": facetId(this.facet, id),
      },
      ConsistentRead: true,
    } as DocumentClient.QueryInput;
    const result = await this.client.query(params).promise();
    return result.Items as Array<Record>;
  }
  async putRecord(r: Record) {
    if (!isFacet(this.facet, r)) {
      throw Error("putRecord: invalid facet");
    }
    const params = {
      TableName: this.table,
      Item: r,
    } as DocumentClient.PutItemInput;
    await this.client.put(params).promise();
  }
  // putData puts data into the table. This is only suitable for adding data that doesn't affect the state of the
  // facet's HEAD record.
  async putData(r: DataRecord) {
    await this.putRecord(r);
  }
  // putEvent puts an event into the table. This is only suitable for adding events that don't affect the state of
  // the facet's HEAD record.
  async putEvent(r: EventRecord) {
    await this.putRecord(r);
  }
}

