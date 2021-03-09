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

// A StateRecord represents the current state of an item.
export type StateRecord = Record;
// InboundRecords represent all of the change events assocated with an item.
export type InboundRecord = Record;
// OutboundRecords are the events sent to external systems due to item changes.
export type OutboundRecord = Record;

const facetId = (facet: string, id: string) => `${facet}/${id}`;

const newRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  rng: string,
  type: string,
  item: T,
  time: Date,
): Record =>
  ({
    _facet: facet,
    _id: facetId(facet, id),
    _seq: seq,
    _rng: rng,
    _typ: type,
    _ts: time.getTime(),
    _date: time.toISOString(),
    _itm: JSON.stringify(item),
  } as Record);

const isFacet = (facet: string, r: Record) => r._facet === facet;

// Create a new state record to represent the state of an item.
// facet: the name of the DynamoDB facet.
export const newStateRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  item: T,
  time: Date,
): StateRecord => newRecord(facet, id, seq, "STATE", facet, item, time);

export const isStateRecord = (r: StateRecord) => r._rng === "STATE";

const inboundRecordRangeKey = (type: string, seq: number) => `INBOUND/${type}/${seq}`;

export const newInboundRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  type: string,
  item: T,
  time: Date,
): InboundRecord => newRecord(facet, id, seq, inboundRecordRangeKey(type, seq), type, item, time);

export const isInboundRecord = (r: InboundRecord) => r._rng.startsWith("INBOUND");

const outboundRecordRangeKey = (type: string, seq: number, index: number) =>
  `OUTBOUND/${type}/${seq}/${index}`;

export const newOutboundRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  index: number,
  type: string,
  item: T,
  time: Date,
): OutboundRecord =>
  newRecord(facet, id, seq, outboundRecordRangeKey(type, seq, index), type, item, time);

export const isOutboundRecord = (r: OutboundRecord) => r._rng.startsWith("OUTBOUND");

const createPut = (table: string, r: Record): DocumentClient.TransactWriteItem => ({
  Put: {
    TableName: table,
    Item: r,
    ConditionExpression: "attribute_not_exists(#_id)",
    ExpressionAttributeNames: {
      "#_id": "_id",
    },
  },
});

const createPutState = (
  table: string,
  r: Record,
  previousSeq: number,
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
      ":_seq": previousSeq,
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
  async getState(id: string): Promise<Record> {
    const params = {
      TableName: this.table,
      Key: {
        _id: facetId(this.facet, id),
        _rng: "STATE",
      },
      ConsistentRead: true,
    } as DocumentClient.GetItemInput;
    const result = await this.client.get(params).promise();
    return result.Item as Record;
  }
  async putState(
    state: StateRecord,
    previousSeq: number,
    inbound: Array<InboundRecord> = [],
    outbound: Array<OutboundRecord> = [],
  ) {
    if (!isStateRecord(state)) {
      throw Error("putState: invalid state record");
    }
    if (!isFacet(this.facet, state)) {
      throw Error(
        `putState: state record has mismatched facet. Expected: "${this.facet}", got: "${state._facet}"`,
      );
    }
    if (inbound.some((d) => !isInboundRecord(d))) {
      throw Error("putState: invalid inbound record");
    }
    if (inbound.some((d) => !isFacet(this.facet, d))) {
      throw Error("putState: invalid facet for inbound record");
    }
    if (outbound.some((e) => !isOutboundRecord(e))) {
      throw Error("putState: invalid outbound record");
    }
    if (outbound.some((e) => !isFacet(this.facet, e))) {
      throw Error("putState: invalid facet for outbound record");
    }
    const outboundCount = outbound?.length + inbound?.length + 1;
    if (outboundCount > 25) {
      throw Error(
        `putState: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write ${outboundCount}.`,
      );
    }
    const transactItems = [
      ...inbound.map((d) => createPut(this.table, d)),
      ...outbound.map((e) => createPut(this.table, e)),
      createPutState(this.table, state, previousSeq),
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
}
