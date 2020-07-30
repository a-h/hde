import {
  GetItemInput,
  DocumentClient,
  Put,
  Converter,
} from "aws-sdk/clients/dynamodb";

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

export interface HeadRecord extends Record {}
export interface DataRecord extends Record {}
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

export const isFacet = (facet: string, r: Record) => r._facet === facet;

export const newHeadRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  typeName: string,
  item: T
): HeadRecord => newRecord(facet, id, seq, "HEAD", typeName, item);

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

export const getHead = async (
  client: DocumentClient,
  table: string,
  facet: string,
  id: string
): Promise<Record> => {
  const params = {
    TableName: table,
    Key: {
      _id: facetId(facet, id),
      _rng: "HEAD",
    },
    ConsistentRead: true,
  } as GetItemInput;
  const result = await client.get(params).promise();
  return result.Item as Record;
};

const createPut = (table: string, r: Record): Put => ({
  TableName: table,
  Item: Converter.marshall(r),
  ConditionExpression: "attribute_not_exists(#_id)",
  ExpressionAttributeNames: {
    "#_id": "_id",
  },
});

const createPutHead = (table: string, r: Record): Put =>
  ({
    TableName: table,
    Item: Converter.marshall(r),
    ConditionExpression: "#_seq = :_seq",
    ExpressionAttributeNames: {
      "#_seq": "_seq",
    },
    ExpressionAttributeValues: {
      ":_seq": r._seq - 1, // Only write if the sequence is a single increment.
    },
  } as Put);

export const putHead = async (
  client: DocumentClient,
  table: string,
  facet: string,
  head: HeadRecord,
  data: Array<DataRecord> = [],
  events: Array<EventRecord> = []
) => {
  if (!isHeadRecord(head)) {
    throw Error("putHead: invalid head record");
  }
  if (!isFacet(facet, head)) {
    throw Error(
      `putHead: head record has mismatched facet. Expected: "${facet}", got: "${head._facet}"`
    );
  }
  if (data.some((d) => !isDataRecord(d))) {
    throw Error("putHead: invalid data record");
  }
  if (data.some((d) => !isFacet(facet, d))) {
    throw Error("putHead: invalid facet for data record");
  }
  if (events.some((e) => !isEventRecord(e))) {
    throw Error("putHead: invalid events record");
  }
  if (events.some((e) => !isFacet(facet, e))) {
    throw Error("putHead: invalid facet for event record");
  }
  const eventCount = events?.length + data?.length + 1;
  if (eventCount > 25) {
    throw Error(
      `putHead: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write ${eventCount}.`
    );
  }
  const transactItems = [
    ...data.map((d) => createPut(table, d)),
    ...events.map((e) => createPut(table, e)),
    createPutHead(table, head),
  ];
  const params = {
    TransactItems: transactItems,
  } as DocumentClient.TransactWriteItemsInput;
  await client.transactWrite(params).promise();
};

// getRecords returns all records grouped under the ID.
export const getRecords = async (
  client: DocumentClient,
  facet: string,
  table: string,
  id: string
): Promise<Array<Record>> => {
  const params = {
    TableName: table,
    KeyConditionExpression: "#_id = :_id",
    ExpressionAttributeNames: {
      "#_id": "_id",
    },
    ExpressionAttributeValues: {
      ":_id": facetId(facet, id),
    },
    ConsistentRead: true,
  } as DocumentClient.QueryInput;
  const result = await client.query(params).promise();
  return result.Items as Array<Record>;
};

const putRecord = async (
  client: DocumentClient,
  facet: string,
  table: string,
  r: Record
) => {
  if (!isFacet(facet, r)) {
    throw Error("putRecord: invalid facet");
  }
  const params = {
    TableName: table,
    Item: r,
  } as DocumentClient.PutItemInput;
  await client.put(params).promise();
};

// putData puts data into the table. This is only suitable for adding data that doesn't affect the state of the
// facet's HEAD record.
export const putData = async (
  client: DocumentClient,
  facet: string,
  table: string,
  r: DataRecord
) => putRecord(client, facet, table, r);

// putEvent puts an event into the table. This is only suitable for adding events that don't affect the state of
// the facet's HEAD record.
export const putEvent = async (
  client: DocumentClient,
  facet: string,
  table: string,
  r: EventRecord
) => putRecord(client, facet, table, r);
