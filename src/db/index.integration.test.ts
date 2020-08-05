import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { DynamoDB } from "aws-sdk";
import { EventDB, newHeadRecord, newDataRecord, DataRecord, newEventRecord } from ".";

describe("EventDB", () => {
  describe("getHead", () => {
    it("can get the head record if it exists", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const head = { key: "value" };
        const headRecord = newHeadRecord<any>("facetName", "idValue", 1, head, new Date());
        await db.putHead(headRecord, 0);

        const actual = await db.getHead("idValue");
        expect(actual).toEqual(headRecord);
      } finally {
        await testDB.delete();
      }
    });
  });
  describe("putHead", () => {
    it("can put a new head record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const head1 = { key: "value1" };
        const headRecord1 = newHeadRecord<any>("facetName", "idValue", 1, head1, new Date());
        await db.putHead(headRecord1, 0);
        const head2 = { key: "value2" };
        const headRecord2 = newHeadRecord<any>("facetName", "idValue", 2, head2, new Date());
        await db.putHead(headRecord2, 1);

        const actual = await db.getHead("idValue");
        expect(actual).toEqual(headRecord2);
      } finally {
        await testDB.delete();
      }
    });
    it("can put data records alongside a new head record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const head = { key: "value1" };
        const headRecord = newHeadRecord<any>("facetName", "idValue", 3, head, new Date());
        const dataRecords = [
          newDataRecord("facetName", "idValue", 1, "data", { record: "data1" }, new Date()),
          newDataRecord("facetName", "idValue", 2, "data", { record: "data2" }, new Date()),
        ];
        await db.putHead(headRecord, 0, dataRecords);

        const actual = await db.getRecords("idValue");
        expect(actual).toEqual([...dataRecords, headRecord]);
      } finally {
        await testDB.delete();
      }
    });
    it("can put event records alongside a new head record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const head = { key: "value1" };
        const headRecord = newHeadRecord<any>("facetName", "idValue", 5, head, new Date());
        const dataRecords = [
          newDataRecord("facetName", "idValue", 1, "data", { record: "data1" }, new Date()),
          newDataRecord("facetName", "idValue", 2, "data", { record: "data2" }, new Date()),
        ];
        const eventRecords = [
          newEventRecord("facetName", "idValue", 3, 0, "data", { record: "data1" }, new Date()),
          newEventRecord("facetName", "idValue", 3, 1, "event", { event: "test1" }, new Date()),
        ];
        await db.putHead(headRecord, 0, dataRecords, eventRecords);

        const actual = await db.getRecords("idValue");
        expect(actual).toEqual([...dataRecords, ...eventRecords, headRecord]);
      } finally {
        await testDB.delete();
      }
    });
    it("validates head records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(newEventRecord("not_important", "", 0, 0, "test", {}, new Date()), 0);
      } catch (e) {
        expect(e.message).toBe("putHead: invalid head record");
      }
    });
    it("validates head records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(newHeadRecord("incorrect_facet", "", 0, {}, new Date()), 0);
      } catch (e) {
        expect(e.message).toBe(
          'putHead: head record has mismatched facet. Expected: "facetName", got: "incorrect_facet"',
        );
      }
    });
    it("validates data records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(newHeadRecord("facetName", "", 0, {}, new Date()), 0, [
          newEventRecord("facetName", "id", 0, 1, "facetEvent", {}, new Date()),
        ]);
      } catch (e) {
        expect(e.message).toBe("putHead: invalid data record");
      }
    });
    it("validates data records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(newHeadRecord("facetName", "", 0, {}, new Date()), 0, [
          newDataRecord("incorrect_facet", "id", 0, "facetEvent", {}, new Date()),
        ]);
      } catch (e) {
        expect(e.message).toBe("putHead: invalid facet for data record");
      }
    });
    it("validates event records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(
          newHeadRecord("facetName", "", 0, {}, new Date()),
          0,
          [newDataRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
          [newDataRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
        );
      } catch (e) {
        expect(e.message).toBe("putHead: invalid event record");
      }
    });
    it("validates event records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putHead(
          newHeadRecord("facetName", "", 0, {}, new Date()),
          0,
          [newDataRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
          [newEventRecord("incorrect_facet", "id", 0, 1, "eventType", {}, new Date())],
        );
      } catch (e) {
        expect(e.message).toBe("putHead: invalid facet for event record");
      }
    });
    it("validates that only 25 records can be posted at once", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      const dataRecords = [...Array(26).keys()].map((i) =>
        newDataRecord("facetName", "id", i, "typeName", {}, new Date()),
      );
      try {
        await db.putHead(newHeadRecord("facetName", "", 0, {}, new Date()), 0, dataRecords);
      } catch (e) {
        expect(e.message).toBe(
          "putHead: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write 27.",
        );
      }
    });
  });
});

interface DB {
  name: string;
  client: DocumentClient;
  delete: () => Promise<any>;
}

const randomTableName = () => `eventdb_test_${new Date().getTime()}`;

const createLocalTable = async (): Promise<DB> => {
  const ddb = new DynamoDB({
    region: "eu-west-1",
    endpoint: "http://localhost:8000",
  });
  const tableName = randomTableName();
  await ddb
    .createTable({
      KeySchema: [
        {
          KeyType: "HASH",
          AttributeName: "_id",
        },
        {
          KeyType: "RANGE",
          AttributeName: "_rng",
        },
      ],
      TableName: tableName,
      AttributeDefinitions: [
        {
          AttributeName: "_id",
          AttributeType: "S",
        },
        {
          AttributeName: "_rng",
          AttributeType: "S",
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    })
    .promise();

  await ddb.waitFor("tableExists", { TableName: tableName }).promise();

  return {
    name: tableName,
    client: new DocumentClient({
      region: "eu-west-1",
      endpoint: "http://localhost:8000",
    }),
    delete: async () => await ddb.deleteTable({ TableName: tableName }).promise(),
  };
};
