import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { DynamoDB } from "aws-sdk";
import { EventDB, newStateRecord, newInboundRecord, newOutboundRecord } from ".";

describe("EventDB", () => {
  describe("getState", () => {
    it("can get the state record if it exists", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const state = { key: "value" };
        const stateRecord = newStateRecord<any>("facetName", "idValue", 1, state, new Date());
        await db.putState(stateRecord, 0);

        const actual = await db.getState("idValue");
        expect(actual).toEqual(stateRecord);
      } finally {
        await testDB.delete();
      }
    });
  });
  describe("putState", () => {
    it("can put a new state record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const state1 = { key: "value1" };
        const stateRecord1 = newStateRecord<any>("facetName", "idValue", 1, state1, new Date());
        await db.putState(stateRecord1, 0);
        const state2 = { key: "value2" };
        const stateRecord2 = newStateRecord<any>("facetName", "idValue", 2, state2, new Date());
        await db.putState(stateRecord2, 1);

        const actual = await db.getState("idValue");
        expect(actual).toEqual(stateRecord2);
      } finally {
        await testDB.delete();
      }
    });
    it("can put inbound records alongside a new state record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const state = { key: "value1" };
        const stateRecord = newStateRecord<any>("facetName", "idValue", 3, state, new Date());
        const inboundRecords = [
          newInboundRecord("facetName", "idValue", 1, "inbound", { record: "inbound1" }, new Date()),
          newInboundRecord("facetName", "idValue", 2, "inbound", { record: "inbound2" }, new Date()),
        ];
        await db.putState(stateRecord, 0, inboundRecords);

        const actual = await db.getRecords("idValue");
        expect(actual).toEqual([...inboundRecords, stateRecord]);
      } finally {
        await testDB.delete();
      }
    });
    it("can put outbound records alongside a new state record", async () => {
      const testDB = await createLocalTable();
      try {
        const db = new EventDB(testDB.client, testDB.name, "facetName");
        const state = { key: "value1" };
        const stateRecord = newStateRecord<any>("facetName", "idValue", 5, state, new Date());
        const inboundRecords = [
          newInboundRecord("facetName", "idValue", 1, "inbound", { record: "inbound1" }, new Date()),
          newInboundRecord("facetName", "idValue", 2, "inbound", { record: "inbound2" }, new Date()),
        ];
        const outboundRecords = [
          newOutboundRecord("facetName", "idValue", 3, 0, "inbound", { record: "inbound1" }, new Date()),
          newOutboundRecord("facetName", "idValue", 3, 1, "outbound", { outbound: "test1" }, new Date()),
        ];
        await db.putState(stateRecord, 0, inboundRecords, outboundRecords);

        const actual = await db.getRecords("idValue");
        expect(actual).toEqual([...inboundRecords, ...outboundRecords, stateRecord]);
      } finally {
        await testDB.delete();
      }
    });
    it("validates state records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(newOutboundRecord("not_important", "", 0, 0, "test", {}, new Date()), 0);
      } catch (e) {
        expect(e.message).toBe("putState: invalid state record");
      }
    });
    it("validates state records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(newStateRecord("incorrect_facet", "", 0, {}, new Date()), 0);
      } catch (e) {
        expect(e.message).toBe(
          'putState: state record has mismatched facet. Expected: "facetName", got: "incorrect_facet"',
        );
      }
    });
    it("validates inbound records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(newStateRecord("facetName", "", 0, {}, new Date()), 0, [
          newOutboundRecord("facetName", "id", 0, 1, "facetEvent", {}, new Date()),
        ]);
      } catch (e) {
        expect(e.message).toBe("putState: invalid inbound record");
      }
    });
    it("validates inbound records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(newStateRecord("facetName", "", 0, {}, new Date()), 0, [
          newInboundRecord("incorrect_facet", "id", 0, "facetEvent", {}, new Date()),
        ]);
      } catch (e) {
        expect(e.message).toBe("putState: invalid facet for inbound record");
      }
    });
    it("validates outbound records are the right type", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(
          newStateRecord("facetName", "", 0, {}, new Date()),
          0,
          [newInboundRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
          [newInboundRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
        );
      } catch (e) {
        expect(e.message).toBe("putState: invalid outbound record");
      }
    });
    it("validates outbound records are the right facet", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      try {
        await db.putState(
          newStateRecord("facetName", "", 0, {}, new Date()),
          0,
          [newInboundRecord("facetName", "id", 0, "facetEvent", {}, new Date())],
          [newOutboundRecord("incorrect_facet", "id", 0, 1, "outboundType", {}, new Date())],
        );
      } catch (e) {
        expect(e.message).toBe("putState: invalid facet for outbound record");
      }
    });
    it("validates that only 25 records can be posted at once", async () => {
      const db = new EventDB({} as DocumentClient, "fakeName", "facetName");
      const inboundRecords = Array.from(new Array(26), (i) =>
        newInboundRecord("facetName", "id", i, "anyTypeName", {}, new Date()),
      );
      try {
        await db.putState(newStateRecord("facetName", "", 0, {}, new Date()), 0, inboundRecords);
      } catch (e) {
        expect(e.message).toBe(
          "putState: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write 27.",
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
