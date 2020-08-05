import { Facet, DB, GetOutput, ChangeOutput } from ".";
import {
  RecordTypeName,
  HeadUpdater,
  RecordType,
  HeadUpdaterInput,
  Processor,
  Data,
} from "./processor";
import {
  Record,
  HeadRecord,
  DataRecord,
  EventRecord,
  newHeadRecord,
  newDataRecord,
  newEventRecord,
} from "./db";
import { Records } from "aws-sdk/clients/rdsdataservice";

type GetHead = (id: string) => Promise<Record>;
type GetRecords = (id: string) => Promise<Array<Record>>;
type PutHead = (
  head: HeadRecord,
  previousSeq: number,
  data: Array<DataRecord>,
  events: Array<EventRecord>
) => Promise<void>;

class MockDB implements DB {
  getHead: GetHead;
  getRecords: GetRecords;
  putHead: PutHead;
  constructor(
    getHead: GetHead = jest.fn(),
    getRecords: GetRecords = jest.fn(),
    putHead: PutHead = jest.fn()
  ) {
    this.getHead = getHead;
    this.getRecords = getRecords;
    this.putHead = putHead;
  }
}

interface TestHead {
  a: string;
  b: string;
}

interface TestData {
  data1: string;
  data2: string;
}

describe("facet", () => {
  describe("get", () => {
    it("returns null when the db returns null", async () => {
      const db = new MockDB();
      const emptyRules = new Map<
        RecordTypeName,
        HeadUpdater<any, RecordType>
      >();
      const processor = new Processor<any>(emptyRules);
      const facet = new Facet<any>("name", db, processor);

      const head = await facet.get("abc");

      expect(head).toBeNull();
    });
    it("returns the _itm when the db returns a record", async () => {
      const expectedHead: TestHead = { a: "a", b: "b" };
      const expectedRecord = { _itm: JSON.stringify(expectedHead) } as Record;

      const expected: GetOutput<TestHead> = {
        record: expectedRecord,
        item: expectedHead,
      };
      const db = new MockDB();
      db.getHead = async () => expectedRecord;
      const emptyRules = new Map<
        RecordTypeName,
        HeadUpdater<any, RecordType>
      >();
      const processor = new Processor<any>(emptyRules);
      const facet = new Facet<any>("name", db, processor);

      const getOutput = await facet.get("abc");

      expect(getOutput).toEqual(expected);
    });
  });
  describe("append", () => {
    it("stores new events in the database", async () => {
      const event1 = {
        name: "event1",
      };
      const event21 = {
        name: "event2.1",
      };
      const event22 = {
        name: "event2.2",
      };
      const initial: TestHead = { a: "0", b: "empty" };
      const db = new MockDB();
      db.putHead = async (head, _previousSeq, _data, events) => {
        expect(head._itm).toEqual(JSON.stringify(initial));
        expect(events.length).toBe(3);
        expect(events[0]._typ).toEqual("eventName1");
        expect(events[0]._itm).toBe(JSON.stringify(event1));
        expect(events[1]._typ).toEqual("eventName2.1");
        expect(events[1]._itm).toBe(JSON.stringify(event21));
        expect(events[2]._typ).toEqual("eventName2.2");
        expect(events[2]._itm).toBe(JSON.stringify(event22));
      };

      // Create the rules.
      const publishEvent = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      publishEvent.set("Record1", (input) => {
        input.publish("eventName1", event1);
        return input.head;
      });
      publishEvent.set("Record2", (input) => {
        input.publish("eventName2.1", event21);
        input.publish("eventName2.2", event22);
        return input.head;
      });

      const processor = new Processor<TestHead>(publishEvent, () => initial);

      const facet = new Facet<TestHead>("name", db, processor);
      await facet.append(
        "id",
        new Data("Record1", {}),
        new Data("Record2", {})
      );
    });
    it("uses defaults if no head record exists", async () => {
      const initial: TestHead = { a: "0", b: "empty" };
      const db = new MockDB();
      // Don't return any records.
      db.getRecords = async (_id: string): Promise<Array<Record>> => [];

      // Create empty rules.
      const publishEvent = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      const processor = new Processor<TestHead>(publishEvent, () => initial);

      const facet = new Facet<TestHead>("name", db, processor);
      facet.appendTo = async (
        id,
        head,
        seq
      ): Promise<ChangeOutput<TestHead>> => {
        expect(id).toEqual("id");
        expect(head).toBe(null);
        expect(seq).toEqual(1);
        return {
          seq: 1,
          item: {},
        } as ChangeOutput<TestHead>;
      };
      await facet.append("id");
    });
    it("uses the head record if it exists", async () => {
      const initial: TestHead = { a: "0", b: "empty" };
      const db = new MockDB();
      const expectedHead: TestHead = { a: "expected", b: "value" };
      // Return a head record.
      db.getHead = async (id: string): Promise<Record> =>
        newHeadRecord("name", id, 1, expectedHead, new Date());

      // Create empty rules.
      const publishEvent = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      const processor = new Processor<TestHead>(publishEvent, () => initial);

      const facet = new Facet<TestHead>("name", db, processor);
      facet.appendTo = async (
        id,
        head,
        seq
      ): Promise<ChangeOutput<TestHead>> => {
        expect(id).toEqual("id");
        expect(head).toEqual(expectedHead);
        expect(seq).toEqual(1);
        return {
          seq: 1,
          item: {},
        } as ChangeOutput<TestHead>;
      };
      await facet.append("id");
    });
  });
  describe("recalculate", () => {
    it("creates an empty head record on first put if it doesn't exist", async () => {
      const db = new MockDB();
      const emptyRules = new Map<
        RecordTypeName,
        HeadUpdater<any, RecordType>
      >();
      const processor = new Processor<any>(emptyRules);
      const facet = new Facet<any>("name", db, processor);
      const data: TestData = { data1: "1", data2: "2" };

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data)
      );

      expect(putOutput.item).toEqual({});
      expect(putOutput.newEvents).toHaveLength(0);
      expect(putOutput.seq).toBe(1);
    });
    it("creates an initial head record on first put if it doesn't exist", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "empty", b: "empty" };
      const emptyRules = new Map<
        RecordTypeName,
        HeadUpdater<any, RecordType>
      >();
      const processor = new Processor<TestHead>(emptyRules, () => initial);
      const facet = new Facet<any>("name", db, processor);
      const data: TestData = { data1: "1", data2: "2" };

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data)
      );

      expect(putOutput.item).toEqual(initial);
      expect(putOutput.newEvents).toHaveLength(0);
      expect(putOutput.seq).toBe(1);
    });
    it("uses the head updater to calculate the head record state based on initial data", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };
      const concatenateDataValuesToHead = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      concatenateDataValuesToHead.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.head.a = `${input.head.a}_${input.current.data1}`;
          return input.head;
        }
      );
      const processor = new Processor<TestHead>(
        concatenateDataValuesToHead,
        () => initial
      );
      const facet = new Facet<TestHead>("name", db, processor);
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data1),
        new Data<TestData>("TestData", data2)
      );

      const expected: TestHead = { a: "0_1_2", b: "empty" };

      expect(putOutput.item).toEqual(expected);
      expect(putOutput.newEvents).toHaveLength(0);
      expect(putOutput.seq).toBe(2);
    });
    it("uses the head updater to re-calculate the head record state based on new data", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const concatenateDataValuesToHead = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      concatenateDataValuesToHead.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.head.a = `${input.head.a}_${input.current.data1}`;
          return input.head;
        }
      );
      const processor = new Processor<TestHead>(
        concatenateDataValuesToHead,
        () => initial
      );
      const facet = new Facet<TestHead>("name", db, processor);
      // Configure the database to already have data1 and data2 present.
      const now = new Date();
      const currentHead: TestHead = { a: "0_1_2", b: "empty" };
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };
      const data3: TestData = { data1: "3", data2: "" };
      db.getRecords = async (_id: string): Promise<Array<Record>> =>
        new Array<Record>(
          newHeadRecord<TestHead>("TestHead", "id", 3, currentHead, now),
          newDataRecord<TestData>("TestHead", "id", 1, "TestData", data1, now),
          newDataRecord<TestData>("TestHead", "id", 2, "TestData", data2, now)
        );

      const expected: TestHead = { a: "0_1_2_3", b: "empty" };
      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data3)
      );

      expect(putOutput.item).toEqual(expected);
      expect(putOutput.newEvents).toHaveLength(0);
      expect(putOutput.seq).toBe(4);
    });
    it("ignores unkown record types in the calculation", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const concatenateDataValuesToHead = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      concatenateDataValuesToHead.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.head.a = `${input.head.a}_${input.current.data1}`;
          return input.head;
        }
      );
      const processor = new Processor<TestHead>(
        concatenateDataValuesToHead,
        () => initial
      );
      const facet = new Facet<TestHead>("name", db, processor);
      // Configure the database to already have data1 and data2 present.
      const now = new Date();
      const currentHead: TestHead = { a: "0_1_2", b: "empty" };
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };
      const data3: TestData = { data1: "3", data2: "" };
      db.getRecords = async (_id: string): Promise<Array<Record>> =>
        new Array<Record>(
          newHeadRecord<TestHead>("TestHead", "id", 3, currentHead, now),
          newDataRecord<TestData>("TestHead", "id", 1, "TestData", data1, now),
          newDataRecord<TestData>("TestHead", "id", 2, "TestData", data2, now),
          {
            _id: "unknown id",
            _seq: 4,
            _rng: "unknown range",
          } as Record
        );

      const expected: TestHead = { a: "0_1_2_3", b: "empty" };
      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data3)
      );

      expect(putOutput.item).toEqual(expected);
      expect(putOutput.newEvents).toHaveLength(0);
      expect(putOutput.seq).toBe(4);
    });
    it("returns a list of historical and new events", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const rules = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      rules.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.publish("eventName", { payload: input.current });
          return input.head;
        }
      );
      const processor = new Processor<TestHead>(rules, () => initial);
      const facet = new Facet<TestHead>("name", db, processor);
      // Configure the database to already have data1 and data2 present.
      const now = new Date();
      const currentHead: TestHead = { a: "0_1_2", b: "empty" };
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };
      const data3: TestData = { data1: "3", data2: "" };
      // These events are in the database, but the rules don't cover them.
      // This means that they get ignored.
      const event1 = { eventName: "event1" };
      const event2 = { eventName: "event2" };
      db.getRecords = async (_id: string): Promise<Array<Record>> =>
        new Array<Record>(
          newDataRecord<TestData>("TestHead", "id", 1, "TestData", data1, now),
          newDataRecord<TestData>("TestHead", "id", 2, "TestData", data2, now),
          newEventRecord("TestHead", "id", 3, 0, "TestEvent", event1, now),
          newEventRecord("TestHead", "id", 4, 1, "TestEvent", event2, now),
          newHeadRecord<TestHead>("TestHead", "id", 5, currentHead, now)
        );

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data3)
      );

      // We get two old events (one raised by data1, one raised by data2).
      expect(putOutput.pastEvents).toHaveLength(2);
      expect(putOutput.pastEvents[0]).toEqual({ payload: data1 });
      expect(putOutput.pastEvents[1]).toEqual({ payload: data2 });
      // We get a new even too, raised by the new record data3.
      expect(putOutput.newEvents).toHaveLength(1);
      expect(putOutput.newEvents[0]).toEqual({ payload: data3 });
    });
    it("sorts data after it's returned by the database", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const rules = new Map<
        RecordTypeName,
        HeadUpdater<TestHead, RecordType>
      >();
      const received = new Array<string>();
      rules.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          received.push(input.current.data1);
          return input.head;
        }
      );
      const processor = new Processor<TestHead>(rules, () => initial);
      const facet = new Facet<TestHead>("name", db, processor);
      // Configure the database to already have data1 and data2 present.
      const now = new Date();
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };
      const data3: TestData = { data1: "3", data2: "" };
      const data4: TestData = { data1: "4", data2: "" };

      // Return data incorrectly sorted.
      db.getRecords = async (_id: string): Promise<Array<Record>> =>
        new Array<Record>(
          newDataRecord<TestData>("TestHead", "id", 2, "TestData", data2, now),
          newDataRecord<TestData>("TestHead", "id", 1, "TestData", data1, now),
          newDataRecord<TestData>("TestHead", "id", 3, "TestData", data3, now),
          newDataRecord<TestData>("TestHead", "id", 3, "TestData", data4, now)
        );

      const putOutput = await facet.recalculate("id");

      expect(putOutput.item).toEqual(initial);
      expect(received).toEqual(["1", "2", "3", "4"]);
    });
  });
});
