import { Facet, DB, GetOutput, Data } from ".";
import {
  RecordTypeName,
  HeadUpdater,
  RecordType,
  HeadUpdaterInput,
  Processor,
} from "./processor";
import {
  Record,
  HeadRecord,
  DataRecord,
  EventRecord,
  newHeadRecord,
  newDataRecord,
} from "./db";

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
      const emptyRules = new Map<RecordTypeName, HeadUpdater<any, RecordType>>();
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
      const emptyRules = new Map<RecordTypeName, HeadUpdater<any, RecordType>>();
      const processor = new Processor<any>(emptyRules);
      const facet = new Facet<any>("name", db, processor);

      const getOutput = await facet.get("abc");

      expect(getOutput).toEqual(expected);
    });
  });
  describe("recalculate", () => {
    it("creates an empty head record on first put if it doesn't exist", async () => {
      const db = new MockDB();
      const emptyRules = new Map<RecordTypeName, HeadUpdater<any, RecordType>>();
      const processor = new Processor<any>(emptyRules);
      const facet = new Facet<any>("name", db, processor);
      const data: TestData = { data1: "1", data2: "2" };

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data)
      );

      expect(putOutput.id).toEqual("id");
      expect(putOutput.item).toEqual({});
      expect(putOutput.events).toHaveLength(0);
      expect(putOutput.seq).toBe(1);
    });
    it("creates an initial head record on first put if it doesn't exist", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "empty", b: "empty" };
      const emptyRules = new Map<RecordTypeName, HeadUpdater<any, RecordType>>();
      const processor = new Processor<TestHead>(emptyRules, () => initial);
      const facet = new Facet<any>("name", db, processor);
      const data: TestData = { data1: "1", data2: "2" };

      const putOutput = await facet.recalculate(
        "id",
        new Data<TestData>("TestData", data)
      );

      expect(putOutput.id).toEqual("id");
      expect(putOutput.item).toEqual(initial);
      expect(putOutput.events).toHaveLength(0);
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

      expect(putOutput.id).toEqual("id");
      expect(putOutput.item).toEqual(expected);
      expect(putOutput.events).toHaveLength(0);
      expect(putOutput.seq).toBe(2);
    });
    it("uses the head updater to re-calculate the head record state based on new data", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const concatenateDataValuesToHead = new Map<
        RecordTypeName,
        HeadUpdater<any, RecordType>
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

      expect(putOutput.id).toEqual("id");
      expect(putOutput.item).toEqual(expected);
      expect(putOutput.events).toHaveLength(0);
      expect(putOutput.seq).toBe(4);
    });
  });
});
