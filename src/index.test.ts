import {
  Facet,
  RecordName,
  HeadUpdater,
  RecordType,
  DB,
  GetOutput,
  Data,
  HeadUpdaterInput,
} from ".";
import {
  Record,
  HeadRecord,
  DataRecord,
  EventRecord,
  newHeadRecord,
  newDataRecord,
} from "./records";

type GetHead = (id: string) => Promise<Record>;
type GetRecords = (id: string) => Promise<Array<Record>>;
type PutHead = (
  head: HeadRecord,
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
      const emptyRules = new Map<RecordName, HeadUpdater<any, RecordType>>();
      const facet = new Facet<any>(db, "name", emptyRules);

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
      const emptyRules = new Map<RecordName, HeadUpdater<any, RecordType>>();
      const facet = new Facet<TestHead>(db, "name", emptyRules);

      const getOutput = await facet.get("abc");

      expect(getOutput).toEqual(expected);
    });
  });
  describe("recalculate", () => {
    it("creates an empty head record on first put if it doesn't exist", async () => {
      const db = new MockDB();
      const emptyRules = new Map<RecordName, HeadUpdater<any, RecordType>>();
      const facet = new Facet<TestHead>(db, "name", emptyRules);
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
      const emptyRules = new Map<RecordName, HeadUpdater<any, RecordType>>();
      const facet = new Facet<TestHead>(db, "name", emptyRules, () => initial);
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
        RecordName,
        HeadUpdater<any, RecordType>
      >();
      concatenateDataValuesToHead.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.head.a = `${input.head.a}_${input.current.data1}`;
          return input.head;
        }
      );
      const facet = new Facet<TestHead>(
        db,
        "name",
        concatenateDataValuesToHead,
        () => initial
      );
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
      expect(putOutput.seq).toBe(1);
    });
    it("uses the head updater to re-calculate the head record state based on new data", async () => {
      const db = new MockDB();
      const initial: TestHead = { a: "0", b: "empty" };

      // Create the rules.
      const concatenateDataValuesToHead = new Map<
        RecordName,
        HeadUpdater<any, RecordType>
      >();
      concatenateDataValuesToHead.set(
        "TestData",
        (input: HeadUpdaterInput<TestHead, TestData>): TestHead => {
          input.head.a = `${input.head.a}_${input.current.data1}`;
          return input.head;
        }
      );
      const facet = new Facet<TestHead>(
        db,
        "TestHead",
        concatenateDataValuesToHead,
        () => initial
      );
      // Configure the database to already have data1 and data2 present.
      const currentHead: TestHead = { a: "0_1_2", b: "empty" };
      const data1: TestData = { data1: "1", data2: "" };
      const data2: TestData = { data1: "2", data2: "" };
      const data3: TestData = { data1: "3", data2: "" };
      db.getRecords = async (_id: string): Promise<Array<Record>> =>
        new Array<Record>(
          newHeadRecord<TestHead>("TestHead", "id", 3, currentHead),
          newDataRecord<TestData>("TestHead", "id", 2, "TestData", data1),
          newDataRecord<TestData>("TestHead", "id", 3, "TestData", data2),
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
