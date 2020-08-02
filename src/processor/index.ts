// HeadUpdater<THead, TCurrent> defines a function used to update head based on the current type.
export type HeadUpdater<THead, TCurrent> = (
  input: HeadUpdaterInput<THead, TCurrent>
) => THead;

// HeadUpdaterInput is the input to the HeadUpdater function.
export interface HeadUpdaterInput<THead, TCurrent> {
  // head value of the facet.
  head: THead;
  // headSeq is the sequence number of the current head value.
  headSeq: number;
  // current data that is modifying the head.
  current: TCurrent;
  currentSeq: number;
  // all allows access to all of the data, new and old.
  all: Array<SequenceData<any>>;
  // current index within the sorted records.
  index: number;
  // publish an event.
  publish: (name: string, event: any) => void;
}

// SequenceData is data that makes up the facet item. Reading through all the data, and applying the rules creates
// a materialised view. This view is the "HEAD" record stored in the database.
export class SequenceData<T> {
  seq: number;
  typeName: string;
  data: T | null;
  constructor(seq: number, typeName: string, data: T | null) {
    this.seq = seq;
    this.typeName = typeName;
    this.data = data;
  }
}

// RecordTypeName should be the name of the record's type, e.g. AccountDetails.
export type RecordTypeName = string;

export type RecordType = any;

// Initializer constructs the default value of an item. For example, if the item is a
// bank account, perhaps the starting balance would be zero, and an overdraft of 1000 would
// be set.
export type Initializer<T> = () => T;

export interface ProcessResult<T> {
  head: SequenceData<T>;
  pastEvents: Array<SequenceData<any>>;
  newEvents: Array<SequenceData<any>>;
}

// A Processor processes events and updates the state of the head record.
export class Processor<T> {
  rules: Map<RecordTypeName, HeadUpdater<T, RecordType>>;
  initial: Initializer<T>;
  constructor(
    rules: Map<RecordTypeName, HeadUpdater<T, RecordType>>,
    initial: Initializer<T> = () => ({} as T)
  ) {
    this.rules = rules;
    this.initial = initial;
  }
  process(
    head: SequenceData<T>,
    existingData: Array<SequenceData<any>>,
    newData: Array<SequenceData<any>>
  ): ProcessResult<T> {
    const result: ProcessResult<T> = {
      head: head,
      pastEvents: new Array<SequenceData<any>>(),
      newEvents: new Array<SequenceData<any>>(),
    };
    // Initialize the head if required.
    if (result.head.data == null) {
      result.head.data = this.initial();
    }
    const rules = this.rules;
    let latestSeq = result.head.seq + newData.length;
    const allData = [...existingData, ...newData];
    allData.forEach((curr, idx) => {
      const updater = rules.get(curr.typeName);
      if (updater) {
        result.head.data = updater({
          head: result.head.data,
          headSeq: result.head.seq,
          current: curr.data,
          currentSeq: curr.seq,
          all: allData,
          index: idx,
          publish: (eventName: string, event: any) => {
            const ed = new SequenceData(latestSeq, eventName, event);
            curr.seq > head.seq
              ? result.newEvents.push(ed)
              : result.pastEvents.push(ed);
            latestSeq++;
          },
        } as HeadUpdaterInput<T, any>);
      }
    });
    result.head.seq = latestSeq;
    return result;
  }
}
