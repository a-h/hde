// HeadUpdater<THead, TCurrent> defines a function used to update head based on the current type.
export type HeadUpdater<THead, TCurrent> = (input: HeadUpdaterInput<THead, TCurrent>) => THead;

// HeadUpdaterInput is the input to the HeadUpdater function.
export interface HeadUpdaterInput<THead, TCurrent> {
  // head value of the facet.
  head: THead;
  // current data that is modifying the head.
  current: TCurrent;
  // data that already exists.
  existingData: Array<Data<any>>;
  // data that is being added.
  newData: Array<Data<any>>;
  // all allows access to all of the data, new and old.
  all: Array<Data<any>>;
  // current index within the data.
  currentIndex: number;
  // The index of the latest datad within all data.
  headIndex: number;
  // publish an event.
  publish: (name: string, event: any) => void;
}

// Data is data that makes up the facet item. Reading through all the data, and applying the rules creates
// a materialised view. This view is the "HEAD" record stored in the database.
export class Data<T> {
  typeName: string;
  data: T | null;
  constructor(typeName: string, data: T | null) {
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
  head: T;
  pastEvents: Array<Data<any>>;
  newEvents: Array<Data<any>>;
}

// A Processor processes events and updates the state of the head record.
export class Processor<T> {
  rules: Map<RecordTypeName, HeadUpdater<T, RecordType>>;
  initial: Initializer<T>;
  constructor(
    rules: Map<RecordTypeName, HeadUpdater<T, RecordType>>,
    initial: Initializer<T> = () => ({} as T),
  ) {
    this.rules = rules;
    this.initial = initial;
  }
  process(
    head: T | null,
    existingData: Array<Data<any>> = new Array<Data<any>>(),
    newData: Array<Data<any>> = new Array<Data<any>>(),
  ): ProcessResult<T> {
    const allData = [...existingData, ...newData];
    const result: ProcessResult<T> = {
      head: head || this.initial(),
      pastEvents: new Array<Data<any>>(),
      newEvents: new Array<Data<any>>(),
    };
    const rules = this.rules;
    allData.forEach((curr, idx) => {
      const updater = rules.get(curr.typeName);
      if (updater) {
        result.head = updater({
          head: result.head,
          existingData: existingData,
          newData: newData,
          current: curr.data,
          currentIndex: idx,
          all: allData,
          headIndex: existingData.length,
          publish: (eventName: string, event: any) => {
            const ed = new Data(eventName, event);
            idx >= existingData.length ? result.newEvents.push(ed) : result.pastEvents.push(ed);
          },
        });
      }
    });
    return result;
  }
}
