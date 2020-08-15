// StateUpdater<TState, TCurrent> defines a function used to update the state given the current incoming event.
export type StateUpdater<TState, TCurrent> = (input: StateUpdaterInput<TState, TCurrent>) => TState;

// StateUpdaterInput is the input to the StateUpdater function.
export interface StateUpdaterInput<TState, TCurrent> {
  // state of the facet.
  state: TState;
  // current event that is modifying the state.
  current: TCurrent;
  // events that already exist.
  pastInboundEvents: Array<Event<any>>;
  // events that are being added.
  newInboundEvents: Array<Event<any>>;
  // all allows access to all of the events, new and old.
  all: Array<Event<any>>;
  // current index within the events.
  currentIndex: number;
  // The index of the latest event within all events.
  stateIndex: number;
  // publish an outbound event.
  publish: (name: string, event: any) => void;
}

// An Event can be an inbound event that makes up the facet state, or an outbound event emitted
// due to a state change. Reading through all the inbound events, and applying the rules creates
// a materialised view. This view is the "STATE" record stored in the database.
export class Event<T> {
  type: string;
  event: T | null;
  constructor(type: string, event: T | null) {
    this.type = type;
    this.event = event;
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
  state: T;
  pastOutboundEvents: Array<Event<any>>;
  newOutboundEvents: Array<Event<any>>;
}

// A Processor processes events and updates the state of the state record.
export class Processor<T> {
  rules: Map<RecordTypeName, StateUpdater<T, RecordType>>;
  initial: Initializer<T>;
  constructor(
    rules: Map<RecordTypeName, StateUpdater<T, RecordType>>,
    initial: Initializer<T> = () => ({} as T),
  ) {
    this.rules = rules;
    this.initial = initial;
  }
  process(
    state: T | null,
    pastInboundEvents: Array<Event<any>> = new Array<Event<any>>(),
    newInboundEvents: Array<Event<any>> = new Array<Event<any>>(),
  ): ProcessResult<T> {
    const result: ProcessResult<T> = {
      state: state || this.initial(),
      pastOutboundEvents: new Array<Event<any>>(),
      newOutboundEvents: new Array<Event<any>>(),
    };
    const allEvents = [...pastInboundEvents, ...newInboundEvents];
    const rules = this.rules;
    allEvents.forEach((curr, idx) => {
      const updater = rules.get(curr.type);
      if (updater) {
        result.state = updater({
          state: result.state,
          pastInboundEvents: pastInboundEvents,
          newInboundEvents: newInboundEvents,
          current: curr.event,
          currentIndex: idx,
          all: allEvents,
          stateIndex: pastInboundEvents.length,
          publish: (eventName: string, event: any) => {
            const ed = new Event(eventName, event);
            idx >= pastInboundEvents.length ? result.newOutboundEvents.push(ed) : result.pastOutboundEvents.push(ed);
          },
        });
      }
    });
    return result;
  }
}
