// StateUpdater<TState, TInputEvents, TOutputEvents, TCurrent> defines a function used to update the state given the current incoming event.
export type StateUpdater<TState, TInputEvents, TOutputEvents, TCurrent extends TInputEvents> = (
  input: StateUpdaterInput<TState, TInputEvents, TOutputEvents, TCurrent>,
) => TState;

// StateUpdaterInput is the input to the StateUpdater function.
export interface StateUpdaterInput<
  TState,
  TInputEvents,
  TOutputEvents,
  TCurrent extends TInputEvents
> {
  // state of the facet.
  state: TState;
  // current event that is modifying the state.
  current: TCurrent;
  // events that already exist.
  pastInboundEvents: Array<Event<TInputEvents>>;
  // events that are being added.
  newInboundEvents: Array<Event<TInputEvents>>;
  // all allows access to all of the events, new and old.
  all: Array<Event<TInputEvents>>;
  // current index within the events.
  currentIndex: number;
  // The index of the latest event within all events.
  stateIndex: number;
  // publish an outbound event.
  publish: (name: string, event: TOutputEvents) => void;
}

// An Event can be an inbound event that makes up the facet state, or an outbound event emitted
// due to a state change. Reading through all the inbound events, and applying the rules creates
// a materialised view. This view is the "STATE" record stored in the database.
export class Event<TEvent> {
  constructor(readonly type: string, readonly event: TEvent) {}
}

// RecordTypeName should be the name of the record's type, e.g. AccountDetails.
export type RecordTypeName = string;

// Initializer constructs the default value of an item. For example, if the item is a
// bank account, perhaps the starting balance would be zero, and an overdraft of 1000 would
// be set.
export type Initializer<T> = () => T;

export interface ProcessResult<T, TOutputEvents> {
  state: T;
  pastOutboundEvents: Array<Event<TOutputEvents>>;
  newOutboundEvents: Array<Event<TOutputEvents>>;
}

// A Processor processes events and updates the state of the state record.
export class Processor<TState, TInputEvents, TOutputEvents> {
  rules: Map<RecordTypeName, StateUpdater<TState, TInputEvents, TOutputEvents, TInputEvents>>;
  initial: Initializer<TState>;
  constructor(
    rules: Map<RecordTypeName, StateUpdater<TState, TInputEvents, TOutputEvents, TInputEvents>>,
    initial: Initializer<TState> = () => ({} as TState),
  ) {
    this.rules = rules;
    this.initial = initial;
  }
  process(
    state: TState | null,
    pastInboundEvents: Array<Event<TInputEvents>> = new Array<Event<TInputEvents>>(),
    newInboundEvents: Array<Event<TInputEvents>> = new Array<Event<TInputEvents>>(),
  ): ProcessResult<TState, TOutputEvents> {
    const result: ProcessResult<TState, TOutputEvents> = {
      state: state || this.initial(),
      pastOutboundEvents: new Array<Event<TOutputEvents>>(),
      newOutboundEvents: new Array<Event<TOutputEvents>>(),
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
          publish: (eventName: string, event: TOutputEvents) => {
            const ed = new Event(eventName, event);
            idx >= pastInboundEvents.length
              ? result.newOutboundEvents.push(ed)
              : result.pastOutboundEvents.push(ed);
          },
        });
      }
    });
    return result;
  }
}
