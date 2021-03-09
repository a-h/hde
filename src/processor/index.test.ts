import { Processor, StateUpdater, Event, StateUpdaterInput } from ".";

class Window {
  size: number;
  values: Array<number>;
  constructor(size: number, values: Array<number>) {
    this.size = size;
    this.values = values;
  }
}

class Sample {
  value: number;
  constructor(value: number) {
    this.value = value;
  }
}

describe("Processor<T>", () => {
  it("uses the initial value to base processing on", () => {
    const rules = new Map<string, StateUpdater<Window, any, any, any>>();
    const initial = () => new Window(5, [1, 2, 3, 4, 5]);
    const p = new Processor<Window, any, any>(rules, initial);

    const actual = p.process(null);

    expect(actual.state).toEqual(initial());
    expect(actual.newOutboundEvents).toHaveLength(0);
    expect(actual.pastOutboundEvents).toHaveLength(0);
  });
  it("uses a default initial value if none is provided", () => {
    const rules = new Map<string, StateUpdater<Window, any, any, any>>();
    const p = new Processor<Window, any, any>(rules);

    const actual = p.process(null);

    expect(actual.state).toEqual({});
  });
  it("uses the state value in preference to the initial function if possible", () => {
    const rules = new Map<string, StateUpdater<Window, any, any, any>>();
    const initial = () => ({} as Window);
    const p = new Processor<Window, any, any>(rules, initial);

    const state = new Window(5, [1, 2, 3, 4, 5]);
    const actual = p.process(state);

    expect(actual.state).toEqual(new Window(5, [1, 2, 3, 4, 5]));
    expect(actual.newOutboundEvents).toHaveLength(0);
    expect(actual.pastOutboundEvents).toHaveLength(0);
  });
  it("does not take action on unknown record types", () => {
    type TestEvent = unknown;

    const rules = new Map<string, StateUpdater<Window, TestEvent, TestEvent, TestEvent>>();
    const unknownHandler = jest.fn();
    rules.set("UNKNOWN", unknownHandler);
    const initial = () => ({} as Window);
    const p = new Processor<Window, TestEvent, TestEvent>(rules, initial);

    const state = new Window(5, [1, 2, 3, 4, 5]);
    const previous = new Array<Event<TestEvent>>(new Event("KNOWN", {}));
    const next = new Array<Event<TestEvent>>(new Event("KNOWN", {}));
    const actual = p.process(state, previous, next);

    expect(actual.state).toEqual(new Window(5, [1, 2, 3, 4, 5]));
    expect(actual.newOutboundEvents).toHaveLength(0);
    expect(actual.pastOutboundEvents).toHaveLength(0);
    expect(unknownHandler).not.toHaveBeenCalled();
  });
  it("takes action on known record types, for previous and new data", () => {
    const rules = new Map<string, StateUpdater<Window, Sample, Sample, Sample>>();
    rules.set("SAMPLE", (input: StateUpdaterInput<Window, Sample, Sample, Sample>) => {
      input.state.values.push(input.current.value);
      if (input.state.values.length > input.state.size) {
        input.state.values = input.state.values.slice(1);
      }
      return input.state;
    });
    const initial = () => ({} as Window);
    const p = new Processor<Window, Sample, Sample>(rules, initial);

    const state = new Window(5, []);
    const previous = new Array<Event<Sample>>(
      new Event("SAMPLE", new Sample(1)),
      new Event("SAMPLE", new Sample(2)),
      new Event("SAMPLE", new Sample(3)),
    );
    const next = new Array<Event<Sample>>(
      new Event("SAMPLE", new Sample(4)),
      new Event("SAMPLE", new Sample(5)),
      new Event("SAMPLE", new Sample(6)),
    );
    const actual = p.process(state, previous, next);

    expect(actual.state).toEqual(new Window(5, [2, 3, 4, 5, 6]));
    expect(actual.newOutboundEvents).toHaveLength(0);
    expect(actual.pastOutboundEvents).toHaveLength(0);
  });
  it("can publish events", () => {
    interface OutputEvent {
      sum: number;
    }
    const rules = new Map<string, StateUpdater<Window, Sample, OutputEvent, Sample>>();
    rules.set("SAMPLE", (input: StateUpdaterInput<Window, Sample, OutputEvent, Sample>) => {
      input.state.values.push(input.current.value);
      if (input.state.values.length > input.state.size) {
        input.state.values = input.state.values.slice(1);
      }
      const sum = input.state.values.reduce((prev, current) => prev + current);
      if (sum === 3) {
        input.publish("3 reached", { sum: 3 });
      }
      if (sum === 20) {
        input.publish("20 reached", { sum: 20 });
      }
      return input.state;
    });
    const p = new Processor<Window, Sample, OutputEvent>(rules);

    const state = new Window(5, []);
    const previous = new Array<Event<Sample>>(
      new Event("SAMPLE", new Sample(1)),
      new Event("SAMPLE", new Sample(2)),
      new Event("SAMPLE", new Sample(3)),
    );
    const next = new Array<Event<Sample>>(
      new Event("SAMPLE", new Sample(4)),
      new Event("SAMPLE", new Sample(5)),
      new Event("SAMPLE", new Sample(6)),
    );
    const actual = p.process(state, previous, next);

    expect(actual.state).toEqual(new Window(5, [2, 3, 4, 5, 6]));
    expect(actual.newOutboundEvents).toHaveLength(1);
    expect(actual.pastOutboundEvents).toHaveLength(1);
  });
});
