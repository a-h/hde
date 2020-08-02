import { Processor, HeadUpdater, Data, HeadUpdaterInput } from ".";

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
    const rules = new Map<string, HeadUpdater<Window, any>>();
    const initial = () => new Window(5, [1, 2, 3, 4, 5]);
    const p = new Processor<Window>(rules, initial);

    const actual = p.process(null);

    expect(actual.head).toEqual(initial());
    expect(actual.newEvents).toHaveLength(0);
    expect(actual.pastEvents).toHaveLength(0);
  });
  it("uses a default initial value if none is provided", () => {
    const rules = new Map<string, HeadUpdater<Window, any>>();
    const p = new Processor<Window>(rules);

    const actual = p.process(null);

    expect(actual.head).toEqual({});
  });
  it("uses the head value in preference to the initial function if possible", () => {
    const rules = new Map<string, HeadUpdater<Window, any>>();
    const initial = () => ({} as Window);
    const p = new Processor<Window>(rules, initial);

    const head = new Window(5, [1, 2, 3, 4, 5]);
    const actual = p.process(head);

    expect(actual.head).toEqual(new Window(5, [1, 2, 3, 4, 5]));
    expect(actual.newEvents).toHaveLength(0);
    expect(actual.pastEvents).toHaveLength(0);
  });
  it("does not take action on unknown record types", () => {
    const rules = new Map<string, HeadUpdater<Window, any>>();
    const unknownHandler = jest.fn();
    rules.set("UNKNOWN", unknownHandler);
    const initial = () => ({} as Window);
    const p = new Processor<Window>(rules, initial);

    const head = new Window(5, [1, 2, 3, 4, 5]);
    const previous = new Array<Data<any>>(new Data("KNOWN", {}));
    const next = new Array<Data<any>>(new Data("KNOWN", {}));
    const actual = p.process(head, previous, next);

    expect(actual.head).toEqual(new Window(5, [1, 2, 3, 4, 5]));
    expect(actual.newEvents).toHaveLength(0);
    expect(actual.pastEvents).toHaveLength(0);
    expect(unknownHandler).not.toHaveBeenCalled();
  });
  it("takes action on known record types, for previous and new data", () => {
    const rules = new Map<string, HeadUpdater<Window, any>>();
    rules.set("SAMPLE", (input: HeadUpdaterInput<Window, Sample>) => {
      input.head.values.push(input.current.value);
      if (input.head.values.length > input.head.size) {
        input.head.values = input.head.values.slice(1);
      }
      return input.head;
    });
    const initial = () => ({} as Window);
    const p = new Processor<Window>(rules, initial);

    const head = new Window(5, []);
    const previous = new Array<Data<any>>(
      new Data("SAMPLE", new Sample(1)),
      new Data("SAMPLE", new Sample(2)),
      new Data("SAMPLE", new Sample(3))
    );
    const next = new Array<Data<any>>(
      new Data("SAMPLE", new Sample(4)),
      new Data("SAMPLE", new Sample(5)),
      new Data("SAMPLE", new Sample(6))
    );
    const actual = p.process(head, previous, next);

    expect(actual.head).toEqual(new Window(5, [2, 3, 4, 5, 6]));
    expect(actual.newEvents).toHaveLength(0);
    expect(actual.pastEvents).toHaveLength(0);
  });
  it("can publish events", () => {
    const rules = new Map<string, HeadUpdater<Window, any>>();
    rules.set("SAMPLE", (input: HeadUpdaterInput<Window, Sample>) => {
      input.head.values.push(input.current.value);
      if (input.head.values.length > input.head.size) {
        input.head.values = input.head.values.slice(1);
      }
      const sum = input.head.values.reduce((prev, current) => prev + current);
      if (sum === 3) {
        input.publish("3 reached", { sum: 3 });
      }
      if (sum === 20) {
        input.publish("20 reached", { sum: 20 });
      }
      return input.head;
    });
    const p = new Processor<Window>(rules);

    const head = new Window(5, []);
    const previous = new Array<Data<any>>(
      new Data("SAMPLE", new Sample(1)),
      new Data("SAMPLE", new Sample(2)),
      new Data("SAMPLE", new Sample(3))
    );
    const next = new Array<Data<any>>(
      new Data("SAMPLE", new Sample(4)),
      new Data("SAMPLE", new Sample(5)),
      new Data("SAMPLE", new Sample(6))
    );
    const actual = p.process(head, previous, next);

    expect(actual.head).toEqual(new Window(5, [2, 3, 4, 5, 6]));
    expect(actual.newEvents).toHaveLength(1);
    expect(actual.pastEvents).toHaveLength(1);
  });
});
