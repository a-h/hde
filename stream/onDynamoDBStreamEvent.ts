import { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";
import { EventBridge } from "aws-sdk";

export const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
  log("processing records", { count: event.Records.length });
  for (let i = 0; i < event.Records.length; i++) {
    const r = event.Records[i];
    if (!hasRequiredKeys(r)) {
      continue;
    }
    const typ = r.dynamodb?.NewImage?._typ.S;
    const itm = r.dynamodb?.NewImage?._itm.S;
    if (typ && itm) {
      log("publishing outbound event", { id: r.dynamodb?.NewImage?._id.S, typ });
      await publish<string>("finance-v2", typ, itm);
      log("published outbound event", { id: r.dynamodb?.NewImage?._id.S, typ, count: 1 });
    }
  }
  log("processed records", { count: event.Records.length });
};

const hasStringKey = (r: DynamoDBRecord, k: string): boolean => !!r.dynamodb?.NewImage?.[k]?.S;

const hasSortKeyThatStartsWithOutbound = (r: DynamoDBRecord): boolean =>
  (hasStringKey(r, "_rng") && r.dynamodb?.NewImage?._rng?.S?.startsWith("OUTBOUND")) || false;
const hasRequiredKeys = (r: DynamoDBRecord): boolean =>
  hasSortKeyThatStartsWithOutbound(r) &&
  hasStringKey(r, "_facet") &&
  hasStringKey(r, "_typ") &&
  hasStringKey(r, "_itm");

const publish = async <TEvent>(
  source: string,
  detailType: string,
  detail: TEvent,
  eventBusName = "default",
) => {
  const eventBus = new EventBridge();
  const res = await eventBus
    .putEvents({
      Entries: [
        {
          EventBusName: eventBusName,
          Source: source,
          DetailType: detailType,
          Detail: typeof detail === "string" ? detail : JSON.stringify(detail),
        },
      ],
    })
    .promise();
  const errors: string[] = [];
  res.Entries?.forEach((entry) => {
    if (entry.ErrorMessage) {
      errors.push(entry.ErrorMessage);
      return;
    }
  });
  if (errors.length > 0) {
    throw new Error(errors.join(", "));
  }
};

const log = (msg: string, data: Record<string, unknown>) =>
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      level: "INFO",
      msg,
      ...data,
    }),
  );
