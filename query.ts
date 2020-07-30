import * as AWS from "aws-sdk";
import { getRecords } from ".";

const client = new AWS.DynamoDB.DocumentClient({
  region: "eu-west-2",
});

const tableName = "events";

const queryData = async () => {
  const records = await getRecords(client, tableName, "a");
  console.log(records.length.toString());
  console.log(JSON.stringify(records[0]));
};

queryData().then(() => console.log("complete"));
