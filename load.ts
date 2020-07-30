import * as AWS from "aws-sdk";
import {putRecord, createDataRecord} from ".";

const client = new AWS.DynamoDB.DocumentClient({
  region: "eu-west-2",
});

//aws dynamodb create-table \
//  --table-name events \
//  --attribute-definitions AttributeName=_id,AttributeType=S AttributeName=_rng,AttributeType=S \
//  --key-schema AttributeName=_id,KeyType=HASH AttributeName=_rng,KeyType=RANGE \
//  --billing-mode PAY_PER_REQUEST 

const tableName = "events";

interface Data {
  count: number,
}

const importData = async () => {
  for(let i = 0; i < 200; i ++) {
    const data = { count: i } as Data;
    const r = createDataRecord<Data>("a", "data", data);
    await putRecord(client, tableName, r);
  }
};

importData()
 .then(() => console.log("complete"));
