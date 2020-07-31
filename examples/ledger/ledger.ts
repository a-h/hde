import * as AWS from "aws-sdk";
import {
  Facet,
  HeadUpdaterInput,
  Data,
  HeadUpdater,
} from "../../src";

// The account Facet has multiple records.
// Head: The current "AccountBalance".
// Data: The account is made up of "Transaction" and "AccountUpdate" records.
// Events: An "AccountOverdrawn" event is emitted when the balance becomes < 0, and this is allowed due to an overdraft.
// Events: A "TransactionFailed" event is emitted when a transaction cannot be completed, due to insufficient funds.
interface AccountBalance {
  id: string;
  ownerFirst: string;
  ownerLast: string;
  balance: number;
}
const AccountBalanceRecordName = "ACCOUNT_BALANCE";

// Data events must have a name.
interface AccountUpdate {
  ownerFirst: string;
  ownerLast: string;
}
const AccountUpdateRecordName = "ACCOUNT_UPDATE";
interface Transaction {
  desc: string;
  amount: number;
}
const TransactionRecordName = "TRANSACTION";

// These events are not named.
interface AccountOverdrawn {
  accountId: string;
}
interface TransactionFailed {
  accountId: string;
  transaction: Transaction;
}

const demonstrateLedger = async () => {
  // Create a table.
  //  aws dynamodb create-table \
  //    --table-name ledger \
  //    --attribute-definitions AttributeName=_id,AttributeType=S AttributeName=_rng,AttributeType=S \
  //    --key-schema AttributeName=_id,KeyType=HASH AttributeName=_rng,KeyType=RANGE \
  //    --billing-mode PAY_PER_REQUEST
  const client = new AWS.DynamoDB.DocumentClient({
    region: "eu-west-2",
  });
  const tableName = "ledger";

  // The rules define how the AccountBalance is updated by incoming Data events.
  // For example, and incoming "TRANSACTION" record modifies the "ACCOUNT_BALANCE" record.
  // The function must be pure, it must not carry out IO (e.g. network requests, or disk
  // access), and it should execute quickly. If it does not, it is more likely that in
  // between the transaction starting (reading all the records), and completing (updating
  // the head), another record will have been inserted, resulting in the transaction
  // failing and needing to be executed again.
  const rules = new Map<string, HeadUpdater<AccountBalance, any>>();
  rules.set(
    TransactionRecordName,
    (input: HeadUpdaterInput<AccountBalance, Transaction>): AccountBalance => {
      input.head.balance += input.current.amount;
      return input.head;
    }
  );
  rules.set(
    AccountUpdateRecordName,
    (
      input: HeadUpdaterInput<AccountBalance, AccountUpdate>
    ): AccountBalance => {
      input.head.ownerFirst = input.current.ownerFirst;
      input.head.ownerLast = input.current.ownerLast;
      return input.head;
    }
  );

  // New accounts start with a balance of zero.
  const emptyAccount = (): AccountBalance => ({
    balance: 0,
  } as AccountBalance);

  // Can now create a ledger "Facet" in our DynamoDB table.
  const ledger = new Facet<AccountBalance>(
    client,
    tableName,
    AccountBalanceRecordName,
    rules,
    emptyAccount,
  );

  // Let's create a new account.
  const accountId = Math.round(Math.random() * 1000000).toString();

  // There is no new data to add.
  await ledger.put(accountId);

  // Update the name of the owner.
  await ledger.put(
    accountId,
    new Data<AccountUpdate>(AccountUpdateRecordName, {
      ownerFirst: "John",
      ownerLast: "Brown",
    } as AccountUpdate)
  );

  // Now, let's add a couple of transactions in a single operation.
  await ledger.put(
    accountId,
    new Data<Transaction>(TransactionRecordName, {
      desc: "Transaction A",
      amount: 200,
    }),
    new Data<Transaction>(TransactionRecordName, {
      desc: "Transaction B",
      amount: -300,
    })
  );

  // Another separate transaction.
  await ledger.put(
    accountId,
    new Data<Transaction>(TransactionRecordName, {
      desc: "Transaction C",
      amount: 50,
    }),
  );

  // Get the final balance.
  const finalBalance = await ledger.get(accountId);
  if (finalBalance != null && finalBalance.item != null) {
    console.log(`Account details: ${JSON.stringify(finalBalance.item)}`);
  }
};

demonstrateLedger()
  .then(() => console.log("complete"))
  .catch((err: Error) => {
    console.log(`stack: ${err.stack}`);
  });
