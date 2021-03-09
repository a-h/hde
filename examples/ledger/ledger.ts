import * as AWS from "aws-sdk";
import { Facet } from "../../src";
import { EventDB } from "../../src/db";
import {
  Processor,
  RecordTypeName,
  StateUpdater,
  StateUpdaterInput,
  Event,
} from "../../src/processor";

// The account Facet has multiple records.
// State: The current "BankAccount".
// Inbound Events: The account is made up of "Transaction" and "AccountUpdate" records.
// Outbound Event 1: An "AccountOverdrawn" event is emitted when the balance becomes < 0, and this is allowed due to an overdraft.
// Outbound Event 2: A "TransactionFailed" event is emitted when a transaction cannot be completed, due to insufficient funds.
interface BankAccount {
  id: string;
  ownerFirst: string;
  ownerLast: string;
  balance: number;
  minimumBalance: number;
}
const BankAccountRecordName = "BANK_ACCOUNT";

// Inbound events must have a name.
type InboundEvents = AccountCreation | AccountUpdate | Transaction

interface AccountCreation {
  id: string;
}
const AccountCreationRecordName = "ACCOUNT_CREATION";
interface AccountUpdate {
  ownerFirst: string;
  ownerLast: string;
}
const AccountUpdateRecordName = "ACCOUNT_UPDATE";
interface Transaction {
  desc: string;
  amount: number;
}
const TransactionRecordName = "TRANSACTION_ACCEPTED";

// Outbound events don't need to be named, they're named when they're sent, so it's still a good
// idea to set the name.
type OutboundEvents = AccountOverdrawn

interface AccountOverdrawn {
  accountId: string;
}
const AccountOverdrawnEventName = "accountOverdrawn";

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
  const db = new EventDB(client, tableName, BankAccountRecordName);

  // The rules define how the BankAccount state is updated by incoming events.
  // For example, an incoming "TRANSACTION" event modifies the "ACCOUNT_BALANCE" state.
  // The function must be pure, it must not carry out IO (e.g. network requests, or disk
  // access), and it should execute quickly. If it does not, it is more likely that in
  // between the transaction starting (reading all the previous events), and completing (updating
  // the state), another event will have been inserted, resulting in the transaction
  // failing and needing to be executed again.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rules = new Map<RecordTypeName, StateUpdater<BankAccount, InboundEvents, OutboundEvents, any>>();
  rules.set(
    AccountCreationRecordName,
    (input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, AccountCreation>): BankAccount => {
      input.state.id = input.current.id;
      return input.state;
    },
  );
  rules.set(
    TransactionRecordName,
    (input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, Transaction>): BankAccount => {
      const previousBalance = input.state.balance;
      const newBalance = input.state.balance + input.current.amount;

      // If they don't have sufficient overdraft, cancel the transaction.
      if (newBalance < input.state.minimumBalance) {
        throw new Error("insufficient funds");
      }

      // If this is the transaction that takes the user overdrawn, notify them.
      if (previousBalance >= 0 && newBalance < 0) {
        const overdrawnEvent = { accountId: input.state.id } as AccountOverdrawn;
        input.publish(AccountOverdrawnEventName, overdrawnEvent);
      }

      input.state.balance = newBalance;
      return input.state;
    },
  );
  rules.set(
    AccountUpdateRecordName,
    (input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, AccountUpdate>): BankAccount => {
      input.state.ownerFirst = input.current.ownerFirst;
      input.state.ownerLast = input.current.ownerLast;
      return input.state;
    },
  );

  // New accounts start with a balance of zero.
  const initialAccount = (): BankAccount =>
    ({
      balance: 0,
      minimumBalance: -1000, // Give the user an overdraft.
    } as BankAccount);

  // Create the processor that handles events.
  const processor = new Processor<BankAccount, InboundEvents, OutboundEvents>(rules, initialAccount);

  // Can now create a ledger "Facet" in our DynamoDB table.
  const ledger = new Facet<BankAccount, InboundEvents, OutboundEvents>(BankAccountRecordName, db, processor);

  // Let's create a new account.
  const accountId = Math.round(Math.random() * 1000000).toString();

  // There is no new data to add.
  await ledger.append(
    accountId,
    new Event<AccountCreation>(AccountCreationRecordName, {
      id: accountId,
    }),
  );

  // Update the name of the owner.
  await ledger.append(
    accountId,
    new Event<AccountUpdate>(AccountUpdateRecordName, {
      ownerFirst: "John",
      ownerLast: "Brown",
    }),
  );

  // Now, let's add a couple of transactions in a single operation.
  const result = await ledger.append(
    accountId,
    new Event<Transaction>(TransactionRecordName, {
      desc: "Transaction A",
      amount: 200,
    }),
    new Event<Transaction>(TransactionRecordName, {
      desc: "Transaction B",
      amount: -300,
    }),
  );
  result.newOutboundEvents.map((e) => console.log(`Published event: ${JSON.stringify(e)}`));

  // Another separate transaction.
  const transactionCResult = await ledger.append(
    accountId,
    new Event<Transaction>(TransactionRecordName, {
      desc: "Transaction C",
      amount: 50,
    }),
  );

  // If we've just read the STATE, we can try appending without doing
  // another database read. If no other records have been written in the
  // meantime, the transaction will succeed.
  await ledger.appendTo(
    accountId,
    transactionCResult.item,
    transactionCResult.seq,
    new Event<Transaction>(TransactionRecordName, {
      desc: "Transaction D",
      amount: 25,
    }),
  );

  // Get the final balance.
  const balance = await ledger.get(accountId);
  if (balance) {
    console.log(`Account details: ${JSON.stringify(balance.item)}`);
  }

  // Verify the final balance by reading all of the transactions and re-calculating.
  const verifiedBalance = await ledger.recalculate(accountId);
  console.log(`Verified balance: ${JSON.stringify(verifiedBalance.item)}`);

  // The re-calculation can also take data to modify the result.
  const finalBalance = await ledger.recalculate(
    accountId,
    new Event<Transaction>(TransactionRecordName, {
      desc: "Transaction E",
      amount: 25,
    }),
  );
  console.log(`Final balance: ${JSON.stringify(finalBalance.item)}`);
};

demonstrateLedger()
  .then(() => console.log("complete"))
  .catch((err: Error) => {
    console.log(`stack: ${err.stack}`);
  });
