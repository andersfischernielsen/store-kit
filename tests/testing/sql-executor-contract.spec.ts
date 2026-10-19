/**
 * The executor contract checks what it claims: executors that each break one rule (ignore the isolation level, run
 * the work outside a transaction, commit work that caught a failed statement, join anything, share one connection
 * without taking turns, rename columns, inline parameters, wrap the work's error) fail the case that guards it, and
 * pass the others where they can.
 */
import pg from 'pg';
import { fromPg, type SqlExecutor, type SqlTransaction } from '../../lib/postgres/index.js';
import { sqlExecutorContract, type SqlExecutorContractTarget } from '../../lib/testing/index.js';
import { endPool } from '../support/postgres.js';
import { onPostgres, pgClient, testDatabase, type Client } from '../postgres/support.js';

const { database, reason } = await testDatabase('executor_contract');

let client: Client | null = null;
beforeAll(async () => {
  client = database ? await pgClient.open(database.url) : null;
});
afterAll(() => client?.close());
onPostgres(reason);

const CASE = {
  statements: 'query() binds its parameters',
  transactions: 'transaction() commits',
  isolation: 'transaction() runs at the isolation level',
  joins: "wrapTransaction() joins the application's transaction",
  connections: 'transactions keep to one connection',
  values: 'values keep their types',
};

/** Runs the case of `executor` whose name starts with `name`: resolves to its error, `null` when it passes. */
async function outcome(executor: SqlExecutor, name: string, target: Partial<SqlExecutorContractTarget> = {}): Promise<Error | null> {
  const cases = sqlExecutorContract(() => ({ executor, transaction: (work) => client!.transaction(work), root: client!.root, ...target }));
  const found = cases.find((c) => c.name.startsWith(name));
  if (!found) {
    throw new Error(`No case "${name}"`);
  }
  return found.run().then(
    () => null,
    (error: Error) => error,
  );
}

/** The pool's executor with one method replaced. */
const broken = (overrides: Partial<SqlExecutor>): SqlExecutor => {
  const base = client!.executor;
  return {
    dialect: base.dialect,
    query: (text, params) => base.query(text, params),
    execute: (text, params) => base.execute!(text, params),
    transaction: (work, options) => base.transaction(work, options),
    wrapTransaction: (transaction) => base.wrapTransaction(transaction),
    ...overrides,
  };
};

describe('sqlExecutorContract()', () => {
  it('passes a correct executor, and covers postgres and mysql executors', async () => {
    for (const name of Object.values(CASE)) {
      expect(await outcome(client!.executor, name)).toBeNull();
    }
    expect((await outcome({ ...broken({}), dialect: 'oracle' as never }, CASE.statements))?.message).toBe(
      'sqlExecutorContract() covers postgres and mysql executors; this one\'s dialect is "oracle".',
    );
  });

  it('fails an executor whose execute() counts other rows than a statement wrote', async () => {
    const executor = broken({ execute: async (text, params) => ({ affectedRows: (await client!.executor.execute!(text, params)).affectedRows + 1 }) });
    expect((await outcome(executor, CASE.statements))?.message).toMatch(/^execute\(\) of an INSERT of two rows: expected \{"affectedRows":2\}, got \{"affectedRows":3\}/);
  });

  it('fails an executor that ignores the isolation level asked for', async () => {
    const executor = broken({ transaction: (work) => client!.executor.transaction(work) });
    expect((await outcome(executor, CASE.isolation))?.message).toMatch(/^a transaction at read uncommitted: expected "read uncommitted", got "read committed"/);
    expect(await outcome(executor, CASE.transactions)).toBeNull();
  });

  it('fails an executor that runs the work outside a transaction', async () => {
    const executor = broken({ transaction: (work) => work(client!.executor) });
    expect((await outcome(executor, CASE.transactions))?.message).toMatch(/^the rows after a committed transaction and two rolled back/);
    expect(await outcome(executor, CASE.connections)).not.toBeNull();
  });

  it('fails an executor that commits work which caught a failed statement, as if PostgreSQL had kept its transaction', async () => {
    const executor = broken({ transaction: (work) => client!.transaction((tx) => work(client!.executor.wrapTransaction(tx))) });
    expect((await outcome(executor, CASE.transactions))?.message).toMatch(/^a transaction whose work catches a failed statement and resolves: expected a rejection/);
  });

  it("fails an executor that joins anything, running statements outside the application's transaction", async () => {
    const executor = broken({ wrapTransaction: () => client!.executor });
    expect((await outcome(executor, CASE.joins))?.message).toMatch(/^the rows after the application's rollback and commit/);
  });

  it('fails an executor that shares one connection without its statements and transactions taking turns', async () => {
    const connection = new pg.Client({ connectionString: database!.url });
    await connection.connect();
    try {
      const run = async <R>(text: string, params: readonly unknown[] = []) => (await connection.query(text, [...params])).rows as R[];
      const onOne: SqlTransaction = { query: run };
      const executor = broken({
        query: run,
        transaction: async (work) => {
          await connection.query('BEGIN');
          try {
            const result = await work(onOne);
            await connection.query('COMMIT');
            return result;
          } catch (error) {
            await connection.query('ROLLBACK');
            throw error;
          }
        },
      });
      expect((await outcome(executor, CASE.connections))?.message).toMatch(/transaction id|distinct transaction ids/);
    } finally {
      await connection.end();
    }
  });

  it("fails an executor that renames the columns it reads, as Kysely's CamelCasePlugin would", async () => {
    const camel = (row: object) => Object.fromEntries(Object.entries(row).map(([key, value]) => [key.replace(/_(\w)/g, (_, c: string) => c.toUpperCase()), value]));
    const executor = broken({ query: async (text, params) => (await client!.executor.query(text, params)).map(camel) as never[] });
    expect((await outcome(executor, CASE.values))?.message).toMatch(/^a row written with SqlParams and read with columns\(\)/);
    expect((await outcome(executor, CASE.statements))?.message).toMatch(/^query\(\) of a SELECT with parameters/);
  });

  it('fails an executor that inlines its parameters into the statement', async () => {
    const inline = (text: string, params: readonly unknown[] = []) =>
      text.replace(/\$(\d+)/g, (_, n: string) => (params[Number(n) - 1] === null ? 'NULL' : `'${String(params[Number(n) - 1])}'`));
    const executor = broken({ query: (text, params) => client!.executor.query(inline(text, params)) });
    expect(await outcome(executor, CASE.statements)).not.toBeNull();
  });

  it("fails an executor whose transactions reject with an error of their own instead of the work's", async () => {
    const executor = broken({
      transaction: (work, options) =>
        client!.executor.transaction(work, options).catch((error: Error) => {
          throw new Error(`transaction failed: ${error.message}`);
        }),
    });
    expect((await outcome(executor, CASE.transactions))?.message).toMatch(/^a transaction whose work rejects: expected it to reject with the work's error/);
  });

  it('disposes of each target after its case', async () => {
    const pool = new pg.Pool({ connectionString: database!.url, max: 2 });
    let disposed = 0;
    const cases = sqlExecutorContract(
      () => ({ executor: fromPg(pool), transaction: (work) => client!.transaction(work), root: pool }),
      { dispose: () => void disposed++ },
    );
    try {
      await cases[0]!.run();
      await cases[1]!.run();
      expect(disposed).toBe(2);
    } finally {
      await endPool(pool);
    }
  });
});
