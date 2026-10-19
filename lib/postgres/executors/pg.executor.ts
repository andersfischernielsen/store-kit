import type { SqlExecuteResult, SqlExecutor, SqlTransaction, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isMysql2Client, isolationSql } from '../../utils/executor.util.js';
import { runTransactionWork } from './transaction-work.js';

/** The part of a node-postgres `Client` (or `PoolClient`) the executor uses. */
export interface PgClientLike {
  query(text: string, values?: any[]): Promise<{ rows: any[]; rowCount?: number | null }>;
  connect(): Promise<unknown>;
}

/** The part of a node-postgres `Pool` the executor uses. */
export interface PgPoolLike {
  query(text: string, values?: any[]): Promise<{ rows: any[]; rowCount?: number | null }>;
  connect(): Promise<PgClientLike & { release(error?: Error | boolean): void }>;
  readonly totalCount: number;
}

/**
 * A `SqlExecutor` on node-postgres (`pg`): a `Pool` (what an application uses: each transaction on a connection of
 * its own), or a connected `Client` (one connection, so the store's statements and transactions take turns on it).
 *
 * ```ts
 * const pool = new Pool({ connectionString: process.env.DATABASE_URL });
 * const executor = fromPg(pool); // a first-party store's `executor` option
 *
 * // The application's transaction, for a store method that takes one: the client it runs on
 * const client = await pool.connect();
 * try {
 *   await client.query('BEGIN');
 *   await client.query('INSERT INTO orders (id, status) VALUES ($1, $2)', [order.id, 'placed']);
 *   // What a store does with the { transaction: client } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(client).query('INSERT INTO audit (order_id) VALUES ($1::text)', [order.id]);
 *   await client.query('COMMIT');
 * } catch (error) {
 *   await client.query('ROLLBACK');
 *   throw error;
 * } finally {
 *   client.release();
 * }
 * ```
 *
 * The transaction object is a client (not the pool) in a transaction; with `pg` 8.21 or later, one that isn't (no
 * `BEGIN` yet, or a failed transaction) is refused as well.
 */
export function fromPg(pool: PgPoolLike | PgClientLike): SqlExecutor<'postgres'> {
  if (isMysql2Client(pool)) {
    throw new TypeError(
      "fromPg() takes a node-postgres Pool (or a connected Client), not a mysql2 pool or connection: a MySQL store takes that, through fromMysql2() from its package's /mysql subpath.",
    );
  }
  if (!hasMethod(pool, 'query')) {
    throw new TypeError(`fromPg() takes a node-postgres Pool (or a connected Client), got ${describeValue(pool)}.`);
  }
  return isPool(pool) ? new PgPoolExecutor(pool) : new PgClientExecutor(pool);
}

class PgPoolExecutor implements SqlExecutor<'postgres'> {
  readonly dialect = 'postgres';

  constructor(private readonly pool: PgPoolLike) {}

  async query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return (await this.pool.query(text, [...params])).rows;
  }

  async execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return affected(await this.pool.query(text, [...params]));
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const begin = beginSql(options);
    const client = await this.pool.connect();
    const outcome = await runTransaction(client, work, begin);

    // A connection whose transaction can't be ended cleanly goes away rather than back to the pool.
    client.release(outcome.broken);
    if ('error' in outcome) {
      throw outcome.error;
    }
    return outcome.result;
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    return pgTransaction(transaction);
  }
}

class PgClientExecutor implements SqlExecutor<'postgres'> {
  readonly dialect = 'postgres';
  private tail: Promise<unknown> = Promise.resolve();

  constructor(private readonly client: PgClientLike) {}

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return this.exclusive(async () => (await this.client.query(text, [...params])).rows);
  }

  execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return this.exclusive(async () => affected(await this.client.query(text, [...params])));
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const begin = beginSql(options);
    return this.exclusive(async () => {
      const outcome = await runTransaction(this.client, work, begin);
      if ('error' in outcome) {
        throw outcome.error;
      }
      return outcome.result;
    });
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    return pgTransaction(transaction);
  }

  /**
   * One connection: a statement sent while a transaction runs on it would join that transaction. So the executor's own
   * statements wait for its transaction to end, and one sent from inside that transaction's work would wait forever:
   * the work has its transaction's `query()` for that.
   */
  private exclusive<T>(task: () => Promise<T>): Promise<T> {
    const run = this.tail.then(task, task);
    this.tail = run.catch(() => undefined);
    return run;
  }
}

type Outcome<T> = { result: T; broken?: Error } | { error: unknown; broken?: Error };

/** The transaction's BEGIN, checked before it takes a connection: an unknown isolation level needs none to be refused. */
function beginSql(options: SqlTransactionOptions): string {
  return options.isolationLevel ? `BEGIN ISOLATION LEVEL ${isolationSql(options.isolationLevel)}` : 'BEGIN';
}

async function runTransaction<T>(client: PgClientLike, work: (transaction: SqlTransaction) => Promise<T>, begin: string): Promise<Outcome<T>> {
  try {
    await client.query(begin);
  } catch (error) {
    return { error, broken: asError(error) };
  }

  let result: T;
  try {
    result = await runTransactionWork(work, clientTransaction(client));
  } catch (error) {
    try {
      await client.query('ROLLBACK');
      return { error };
    } catch (rollbackError) {
      return { error, broken: asError(rollbackError) };
    }
  }

  try {
    await client.query('COMMIT');
    return { result };
  } catch (error) {
    // PostgreSQL ends a transaction whose COMMIT fails (a serialization failure, a deferred constraint): the connection
    // is idle and goes back to the pool once a statement shows it is. A lost one is broken.
    const idle = await client.query('SELECT 1').then(
      () => ((client as { getTransactionStatus?(): string | null }).getTransactionStatus?.() ?? 'I') === 'I',
      () => false,
    );
    return idle ? { error } : { error, broken: asError(error) };
  }
}

function pgTransaction(transaction: unknown): SqlTransaction {
  if (isMysql2Client(transaction)) {
    throw notATransaction("Pass the node-postgres client your transaction runs on, not a mysql2 connection: the store's statements run on PostgreSQL.");
  }
  if (!hasMethod(transaction, 'query') || !hasMethod(transaction, 'connect') || isPool(transaction)) {
    throw notATransaction(
      "Pass the node-postgres client your transaction runs on (const client = await pool.connect(); await client.query('BEGIN')), " +
        (isPool(transaction) ? 'not the pool: it runs each statement on any of its connections, outside your transaction.' : `got ${describeValue(transaction)}.`),
    );
  }

  const client = transaction as PgClientLike & { getTransactionStatus?(): string | null };
  if (typeof client.getTransactionStatus === 'function') {
    const status = client.getTransactionStatus();
    if (status === 'E') {
      throw notATransaction('The node-postgres client is in a failed transaction: roll it back.');
    }
    if (status !== 'T') {
      throw notATransaction("The node-postgres client isn't in a transaction: send BEGIN on it first, or each statement commits on its own.");
    }
  }
  return clientTransaction(client);
}

function clientTransaction(client: PgClientLike): SqlTransaction {
  return {
    query: async <R extends object>(text: string, params: readonly unknown[] = []) => (await client.query(text, [...params])).rows as R[],
    execute: async (text: string, params: readonly unknown[] = []) => affected(await client.query(text, [...params])),
  };
}

/** node-postgres's `rowCount`: the rows an INSERT, UPDATE or DELETE wrote (`null` for a statement without a count). */
function affected(result: { rowCount?: number | null }): SqlExecuteResult {
  return { affectedRows: result.rowCount ?? 0 };
}

function isPool(value: unknown): value is PgPoolLike {
  return hasMethod(value, 'connect') && typeof (value as { totalCount?: unknown }).totalCount === 'number';
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
