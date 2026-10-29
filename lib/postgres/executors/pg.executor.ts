import type { SqlExecuteResult, SqlExecutor, SqlTransaction, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isMysql2Client, isolationSql } from '../../utils/executor.util.js';
import { runTransactionWork, transactionEnded } from './transaction-work.js';

/**
 * The part of a node-postgres `Client` (or `PoolClient`) the executor uses. A result's `command` is the statement's
 * command tag, which tells a COMMIT that committed from one PostgreSQL answered with a ROLLBACK.
 */
export interface PgClientLike {
  query(text: string, values?: any[]): Promise<{ rows: any[]; rowCount?: number | null; command?: string }>;
  connect(): Promise<unknown>;
  /** A `PoolClient` is an `EventEmitter`: a pool transaction listens for its `error` while it holds the connection. */
  on?(event: 'error', listener: (error: Error) => void): unknown;
  removeListener?(event: 'error', listener: (error: Error) => void): unknown;
}

/** A connection a node-postgres `Pool` hands out. */
type PgPoolClientLike = PgClientLike & { release(error?: Error | boolean): void };

/** The part of a node-postgres `Pool` the executor uses. */
export interface PgPoolLike {
  query(text: string, values?: any[]): Promise<{ rows: any[]; rowCount?: number | null }>;
  connect(): Promise<PgPoolClientLike>;
  /** pg-pool calls back in the tick it hands the connection out, the one it stops listening to its errors in. */
  connect(callback: (error: Error | undefined, client: PgPoolClientLike | undefined) => void): void;
  readonly totalCount: number;
}

/**
 * A `SqlExecutor` on node-postgres (`pg`): a `Pool` (what an application uses: each transaction on a connection of
 * its own), or a connected `Client` (one connection, so the store's statements and transactions take turns on it).
 *
 * ```ts
 * const pool = new Pool({ connectionString: process.env.DATABASE_URL });
 * // A connection's error, which would end the process: the pool listens only while it's idle, so each connection
 * // listens too, for the transactions the application runs on one (a store's own transactions listen themselves)
 * pool.on('error', (error) => logger.error(error));
 * pool.on('connect', (client) => client.on('error', (error) => logger.error(error)));
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
 * `BEGIN` yet, or a failed transaction) is refused as well. A connected `Client` given to `fromPg()` needs a
 * `client.on('error', ...)` of the application's, as any `Client` does.
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
    // The pool listens to a connection's errors only while it's idle, and the client emits 'error' when the server ends
    // the connection (a restart, a failover), during a statement too: an 'error' nothing listens to ends the process.
    // The transaction listens from the tick the pool hands the connection out in, as an `await` would resume too late:
    // the pool can hand it out while it reads from the socket, and the rest of that read (the server's FATAL right
    // after a ReadyForQuery) is handled first.
    let lost: Error | undefined;
    const onError = (error: Error) => {
      lost ??= error;
    };
    const client = await new Promise<PgPoolClientLike>((resolve, reject) =>
      this.pool.connect((error, client) => {
        if (error || !client) {
          reject(error);
          return;
        }
        client.on?.('error', onError);
        resolve(client);
      }),
    );
    // A statement after the connection is lost fails with what ended it (the server's 25P03 or 57P01, a reset...),
    // rather than with pg's "Client has encountered a connection error and is not queryable". The work's native is the
    // client itself, as the application holds one.
    const connection = {
      query: (text: string, values?: any[]) => (lost ? Promise.reject(lost) : client.query(text, values)),
      getTransactionStatus: () => (client as TransactionStatusLike).getTransactionStatus?.() ?? null,
    };
    const outcome = await runTransaction(connection, work, begin, client);
    client.removeListener?.('error', onError);

    // A connection whose transaction can't be ended cleanly, or that failed, goes away rather than back to the pool.
    client.release(outcome.broken ?? lost);
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
      const outcome = await runTransaction(this.client, work, begin, this.client);
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

/** node-postgres 8.21 and later: the connection's transaction status from its last ReadyForQuery ('I', 'T' or 'E'). */
type TransactionStatusLike = { getTransactionStatus?(): string | null };

/** The transaction's BEGIN, checked before it takes a connection: an unknown isolation level needs none to be refused. */
function beginSql(options: SqlTransactionOptions): string {
  return options.isolationLevel ? `BEGIN ISOLATION LEVEL ${isolationSql(options.isolationLevel)}` : 'BEGIN';
}

/**
 * BEGIN, the work, then COMMIT (or ROLLBACK when the work rejects), on `client`; `native` is the client the work gets as
 * its transaction's own object. node-postgres replies to each statement with its command tag: the BEGIN's says the
 * COMMIT's will tell (`ROLLBACK` for a transaction PostgreSQL ended), so the work's reading `native` costs no statement
 * (`commitReplyTells`). A client whose replies carry no tag gets the work runner's statement at the end instead.
 */
async function runTransaction<T>(
  client: Pick<PgClientLike, 'query'> & TransactionStatusLike,
  work: (transaction: SqlTransaction) => Promise<T>,
  begin: string,
  native: unknown,
): Promise<Outcome<T>> {
  let tagged: boolean;
  try {
    tagged = (await client.query(begin)).command === 'BEGIN';
  } catch (error) {
    return { error, broken: asError(error) };
  }

  let result: T;
  try {
    result = await runTransactionWork(work, clientTransaction(client, native), { commitReplyTells: tagged });
  } catch (error) {
    try {
      await client.query('ROLLBACK');
      return { error };
    } catch (rollbackError) {
      return { error, broken: asError(rollbackError) };
    }
  }

  try {
    // PostgreSQL answers the COMMIT of a transaction it ended at a failed statement (one the work sent through its
    // native and caught, which the work runner doesn't see) with a ROLLBACK: nothing committed, the connection idle.
    const committed = await client.query('COMMIT');
    if (tagged && committed.command === 'ROLLBACK') {
      return { error: transactionEnded() };
    }
    return { result };
  } catch (error) {
    // PostgreSQL ends a transaction whose COMMIT fails (a serialization failure, a deferred constraint): the connection
    // is idle and goes back to the pool once a statement shows it is. A lost one is broken.
    const idle = await client.query('SELECT 1').then(
      () => (client.getTransactionStatus?.() ?? 'I') === 'I',
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

  const client = transaction as PgClientLike & TransactionStatusLike;
  if (typeof client.getTransactionStatus === 'function') {
    const status = client.getTransactionStatus();
    if (status === 'E') {
      throw notATransaction('The node-postgres client is in a failed transaction: roll it back.');
    }
    if (status !== 'T') {
      throw notATransaction("The node-postgres client isn't in a transaction: send BEGIN on it first, or each statement commits on its own.");
    }
  }
  return clientTransaction(client, client);
}

/** Statements on `client`; `native` is the client as the application holds it (a pool transaction's statements run through a wrapper of it). */
function clientTransaction(client: Pick<PgClientLike, 'query'>, native: unknown): SqlTransaction {
  return {
    query: async <R extends object>(text: string, params: readonly unknown[] = []) => (await client.query(text, [...params])).rows as R[],
    execute: async (text: string, params: readonly unknown[] = []) => affected(await client.query(text, [...params])),
    native,
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
