import type { SqlExecuteResult, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isMysql2Client, isolationSql } from '../../utils/executor.util.js';
import type { SqlExecutor, SqlTransaction } from '../interfaces/mysql-executor.interface.js';
import { assertFoundRows, checkPlaceholders, isPgClient, mysql2Flags } from './mysql-client.util.js';

/** The part of a mysql2 connection (`mysql2/promise`) the executor uses. */
export interface Mysql2ConnectionLike {
  query(sql: string, values?: any[]): Promise<[any, any]>;
  execute(sql: string, values?: any[]): Promise<[any, any]>;
}

/** The part of a mysql2 pool (`mysql2/promise`) the executor uses. */
export interface Mysql2PoolLike extends Mysql2ConnectionLike {
  getConnection(): Promise<Mysql2ConnectionLike & { release(): void; destroy(): void }>;
}

/**
 * A `SqlExecutor` on mysql2: a pool (what an application uses: each transaction on a connection of its own), or a
 * connection (one connection, so the store's statements and transactions take turns on it). The promise API
 * (`mysql2/promise`) or the callback one. Statements go through `query()`, which binds `?` on the client: no server-side
 * prepared statements pile up against the server's `max_prepared_stmt_count`.
 *
 * ```ts
 * const pool = mysql.createPool({ uri: process.env.DATABASE_URL, connectionLimit: 10 });
 * const executor = fromMysql2(pool); // a first-party store's `executor` option
 *
 * // The application's transaction, for a store method that takes one: the connection it runs on
 * const connection = await pool.getConnection();
 * try {
 *   await connection.beginTransaction();
 *   await connection.query('INSERT INTO orders (id, status) VALUES (?, ?)', [order.id, 'placed']);
 *   // What a store does with the { transaction: connection } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(connection).execute('INSERT INTO audit (order_id) VALUES (?)', [order.id]);
 *   await connection.commit();
 * } catch (error) {
 *   await connection.rollback();
 *   throw error;
 * } finally {
 *   connection.release();
 * }
 * ```
 *
 * The transaction object is a connection (not the pool) after `beginTransaction()`; one that isn't in a transaction
 * is refused at the store's first statement, before anything is written (mysql2 doesn't track it, the server's reply
 * to that statement's check says it). A pool or connection configured without the `FOUND_ROWS` client flag (on by
 * default) is refused: a store's `execute()` counts the rows an UPDATE matched.
 */
export function fromMysql2(pool: Mysql2PoolLike | Mysql2ConnectionLike): SqlExecutor {
  if (!isMysql2Client(pool)) {
    throw new TypeError(
      isPgClient(pool)
        ? "fromMysql2() takes a mysql2 pool or connection, not a node-postgres one: a PostgreSQL store takes that, through fromPg() from its package's /postgres subpath."
        : `fromMysql2() takes a mysql2 pool or connection (mysql2/promise), got ${describeValue(pool)}.`,
    );
  }

  assertFoundRows(mysql2Flags(pool), 'fromMysql2()');
  // The promise API runs the statements; a transaction's `native` is of the API the application gave.
  const client = promised(pool);
  return hasMethod(client, 'getConnection') ? new Mysql2PoolExecutor(client as Mysql2PoolLike, client !== pool) : new Mysql2ConnectionExecutor(client, pool);
}

class Mysql2PoolExecutor implements SqlExecutor {
  readonly dialect = 'mysql';

  /** `callbacks`: the pool given was of the callback API, so its transactions hand out callback-API connections. */
  constructor(
    private readonly pool: Mysql2PoolLike,
    private readonly callbacks: boolean,
  ) {}

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return rows<R>(this.pool, text, params);
  }

  execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return written(this.pool, text, params);
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const isolation = options.isolationLevel ? isolationSql(options.isolationLevel) : undefined;
    const connection = await this.pool.getConnection();
    // A promise-API connection wraps the callback-API one (`connection`), which the application's own code holds.
    const native = this.callbacks ? ((connection as { connection?: unknown }).connection ?? connection) : connection;
    const outcome = await runTransaction(connection, work, isolation, native);

    // A connection whose transaction can't be ended cleanly goes away rather than back to the pool.
    if (outcome.broken) {
      connection.destroy();
    } else {
      connection.release();
    }
    if ('error' in outcome) {
      throw outcome.error;
    }
    return outcome.result;
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    return mysql2Transaction(transaction);
  }
}

class Mysql2ConnectionExecutor implements SqlExecutor {
  readonly dialect = 'mysql';
  private tail: Promise<unknown> = Promise.resolve();

  /** `native`: the connection as `fromMysql2()` was given it, of either API. */
  constructor(
    private readonly connection: Mysql2ConnectionLike,
    private readonly native: unknown,
  ) {}

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return this.exclusive(() => rows<R>(this.connection, text, params));
  }

  execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return this.exclusive(() => written(this.connection, text, params));
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const isolation = options.isolationLevel ? isolationSql(options.isolationLevel) : undefined;
    return this.exclusive(async () => {
      const outcome = await runTransaction(this.connection, work, isolation, this.native);
      if ('error' in outcome) {
        throw outcome.error;
      }
      return outcome.result;
    });
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    return mysql2Transaction(transaction);
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

type Outcome<T> = { result: T; broken?: boolean } | { error: unknown; broken?: boolean };

/**
 * `SET TRANSACTION ISOLATION LEVEL` sets the level of the next transaction only, so the connection's own level, which
 * the application may have set, is left as it was.
 */
async function runTransaction<T>(
  connection: Mysql2ConnectionLike,
  work: (transaction: SqlTransaction) => Promise<T>,
  isolation: string | undefined,
  native: unknown,
): Promise<Outcome<T>> {
  try {
    if (isolation) {
      await connection.query(`SET TRANSACTION ISOLATION LEVEL ${isolation}`);
    }
    await connection.query('START TRANSACTION');
  } catch (error) {
    return { error, broken: true };
  }

  let result: T;
  try {
    result = await work(connectionTransaction(connection, native));
  } catch (error) {
    try {
      await connection.query('ROLLBACK');
      return { error };
    } catch {
      return { error, broken: true };
    }
  }

  try {
    await connection.query('COMMIT');
    return { result };
  } catch (error) {
    return { error, broken: true };
  }
}

function mysql2Transaction(transaction: unknown): SqlTransaction {
  if (!isMysql2Client(transaction) || hasMethod(transaction, 'getConnection')) {
    throw notATransaction(
      "Pass the mysql2 connection your transaction runs on (const connection = await pool.getConnection(); await connection.beginTransaction()), " +
        (hasMethod(transaction, 'getConnection')
          ? 'not the pool: it runs each statement on any of its connections, outside your transaction.'
          : isPgClient(transaction)
            ? "not a node-postgres client: the store's statements run on MySQL."
            : `got ${describeValue(transaction)}.`),
    );
  }

  // mysql2 doesn't track whether a connection is in a transaction, but the server says so in every OK packet: the first
  // statement checks it, so a connection outside one is refused before the store writes anything that would commit.
  const connection = promised(transaction);
  let checked: Promise<void> | undefined;
  const inTransaction = () => (checked ??= assertInTransaction(connection));
  return {
    query: async <R extends object>(text: string, params: readonly unknown[] = []) => {
      await inTransaction();
      return rows<R>(connection, text, params);
    },
    execute: async (text: string, params: readonly unknown[] = []) => {
      await inTransaction();
      return written(connection, text, params);
    },
    native: transaction,
  };
}

/** SERVER_STATUS_IN_TRANS: the connection is in a transaction. */
const IN_TRANSACTION = 1;

async function assertInTransaction(connection: Mysql2ConnectionLike): Promise<void> {
  const [header] = await connection.query('DO 0');
  const status = (header as { serverStatus?: unknown } | undefined)?.serverStatus;
  if (typeof status === 'number' && (status & IN_TRANSACTION) === 0) {
    throw notATransaction("The mysql2 connection isn't in a transaction: call beginTransaction() on it first, or each statement commits on its own.");
  }
}

/** Statements on `connection` (the promise API's), in the transaction `runTransaction()` began on it. */
function connectionTransaction(connection: Mysql2ConnectionLike, native: unknown): SqlTransaction {
  return {
    query: <R extends object>(text: string, params: readonly unknown[] = []) => rows<R>(connection, text, params),
    execute: (text: string, params: readonly unknown[] = []) => written(connection, text, params),
    native,
  };
}

async function rows<R extends object>(client: Mysql2ConnectionLike, text: string, params: readonly unknown[]): Promise<R[]> {
  checkPlaceholders(text, params);
  const [result] = await client.query(text, [...params]);
  return (Array.isArray(result) ? result : []) as R[];
}

async function written(client: Mysql2ConnectionLike, text: string, params: readonly unknown[]): Promise<SqlExecuteResult> {
  checkPlaceholders(text, params);
  const [result] = await client.query(text, [...params]);
  return { affectedRows: Array.isArray(result) ? 0 : Number(result?.affectedRows ?? 0) };
}

/** The promise API of a mysql2 pool or connection: the callback API's objects have `promise()`. */
function promised(client: unknown): Mysql2ConnectionLike {
  const candidate = client as { promise?: () => Mysql2ConnectionLike };
  return typeof candidate.promise === 'function' ? candidate.promise() : (client as Mysql2ConnectionLike);
}
