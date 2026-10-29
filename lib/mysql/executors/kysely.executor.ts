import type { SqlExecuteResult, SqlIsolationLevel, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isolationSql } from '../../utils/executor.util.js';
import type { SqlExecutor, SqlTransaction } from '../interfaces/mysql-executor.interface.js';
import { checkPlaceholders } from './mysql-client.util.js';

/** The part of a Kysely instance (or of a `Transaction`) the executor uses. */
export interface KyselyMySqlLike {
  readonly isTransaction: boolean;
  executeQuery(query: any): Promise<{ rows: any[]; numAffectedRows?: bigint }>;
  withoutPlugins(): KyselyMySqlLike;
  transaction(): { setIsolationLevel(level: SqlIsolationLevel): { execute<T>(work: (trx: any) => Promise<T>): Promise<T> }; execute<T>(work: (trx: any) => Promise<T>): Promise<T> };
}

/**
 * A `SqlExecutor` on a Kysely instance with a MySQL dialect (`MysqlDialect` on a mysql2 pool). A store's statements
 * skip the instance's plugins (a `CamelCasePlugin` would rename the columns it reads). The transaction object is the
 * `trx` that `db.transaction().execute()` hands its callback (or a controlled transaction).
 *
 * ```ts
 * const db = new Kysely<Database>({ dialect: new MysqlDialect({ pool: createPool({ uri: process.env.DATABASE_URL }) }) });
 * const executor = fromKysely(db); // a first-party store's `executor` option
 *
 * await db.transaction().execute(async (trx) => {
 *   await trx.insertInto('orders').values(order).execute();
 *   // What a store does with the { transaction: trx } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(trx).execute('INSERT INTO audit (order_id) VALUES (?)', [order.id]);
 * });
 * ```
 *
 * Keep the pool's `FOUND_ROWS` client flag (mysql2's default): a store's `execute()` counts the rows an UPDATE
 * matched, and the executor can't reach Kysely's pool to check.
 */
export function fromKysely(db: KyselyMySqlLike): SqlExecutor {
  if (!isKysely(db) || db.isTransaction) {
    throw new TypeError(
      isKysely(db)
        ? 'fromKysely() takes the Kysely instance, not a transaction: pass that to a store method that takes your transaction ({ transaction: trx }).'
        : `fromKysely() takes a Kysely instance, got ${describeValue(db)}.`,
    );
  }

  const database = otherDatabase(db);
  if (database) {
    throw new TypeError(`fromKysely() takes a Kysely instance with a MySQL dialect, not a ${database} one.`);
  }
  return new KyselyExecutor(db);
}

class KyselyExecutor implements SqlExecutor {
  readonly dialect = 'mysql';
  private readonly raw: KyselyMySqlLike;

  constructor(private readonly db: KyselyMySqlLike) {
    this.raw = db.withoutPlugins();
  }

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return run<R>(this.raw, text, params);
  }

  execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return write(this.raw, text, params);
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const builder = this.db.transaction();
    const inTransaction = (trx: KyselyMySqlLike) => work(kyselyTransaction(trx));
    if (!options.isolationLevel) {
      return builder.execute(inTransaction);
    }

    // Kysely sends `set transaction isolation level ...` (the next transaction's alone) before `begin`.
    isolationSql(options.isolationLevel);
    return builder.setIsolationLevel(options.isolationLevel).execute(inTransaction);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    if (!isKysely(transaction) || !transaction.isTransaction) {
      throw notATransaction(
        isKysely(transaction)
          ? 'Pass the trx your db.transaction().execute() callback receives, not the Kysely instance: it runs each statement outside your transaction.'
          : `Pass the trx your Kysely db.transaction().execute() callback receives, got ${describeValue(transaction)}.`,
      );
    }

    const database = otherDatabase(transaction);
    if (database) {
      throw notATransaction(`Pass the trx of a Kysely instance with a MySQL dialect, not a ${database} one: the store's statements run on MySQL.`);
    }
    return kyselyTransaction(transaction);
  }
}

function kyselyTransaction(trx: KyselyMySqlLike): SqlTransaction {
  const raw = trx.withoutPlugins();
  return {
    query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(raw, text, params),
    execute: (text: string, params: readonly unknown[] = []) => write(raw, text, params),
    native: trx,
  };
}

let kysely: Promise<typeof import('kysely')> | undefined;

async function send(db: KyselyMySqlLike, text: string, params: readonly unknown[]): Promise<{ rows: any[]; numAffectedRows?: bigint }> {
  checkPlaceholders(text, params);
  const { CompiledQuery } = await (kysely ??= import('kysely'));
  return db.executeQuery(CompiledQuery.raw(text, [...params]));
}

async function run<R extends object>(db: KyselyMySqlLike, text: string, params: readonly unknown[]): Promise<R[]> {
  return (await send(db, text, params)).rows as R[];
}

/** `numAffectedRows`: mysql2's `affectedRows`. */
async function write(db: KyselyMySqlLike, text: string, params: readonly unknown[]): Promise<SqlExecuteResult> {
  return { affectedRows: Number((await send(db, text, params)).numAffectedRows ?? 0) };
}

/** Kysely's adapters of other databases, by class name: an adapter of a known one is refused. */
const OTHER_ADAPTERS = new Map([
  ['PostgresAdapter', 'PostgreSQL'],
  ['SqliteAdapter', 'SQLite'],
  ['MssqlAdapter', 'SQL Server'],
]);

/**
 * The database a Kysely instance's adapter is known to be for, if it isn't MySQL. Only Kysely's own adapters of other
 * databases are recognized, by class name up the chain (the dialect is private): a MySQL dialect of any kind, a custom
 * adapter, or names a minifier rewrote are never refused.
 */
function otherDatabase(db: KyselyMySqlLike): string | undefined {
  let adapter: unknown;
  try {
    adapter = (db as { getExecutor?(): { adapter?: unknown } }).getExecutor?.()?.adapter;
  } catch {
    return undefined;
  }

  for (let type = (adapter as object | undefined)?.constructor; typeof type === 'function'; type = Object.getPrototypeOf(type)) {
    const database = OTHER_ADAPTERS.get(type.name);
    if (database) {
      return database;
    }
  }
  return undefined;
}

function isKysely(value: unknown): value is KyselyMySqlLike {
  return hasMethod(value, 'executeQuery') && hasMethod(value, 'withoutPlugins') && typeof (value as { isTransaction?: unknown }).isTransaction === 'boolean';
}
