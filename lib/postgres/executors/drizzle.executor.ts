import type { SqlExecuteResult, SqlExecutor, SqlIsolationLevel, SqlTransaction, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isolationSql } from '../../utils/executor.util.js';
import { runTransactionWork } from './transaction-work.js';

/** The part of a Drizzle PostgreSQL database (or its `tx`) the executor uses. */
export interface DrizzleDatabaseLike {
  execute(query: any): PromiseLike<unknown>;
  transaction<T>(work: (tx: any) => Promise<T>, config?: { isolationLevel?: SqlIsolationLevel }): Promise<T>;
}

/** Drizzle marks its classes with this: `is()` compares these names up the class chain, across copies of the package. */
const ENTITY_KIND = Symbol.for('drizzle:entityKind');

/**
 * A `SqlExecutor` on a Drizzle PostgreSQL database, whatever its driver (`drizzle-orm/node-postgres`, `/pglite`,
 * `/postgres-js`...). The transaction object is the `tx` that `db.transaction()` hands its callback.
 *
 * ```ts
 * const executor = fromDrizzle(db); // a first-party store's `executor` option
 *
 * await db.transaction(async (tx) => {
 *   await tx.insert(orders).values(order);
 *   // What a store does with the { transaction: tx } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(tx).query('INSERT INTO audit (order_id) VALUES ($1::text)', [order.id]);
 * });
 * ```
 */
export function fromDrizzle(db: DrizzleDatabaseLike): SqlExecutor<'postgres'> {
  if (!isDatabase(db) || isTransaction(db) || !hasMethod(db, 'execute')) {
    throw new TypeError(
      isTransaction(db)
        ? 'fromDrizzle() takes the database drizzle() returns, not a transaction: pass the tx to a store method that takes your transaction ({ transaction: tx }).'
        : `fromDrizzle() takes a Drizzle PostgreSQL database (drizzle() of drizzle-orm/node-postgres, /pglite...), got ${describeValue(db)}.`,
    );
  }
  return new DrizzleExecutor(db);
}

class DrizzleExecutor implements SqlExecutor<'postgres'> {
  readonly dialect = 'postgres';

  constructor(private readonly db: DrizzleDatabaseLike) {}

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return run<R>(this.db, text, params);
  }

  execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    return write(this.db, text, params);
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const config = options.isolationLevel ? { isolationLevel: options.isolationLevel } : undefined;
    if (config) {
      isolationSql(config.isolationLevel);
    }
    return this.db.transaction((tx) => runTransactionWork(work, drizzleTransaction(tx)), config);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    if (!isTransaction(transaction)) {
      throw notATransaction(
        isDatabase(transaction)
          ? 'Pass the tx your db.transaction() callback receives, not the database: a statement on the database runs outside your transaction.'
          : `Pass the tx your Drizzle db.transaction() callback receives, got ${describeValue(transaction)}.`,
      );
    }
    return drizzleTransaction(transaction as DrizzleDatabaseLike);
  }
}

function drizzleTransaction(tx: DrizzleDatabaseLike): SqlTransaction {
  return {
    query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(tx, text, params),
    execute: (text: string, params: readonly unknown[] = []) => write(tx, text, params),
  };
}

let drizzleOrm: Promise<typeof import('drizzle-orm')> | undefined;

/** Runs a `$1`-style statement through Drizzle's `sql`, a `sql.param()` per placeholder, and resolves to the driver's result. */
async function send(db: DrizzleDatabaseLike, text: string, params: readonly unknown[]): Promise<unknown> {
  const { sql } = await (drizzleOrm ??= import('drizzle-orm'));
  const chunks = [];
  let last = 0;
  for (const match of text.matchAll(/\$(\d+)/g)) {
    chunks.push(sql.raw(text.slice(last, match.index)), sql.param(params[Number(match[1]) - 1]));
    last = match.index + match[0].length;
  }
  chunks.push(sql.raw(text.slice(last)));
  return db.execute(sql.join(chunks));
}

async function run<R extends object>(db: DrizzleDatabaseLike, text: string, params: readonly unknown[]): Promise<R[]> {
  // node-postgres and PGlite results carry `rows`; postgres-js's are the rows.
  const result = await send(db, text, params);
  return (Array.isArray(result) ? result : (result as { rows: R[] }).rows) as R[];
}

async function write(db: DrizzleDatabaseLike, text: string, params: readonly unknown[]): Promise<SqlExecuteResult> {
  // node-postgres counts in `rowCount`, PGlite in `affectedRows`, postgres-js in the rows' `count`.
  const result = (await send(db, text, params)) as { rowCount?: number | null; affectedRows?: number; count?: number };
  return { affectedRows: result.rowCount ?? result.affectedRows ?? result.count ?? 0 };
}

/** A Drizzle PostgreSQL database or transaction: `PgDatabase`, `PgliteDatabase`, `PgAsyncDatabase` (1.0)... */
function isDatabase(value: unknown): boolean {
  return entityKinds(value).some((kind) => /^Pg\w*Database$/.test(kind));
}

/** A Drizzle PostgreSQL transaction: `PgTransaction`, `PgAsyncTransaction` (1.0)... */
function isTransaction(value: unknown): boolean {
  return entityKinds(value).some((kind) => /^Pg\w*Transaction$/.test(kind));
}

/** The Drizzle class names of `value` and its ancestors (`NodePgTransaction`, `PgTransaction`, `PgDatabase`...). */
function entityKinds(value: unknown): string[] {
  const kinds: string[] = [];
  if (typeof value !== 'object' || value === null) {
    return kinds;
  }

  for (let type = (value as object).constructor as unknown; typeof type === 'function'; type = Object.getPrototypeOf(type)) {
    const kind = (type as unknown as Record<symbol, unknown>)[ENTITY_KIND];
    if (typeof kind === 'string') {
      kinds.push(kind);
    }
  }
  return kinds;
}
