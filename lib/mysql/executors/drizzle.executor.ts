import type { SqlExecuteResult, SqlIsolationLevel, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isMysql2Client, isolationSql } from '../../utils/executor.util.js';
import type { SqlExecutor, SqlTransaction } from '../interfaces/mysql-executor.interface.js';
import { assertFoundRows, checkPlaceholders, mysql2Flags } from './mysql-client.util.js';

/** The part of a Drizzle MySQL database (or its `tx`) the executor uses. */
export interface DrizzleMySqlDatabaseLike {
  execute(query: any): PromiseLike<unknown>;
  transaction<T>(work: (tx: any) => Promise<T>, config?: { isolationLevel?: SqlIsolationLevel }): Promise<T>;
}

/** Drizzle marks its classes with this: `is()` compares these names up the class chain, across copies of the package. */
const ENTITY_KIND = Symbol.for('drizzle:entityKind');

/**
 * A `SqlExecutor` on a Drizzle MySQL database (`drizzle-orm/mysql2`). The transaction object is the `tx` that
 * `db.transaction()` hands its callback. A database whose mysql2 client lacks the `FOUND_ROWS` flag (on by default) is
 * refused: a store's `execute()` counts the rows an UPDATE matched.
 *
 * ```ts
 * const executor = fromDrizzle(db); // a first-party store's `executor` option
 *
 * await db.transaction(async (tx) => {
 *   await tx.insert(orders).values(order);
 *   // What a store does with the { transaction: tx } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(tx).execute('INSERT INTO audit (order_id) VALUES (?)', [order.id]);
 * });
 * ```
 */
export function fromDrizzle(db: DrizzleMySqlDatabaseLike): SqlExecutor {
  if (!isDatabase(db) || isTransaction(db) || !hasMethod(db, 'execute')) {
    const other = otherDatabase(db);
    throw new TypeError(
      isTransaction(db)
        ? 'fromDrizzle() takes the database drizzle() returns, not a transaction: pass the tx to a store method that takes your transaction ({ transaction: tx }).'
        : other
          ? `fromDrizzle() takes a Drizzle MySQL database (drizzle() of drizzle-orm/mysql2), not a ${other} one` +
              (other === 'PostgreSQL' ? ": a PostgreSQL store takes that, through fromDrizzle() from its package's /postgres subpath." : '.')
          : `fromDrizzle() takes a Drizzle MySQL database (drizzle() of drizzle-orm/mysql2), got ${describeValue(db)}.`,
    );
  }

  const client = (db as { $client?: unknown }).$client;
  if (isMysql2Client(client)) {
    assertFoundRows(mysql2Flags(client), 'fromDrizzle()');
  }
  return new DrizzleExecutor(db);
}

class DrizzleExecutor implements SqlExecutor {
  readonly dialect = 'mysql';

  constructor(private readonly db: DrizzleMySqlDatabaseLike) {}

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
    // Drizzle sends `set transaction isolation level ...` (the next transaction's alone) before `begin`.
    return this.db.transaction((tx) => work(drizzleTransaction(tx)), config);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    if (!isTransaction(transaction)) {
      const other = otherDatabase(transaction);
      throw notATransaction(
        isDatabase(transaction)
          ? 'Pass the tx your db.transaction() callback receives, not the database: a statement on the database runs outside your transaction.'
          : other
            ? `Pass the tx of a Drizzle MySQL database, not of a ${other} one: the store's statements run on MySQL.`
            : `Pass the tx your Drizzle db.transaction() callback receives, got ${describeValue(transaction)}.`,
      );
    }
    return drizzleTransaction(transaction as DrizzleMySqlDatabaseLike);
  }
}

function drizzleTransaction(tx: DrizzleMySqlDatabaseLike): SqlTransaction {
  return {
    query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(tx, text, params),
    execute: (text: string, params: readonly unknown[] = []) => write(tx, text, params),
    native: tx,
  };
}

let drizzleOrm: Promise<typeof import('drizzle-orm')> | undefined;

/**
 * Runs a `?`-style statement through Drizzle's `sql`, a `sql.param()` per placeholder, and resolves to mysql2's
 * `[result, fields]`: the rows of a SELECT, or the header of a write.
 */
async function send(db: DrizzleMySqlDatabaseLike, text: string, params: readonly unknown[]): Promise<unknown> {
  checkPlaceholders(text, params);
  const { sql } = await (drizzleOrm ??= import('drizzle-orm'));
  const pieces = text.split('?');
  const chunks = [];
  chunks.push(sql.raw(pieces[0]!));
  for (const [i, piece] of pieces.slice(1).entries()) {
    chunks.push(sql.param(params[i]), sql.raw(piece));
  }
  const [result] = (await db.execute(sql.join(chunks))) as [unknown, unknown];
  return result;
}

async function run<R extends object>(db: DrizzleMySqlDatabaseLike, text: string, params: readonly unknown[]): Promise<R[]> {
  const result = await send(db, text, params);
  return (Array.isArray(result) ? result : []) as R[];
}

async function write(db: DrizzleMySqlDatabaseLike, text: string, params: readonly unknown[]): Promise<SqlExecuteResult> {
  const result = (await send(db, text, params)) as { affectedRows?: number } | unknown[];
  return { affectedRows: Array.isArray(result) ? 0 : Number(result?.affectedRows ?? 0) };
}

/** A Drizzle MySQL database or transaction: `MySql2Database`, `MySqlDatabase`... */
function isDatabase(value: unknown): boolean {
  return entityKinds(value).some((kind) => /^MySql\w*Database$/.test(kind));
}

/** A Drizzle MySQL transaction: `MySql2Transaction`, `MySqlTransaction`... */
function isTransaction(value: unknown): boolean {
  return entityKinds(value).some((kind) => /^MySql\w*Transaction$/.test(kind));
}

/** The database a Drizzle database or transaction of another dialect is for. */
function otherDatabase(value: unknown): string | undefined {
  const kinds = entityKinds(value);
  if (kinds.some((kind) => /^Pg\w*(Database|Transaction)$/.test(kind))) {
    return 'PostgreSQL';
  }
  if (kinds.some((kind) => /SQLite\w*(Database|Transaction)$/.test(kind))) {
    return 'SQLite';
  }
  return undefined;
}

/** The Drizzle class names of `value` and its ancestors (`MySql2Transaction`, `MySqlTransaction`, `MySqlDatabase`...). */
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
