import type { SqlExecuteResult, SqlExecutor, SqlTransaction, SqlTransactionOptions } from '../../interfaces/sql-executor.interface.js';
import { notATransaction } from '../../sql/not-a-transaction.js';
import { describeValue, hasMethod, isolationSql } from '../../utils/executor.util.js';
import { runTransactionWork } from './transaction-work.js';

/** The part of a TypeORM `QueryRunner` the executor uses. */
export interface TypeOrmQueryRunnerLike {
  query(query: string, parameters?: any[], useStructuredResult?: boolean): Promise<any>;
  readonly isTransactionActive: boolean;
  release(): Promise<void>;
}

/** The part of a TypeORM `DataSource` the executor uses. */
export interface TypeOrmDataSourceLike {
  createQueryRunner(): TypeOrmQueryRunnerLike;
  transaction<T>(work: (manager: any) => Promise<T>): Promise<T>;
  transaction<T>(isolationLevel: any, work: (manager: any) => Promise<T>): Promise<T>;
  readonly options: { readonly type: string };
}

/** The part of a TypeORM `EntityManager` the executor uses. */
export interface TypeOrmEntityManagerLike {
  readonly connection: TypeOrmDataSourceLike;
  readonly queryRunner?: TypeOrmQueryRunnerLike;
}

const DATA_SOURCE = Symbol.for('DataSource');
const ENTITY_MANAGER = Symbol.for('EntityManager');

/**
 * A `SqlExecutor` on a TypeORM `DataSource` for PostgreSQL (or its `manager`). The transaction object is the
 * `EntityManager` that `dataSource.transaction()` hands its callback, or a `QueryRunner` (or its `manager`) after
 * `startTransaction()`.
 *
 * ```ts
 * const executor = fromTypeOrm(dataSource); // a first-party store's `executor` option
 *
 * await dataSource.transaction(async (manager) => {
 *   await manager.save(OrderEntity, order);
 *   // What a store does with the { transaction: manager } you pass it: its writes commit or roll back with yours
 *   await executor.wrapTransaction(manager).query('INSERT INTO audit (order_id) VALUES ($1::text)', [order.id]);
 * });
 * ```
 */
export function fromTypeOrm(dataSource: TypeOrmDataSourceLike | TypeOrmEntityManagerLike): SqlExecutor<'postgres'> {
  const source = isEntityManager(dataSource) ? dataSource.connection : dataSource;
  if (isEntityManager(dataSource) && dataSource.queryRunner?.isTransactionActive) {
    throw new TypeError("fromTypeOrm() takes the DataSource (or its manager), not a transaction's manager: pass that to a store method that takes your transaction ({ transaction: manager }).");
  }
  if (!isDataSource(source)) {
    throw new TypeError(`fromTypeOrm() takes a TypeORM DataSource (or its manager), got ${describeValue(dataSource)}.`);
  }
  if (source.options.type !== 'postgres') {
    throw new TypeError(`fromTypeOrm() takes a DataSource of type 'postgres', not '${source.options.type}'.`);
  }
  return new TypeOrmExecutor(source);
}

class TypeOrmExecutor implements SqlExecutor<'postgres'> {
  readonly dialect = 'postgres';

  constructor(private readonly dataSource: TypeOrmDataSourceLike) {}

  async query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    const runner = this.dataSource.createQueryRunner();
    try {
      return await run<R>(runner, text, params);
    } finally {
      await runner.release();
    }
  }

  async execute(text: string, params: readonly unknown[] = []): Promise<SqlExecuteResult> {
    const runner = this.dataSource.createQueryRunner();
    try {
      return await write(runner, text, params);
    } finally {
      await runner.release();
    }
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const inTransaction = (manager: TypeOrmEntityManagerLike) => runTransactionWork(work, runnerTransaction(manager.queryRunner!, manager));
    return options.isolationLevel ? this.dataSource.transaction(isolationSql(options.isolationLevel), inTransaction) : this.dataSource.transaction(inTransaction);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    const runner = isEntityManager(transaction) ? transaction.queryRunner : isQueryRunner(transaction) ? transaction : undefined;
    if (!runner?.isTransactionActive) {
      throw notATransaction(
        isEntityManager(transaction) || isQueryRunner(transaction)
          ? 'Pass the EntityManager your dataSource.transaction() callback receives (or a QueryRunner after startTransaction()), not dataSource.manager or a runner outside a transaction: each statement would commit on its own.'
          : `Pass the EntityManager your TypeORM dataSource.transaction() callback receives, got ${describeValue(transaction)}.`,
      );
    }

    const type = (runner as { connection?: { options?: { type?: unknown } } }).connection?.options?.type;
    if (type !== undefined && type !== 'postgres') {
      throw notATransaction(`Pass the EntityManager of a DataSource of type 'postgres', not '${String(type)}': the store's statements run on PostgreSQL.`);
    }
    return runnerTransaction(runner, transaction);
  }
}

/** Statements on `runner`; `native` is what the application holds: the `EntityManager`, or the `QueryRunner` given. */
function runnerTransaction(runner: TypeOrmQueryRunnerLike, native: unknown): SqlTransaction {
  return {
    query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(runner, text, params),
    execute: (text: string, params: readonly unknown[] = []) => write(runner, text, params),
    native,
  };
}

/** The structured result: a plain `query()` answers an UPDATE or DELETE with `[rows, count]`. */
async function run<R extends object>(runner: TypeOrmQueryRunnerLike, text: string, params: readonly unknown[]): Promise<R[]> {
  const result = await runner.query(text, [...params], true);
  return (result?.records ?? []) as R[];
}

/** `affected`: node-postgres's `rowCount`. */
async function write(runner: TypeOrmQueryRunnerLike, text: string, params: readonly unknown[]): Promise<SqlExecuteResult> {
  const result = await runner.query(text, [...params], true);
  return { affectedRows: result?.affected ?? 0 };
}

function isDataSource(value: unknown): value is TypeOrmDataSourceLike {
  return (value as { '@instanceof'?: unknown } | null)?.['@instanceof'] === DATA_SOURCE && hasMethod(value, 'createQueryRunner');
}

function isEntityManager(value: unknown): value is TypeOrmEntityManagerLike {
  return (value as { '@instanceof'?: unknown } | null)?.['@instanceof'] === ENTITY_MANAGER;
}

function isQueryRunner(value: unknown): value is TypeOrmQueryRunnerLike & { manager: unknown } {
  return hasMethod(value, 'query') && hasMethod(value, 'startTransaction') && typeof (value as { isTransactionActive?: unknown }).isTransactionActive === 'boolean';
}
