/**
 * A database a store can run on: what an executor's `dialect` says, and `SqlExecutor`'s type parameter.
 *
 * ```ts
 * const dialect: SqlDialect = executor.dialect; // 'postgres' or 'mysql'
 * ```
 */
export type SqlDialect = 'postgres' | 'mysql';

/**
 * A transaction isolation level: the four of the SQL standard, which PostgreSQL and MySQL share.
 *
 * ```ts
 * const isolationLevel: SqlIsolationLevel = 'read committed';
 * ```
 */
export type SqlIsolationLevel = 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';

/**
 * What `SqlExecutor.transaction()` takes.
 *
 * ```ts
 * await executor.transaction((tx) => tx.query('SELECT pg_advisory_xact_lock(hashtext($1::text))', ['emails']), { isolationLevel: 'read committed' });
 * ```
 */
export interface SqlTransactionOptions {
  /**
   * Default: the database's (PostgreSQL's `default_transaction_isolation`, `read committed` unless changed; MySQL's
   * `transaction_isolation`, `repeatable read` unless changed). It applies to this transaction alone, never to the
   * connection.
   */
  isolationLevel?: SqlIsolationLevel;
}

/**
 * What `execute()` resolves to: how many rows a statement wrote.
 *
 * ```ts
 * const { affectedRows } = await tx.execute('UPDATE nest_queues_jobs SET lease_until = CAST(? AS SIGNED) WHERE id = ? AND lease_token = ?', p.values);
 * ```
 */
export interface SqlExecuteResult {
  /**
   * The rows the statement inserted or deleted, or, for an UPDATE, the rows its WHERE matched, whether or not the
   * update changed their values (as PostgreSQL counts them, and MySQL with the `FOUND_ROWS` client flag, which every
   * client the kit covers sets by default).
   */
  affectedRows: number;
}

/**
 * Runs SQL on one transaction's connection: what `SqlExecutor.transaction()` hands its callback, and what
 * `SqlExecutor.wrapTransaction()` makes of the application's own transaction object.
 *
 * ```ts
 * const tx = executor.wrapTransaction(applicationTx);
 * await tx.query('INSERT INTO nest_queues.jobs (id, data) VALUES ($1::text, $2::text::jsonb)', [id, JSON.stringify(data)]);
 * ```
 */
export interface SqlTransaction {
  /**
   * Runs one statement, with its placeholders (`$1`, `$2`... on PostgreSQL, `?` on MySQL) bound to `params` in order,
   * and resolves to its rows (`[]` for a statement that returns none).
   */
  query<R extends object = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  /**
   * Runs one statement that writes (INSERT, UPDATE, DELETE) as `query()` does, and resolves to the rows it wrote: what
   * a MySQL store reads where a PostgreSQL one has `RETURNING`. Every executor of the kit has it, and a MySQL one must
   * (`@nestjs/store-kit/mysql`'s `SqlTransaction` requires it); it's optional here, so an executor written before it
   * still satisfies this interface. For a statement that returns rows, use `query()`.
   */
  execute?(text: string, params?: readonly unknown[]): Promise<SqlExecuteResult>;
  /**
   * The client's own transaction object this transaction runs on: the kind the application's code holds, which
   * `SqlExecutor.wrapTransaction()` takes. With it, a store that opens a transaction (`executor.transaction()`) can
   * hand its caller the application's kind of object, whose own API (an ORM's queries, a client's `query()`) writes in
   * that transaction. Per client:
   * - node-postgres: the client in its transaction, after `BEGIN` (a pool's `PoolClient`, or the `Client` given to
   *   `fromPg()`).
   * - Drizzle: the `tx` that `db.transaction()` hands its callback.
   * - TypeORM: the `EntityManager` that `dataSource.transaction()` hands its callback (or the `QueryRunner` given to
   *   `wrapTransaction()`).
   * - Prisma: the transaction client that `prisma.$transaction(async (tx) => ...)` hands its callback.
   * - Kysely: the `Transaction` that `db.transaction().execute()` hands its callback (its plugins included).
   * - mysql2: the connection in its transaction, after `START TRANSACTION`, of the API `fromMysql2()` was given (a
   *   callback-API pool's transactions hand out callback-API connections).
   *
   * `wrapTransaction(x).native` is `x`, as it was given. Every executor of the kit sets it; it's optional here, so an
   * executor written without it still satisfies this interface.
   *
   * ```ts
   * const executor = fromDrizzle(db);
   * await executor.transaction(async (tx) => {
   *   await (tx.native as typeof db).insert(orders).values(order); // Drizzle's tx: the same transaction
   *   await tx.query('INSERT INTO audit (order_id) VALUES ($1::text)', [order.id]);
   * });
   * ```
   */
  readonly native?: unknown;
}

/**
 * How a store reaches its database through the client the application already has. `fromPg()`, `fromDrizzle()`,
 * `fromTypeOrm()`, `fromPrisma()` and `fromKysely()` from `@nestjs/store-kit/postgres` make one of a pool or an ORM
 * (`fromMysql2()` and the rest from `@nestjs/store-kit/mysql` on MySQL); anything else can implement it
 * (`sqlExecutorContract()` from `@nestjs/store-kit/testing` checks one). Nothing in it belongs to a store: every
 * first-party store of a dialect takes the same executors, and with them the same transaction objects.
 *
 * ```ts
 * const executor = fromDrizzle(db);
 * const [job] = await executor.query<{ id: string }>('SELECT id FROM nest_queues.jobs WHERE state = $1::text LIMIT 1', ['waiting']);
 * await executor.transaction(async (tx) => {
 *   await tx.query('UPDATE nest_queues.jobs SET state = $1::text WHERE id = $2::text', ['active', job.id]);
 * });
 * ```
 *
 * `D` is its dialect: `fromPg()` and the rest of `/postgres` return a `SqlExecutor<'postgres'>`, the executors of
 * `/mysql` a `SqlExecutor<'mysql'>`, so a store's options that take `SqlExecutor<'postgres'>` refuse a MySQL executor
 * when the application compiles, before the store refuses it at run time. Each dialect's entry exports its own as
 * `SqlExecutor` (and a package's `/postgres` and `/mysql` re-export it): annotated with it, an executor fits that
 * dialect's stores. Here, at the root, `SqlExecutor` alone is an executor of either dialect, for code that serves both.
 *
 * ```ts
 * export interface PostgresOutboxStoreOptions {
 *   executor: SqlExecutor<'postgres'>; // fromMysql2(pool) here is a compile error
 * }
 * ```
 */
export interface SqlExecutor<D extends SqlDialect = SqlDialect> {
  /**
   * The database it runs on, which decides the SQL it takes: a store refuses an executor of another dialect (a
   * PostgreSQL store's statements can't run on MySQL).
   */
  readonly dialect: D;
  /** Runs one statement outside any transaction (on a pool, on any of its connections), as `SqlTransaction.query()`. */
  query<R extends object = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  /**
   * Runs one statement that writes, outside any transaction, as `SqlTransaction.execute()`: optional here, on every
   * executor of the kit, and required of a MySQL one.
   */
  execute?(text: string, params?: readonly unknown[]): Promise<SqlExecuteResult>;
  /**
   * Runs `work` in a transaction of its own, on one connection: commits when `work` resolves, rolls back and rethrows
   * when it rejects. On PostgreSQL, which ends a transaction at its first failed statement, `work` that caught a
   * statement's error and resolved without a `ROLLBACK TO SAVEPOINT` rolls back and rejects as well: nothing of it
   * could commit.
   */
  transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options?: SqlTransactionOptions): Promise<T>;
  /**
   * The application's transaction object (Drizzle's `tx`, a TypeORM `EntityManager`, a Prisma transaction client, a
   * Kysely `Transaction`, a node-postgres client after `BEGIN`, a mysql2 connection after `START TRANSACTION`), so
   * statements run in it and commit or roll back with the application's own writes; the object comes back as the
   * `SqlTransaction`'s `native`, as it was given. Throws a `TypeError` for anything else, such as the database or pool
   * itself. The kit's executors give that `TypeError` the `code`
   * `'ERR_SQL_NOT_A_TRANSACTION'` (`isNotATransactionError()` recognizes it), which a store can turn into its own
   * error: mysql2's executor throws it from the transaction's first statement, as only the server knows whether the
   * connection is in a transaction.
   */
  wrapTransaction(transaction: unknown): SqlTransaction;
}
