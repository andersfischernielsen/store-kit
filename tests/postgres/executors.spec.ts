/**
 * The executors: the executor contract (`@nestjs/store-kit/testing`) through each of them on PostgreSQL (and through
 * fromPg() on a single node-postgres Client, and fromDrizzle() on PGlite), the words of their refusals, and each
 * client's particulars: what each takes as the application's client and transaction object (another database's
 * refused), a client in a failed transaction, a connection the server ends in a pool transaction (during a statement
 * and between two), Kysely's plugins, Prisma's transaction limits.
 */
import { PrismaPg } from '@prisma/adapter-pg';
import { CamelCasePlugin, Kysely, MysqlDialect, PostgresAdapter, PostgresDialect, SqliteDialect, sql } from 'kysely';
import mysql from 'mysql2/promise';
import pg from 'pg';
import { isNotATransactionError as fromTheRoot } from '../../lib/index.js';
import { isNotATransactionError as fromMysqlEntry } from '../../lib/mysql/index.js';
import { advisoryLock, fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm, isNotATransactionError } from '../../lib/postgres/index.js';
import { sqlExecutorContract } from '../../lib/testing/index.js';
import { PrismaClient } from '../fixtures/prisma/generated/client.js';
import { endPool } from '../support/postgres.js';
import { clients, onPostgres, openPglite, testDatabase, type Client } from './support.js';

const { database, reason } = await testDatabase('executors');

/** fromPg() on one connection: its statements and transactions take turns, the application's run on it directly. */
async function openSingleClient(url: string): Promise<Client> {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  return {
    name: 'fromPg (a single node-postgres Client)',
    executor: fromPg(client),
    root: client,
    async transaction(work) {
      await client.query('BEGIN');
      try {
        const result = await work(client);
        await client.query('COMMIT');
        return result;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    },
    insertOrder: async (tx, id) => {
      await (tx as pg.Client).query("INSERT INTO orders (id, status) VALUES ($1, 'placed')", [id]);
    },
    isNative: (value) => value === client,
    close: () => client.end(),
  };
}

const targets = [
  ...clients.map((factory) => ({ name: `${factory.name} on PostgreSQL`, open: () => factory.open(database!.url), skip: reason })),
  { name: 'fromPg (a single node-postgres Client) on PostgreSQL', open: () => openSingleClient(database!.url), skip: reason },
  { name: 'fromDrizzle (PGlite)', open: openPglite, skip: undefined },
];

describe.each(targets)('$name', ({ open, skip }) => {
  let client: Client;

  beforeAll(async () => {
    if (!skip) {
      client = await open();
    }
  });
  afterAll(() => client?.close());
  if (skip) {
    beforeEach((context) => context.skip(skip));
  }

  for (const c of sqlExecutorContract(() => ({ executor: client.executor, transaction: (work) => client.transaction(work), root: client.root }))) {
    it(c.name, c.run);
  }

  it('takes advisory locks through this client: one key, several in one statement, and in a namespace', async () => {
    const taken = await client.executor.transaction(async (tx) => {
      await advisoryLock(tx, '@nestjs/notes:s:one');
      await advisoryLock(tx, ['@nestjs/notes:s:b', '@nestjs/notes:s:a', '@nestjs/notes:s:b'], { shared: true });
      await advisoryLock(tx, ['k:1', 'k:2'], { namespace: '@nestjs/notes:s:key' });
      return (await tx.query<{ n: string }>("SELECT count(*)::text AS n FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()"))[0]!.n;
    });
    expect(taken).toBe('5');
  });

  it('is a PostgreSQL executor with execute(), and says what to pass instead of the database, pool or client, or of anything else', () => {
    expect(client.executor.dialect).toBe('postgres');
    expect(typeof client.executor.execute).toBe('function');
    expect(() => client.executor.wrapTransaction(client.root)).toThrow(/^(Pass the .* not |The node-postgres client isn't in a transaction)/);
    expect(() => client.executor.wrapTransaction({})).toThrow(/^Pass the .* got an object\.$/);
  });

  it('refuses them with a TypeError whose code a store recognizes: ERR_SQL_NOT_A_TRANSACTION', () => {
    for (const notATransaction of [client.root, {}, undefined]) {
      const refusal = thrown(() => client.executor.wrapTransaction(notATransaction));
      expect(refusal).toBeInstanceOf(TypeError);
      expect(refusal).toMatchObject({ name: 'TypeError', code: 'ERR_SQL_NOT_A_TRANSACTION' });
      expect(isNotATransactionError(refusal)).toBe(true);
    }
  });

  it("hands its transactions' work the client's own transaction object as native, and gives the application's back as it was given", async () => {
    expect(await client.executor.transaction(async (tx) => client.isNative(tx.native))).toBe(true);
    await client.transaction(async (tx) => {
      expect(client.isNative(tx)).toBe(true);
      expect(client.executor.wrapTransaction(tx).native).toBe(tx);
    });
  });
});

/** What `act()` throws. */
function thrown(act: () => unknown): unknown {
  try {
    act();
  } catch (error) {
    return error;
  }
  throw new Error('It threw nothing.');
}

describe('the clients each executor takes', () => {
  onPostgres(reason);

  it('fromPg() takes a pool or a client, and joins a client in a transaction (not the pool, not one idle or failed)', async () => {
    expect(() => fromPg({} as pg.Pool)).toThrow('fromPg() takes a node-postgres Pool (or a connected Client), got an object.');
    const pool = new pg.Pool({ connectionString: database!.url, max: 2 });
    const client = await pool.connect();
    try {
      const executor = fromPg(pool);
      expect(() => executor.wrapTransaction(pool)).toThrow('not the pool: it runs each statement on any of its connections, outside your transaction.');
      expect(() => executor.wrapTransaction(client)).toThrow("The node-postgres client isn't in a transaction: send BEGIN on it first, or each statement commits on its own.");

      await client.query('BEGIN');
      expect(await executor.wrapTransaction(client).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      await client.query('SELECT 1 / 0').catch(() => undefined);
      // pg rejects a query at its error, before the ReadyForQuery that carries the transaction's state; it sends the
      // next statement only after that one arrived.
      await client.query('SELECT 1').catch(() => undefined);
      expect(() => executor.wrapTransaction(client)).toThrow('The node-postgres client is in a failed transaction: roll it back.');
      await client.query('ROLLBACK');
    } finally {
      client.release();
      await endPool(pool);
    }
  });

  it('fromDrizzle() takes a PostgreSQL database, not its tx or another dialect', async () => {
    const drizzle = await clients[1]!.open(database!.url);
    try {
      await drizzle.transaction(async (tx) => {
        expect(() => fromDrizzle(tx as never)).toThrow('fromDrizzle() takes the database drizzle() returns, not a transaction');
      });
      class MySqlDatabase {
        execute() {}
      }
      Object.assign(MySqlDatabase, { [Symbol.for('drizzle:entityKind')]: 'MySqlDatabase' });
      expect(() => fromDrizzle(new MySqlDatabase() as never)).toThrow(
        'fromDrizzle() takes a Drizzle PostgreSQL database (drizzle() of drizzle-orm/node-postgres, /pglite...), got a MySqlDatabase.',
      );
      expect(() => fromDrizzle({} as never)).toThrow(TypeError);

      // Drizzle 1.0's classes (1.0.0-rc.4): PgAsyncDatabase, and PgAsyncTransaction extending it.
      const kind = (type: object, name: string) => Object.assign(type, { [Symbol.for('drizzle:entityKind')]: name });
      class PgAsyncDatabase {
        execute() {}
        transaction() {}
      }
      class NodePgDatabase extends PgAsyncDatabase {}
      class PgAsyncTransaction extends PgAsyncDatabase {}
      class NodePgTransaction extends PgAsyncTransaction {}
      [
        [PgAsyncDatabase, 'PgAsyncDatabase'],
        [NodePgDatabase, 'NodePgDatabase'],
        [PgAsyncTransaction, 'PgAsyncTransaction'],
        [NodePgTransaction, 'NodePgTransaction'],
      ].forEach(([type, name]) => kind(type as object, name as string));
      const executor = fromDrizzle(new NodePgDatabase() as never);
      expect(() => executor.wrapTransaction(new NodePgTransaction())).not.toThrow();
      expect(() => executor.wrapTransaction(new NodePgDatabase())).toThrow('Pass the tx your db.transaction() callback receives, not the database');
      expect(() => fromDrizzle(new NodePgTransaction() as never)).toThrow('not a transaction');
    } finally {
      await drizzle.close();
    }
  });

  it("fromTypeOrm() takes a PostgreSQL DataSource or its manager, not a transaction's manager, and joins only a PostgreSQL transaction", async () => {
    const typeorm = await clients[2]!.open(database!.url);
    try {
      const manager = typeorm.root as { connection: unknown };
      expect(await fromTypeOrm(manager as never).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      await typeorm.transaction(async (tx) => {
        expect(() => fromTypeOrm(tx as never)).toThrow("fromTypeOrm() takes the DataSource (or its manager), not a transaction's manager");
      });
      const mysql = { '@instanceof': Symbol.for('DataSource'), createQueryRunner() {}, options: { type: 'mysql' } };
      expect(() => fromTypeOrm(mysql as never)).toThrow("fromTypeOrm() takes a DataSource of type 'postgres', not 'mysql'.");
      expect(() => fromTypeOrm({} as never)).toThrow('fromTypeOrm() takes a TypeORM DataSource (or its manager), got an object.');

      // A QueryRunner of the application's, after startTransaction().
      const runner = (manager.connection as { createQueryRunner(): any }).createQueryRunner();
      await runner.startTransaction();
      expect(await typeorm.executor.wrapTransaction(runner).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      // The native is what was given: the runner, or its manager.
      expect(typeorm.executor.wrapTransaction(runner).native).toBe(runner);
      expect(typeorm.executor.wrapTransaction(runner.manager).native).toBe(runner.manager);
      await runner.rollbackTransaction();
      expect(() => typeorm.executor.wrapTransaction(runner)).toThrow('Pass the EntityManager your dataSource.transaction() callback receives');
      await runner.release();

      // A transaction of a MySQL DataSource's, as an application with both might pass by mistake.
      const mysqlManager = { '@instanceof': Symbol.for('EntityManager'), connection: mysql, queryRunner: { isTransactionActive: true, connection: mysql, query() {} } };
      expect(() => typeorm.executor.wrapTransaction(mysqlManager)).toThrow(
        "Pass the EntityManager of a DataSource of type 'postgres', not 'mysql': the store's statements run on PostgreSQL.",
      );
    } finally {
      await typeorm.close();
    }
  });

  it('fromPrisma() takes the client, not a transaction client, and gives its own transactions the limits asked for', async () => {
    const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: database!.url, max: 2 }) });
    try {
      await prisma.$transaction(async (tx) => {
        expect(() => fromPrisma(tx as never)).toThrow('fromPrisma() takes the Prisma client, not a transaction client');
      });
      expect(() => fromPrisma({} as never)).toThrow('fromPrisma() takes a Prisma client, got an object.');

      const hurried = fromPrisma(prisma, { timeout: '500ms' });
      await expect(hurried.transaction((tx) => tx.query('SELECT pg_sleep(1)::text'))).rejects.toThrow(/timeout|expired|closed/i);
      expect(await fromPrisma(prisma).transaction((tx) => tx.query('SELECT pg_sleep(1)::text AS slept'))).toEqual([{ slept: '' }]);
      expect(() => fromPrisma(prisma, { maxWait: 'soon' as never })).toThrow('Invalid duration "soon"');
    } finally {
      await prisma.$disconnect();
    }
  });

  it("fromKysely() takes the Kysely instance, not a transaction, and runs the store's statements without its plugins, not its transactions' native", async () => {
    const db = new Kysely<object>({ dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: database!.url, max: 2 }) }), plugins: [new CamelCasePlugin()] });
    try {
      const executor = fromKysely(db);
      expect(await executor.query('SELECT 1::text AS lease_until')).toEqual([{ lease_until: '1' }]);
      expect(await executor.transaction((tx) => tx.query('SELECT 1::text AS lease_until'))).toEqual([{ lease_until: '1' }]);
      // The native is the application's trx, plugins and all, in the same transaction as the store's statements.
      const [native, store] = await executor.transaction(async (tx) => [
        (await sql<object>`SELECT 1::text AS lease_until, txid_current()::text AS tx_id`.execute(tx.native as Kysely<object>)).rows,
        await tx.query('SELECT txid_current()::text AS tx_id'),
      ]);
      expect(native).toEqual([{ leaseUntil: '1', txId: (store[0] as { tx_id: string }).tx_id }]);
      await db.transaction().execute(async (trx) => {
        expect(await executor.wrapTransaction(trx).query('SELECT 1::text AS lease_until')).toEqual([{ lease_until: '1' }]);
        expect(() => fromKysely(trx as never)).toThrow('fromKysely() takes the Kysely instance, not a transaction');
      });
      expect(() => fromKysely({} as never)).toThrow('fromKysely() takes a Kysely instance, got an object.');
    } finally {
      await db.destroy();
    }
  });
});

describe('fromPg() and a connection the server ends', () => {
  onPostgres(reason);

  it("fails the pool transaction on it with the server's error, with no uncaught 'error' event, and the pool drops it for a new one", async () => {
    const pool = new pg.Pool({ connectionString: database!.url, max: 1 });
    try {
      const executor = fromPg(pool);
      // pg_terminate_backend() ends the connection mid-statement, as a restart or a failover does: the statement fails
      // (57P01), and the client, out of the pool, emits 'error' too: an uncaught exception unless something listens.
      await expect(
        executor.transaction(async (tx) => {
          const pid = (await tx.query<{ pid: number }>('SELECT pg_backend_pid() AS pid'))[0]!.pid;
          // Under the test's timeout: a termination that didn't happen fails on the statement's 57014, not on a timeout.
          await tx.query("SET LOCAL statement_timeout = '10s'");
          await Promise.all([tx.query('SELECT pg_sleep(30)'), database!.admin.query('SELECT pg_terminate_backend($1)', [pid])]);
        }),
      ).rejects.toMatchObject({ code: '57P01' });

      expect(pool.totalCount).toBe(0);
      expect(await executor.transaction((tx) => tx.query('SELECT 1::text AS one'))).toEqual([{ one: '1' }]);
      // A connection the pool hands out has no listener left from a transaction (the pool takes its own off).
      const client = await pool.connect();
      const listeners = client.listenerCount('error');
      client.release();
      expect(listeners).toBe(0);
    } finally {
      await endPool(pool);
    }
  });

  it("fails the pool transaction with the server's error when it ends the connection between two statements", async () => {
    const pool = new pg.Pool({ connectionString: database!.url, max: 1 });
    try {
      const executor = fromPg(pool);
      // The server ends a connection idle in a transaction with a FATAL no statement receives: the client emits 'error'
      // with it, then again as the socket closes, both while the transaction holds the connection. The next statement
      // fails with that FATAL (25P03), not with pg's "not queryable".
      await expect(
        executor.transaction(async (tx) => {
          await tx.query("SET LOCAL idle_in_transaction_session_timeout = '50ms'");
          await new Promise((resolve) => setTimeout(resolve, 500));
          await tx.query('SELECT 1');
        }),
      ).rejects.toMatchObject({ code: '25P03' });

      expect(pool.totalCount).toBe(0);
    } finally {
      await endPool(pool);
    }
  });
});

describe('fromKysely() and another database', () => {
  // Nothing connects: Kysely opens its pool at the first query.
  const unreachable = async () => {
    throw new Error('no database here');
  };

  it("refuses a Kysely instance whose dialect is Kysely's MySQL or SQLite one, and a transaction of it", () => {
    const mysql = new Kysely<object>({ dialect: new MysqlDialect({ pool: unreachable as never }) });
    const sqlite = new Kysely<object>({ dialect: new SqliteDialect({ database: unreachable as never }) });
    expect(() => fromKysely(mysql as never)).toThrow('fromKysely() takes a Kysely instance with a PostgreSQL dialect, not a MySQL one.');
    expect(() => fromKysely(sqlite as never)).toThrow('fromKysely() takes a Kysely instance with a PostgreSQL dialect, not a SQLite one.');

    const postgres = fromKysely(new Kysely<object>({ dialect: new PostgresDialect({ pool: unreachable as never }) }) as never);
    const mysqlTrx = { isTransaction: true, executeQuery() {}, withoutPlugins() {}, getExecutor: () => mysql.getExecutor() };
    expect(() => postgres.wrapTransaction(mysqlTrx)).toThrow('Pass the trx of a Kysely instance with a PostgreSQL dialect, not a MySQL one');
  });

  it('takes any PostgreSQL dialect, whatever its adapter is called, since only the known other databases are refused', () => {
    class NeonAdapter extends PostgresAdapter {}
    const custom = new Kysely<object>({
      dialect: {
        createAdapter: () => new NeonAdapter(),
        createDriver: () => new PostgresDialect({ pool: unreachable as never }).createDriver(),
        createIntrospector: (db) => new PostgresDialect({ pool: unreachable as never }).createIntrospector(db),
        createQueryCompiler: () => new PostgresDialect({ pool: unreachable as never }).createQueryCompiler(),
      },
    });
    expect(fromKysely(custom as never).dialect).toBe('postgres');
  });
});

describe('fromPg() and MySQL', () => {
  it('refuses a mysql2 pool or connection, of the promise API or the callback one, and joins no mysql2 connection', async () => {
    // Nothing connects: a mysql2 pool opens its connections at the first query.
    const pool = mysql.createPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 });
    try {
      const refusal =
        "fromPg() takes a node-postgres Pool (or a connected Client), not a mysql2 pool or connection: a MySQL store takes that, through fromMysql2() from its package's /mysql subpath.";
      expect(() => fromPg(pool as never)).toThrow(refusal);
      expect(() => fromPg(pool.pool as never)).toThrow(refusal);

      const executor = fromPg(new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' }));
      expect(() => executor.wrapTransaction(pool)).toThrow(
        "Pass the node-postgres client your transaction runs on, not a mysql2 connection: the store's statements run on PostgreSQL.",
      );
    } finally {
      await pool.end();
    }
  });
});

describe('the code of a refusal of anything but a transaction', () => {
  // Nothing connects: the pools open their connections at the first query.
  const unreachable = async () => {
    throw new Error('no database here');
  };

  it("is on each executor's refusal of another dialect's transaction too, and on no other TypeError", async () => {
    const mysqlPool = mysql.createPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 });
    const pgPool = new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' });
    try {
      const mysqlKysely = new Kysely<object>({ dialect: new MysqlDialect({ pool: unreachable as never }) });
      const kysely = fromKysely(new Kysely<object>({ dialect: new PostgresDialect({ pool: unreachable as never }) }) as never);
      const mysqlSource = { '@instanceof': Symbol.for('DataSource'), createQueryRunner() {}, options: { type: 'mysql' } };
      const source = { '@instanceof': Symbol.for('DataSource'), createQueryRunner() {}, transaction() {}, options: { type: 'postgres' } };
      const refusals = [
        () => fromPg(pgPool).wrapTransaction(mysqlPool),
        () => fromPg(pgPool).wrapTransaction(pgPool),
        () => kysely.wrapTransaction({ isTransaction: true, executeQuery() {}, withoutPlugins() {}, getExecutor: () => mysqlKysely.getExecutor() }),
        () => fromTypeOrm(source as never).wrapTransaction({ '@instanceof': Symbol.for('EntityManager'), connection: mysqlSource, queryRunner: { isTransactionActive: true, connection: mysqlSource } }),
        () => fromPrisma({ $queryRawUnsafe() {}, $executeRawUnsafe() {}, $transaction() {}, $connect() {} } as never).wrapTransaction({ $connect() {}, $queryRawUnsafe() {} }),
      ];
      for (const refusal of refusals) {
        expect(thrown(refusal)).toMatchObject({ name: 'TypeError', code: 'ERR_SQL_NOT_A_TRANSACTION' });
      }

      // The executors' other TypeErrors are about their own arguments: no code.
      for (const misuse of [() => fromPg({} as never), () => fromKysely(mysqlKysely as never), () => fromTypeOrm(mysqlSource as never)]) {
        const error = thrown(misuse);
        expect(error).toBeInstanceOf(TypeError);
        expect(isNotATransactionError(error)).toBe(false);
      }
    } finally {
      await mysqlPool.end();
      await pgPool.end();
    }
  });

  it("is recognized by its code, whichever copy of the kit threw it, from every entry: never by a class", () => {
    const elsewhere = Object.assign(new TypeError('Pass the tx your db.transaction() callback receives, not the database.'), { code: 'ERR_SQL_NOT_A_TRANSACTION' });
    for (const recognizes of [isNotATransactionError, fromTheRoot, fromMysqlEntry]) {
      expect(recognizes(elsewhere)).toBe(true);
      expect(recognizes(new TypeError('Pass the tx your db.transaction() callback receives, not the database.'))).toBe(false);
      expect(recognizes(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))).toBe(false);
      expect([undefined, null, 'ERR_SQL_NOT_A_TRANSACTION'].map(recognizes)).toEqual([false, false, false]);
    }
  });
});
