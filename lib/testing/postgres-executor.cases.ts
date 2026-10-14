import type { SqlIsolationLevel, SqlTransaction } from '../interfaces/sql-executor.interface.js';
import { columns, toBool, toInt, toJson, toText } from '../postgres/sql/columns.js';
import { SqlParams } from '../postgres/sql/sql-params.js';
import { equal, isTypeError, jitter, messages, rejection, setsNative } from './assertions.js';
import type { DialectCases } from './sql-executor.contract.js';

const ISOLATION_LEVELS: SqlIsolationLevel[] = ['read uncommitted', 'read committed', 'repeatable read', 'serializable'];

/** The contract's cases in PostgreSQL's SQL. */
export const POSTGRES_CASES: DialectCases = {
  createTable: (table) => `CREATE TABLE ${table} (id text PRIMARY KEY, small_n integer, big_n bigint, is_flag boolean, doc_json jsonb, note_text text)`,
  dropTable: (table) => `DROP TABLE IF EXISTS ${table}`,
  cases: {
    async statements({ executor }, table) {
      const rows = await executor.query('SELECT $2::text AS first_value, $1::text AS second_value, $3::text::bigint::text AS big_value', [
        null,
        "O'Reilly — ü 🚀 $1",
        '9007199254740993',
      ]);
      equal(rows, [{ first_value: "O'Reilly — ü 🚀 $1", second_value: null, big_value: '9007199254740993' }], 'query() of a SELECT with parameters');
      equal(await executor.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['plain']), [], 'query() of an INSERT without RETURNING');
      equal(await executor.query(`DELETE FROM ${table} WHERE id = $1::text RETURNING id`, ['plain']), [{ id: 'plain' }], 'query() of a DELETE ... RETURNING');

      const failure = await rejection(executor.query('SELECT 1 / 0'), 'query() of a failing statement');
      includes(messages(failure), 'division by zero', 'query() of a failing statement');
      equal(await executor.query("SELECT 'next'::text AS status"), [{ status: 'next' }], 'query() after a failed statement');

      // execute() is optional in SqlExecutor; every executor of the kit has one.
      if (executor.execute) {
        const execute = executor.execute.bind(executor);
        equal(await execute(`INSERT INTO ${table} (id, small_n) VALUES ($1::text, 1), ($2::text, 1)`, ['w1', 'w2']), { affectedRows: 2 }, 'execute() of an INSERT of two rows');
        equal(
          await execute(`UPDATE ${table} SET small_n = small_n WHERE id IN ($1::text, $2::text, $3::text)`, ['w1', 'w2', 'missing']),
          { affectedRows: 2 },
          'execute() of an UPDATE that matches two rows and changes nothing in them',
        );
        equal(await execute(`DELETE FROM ${table} WHERE id = $1::text`, ['w1']), { affectedRows: 1 }, 'execute() of a DELETE of one row');
        equal(await execute(`DELETE FROM ${table} WHERE id = $1::text`, ['w1']), { affectedRows: 0 }, 'execute() of a DELETE of no rows');
        await execute(`DELETE FROM ${table}`);
      }
    },

    async transactions({ executor }, table) {
      const result = await executor.transaction(async (tx) => {
        await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['kept']);
        if (executor.execute) {
          equal(await tx.execute?.(`UPDATE ${table} SET note_text = $1::text WHERE id = $2::text`, ['in the transaction', 'kept']), { affectedRows: 1 }, "execute() in a transaction");
        }
        return (await tx.query(`SELECT id FROM ${table} WHERE id = $1::text`, ['kept'])).length;
      });
      equal(result, 1, "a committed transaction's result");

      const failure = new Error('changed my mind');
      const rolledBack = await rejection(
        executor.transaction(async (tx) => {
          await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['undone']);
          throw failure;
        }),
        'a transaction whose work rejects',
      );
      if (rolledBack !== failure) {
        throw new Error(`a transaction whose work rejects: expected it to reject with the work's error, got ${messages(rolledBack)}`);
      }

      const duplicate = await rejection(
        executor.transaction(async (tx) => {
          await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['partial']);
          await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['kept']);
        }),
        'a transaction whose statement fails',
      );
      includes(messages(duplicate), 'duplicate key', 'a transaction whose statement fails');
      equal(await ids(executor, table), ['kept'], 'the rows after a committed transaction and two rolled back');

      // PostgreSQL ends a transaction at its first failed statement, and answers its COMMIT with a ROLLBACK, no error:
      // work that caught the statement's error and resolved must not look committed.
      const swallowed = await rejection(
        executor.transaction(async (tx) => {
          await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['swallowed']);
          await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['kept']).catch(() => undefined);
          return 'resolved';
        }),
        'a transaction whose work catches a failed statement and resolves',
      );
      includes(messages(swallowed), 'duplicate key', 'a transaction whose work catches a failed statement and resolves');
      equal(await ids(executor, table), ['kept'], 'the rows after a transaction whose work caught a failed statement');

      const recovered = await executor.transaction(async (tx) => {
        await tx.query('SAVEPOINT attempt');
        await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['kept']).catch(() => tx.query('ROLLBACK TO SAVEPOINT attempt'));
        await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['after-savepoint']);
        return 'committed';
      });
      equal(recovered, 'committed', 'a transaction whose work rolled back to a savepoint after a failed statement');
      equal(await ids(executor, table), ['after-savepoint', 'kept'], 'the rows after a transaction that rolled back to a savepoint');
      equal(await executor.transaction((tx) => tx.query("SELECT 'next'::text AS status")), [{ status: 'next' }], 'a transaction after failed ones');
    },

    async isolation({ executor }, table) {
      const level = (isolationLevel?: SqlIsolationLevel) =>
        executor.transaction(
          async (tx) => (await tx.query<{ level: string }>("SELECT current_setting('transaction_isolation') AS level"))[0]?.level,
          isolationLevel ? { isolationLevel } : undefined,
        );
      for (const isolationLevel of ISOLATION_LEVELS) {
        equal(await level(isolationLevel), isolationLevel, `a transaction at ${isolationLevel}`);
      }
      const [defaults] = await executor.query<{ level: string }>("SELECT current_setting('default_transaction_isolation') AS level");
      equal(await level(), defaults?.level, "a transaction without an isolation level (the database's default)");

      const refused = await rejection(
        executor.transaction(
          async (tx) => {
            await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['ran']);
          },
          { isolationLevel: `snapshot; DROP TABLE ${table}` as SqlIsolationLevel },
        ),
        'a transaction at an unknown isolation level',
      );
      isTypeError(refused, 'a transaction at an unknown isolation level');
      equal(await ids(executor, table), [], 'the rows after a transaction at an unknown isolation level');
    },

    async joins({ executor, transaction, root }, table) {
      const failure = new Error('rolled back by the application');
      const rolledBack = await rejection(
        transaction(async (tx) => {
          await executor.wrapTransaction(tx).query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['rolled-back']);
          throw failure;
        }),
        "the application's transaction that rejects",
      );
      includes(messages(rolledBack), failure.message, "the application's transaction that rejects");
      await transaction((tx) => executor.wrapTransaction(tx).query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['committed']));
      equal(await ids(executor, table), ['committed'], "the rows after the application's rollback and commit");
      if (executor.execute) {
        const written = await transaction((tx) => executor.wrapTransaction(tx).execute!(`DELETE FROM ${table} WHERE id = $1::text`, ['committed']));
        equal(written, { affectedRows: 1 }, "execute() in the application's transaction");
        equal(await ids(executor, table), [], "the rows after execute() in the application's transaction");
      }

      const refused: Array<[string, unknown]> = [
        ["the application's database, pool or client", root],
        ['the executor', executor],
        ['an object', {}],
        ['null', null],
        ['undefined', undefined],
        ['a number', 42],
        ['a string', 'tx'],
      ];
      for (const [label, value] of refused) {
        let thrown: unknown;
        try {
          executor.wrapTransaction(value);
        } catch (error) {
          thrown = error;
        }
        isTypeError(thrown, `wrapTransaction() of ${label}`);
      }
    },

    async native(target, table) {
      if (!(await setsNative(target))) {
        return;
      }

      // Nothing reads from outside while the work runs: on one connection, that read would wait for the transaction.
      const { executor } = target;
      const insert = `INSERT INTO ${table} (id) VALUES ($1::text)`;
      const seen = await executor.transaction(async (tx) => {
        const joined = executor.wrapTransaction(tx.native);
        if (joined.native !== tx.native) {
          throw new Error("wrapTransaction() of a transaction's native: expected that native back, as it was given, got another object");
        }
        await joined.query(insert, ['kept']);
        return ids(tx, table);
      });
      equal(seen, ['kept'], "the rows a transaction sees after a write through wrapTransaction() of its native");
      equal(await ids(executor, table), ['kept'], "the rows after that transaction committed");

      const failure = new Error('changed my mind');
      let before: string[] = [];
      const rolledBack = await rejection(
        executor.transaction(async (tx) => {
          await executor.wrapTransaction(tx.native).query(insert, ['undone']);
          before = await ids(tx, table);
          throw failure;
        }),
        "a transaction whose work rejects after a write through wrapTransaction() of its native",
      );
      if (rolledBack !== failure) {
        throw new Error(`a transaction whose work rejects after a write through wrapTransaction() of its native: expected it to reject with the work's error, got ${messages(rolledBack)}`);
      }
      equal(before, ['kept', 'undone'], "the rows a transaction sees after a write through wrapTransaction() of its native, before its work rejects");
      equal(await ids(executor, table), ['kept'], "the rows after that transaction rolled back (the write through its native with it)");
    },

    async connections({ executor }) {
      const txid = async (db: SqlTransaction) => (await db.query<{ id: string }>('SELECT txid_current()::text AS id'))[0]!.id;
      const outside = Array.from({ length: 10 }, () => txid(executor));
      const inside = await Promise.all(
        Array.from({ length: 10 }, () =>
          executor.transaction(async (tx) => {
            const first = await txid(tx);
            await jitter();
            return [first, await txid(tx)] as const;
          }),
        ),
      );

      for (const [first, second] of inside) {
        equal(second, first, "the transaction id of a transaction's second statement");
      }
      const distinct = new Set([...inside.map(([first]) => first), ...(await Promise.all(outside))]);
      equal(distinct.size, 20, 'distinct transaction ids of 10 transactions and 10 statements outside them, run at once');
    },

    async values({ executor }, table) {
      const rows = [
        { id: "O'Reilly — ü 🚀 $1 \\ \n", small: -2147483648, big: 9007199254740991, flag: true, doc: { a: [1, 'two', null, { deep: "it's $2" }], n: 1.5 }, note: '' },
        { id: 'nulls', small: null, big: null, flag: false, doc: 'a JSON string', note: null },
        { id: 'no-json', small: 0, big: 0, flag: false, doc: null, note: 'x' },
      ];
      for (const row of rows) {
        const p = new SqlParams();
        await executor.query(
          `INSERT INTO ${table} (id, small_n, big_n, is_flag, doc_json, note_text)
VALUES (${p.text(row.id)}, ${p.int(row.small)}, ${p.bigint(row.big)}, ${p.bool(row.flag)}, ${p.json(row.doc)}, ${p.text(row.note)})`,
          p.values,
        );
      }

      for (const row of rows) {
        const p = new SqlParams();
        const [read] = await executor.query<Record<string, unknown>>(
          `SELECT ${columns(['id', 'small_n', 'big_n', 'is_flag', 'doc_json', 'note_text'], 't')} FROM ${table} t WHERE t.id = ${p.text(row.id)}`,
          p.values,
        );
        const values = { id: toText(read?.id), small: toInt(read?.small_n), big: toInt(read?.big_n), flag: toBool(read?.is_flag), doc: toJson(read?.doc_json), note: toText(read?.note_text) };
        equal(values, row, `a row written with SqlParams and read with columns() (${JSON.stringify(row.id)})`);
      }
    },
  },
};

async function ids(executor: SqlTransaction, table: string): Promise<string[]> {
  return (await executor.query<{ id: string }>(`SELECT id FROM ${table}`)).map((row) => row.id).sort();
}

function includes(text: string, part: string, label: string): void {
  if (!text.includes(part)) {
    throw new Error(`${label}: expected an error with "${part}", got "${text}"`);
  }
}
