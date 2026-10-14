/**
 * A store's writes in the application's transaction, through each client's own transaction object (node-postgres's
 * client after BEGIN, Drizzle's `tx`, TypeORM's `EntityManager`, Prisma's transaction client, Kysely's `trx`): they
 * commit with the application's row, unseen by other connections until then, or roll back with it. The same object as
 * the `native` of a transaction the executor opened: the client's own API writes in it, and so does a store given it.
 * And assertReadCommittedTransaction() refuses a transaction that isn't READ COMMITTED, with the store's words.
 */
import { assertReadCommittedTransaction } from '../../lib/postgres/index.js';
import { noteSchema } from '../fixtures/notes/note.schema.js';
import { PostgresNoteStore } from '../fixtures/notes/postgres-note.store.js';
import { clients, openPglite, testDatabase, type Client } from './support.js';

const { database, reason } = await testDatabase('transactions');

const targets = [
  ...clients.map((factory, i) => ({ name: `${factory.name} on PostgreSQL`, schema: `tx_${i}`, open: () => factory.open(database!.url), postgres: true, skip: reason })),
  { name: 'fromDrizzle (PGlite)', schema: 'tx_pglite', open: openPglite, postgres: false, skip: undefined },
];

describe.each(targets)('$name', ({ schema, open, postgres, skip }) => {
  let client: Client;
  let store: PostgresNoteStore;

  beforeAll(async () => {
    if (skip) {
      return;
    }

    client = await open();
    store = new PostgresNoteStore({ executor: client.executor, schema });
    await store.onModuleInit();
  });
  afterAll(() => client?.close());
  if (skip) {
    beforeEach((context) => context.skip(skip));
  }

  const orders = async (id: string) => (await client.executor.query<{ id: string }>('SELECT id FROM orders WHERE id = $1::text', [id])).length;

  it("commits the store's writes with the application's row, unseen by other connections until then", async () => {
    const id = `${schema}-committed`;
    await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      expect(await store.addInTransaction(tx, { id, body: 'order placed', tags: ['orders'], createdAt: 1 })).toBe(true);
      if (postgres) {
        expect(await database!.admin.query(`SELECT id FROM "${schema}".notes WHERE id = $1`, [id])).toMatchObject({ rowCount: 0 });
      }
    });

    expect(await orders(id)).toBe(1);
    expect(await store.get(id)).toEqual({ id, body: 'order placed', tags: ['orders'], archived: false, createdAt: 1 });
  });

  it("rolls the store's writes back with the application's", async () => {
    const id = `${schema}-rolled-back`;
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        await store.addInTransaction(tx, { id, body: 'order placed', tags: ['orders'], createdAt: 1 });
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');

    expect(await orders(id)).toBe(0);
    expect(await store.get(id)).toBeNull();
    expect(await client.executor.query(`SELECT tag FROM "${schema}".note_tags WHERE note_id = $1::text`, [id])).toEqual([]);
  });

  it("hands the executor's own transaction the client's object as native, whose own API and a store given it write in that transaction", async () => {
    const id = `${schema}-native`;
    await client.executor.transaction(async (tx) => {
      await client.insertOrder(tx.native, id);
      expect(await store.addInTransaction(tx.native, { id, body: 'order placed', tags: ['orders'], createdAt: 1 })).toBe(true);
      expect(await tx.query('SELECT id FROM orders WHERE id = $1::text', [id])).toEqual([{ id }]);
      if (postgres) {
        expect(await database!.admin.query('SELECT id FROM orders WHERE id = $1', [id])).toMatchObject({ rowCount: 0 });
      }
    });
    expect(await orders(id)).toBe(1);
    expect(await store.get(id)).toEqual({ id, body: 'order placed', tags: ['orders'], archived: false, createdAt: 1 });

    const undone = `${schema}-native-undone`;
    await expect(
      client.executor.transaction(async (tx) => {
        await client.insertOrder(tx.native, undone);
        await store.addInTransaction(tx.native, { id: undone, body: 'order placed', tags: ['orders'], createdAt: 1 });
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');
    expect(await orders(undone)).toBe(0);
    expect(await store.get(undone)).toBeNull();
  });

  it("assertReadCommittedTransaction() refuses a transaction that isn't READ COMMITTED, in the store's words, and takes one that is", async () => {
    const words = { operation: 'addInTransaction()', reason: 'its tag counts would miss the notes committed after its snapshot' };
    await expect(client.transaction((tx) => assertReadCommittedTransaction(client.executor.wrapTransaction(tx), words), 'repeatable read')).rejects.toThrow(
      "addInTransaction() needs a READ COMMITTED transaction (PostgreSQL's default); this one is repeatable read: its tag counts would miss the notes committed after its snapshot.",
    );
    await expect(client.transaction((tx) => assertReadCommittedTransaction(client.executor.wrapTransaction(tx), words), 'repeatable read')).rejects.toThrow(TypeError);
    await expect(client.transaction((tx) => assertReadCommittedTransaction(client.executor.wrapTransaction(tx), words))).resolves.toBeUndefined();
    expect(noteSchema.dialect).toBe(client.executor.dialect);
  });
});
