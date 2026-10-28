import type { SqlTransaction } from '../../interfaces/sql-executor.interface.js';

/** What `runTransactionWork()` takes besides the work and its transaction. */
export interface TransactionWorkOptions {
  /**
   * The executor reads its COMMIT's reply, which says whether PostgreSQL committed (`COMMIT`) or had ended the
   * transaction (`ROLLBACK`): node-postgres's command tag. Then the work's reading `native` costs nothing here; the
   * executor rejects at that reply instead (`transactionEnded()`).
   */
  commitReplyTells?: boolean;
}

/**
 * Runs a store's `work` on the transaction an executor opened for it, and rejects rather than let the executor commit
 * a transaction PostgreSQL has ended: PostgreSQL fails a transaction at its first failed statement, and answers its
 * COMMIT with a ROLLBACK, no error. So `work` that caught a statement's error and resolved would look committed while
 * nothing of it was. A rejection makes the executor roll back.
 *
 * `work` gets the transaction's `query()` and `execute()`, whose statements it watches, and its `native` (the client's
 * own transaction object), whose statements it can't see: a getter notes that the work read it. Once the work
 * resolved, one statement more (`SELECT 1`) tells a transaction PostgreSQL ended, for work whose last watched statement
 * failed, and for work that read `native`, whatever its statements through it did. The first is rare: a statement the
 * driver failed before sending it leaves the transaction as it was, and `ROLLBACK TO SAVEPOINT` after a failure is a
 * statement that didn't fail. The second is a transaction's price for its native, one statement at its end, unless
 * the executor learns it from its COMMIT's reply (`commitReplyTells`). Work that did neither costs nothing more.
 */
export async function runTransactionWork<T>(
  work: (transaction: SqlTransaction) => Promise<T>,
  transaction: SqlTransaction,
  options: TransactionWorkOptions = {},
): Promise<T> {
  let failure: { error: unknown } | undefined;
  let nativeRead = false;
  const watch = <R>(statement: Promise<R>): Promise<R> =>
    statement.then(
      (value) => {
        failure = undefined;
        return value;
      },
      (error: unknown) => {
        failure = { error };
        throw error;
      },
    );

  const execute = transaction.execute?.bind(transaction);
  const handed: SqlTransaction = {
    query: <R extends object>(text: string, params?: readonly unknown[]) => watch(transaction.query<R>(text, params)),
    ...(execute ? { execute: (text: string, params?: readonly unknown[]) => watch(execute(text, params)) } : {}),
  };
  // A getter, not a copy: reading it is what tells the transaction's end that statements may have gone around the
  // watch. Set only where the executor has one, as `SqlTransaction.native` is optional.
  const { native } = transaction;
  if (native !== undefined) {
    Object.defineProperty(handed, 'native', {
      enumerable: true,
      get: () => {
        nativeRead = true;
        return native;
      },
    });
  }

  const result = await work(handed);
  if (failure || (nativeRead && !options.commitReplyTells)) {
    const probe = await transaction.query('SELECT 1').then(
      () => undefined,
      (error: unknown) => ({ error }),
    );
    if (probe && failure) {
      throw transactionEnded(failure.error);
    }
    if (probe) {
      // Work that read its native: PostgreSQL's 25P02 says a statement ended the transaction; anything else (a lost
      // connection) rejects with its own error.
      throw failedTransaction(probe.error) ? transactionEnded(probe.error) : probe.error;
    }
  }
  return result;
}

/**
 * Whether `error`, or one of its causes, is PostgreSQL's 25P02 ("current transaction is aborted, commands ignored until
 * end of transaction block"), as each client carries it: its code (node-postgres, PGlite, Kysely), a TypeORM error's
 * `driverError`, or only its words (Prisma's).
 */
function failedTransaction(error: unknown): boolean {
  for (let current = error, depth = 0; current !== null && typeof current === 'object' && depth < 5; current = (current as { cause?: unknown }).cause, depth++) {
    const { code, message, driverError } = current as { code?: unknown; message?: unknown; driverError?: { code?: unknown } };
    if (code === '25P02' || driverError?.code === '25P02' || (typeof message === 'string' && message.includes('current transaction is aborted'))) {
      return true;
    }
  }
  return false;
}

/**
 * The rejection of work whose transaction PostgreSQL ended at a failed statement the work caught: its `cause` is that
 * statement's error when the work runner watched it, else the error of the statement that found the transaction ended
 * (25P02, "current transaction is aborted"), else none (a COMMIT answered with a ROLLBACK).
 */
export function transactionEnded(cause?: unknown): Error {
  return new Error(
    "The transaction was rolled back: one of its statements failed and its error was caught, but PostgreSQL ends a transaction at its first failed statement. Let the statement's error reject the work, or run the statement after a SAVEPOINT and ROLLBACK TO SAVEPOINT when it fails.",
    cause === undefined ? undefined : { cause },
  );
}
