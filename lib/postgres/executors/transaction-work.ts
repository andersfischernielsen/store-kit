import type { SqlTransaction } from '../../interfaces/sql-executor.interface.js';

/**
 * Runs a store's `work` on the transaction an executor opened for it, and rejects rather than let the executor commit
 * a transaction PostgreSQL has ended: PostgreSQL fails a transaction at its first failed statement, and answers its
 * COMMIT with a ROLLBACK, no error. So `work` that caught a statement's error and resolved would look committed while
 * nothing of it was. A rejection makes the executor roll back.
 *
 * Costs nothing on the way to a commit: only `work` whose last statement failed gets one statement more, which tells a
 * transaction PostgreSQL ended from a statement the driver failed before sending it (that leaves the transaction as it
 * was) and from a `ROLLBACK TO SAVEPOINT` that followed a failure (the last statement didn't fail).
 */
export async function runTransactionWork<T>(work: (transaction: SqlTransaction) => Promise<T>, transaction: SqlTransaction): Promise<T> {
  let failure: { error: unknown } | undefined;
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
  const result = await work({
    query: <R extends object>(text: string, params?: readonly unknown[]) => watch(transaction.query<R>(text, params)),
    ...(execute ? { execute: (text: string, params?: readonly unknown[]) => watch(execute(text, params)) } : {}),
  });

  if (failure && (await transaction.query('SELECT 1').then(() => false, () => true))) {
    throw new Error(
      "The transaction was rolled back: one of its statements failed and its error was caught, but PostgreSQL ends a transaction at its first failed statement. Let the statement's error reject the work, or run the statement after a SAVEPOINT and ROLLBACK TO SAVEPOINT when it fails.",
      { cause: failure.error },
    );
  }
  return result;
}
