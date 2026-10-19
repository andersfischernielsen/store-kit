import type { SqlExecutor } from '../../interfaces/sql-executor.interface.js';
import { fromPg } from '../executors/pg.executor.js';

/**
 * A pool of one connection on `url`, for the command line's `migrate` and `status`; `null` without the `pg` package,
 * which comes from the application's dependencies.
 */
export async function openPostgres(url: string): Promise<{ executor: SqlExecutor; close(): Promise<void> } | null> {
  let Pool: typeof import('pg').Pool;
  try {
    ({ Pool } = (await import('pg')).default);
  } catch {
    return null;
  }

  const pool = new Pool({ connectionString: url, max: 1 });
  // The server ending the idle connection (a restart, or a FATAL as `close()` ends it) would end the process with an
  // uncaught error, after a command that succeeded too. The pool drops that connection, and a statement that needed it
  // fails with its own error, which the command reports.
  pool.on('error', () => undefined);
  return { executor: fromPg(pool), close: () => pool.end() };
}
