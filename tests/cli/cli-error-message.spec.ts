/**
 * `cliErrorMessage()`, what `runStoreCli()` prints for a failure: the error's message, else its errors' (an
 * `AggregateError` of each address a host resolved to), its cause's, its code, or what it is, and never an empty line
 * or a password.
 */
import { cliErrorMessage } from '../../lib/cli/cli-error-message.js';

const NO_MESSAGE = 'The command failed with an error that has no message.';

function refused(address: string, port: number) {
  return Object.assign(new Error(`connect ECONNREFUSED ${address}:${port}`), { code: 'ECONNREFUSED', address, port });
}

describe('cliErrorMessage()', () => {
  it("prints an error's own message", () => {
    expect(cliErrorMessage(new Error('relation "notes" does not exist'))).toBe('relation "notes" does not exist');
    expect(cliErrorMessage(new AggregateError([refused('::1', 5432)], 'all failed'))).toBe('all failed');
  });

  it("prints an AggregateError's errors without a message of its own, one per line and each once", () => {
    const aggregate = Object.assign(new AggregateError([refused('::1', 5432), refused('127.0.0.1', 5432)], ''), { code: 'ECONNREFUSED' });
    expect(cliErrorMessage(aggregate)).toBe('connect ECONNREFUSED ::1:5432\nconnect ECONNREFUSED 127.0.0.1:5432');
    expect(cliErrorMessage(new AggregateError([refused('::1', 5432), refused('::1', 5432)]))).toBe('connect ECONNREFUSED ::1:5432');
    // Errors without messages: their codes, with the address each failed on.
    const bare = new AggregateError([Object.assign(new Error(), { code: 'ECONNREFUSED', address: '::1', port: 5432 }), Object.assign(new Error(), { code: 'ETIMEDOUT' })]);
    expect(cliErrorMessage(bare)).toBe('ECONNREFUSED ::1:5432\nETIMEDOUT');
    // No errors to speak of: the aggregate's own code, else its name.
    expect(cliErrorMessage(Object.assign(new AggregateError([]), { code: 'ECONNREFUSED' }))).toBe('ECONNREFUSED');
    expect(cliErrorMessage(new AggregateError([new Error('')]))).toBe('Error');
  });

  it('follows the cause of an error without a message, and stops at a cycle', () => {
    expect(cliErrorMessage(new Error('', { cause: new Error(' ', { cause: refused('127.0.0.1', 3306) }) }))).toBe('connect ECONNREFUSED 127.0.0.1:3306');
    expect(cliErrorMessage(new Error('', { cause: new AggregateError([refused('::1', 1), refused('127.0.0.1', 1)]) }))).toBe('connect ECONNREFUSED ::1:1\nconnect ECONNREFUSED 127.0.0.1:1');
    // A message of its own wins: the store's errors already end with their cause's.
    expect(cliErrorMessage(new Error('PostgresNoteStore: migrating failed: boom', { cause: new Error('boom') }))).toBe('PostgresNoteStore: migrating failed: boom');

    const first: { message: string; cause?: unknown } = { message: '' };
    first.cause = { message: '', cause: first };
    expect(cliErrorMessage(first)).toBe(NO_MESSAGE);
  });

  it('prints what was thrown when it is not an Error, and never an empty line', () => {
    expect(cliErrorMessage('no route to the database')).toBe('no route to the database');
    expect(cliErrorMessage(42)).toBe('42');
    expect(cliErrorMessage(undefined)).toBe('undefined');
    expect(cliErrorMessage({ code: 'ECONNRESET' })).toBe('ECONNRESET');
    expect(cliErrorMessage({ code: 1045 })).toBe('1045');
    expect(cliErrorMessage(new Error(''))).toBe('Error');

    expect(cliErrorMessage({})).toBe(NO_MESSAGE);
    expect(cliErrorMessage('')).toBe(NO_MESSAGE);
    expect(cliErrorMessage(Object.create(null))).toBe(NO_MESSAGE);
    expect(cliErrorMessage({ toString: () => Symbol('unprintable') })).toBe(NO_MESSAGE);
    expect(cliErrorMessage({ message: 42 })).toBe(NO_MESSAGE);
  });

  it("masks the password of the command's URL, and of any URL with credentials, in what it prints", () => {
    const url = 'postgres://shop:s3cr%40t@db.internal:5432/shop';
    expect(cliErrorMessage(new Error(`could not connect to ${url}`), url)).toBe('could not connect to postgres://shop:***@db.internal:5432/shop');
    expect(cliErrorMessage(new Error('bad url mysql://root:hunter2@10.0.0.5/app?ssl=x'))).toBe('bad url mysql://root:***@10.0.0.5/app?ssl=x');
    expect(cliErrorMessage(new AggregateError([new Error('postgresql://a:b@h/d refused')]))).toBe('postgresql://a:***@h/d refused');
    // Only the fields it reads: Node's ERR_INVALID_URL keeps the whole URL in `input`.
    expect(cliErrorMessage(Object.assign(new TypeError('Invalid URL'), { code: 'ERR_INVALID_URL', input: url }), url)).toBe('Invalid URL');
    // A URL without a password, and text that merely looks like one, are left as they are.
    expect(cliErrorMessage(new Error('postgres://shop@db.internal/shop: refused'), 'postgres://shop@db.internal/shop')).toBe('postgres://shop@db.internal/shop: refused');
    expect(cliErrorMessage(new Error('expected key: value@2'))).toBe('expected key: value@2');
  });
});
