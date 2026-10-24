/** How deep the message is looked for in `errors` and `cause`: a cycle, or a pathological chain, ends there. */
const MAX_DEPTH = 8;

/**
 * What the command line prints for a failure, never empty: the error's own message; else, for an error without one
 * (Node's `AggregateError` of a host whose every address refused the connection, as `localhost`'s `::1` and
 * `127.0.0.1` do), its errors' messages, one per line and each once; else its `cause`'s; else its `code` (with the
 * address it failed on); else what `String()` makes of it. Only those fields are read: an error's other fields can hold
 * the URL (Node's `ERR_INVALID_URL` keeps it in `input`). A URL with a password in the text, `url` above all, has the
 * password masked.
 */
export function cliErrorMessage(error: unknown, url?: string): string {
  const messages = [...new Set(messagesOf(error, 0))];
  return redact(messages.length > 0 ? messages.join('\n') : 'The command failed with an error that has no message.', url);
}

function messagesOf(error: unknown, depth: number): string[] {
  if (typeof error !== 'object' || error === null) {
    const text = stringOf(error);
    return text ? [text] : [];
  }

  const { message, errors, cause, code, address, port } = error as Partial<Record<'message' | 'errors' | 'cause' | 'code' | 'address' | 'port', unknown>>;
  if (typeof message === 'string' && message.trim() !== '') {
    return [message];
  }
  if (depth < MAX_DEPTH && Array.isArray(errors)) {
    const nested = errors.flatMap((each) => messagesOf(each, depth + 1));
    if (nested.length > 0) {
      return nested;
    }
  }
  if (depth < MAX_DEPTH && cause !== undefined && cause !== error) {
    const nested = messagesOf(cause, depth + 1);
    if (nested.length > 0) {
      return nested;
    }
  }
  if ((typeof code === 'string' && code !== '') || typeof code === 'number') {
    return [typeof address === 'string' && address !== '' ? `${code} ${address}${typeof port === 'number' ? `:${port}` : ''}` : String(code)];
  }

  // An `Error` without a message is its name ("AggregateError"); a plain object is "[object Object]", which says nothing.
  const text = stringOf(error);
  return text && text !== '[object Object]' ? [text] : [];
}

/** `String(value)`, or `''` for a value whose `toString()` throws or that has none (`Object.create(null)`). */
function stringOf(value: unknown): string {
  try {
    return String(value).trim();
  } catch {
    return '';
  }
}

/** `text` with the password of `url`, and of any URL in it with credentials, masked. */
function redact(text: string, url: string | undefined): string {
  let redacted = text;
  if (url) {
    const masked = maskPassword(url);
    if (masked !== url) {
      redacted = redacted.replaceAll(url, masked);
    }
  }
  // scheme://user:password@ -> scheme://user:***@
  return redacted.replace(/\b([a-z][a-z\d+.-]*:\/\/[^\s/?#@:]*:)[^\s/?#@]+@/gi, '$1***@');
}

function maskPassword(url: string): string {
  try {
    const parsed = new URL(url);
    if (!parsed.password) {
      return url;
    }
    parsed.password = '***';
    return parsed.href;
  } catch {
    return url;
  }
}
