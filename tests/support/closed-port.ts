/**
 * A port nothing listens on at `localhost`, for the command line's report of a database it cannot reach: it runs
 * anywhere, as no server is needed. Where `localhost` is both `::1` and `127.0.0.1` (macOS), a client tries each (Node's
 * happy eyeballs) and fails with an `AggregateError` of both, whose own message is empty; where it isn't (no IPv6, or a
 * hosts file that names `127.0.0.1` alone), `dualStackReason` says why, and the tests of that shape skip with it.
 */
import { lookup } from 'node:dns/promises';
import { createServer, type AddressInfo, type Server } from 'node:net';

export interface ClosedLocalhostPort {
  port: number;
  /** Why the port's connections don't fail on both `::1` and `127.0.0.1` here; `undefined` where they do. */
  dualStackReason?: string;
}

export async function closedLocalhostPort(): Promise<ClosedLocalhostPort> {
  const addresses = (await lookup('localhost', { all: true })).map((entry) => entry.address);
  const ipv4 = await listen(0, '127.0.0.1');
  const { port } = ipv4.address() as AddressInfo;
  // The same port on ::1, held while it's checked, so neither address has a listener when the test connects.
  const ipv6 = addresses.includes('::1') ? await listen(port, '::1').catch((error: Error) => error) : undefined;
  await close(ipv4);
  if (ipv6 && !(ipv6 instanceof Error)) {
    await close(ipv6);
  }

  const dualStackReason =
    !addresses.includes('::1') || !addresses.includes('127.0.0.1')
      ? `localhost is ${addresses.join(' and ')} here, not both ::1 and 127.0.0.1`
      : ipv6 instanceof Error
        ? `::1 cannot be listened on here: ${ipv6.message}`
        : undefined;
  return { port, dualStackReason };
}

function listen(port: number, host: string): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(port, host, () => resolve(server));
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}
