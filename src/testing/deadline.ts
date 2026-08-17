/**
 * Fail a test that hangs, without leaving the guard timer armed.
 *
 * An uncleared guard outlives the race it guards: it rejects a promise nobody
 * is observing any more, and holds the event loop open until it fires. A guard
 * exists to tell a hung subject from a healthy one, so a guard that can fail
 * for its own reasons defeats its own purpose — a broken harness then looks
 * exactly like a broken subject.
 */
export async function withDeadline<T>(work: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), ms) }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}
