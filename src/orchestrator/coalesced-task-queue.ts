export type CoalescedTaskQueueOptions<TKey> = {
  delayMs: number
  run: (key: TKey) => Promise<void>
  onError?: (error: unknown, key: TKey) => void
}

/**
 * Coalesces repeated wake requests for a key into one task turn. The durable
 * payload remains the caller's responsibility, so stopping this in-process
 * scheduler never loses work and a replacement process can schedule it again.
 */
export class CoalescedTaskQueue<TKey> {
  readonly #delayMs: number
  readonly #run: (key: TKey) => Promise<void>
  readonly #onError?: (error: unknown, key: TKey) => void
  readonly #timers = new Map<TKey, ReturnType<typeof setTimeout>>()
  readonly #inFlight = new Set<Promise<void>>()
  #stopped = false

  constructor(options: CoalescedTaskQueueOptions<TKey>) {
    this.#delayMs = Math.max(0, Math.floor(options.delayMs))
    this.#run = options.run
    this.#onError = options.onError
  }

  schedule(key: TKey, delayMs = this.#delayMs): void {
    if (this.#stopped || this.#timers.has(key)) return
    const timer = setTimeout(() => {
      this.#timers.delete(key)
      if (this.#stopped) return
      const turn = Promise.resolve()
        .then(() => this.#run(key))
        .catch((error: unknown) => {
          try {
            this.#onError?.(error, key)
          } catch {
            // Observability must not turn a handled task failure into an
            // unhandled rejection from the scheduler.
          }
        })
        .finally(() => this.#inFlight.delete(turn))
      this.#inFlight.add(turn)
    }, Math.max(0, Math.floor(delayMs)))
    timer.unref?.()
    this.#timers.set(key, timer)
  }

  async stop(): Promise<void> {
    this.#stopped = true
    for (const timer of this.#timers.values()) clearTimeout(timer)
    this.#timers.clear()
    while (this.#inFlight.size > 0) {
      await Promise.allSettled([...this.#inFlight])
    }
  }
}
