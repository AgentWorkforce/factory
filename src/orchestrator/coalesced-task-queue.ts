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
  readonly #inFlightByKey = new Map<TKey, Promise<void>>()
  readonly #deferred = new Map<TKey, number>()
  #stopped = false

  constructor(options: CoalescedTaskQueueOptions<TKey>) {
    this.#delayMs = Math.max(0, Math.floor(options.delayMs))
    this.#run = options.run
    this.#onError = options.onError
  }

  schedule(key: TKey, delayMs = this.#delayMs): void {
    if (this.#stopped) return
    const delay = Math.max(0, Math.floor(delayMs))
    if (this.#inFlightByKey.has(key)) {
      this.#deferred.set(key, Math.min(this.#deferred.get(key) ?? delay, delay))
      return
    }
    const existing = this.#timers.get(key)
    if (existing) clearTimeout(existing)
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
        .finally(() => {
          this.#inFlight.delete(turn)
          this.#inFlightByKey.delete(key)
          const deferredDelay = this.#deferred.get(key)
          this.#deferred.delete(key)
          if (deferredDelay !== undefined && !this.#stopped) this.schedule(key, deferredDelay)
        })
      this.#inFlight.add(turn)
      this.#inFlightByKey.set(key, turn)
    }, delay)
    timer.unref?.()
    this.#timers.set(key, timer)
  }

  /** Stop queued work for one key and wait until its active turn is fenced. */
  async cancel(key: TKey): Promise<void> {
    const timer = this.#timers.get(key)
    if (timer) clearTimeout(timer)
    this.#timers.delete(key)
    this.#deferred.delete(key)
    await this.#inFlightByKey.get(key)
    const rescheduled = this.#timers.get(key)
    if (rescheduled) clearTimeout(rescheduled)
    this.#timers.delete(key)
    this.#deferred.delete(key)
  }

  async stop(): Promise<void> {
    this.#stopped = true
    for (const timer of this.#timers.values()) clearTimeout(timer)
    this.#timers.clear()
    this.#deferred.clear()
    while (this.#inFlight.size > 0) {
      await Promise.allSettled([...this.#inFlight])
    }
  }
}
