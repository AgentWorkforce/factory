import { describe, expect, it, vi } from 'vitest'

import { CoalescedTaskQueue } from './coalesced-task-queue'

describe('CoalescedTaskQueue', () => {
  it('folds repeated schedules for one key into one task while retaining other keys', async () => {
    vi.useFakeTimers()
    try {
      const turns: string[] = []
      const queue = new CoalescedTaskQueue<string>({
        delayMs: 750,
        run: async (key) => { turns.push(key) },
      })

      queue.schedule('thread-1')
      queue.schedule('thread-1')
      queue.schedule('thread-2')
      await vi.advanceTimersByTimeAsync(750)

      expect(turns.sort()).toEqual(['thread-1', 'thread-2'])
      await queue.stop()
    } finally {
      vi.useRealTimers()
    }
  })

  it('never overlaps work for one key and retains one deferred turn', async () => {
    vi.useFakeTimers()
    try {
      let release!: () => void
      const first = new Promise<void>((resolve) => { release = resolve })
      let active = 0
      let maxActive = 0
      let calls = 0
      const queue = new CoalescedTaskQueue<string>({
        delayMs: 10,
        run: async () => {
          calls += 1
          active += 1
          maxActive = Math.max(maxActive, active)
          if (calls === 1) await first
          active -= 1
        },
      })

      queue.schedule('thread-1')
      await vi.advanceTimersByTimeAsync(10)
      queue.schedule('thread-1')
      await vi.advanceTimersByTimeAsync(10)
      expect(calls).toBe(1)

      release()
      await Promise.resolve()
      await vi.advanceTimersByTimeAsync(10)
      expect(calls).toBe(2)
      expect(maxActive).toBe(1)
      await queue.stop()
    } finally {
      vi.useRealTimers()
    }
  })

  it('cancels queued work and drains an active key', async () => {
    vi.useFakeTimers()
    try {
      let release!: () => void
      const blocked = new Promise<void>((resolve) => { release = resolve })
      let calls = 0
      const queue = new CoalescedTaskQueue<string>({
        delayMs: 0,
        run: async () => {
          calls += 1
          await blocked
        },
      })
      queue.schedule('thread-1')
      await vi.advanceTimersByTimeAsync(0)
      queue.schedule('thread-1')
      const cancelled = queue.cancel('thread-1')
      release()
      await cancelled
      await vi.runAllTimersAsync()
      expect(calls).toBe(1)
      await queue.stop()
    } finally {
      vi.useRealTimers()
    }
  })
})
