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
})
