import { describe, expect, it, vi } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

/**
 * Review follow-up on #300 (Major, CodeRabbit).
 *
 * The health projection is new code on the heartbeat write path, and the
 * heartbeat is what the crash reaper and `/healthz` read to decide whether the
 * daemon is alive. If the projection ever throws, an unguarded call would fail
 * every heartbeat write and make a healthy daemon look wedged — the diagnostic
 * causing the outage it exists to explain.
 *
 * Nothing in `publicHealthFromHeartbeat` throws today; this pins the guard, not
 * the absence of a throw. The module is mocked to throw, which is the only
 * honest way to test a defence against a condition the code does not currently
 * produce.
 */
vi.mock('./public-health', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./public-health')>()
  return {
    ...actual,
    publicHealthFromHeartbeat: () => {
      throw new TypeError('projection exploded')
    },
    derivedReadinessReconcileState: () => {
      throw new TypeError('derivation exploded')
    },
  }
})

const { createFactory, readFactoryLoopHeartbeat } = await import('./factory')
const { FakeFleetClient, FakeMountClient } = await import('../testing')
const { FactoryConfigSchema } = await import('../config/schema')

describe('health projection failures never break the heartbeat (#295)', () => {
  it('still writes a heartbeat, and still reports status, when the projection throws', async () => {
    const root = await mkdtemp(join(tmpdir(), 'factory-health-guard-'))
    const heartbeatPath = join(root, 'heartbeat.json')
    const mount = new FakeMountClient()
    mount.setSubRoot('/linear/issues', 'absent')
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }
    const factory = createFactory(
      FactoryConfigSchema.parse({
        workspaceId: 'factory-health-guard',
        issueSource: 'github',
        repos: {
          byLabel: { pear: 'AgentWorkforce/pear' },
          clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
          default: 'AgentWorkforce/pear',
        },
        stateIds: {
          readyForAgent: 'state-ready-for-agent',
          agentImplementing: 'state-agent-implementing',
          done: 'state-done',
          inPlanning: 'state-in-planning',
          humanReview: 'state-human-review',
        },
        loop: { heartbeatPath, registryPath: join(root, 'registry.json'), heartbeatStaleMs: 1_000 },
      }),
      { mount, fleet: new FakeFleetClient(), logger },
    )

    await factory.start({
      mode: 'live',
      liveSubscription: { transport: 'subscribe', reconcileIntervalMs: 50 },
    })
    try {
      // The liveness contract the reaper depends on: the file exists and its
      // timestamp advances, projection or no projection.
      await vi.waitFor(async () => {
        const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
        expect(heartbeat?.pid).toBe(process.pid)
        expect(heartbeat?.updatedAtMs).toEqual(expect.any(Number))
      }, { timeout: 3_000 })

      const heartbeat = await readFactoryLoopHeartbeat(heartbeatPath)
      // The block is omitted rather than half-written...
      expect(heartbeat?.health).toBeUndefined()
      // ...and the authenticated detail an operator reads is untouched.
      expect(heartbeat?.readinessReconcile?.state).toEqual(expect.any(String))

      // status() must survive the same failure: a CLI asking a wedged daemon
      // what is wrong should not get an exception instead of an answer.
      expect(() => factory.status()).not.toThrow()
      expect(factory.status().readinessReconcile?.state).toEqual(expect.any(String))

      // The failure is not swallowed silently — it is logged where an operator
      // reading logs will find it.
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('health'),
        expect.anything(),
      )
    } finally {
      await factory.stop()
      await rm(root, { recursive: true, force: true })
    }
  })
})
