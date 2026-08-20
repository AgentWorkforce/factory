import { describe, expect, it } from 'vitest'

import { runFleetCli } from './fleet'

const BASE = 'https://factory.example.com'
const NOW_MS = 1_787_224_000_000

const buffer = () => {
  let value = ''
  return {
    write(chunk: string) {
      value += chunk
      return true
    },
    text() {
      return value
    },
  }
}

interface StubRoutes {
  healthz?: { status: number; body: unknown }
  evidence?: { status: number; body: unknown }
}

const stubFetch = (routes: StubRoutes, seen: string[] = []): typeof fetch =>
  (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url
    const authorization = new Headers(init?.headers ?? {}).get('authorization')
    seen.push(`${url}${authorization ? ` auth=${authorization}` : ''}`)
    const route = url.endsWith('/evidence') ? routes.evidence : routes.healthz
    if (!route) throw Object.assign(new Error('fetch failed'), { name: 'TypeError' })
    return new Response(JSON.stringify(route.body), {
      status: route.status,
      headers: { 'content-type': 'application/json' },
    })
  }) as typeof fetch

const healthy = {
  ok: true,
  phase: 'running',
  factoryProcess: 'running',
  health: {
    schemaVersion: 1,
    ok: true,
    status: 'ok',
    stale: false,
    updatedAtMs: NOW_MS,
    ageMs: 12_000,
    loopStatus: 'running',
    degradedSubsystems: [],
    readinessReconcile: {
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      intervalMs: 60_000,
      lastStartedAtMs: NOW_MS - 30_000,
      lastCompletedAtMs: NOW_MS - 29_000,
    },
    eventListener: { state: 'subscribed' },
  },
}

describe('factory diagnose --deployed (#295)', () => {
  it('reports a healthy deployed instance and exits zero without any credential', async () => {
    const seen: string[] = []
    const out = buffer()
    const err = buffer()

    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: err,
      diagnoseFetch: stubFetch({ healthz: { status: 200, body: healthy } }, seen),
    })

    expect(code).toBe(0)
    expect(seen).toEqual([`${BASE}/healthz`])
    expect(out.text()).toContain('dispatching')
    expect(out.text()).toContain('readinessReconcile')
  })

  // The 2026-08-19/20 outage: eight consecutive failures behind `ok: true`.
  it('names the failing subsystem, its failure count and its error class', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            health: {
              schemaVersion: 1,
              ok: true,
              status: 'degraded',
              stale: false,
              loopStatus: 'running',
              degradedSubsystems: ['readinessReconcile'],
              readinessReconcile: {
                state: 'degraded',
                consecutiveFailures: 8,
                failureThreshold: 3,
                intervalMs: 60_000,
                lastErrorClass: 'DispatchLifecycleError',
              },
            },
          },
        },
      }),
    })

    // A lane briefed to "find out why production is not dispatching" must get
    // a non-zero exit and a named subsystem out of one command.
    expect(code).not.toBe(0)
    const text = out.text()
    expect(text).toContain('readinessReconcile')
    expect(text).toContain('8')
    expect(text).toContain('DispatchLifecycleError')
    expect(text).toContain('not dispatching')
  })

  // The 2026-08-20 case: every settled field green, one pass wedged for 77m.
  it('calls out a stalled sweep and how many passes it has missed', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            health: {
              schemaVersion: 1,
              ok: true,
              status: 'degraded',
              stale: false,
              loopStatus: 'running',
              degradedSubsystems: ['readinessReconcile'],
              readinessReconcile: {
                state: 'stalled',
                consecutiveFailures: 0,
                failureThreshold: 3,
                intervalMs: 60_000,
                inFlightMs: 4_620_000,
                missedPasses: 77,
                lastStartedAtMs: NOW_MS,
                lastCompletedAtMs: NOW_MS - 60_003,
              },
            },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as {
      dispatching: boolean
      verdict: string
      health?: { readinessReconcile?: { state?: string; missedPasses?: number } }
    }
    expect(report.dispatching).toBe(false)
    expect(report.health?.readinessReconcile).toMatchObject({ state: 'stalled', missedPasses: 77 })
    expect(report.verdict).toContain('stalled')
  })

  // The deployed container serves the block inside its heartbeat projection.
  it('reads the health block where the container actually serves it', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            heartbeat: {
              status: 'running',
              readinessReconcile: 'healthy',
              eventListener: 'subscribed',
              health: healthy.health,
            },
          },
        },
      }),
    })

    expect(code).toBe(0)
    const report = JSON.parse(out.text()) as { dispatching: boolean; health?: { status?: string } }
    expect(report.dispatching).toBe(true)
    expect(report.health?.status).toBe('ok')
  })

  it('says what is missing when the instance predates the diagnostics block', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            heartbeat: { status: 'running', readinessReconcile: 'degraded', eventListener: 'subscribed' },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    expect(out.text()).toContain('state strings only')
    expect(out.text()).toContain('degraded')
  })

  it('reads the gated evidence surface when an operator token is supplied', async () => {
    const seen: string[] = []
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--token', 'op-token', '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: { status: 200, body: healthy },
        evidence: {
          status: 200,
          body: {
            phase: 'running',
            heartbeat: { status: 'running' },
            readinessReconcile: {
              state: 'degraded',
              consecutiveFailures: 8,
              lastError: 'Refusing to dispatch AR-241: dispatch lifecycle is already terminal',
            },
          },
        },
      }, seen),
    })

    expect(code).toBe(0)
    expect(seen).toEqual([`${BASE}/healthz`, `${BASE}/evidence auth=Bearer op-token`])
    const report = JSON.parse(out.text()) as { evidence?: { fetched?: boolean; lastError?: string } }
    expect(report.evidence?.fetched).toBe(true)
    expect(report.evidence?.lastError).toContain('dispatch lifecycle is already terminal')
  })

  it('still diagnoses when the evidence token is rejected', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--token', 'stale-token', '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: { status: 200, body: healthy },
        evidence: { status: 401, body: { error: 'unauthorized' } },
      }),
    })

    expect(code).toBe(0)
    const report = JSON.parse(out.text()) as { evidence?: { fetched?: boolean; reason?: string } }
    expect(report.evidence?.fetched).toBe(false)
    expect(report.evidence?.reason).toContain('401')
  })

  it('reports an unreachable instance as a failure rather than silence', async () => {
    const out = buffer()
    const err = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: err,
      diagnoseFetch: (async () => {
        throw Object.assign(new Error('connect ECONNREFUSED 10.0.0.1:443'), { name: 'TypeError' })
      }) as typeof fetch,
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { reachable: boolean; errorClass?: string }
    expect(report.reachable).toBe(false)
    // Class only: an error message from a private-networked host names hosts
    // and paths the report does not need.
    expect(report.errorClass).toBe('TypeError')
  })

  it('requires the target url', async () => {
    const err = buffer()
    const code = await runFleetCli(['diagnose'], { stdout: buffer(), stderr: err })

    expect(code).not.toBe(0)
    expect(err.text()).toContain('--deployed')
  })

  it('is listed in help so a lane brief can name it', async () => {
    const out = buffer()
    await runFleetCli(['--help'], { stdout: out, stderr: buffer() })

    expect(out.text()).toContain('diagnose --deployed <url>')
  })
  // Review follow-up on #300 (P1, codex). The daemon stamps the block at write
  // time, so its `ageMs` is 0 and `stale` false *in the file*. If the daemon
  // dies and the container keeps serving that file, believing the embedded
  // snapshot reports green forever — the exact failure this command exists to
  // catch. The container computes liveness against its own clock; that verdict
  // wins.
  it('believes the container liveness verdict over a frozen health snapshot', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 503,
          body: {
            // The container's own staleness check, against its own clock.
            ok: false,
            phase: 'running',
            factoryProcess: 'running',
            heartbeat: {
              status: 'running',
              updatedAtMs: NOW_MS - 3_600_000,
              readinessReconcile: 'healthy',
              health: healthy.health,
            },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { dispatching: boolean; verdict: string }
    expect(report.dispatching).toBe(false)
    expect(report.verdict).toMatch(/heartbeat|liveness|not alive/iu)
  })

  // Review follow-up on #300 (P2, codex). `new Date(1e300).toISOString()`
  // throws, and a remote instance chooses these numbers.
  it('renders an out-of-range remote timestamp as unknown instead of aborting', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            health: {
              ...healthy.health,
              readinessReconcile: {
                ...healthy.health.readinessReconcile,
                lastStartedAtMs: 1e300,
                lastCompletedAtMs: 1e300,
              },
            },
          },
        },
      }),
    })

    expect(code).toBe(0)
    const text = out.text()
    expect(text).toContain('readinessReconcile')
    expect(text).not.toContain('Invalid time value')
  })
  // Review follow-up on #300 (P1, cubic). A block with no readiness subsystem
  // in it says nothing about dispatch; "no degraded subsystem listed" is not
  // the same statement as "the sweep is healthy".
  it('refuses to call an incomplete health block dispatching', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            health: {
              schemaVersion: 1,
              ok: true,
              status: 'ok',
              stale: false,
              loopStatus: 'running',
              degradedSubsystems: [],
            },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { dispatching: boolean; verdict: string }
    expect(report.dispatching).toBe(false)
    expect(report.verdict).toMatch(/cannot tell|no readiness/iu)
  })

  // Review follow-up on factory-cloud#40 (P2, codex). In event-driven
  // short-sleep mode the Worker answers /healthz itself and never probes the
  // container, deliberately, so anonymous polling cannot defeat scale-to-zero.
  // That response is Worker liveness — reading it as "Factory is dispatching"
  // is exactly the false green this command exists to prevent.
  it('does not read a worker-only short-sleep response as a dispatching Factory', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: { ok: true, phase: 'worker-ready', container: 'not-probed', eventDrivenSleep: true },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { dispatching: boolean; verdict: string }
    expect(report.dispatching).toBe(false)
    expect(report.verdict).toMatch(/short-sleep|not probed|worker/iu)
    expect(report.verdict).toContain('/evidence')
  })

  // Review follow-up on #300 (P2, cubic). A hermetic env must be honoured, or
  // a test — or an embedder — silently skips the authenticated read.
  it('takes the evidence token from the injected environment', async () => {
    const seen: string[] = []
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: buffer(),
      stderr: buffer(),
      env: { FACTORY_EVIDENCE_TOKEN: 'env-token' } as NodeJS.ProcessEnv,
      diagnoseFetch: stubFetch({
        healthz: { status: 200, body: healthy },
        evidence: { status: 200, body: { phase: 'running' } },
      }, seen),
    })

    expect(code).toBe(0)
    expect(seen).toEqual([`${BASE}/healthz`, `${BASE}/evidence auth=Bearer env-token`])
  })
  // A container in `preflight` also answers ok:false, and "the Factory process
  // is gone" is the wrong thing to tell someone whose instance is three
  // minutes into a boot. The phase is right there in the response.
  it('distinguishes a booting instance from a wedged one', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 503,
          body: { ok: false, phase: 'preflight', factoryProcess: 'not-running' },
        },
      }),
    })

    expect(code).not.toBe(0)
    const text = out.text()
    expect(text).toContain('preflight')
    expect(text).toMatch(/still starting|booting/iu)
    expect(text).not.toContain('the Factory process is gone')
  })
  // Found while running the built CLI against a short-sleep stub: the report
  // said "instance predates #295" about an instance whose age it cannot know,
  // and printed `phase` twice. Sending an operator to "upgrade the deployed
  // Factory" when the real answer is "the Worker never asked the container" is
  // the wrong-problem failure this command exists to prevent.
  it('does not blame the instance version when the Worker simply did not probe it', async () => {
    const out = buffer()
    await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: { ok: true, phase: 'worker-ready', container: 'not-probed', eventDrivenSleep: true },
        },
      }),
    })

    const text = out.text()
    expect(text).not.toContain('predates')
    expect(text).toMatch(/short-sleep/iu)
    expect(text.match(/^ +phase +:/gmu)?.length ?? 0).toBe(1)
  })

  it('prints the phase once when the instance predates the block', async () => {
    const out = buffer()
    await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            heartbeat: { status: 'running', readinessReconcile: 'degraded', eventListener: 'subscribed' },
          },
        },
      }),
    })

    const text = out.text()
    expect(text).toContain('predates')
    expect(text.match(/^ +phase +:/gmu)?.length ?? 0).toBe(1)
  })
})

