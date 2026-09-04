import { describe, expect, it } from 'vitest'

import { formatSweepOutcome } from './diagnose'
import { runFleetCli } from './fleet'

const BASE = 'https://factory.example.com'
// An ambient FACTORY_EVIDENCE_TOKEN on a developer machine or CI runner would
// otherwise make these commands request /evidence as well, which the stubs do
// not route (#300 review, CodeRabbit).
const HERMETIC_ENV = {} as NodeJS.ProcessEnv
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
    fleetConnect: {
      state: 'connected',
      attempts: 1,
      lastAttemptAtMs: NOW_MS - 32_000,
      lastDialedAtMs: NOW_MS - 31_000,
      firstEventAtMs: NOW_MS - 30_500,
      lastConnectedAtMs: NOW_MS - 30_000,
    },
    eventListener: { state: 'subscribed' },
  },
}

describe('formatSweepOutcome (#359)', () => {
  it('attributes a count-free deferral only to the latest pass', () => {
    const outcome = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      discoveryDeferred: 'sweep-in-flight',
    })

    expect(outcome).toBe(
      'nothing has enumerated successfully yet — the most recent pass deferred ' +
      'to another process holding the discovery lease',
    )
    expect(outcome).not.toContain('every pass')
  })

  it('does not present an older daemon\'s unstamped deferred zeroes as a measurement', () => {
    const outcome = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      discoveryDeferred: 'sweep-in-flight',
    })

    expect(outcome).toBe(
      'not attributable (legacy deferred report has counts without an enumeration timestamp; ' +
      'the most recent pass deferred to another process holding the discovery lease)',
    )
    expect(outcome).not.toContain('candidate(s)')
  })

  it('renders a rejected count snapshot as unknown rather than never enumerated', () => {
    const outcome = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      discoveryDeferred: 'sweep-in-flight',
      enumerationCountsInvalid: true,
    })

    expect(outcome).toBe(
      'not attributable (the report supplied an incomplete or invalid count snapshot; ' +
      'whether an earlier pass enumerated is unknown)',
    )
    expect(outcome).not.toContain('nothing has enumerated successfully yet')
  })

  // MUST-FIRE (#363 review, codex P1). The numbers reaching `/healthz` is only
  // half of getting the signal to the operator: `candidates: 0` with every
  // tree read empty and `candidates: 0` with content served are opposite
  // diagnoses, and this sentence is where a deployed operator meets them. It
  // is also the sentence the verdict line embeds, so the reading travels with
  // the "dispatching" claim it qualifies.
  it('names a silent mount rather than leaving a zero candidate count to speak for itself', () => {
    const outcome = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: 3,
      emptyTreeReads: 3,
    })

    expect(outcome).toContain('0 candidate(s)')
    expect(outcome).toContain('every one of 3 tree read(s) came back empty')
    expect(outcome).toContain('the mount served nothing at all')
  })

  // MUST-NOT-FIRE, and it is the reason this is a pair rather than a count: a
  // healthy sweep lists two path forms per repo and only one exists, so an
  // empty read is ORDINARY. A renderer that shouted on any empty read would
  // shout on every healthy instance, and an operator would learn to ignore it.
  it('stays quiet when the mount served content, and when no read was made', () => {
    const emptyWorkspace = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: 3,
      emptyTreeReads: 1,
    })
    expect(emptyWorkspace).not.toContain('served nothing at all')
    expect(emptyWorkspace).toContain('1/3 tree read(s) empty')

    // An incremental sweep that answered every root from the discovery cache
    // issued no read at all. A ratio over zero reads is not a fact about the
    // mount, so it claims neither direction.
    const noReads = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      candidates: 0,
      dispatched: 0,
      skipped: 0,
      treeReads: 0,
      emptyTreeReads: 0,
    })
    expect(noReads).toBe('0 candidate(s), 0 dispatched, 0 skipped')

    // A producer that predates the pair says nothing about it either.
    const olderProducer = formatSweepOutcome({
      state: 'healthy',
      consecutiveFailures: 0,
      failureThreshold: 3,
      candidates: 0,
      dispatched: 0,
      skipped: 0,
    })
    expect(olderProducer).toBe('0 candidate(s), 0 dispatched, 0 skipped')
  })
})

describe('factory diagnose --deployed reads back the running build (#446)', () => {
  const commit = '9'.repeat(40)

  it('names the version and commit the instance reported', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: { ...healthy, health: { ...healthy.health, build: { version: '0.1.90', commit } } },
        },
      }),
    })

    expect(code).toBe(0)
    // The lookup that replaces the argument: "is the fix I merged running?"
    // answered by reading the build, not by comparing a boot time to a merge.
    expect(out.text()).toContain(`build                : 0.1.90 @ ${commit}`)
  })

  it('distinguishes "too old to tell you" from "it told you unknown"', async () => {
    const render = async (health: Record<string, unknown>) => {
      const out = buffer()
      await runFleetCli(['diagnose', '--deployed', BASE], {
        stdout: out,
        stderr: buffer(),
        env: HERMETIC_ENV,
        diagnoseFetch: stubFetch({ healthz: { status: 200, body: { ...healthy, health } } }),
      })
      return out.text()
    }

    // An instance predating this change publishes no `build` at all. Saying so
    // IS the answer — it means the deployed image is older than #446.
    expect(await render({ ...healthy.health })).toContain(
      'build                : not reported (instance predates the build identity field)',
    )

    // An instance that ran an UNSTAMPED artifact answered the question and its
    // answer was "I cannot tell". Different fact, different remedy, so it must
    // not be rendered as the same line.
    expect(await render({ ...healthy.health, build: { version: '0.1.90', commit: 'unknown' } }))
      .toContain('build                : 0.1.90 @ unknown')
  })

  it('never renders a commit the instance did not send', async () => {
    const out = buffer()
    await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          // A remote running an unknown version, or a hostile responder.
          body: { ...healthy, health: { ...healthy.health, build: { version: 12, commit: 'HEAD' } } },
        },
      }),
    })

    expect(out.text()).toContain('build                : unknown @ unknown')
    expect(out.text()).not.toContain('HEAD')
  })
})

describe('factory diagnose --deployed (#295)', () => {
  it('reports a healthy deployed instance and exits zero without any credential', async () => {
    const seen: string[] = []
    const out = buffer()
    const err = buffer()

    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: err,
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({ healthz: { status: 200, body: healthy } }, seen),
    })

    expect(code).toBe(0)
    expect(seen).toEqual([`${BASE}/healthz`])
    expect(out.text()).toContain('dispatching')
    expect(out.text()).toContain('readinessReconcile')
    expect(out.text()).toContain('fleetConnect')
    expect(out.text()).toContain('connected')
    expect(out.text()).toContain(`lastAttemptAt      : ${new Date(NOW_MS - 32_000).toISOString()}`)
    expect(out.text()).toContain(`lastConnectedAt    : ${new Date(NOW_MS - 30_000).toISOString()}`)
  })

  it('renders dialed as unconfirmed until a stream event is observed', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ...healthy,
            health: {
              ...healthy.health,
              fleetConnect: {
                ...healthy.health.fleetConnect,
                state: 'dialed',
                firstEventAtMs: undefined,
                lastConnectedAtMs: undefined,
              },
            },
          },
        },
      }),
    })

    expect(code).toBe(0)
    expect(out.text()).toContain('unconfirmed — the SDK accepted connect()')
    expect(out.text()).toContain('healthy silent workspace may remain dialed')
  })

  // The 2026-08-19/20 outage: eight consecutive failures behind `ok: true`.
  it('names the failing subsystem, its failure count and its error class', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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

  // #303: a wedged batch was the one dispatch-gating condition every surface
  // reported as healthy. `factory diagnose` has to name it, or the operator is
  // back to reading the state document.
  it('names a wedged batch when everything else reads healthy', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
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
              degradedSubsystems: ['dispatchCapacity'],
              readinessReconcile: {
                state: 'healthy',
                consecutiveFailures: 0,
                failureThreshold: 3,
                intervalMs: 60_000,
                lastStartedAtMs: NOW_MS - 30_000,
                lastCompletedAtMs: NOW_MS - 29_000,
              },
              eventListener: { state: 'subscribed' },
              dispatchCapacity: {
                state: 'stalled',
                batchSize: 1,
                active: 1,
                waiting: 7,
                waitWarnMs: 1_800_000,
                agentlessHoldTimeoutMs: 1_800_000,
                longestWaitMs: 46_800_000,
                agentlessOccupants: 1,
              },
            },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { dispatching: boolean; verdict: string }
    expect(report.dispatching).toBe(false)
    expect(report.verdict).toContain('7 issue(s) have been waiting for batch capacity')
    expect(report.verdict).toContain('1/1 slot(s) occupied')
    expect(report.verdict).toContain('never placed an agent and are past the 30m 0s reap deadline')
    // `longestWaitMs` is a queue wait, so the verdict must not present it as
    // how long the slots have been held (#303 review, cubic).
    expect(report.verdict).not.toContain('slot(s) have been occupied for')
  })

  // The deployed container serves the block inside its heartbeat projection.
  it('reads the health block where the container actually serves it', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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

  it('reads and renders fleet socket errors from authenticated evidence', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--token', 'op-token'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ...healthy,
            health: {
              ...healthy.health,
              fleetConnect: {
                state: 'failed',
                attempts: 2,
                lastFailureAtMs: NOW_MS - 1_000,
              },
            },
          },
        },
        evidence: {
          status: 200,
          body: {
            phase: 'running',
            fleetConnect: { lastError: 'FactoryAgentRegistrationError (AGENT_EXISTS)' },
          },
        },
      }),
    })

    expect(code).toBe(0)
    expect(out.text()).toContain('fleetConnect error : FactoryAgentRegistrationError (AGENT_EXISTS)')
  })

  it('still diagnoses when the evidence token is rejected', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--token', 'stale-token', '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
      env: HERMETIC_ENV,
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
  // Review follow-up on #300 (P3, cubic). Introduced by my own refactor: with
  // only one legacy state string present, the other rendered as the literal
  // "undefined" in the verdict.
  it('says unknown, never undefined, for a legacy state string that is missing', async () => {
    const out = buffer()
    await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            heartbeat: { status: 'running', eventListener: 'subscribed' },
          },
        },
      }),
    })

    const report = JSON.parse(out.text()) as { verdict: string }
    expect(report.verdict).not.toContain('undefined')
    expect(report.verdict).toContain('readinessReconcile=unknown')
  })
  // Review follow-up on #300 (Major, CodeRabbit). `live` was false for any
  // non-200, so a fronting proxy answering 404/502 produced a confident
  // statement about a Factory that was never asked.
  it('does not claim a Factory diagnosis from a proxy answer', async () => {
    for (const status of [404, 401, 502]) {
      const out = buffer()
      const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
        stdout: out,
        stderr: buffer(),
        env: HERMETIC_ENV,
        diagnoseFetch: stubFetch({ healthz: { status, body: { error: 'nope' } } }),
      })

      expect(code).not.toBe(0)
      const report = JSON.parse(out.text()) as { dispatching: boolean; verdict: string }
      expect(report.dispatching).toBe(false)
      expect(report.verdict).toContain(`HTTP ${status}`)
      expect(report.verdict).toMatch(/cannot tell/iu)
      // The claims this response cannot support.
      expect(report.verdict).not.toContain('reports itself not live')
      expect(report.verdict).not.toContain('Factory process is gone')
    }
  })

  it('still treats a 503 carrying the instance own verdict as the instance speaking', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: { status: 503, body: { ok: false, phase: 'running', factoryProcess: 'running' } },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { verdict: string }
    expect(report.verdict).toContain('reports itself not live')
  })

  // Review follow-up on #300 (Minor, CodeRabbit). `unknown` means the block did
  // not say, which is not the same statement as "a subsystem is degraded".
  it('reports an unreadable status as cannot tell rather than as degradation', async () => {
    const out = buffer()
    const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
      stdout: out,
      stderr: buffer(),
      env: HERMETIC_ENV,
      diagnoseFetch: stubFetch({
        healthz: {
          status: 200,
          body: {
            ok: true,
            phase: 'running',
            health: {
              schemaVersion: 1,
              ok: true,
              status: 'sideways',
              stale: false,
              loopStatus: 'running',
              degradedSubsystems: [],
              readinessReconcile: { state: 'healthy', consecutiveFailures: 0, failureThreshold: 3 },
            },
          },
        },
      }),
    })

    expect(code).not.toBe(0)
    const report = JSON.parse(out.text()) as { verdict: string }
    expect(report.verdict).toMatch(/cannot tell/iu)
    expect(report.verdict).not.toContain('a subsystem is degraded')
  })

  // Review follow-up on #300 (Minor, CodeRabbit). A 404 means the deployment
  // has no /evidence route and a 5xx means it failed; neither is a reason to
  // send someone rotating a credential that works.
  it('blames the token only when the token is what was refused', async () => {
    const cases = [
      { status: 401, expect: /token/iu },
      { status: 403, expect: /token/iu },
      { status: 404, expect: /no \/evidence route|not exposed/iu },
      { status: 502, expect: /failed|error/iu },
    ]
    for (const testCase of cases) {
      const out = buffer()
      await runFleetCli(['diagnose', '--deployed', BASE, '--token', 'op-token', '--json'], {
        stdout: out,
        stderr: buffer(),
        env: HERMETIC_ENV,
        diagnoseFetch: stubFetch({
          healthz: { status: 200, body: healthy },
          evidence: { status: testCase.status, body: {} },
        }),
      })

      const report = JSON.parse(out.text()) as { evidence?: { reason?: string } }
      expect(report.evidence?.reason).toMatch(testCase.expect)
      if (testCase.status === 404 || testCase.status === 502) {
        expect(report.evidence?.reason).not.toContain('token was not accepted')
      }
    }
  })

  // Review follow-up on #300 (Security, CodeRabbit). `factory diagnose <url>
  // my-evidence-token` puts a credential in argv; echoing it into the error
  // puts it on stderr and from there into CI logs.
  it('never echoes an unrecognized argument value into the error', async () => {
    const err = buffer()
    const code = await runFleetCli(['diagnose', BASE, 's3cr3t-evidence-token'], {
      stdout: buffer(),
      stderr: err,
      env: HERMETIC_ENV,
    })

    expect(code).not.toBe(0)
    expect(err.text()).not.toContain('s3cr3t-evidence-token')
    expect(err.text()).toMatch(/argument 2|second argument|position/iu)
  })
  // Review follow-up on #300 (P2, cubic). My own no-echo fix was incomplete:
  // a token in the URL slot still reached stderr through the scheme check.
  it('never echoes a value that landed in the url slot either', async () => {
    for (const argv of [
      ['diagnose', 's3cr3t-evidence-token'],
      ['diagnose', '--deployed', 's3cr3t-evidence-token'],
      ['diagnose', '--url', 's3cr3t-evidence-token'],
    ]) {
      const err = buffer()
      const code = await runFleetCli(argv, { stdout: buffer(), stderr: err, env: HERMETIC_ENV })

      expect(code).not.toBe(0)
      expect(err.text()).not.toContain('s3cr3t-evidence-token')
      expect(err.text()).toMatch(/http/iu)
    }
  })

  // Review follow-up on #300 (P2, cubic). A gateway can answer 200 or 503 with
  // its own body; the container's health response always carries `ok`.
  it('treats a 200 or 503 with no ok field as something other than the instance', async () => {
    for (const status of [200, 503]) {
      const out = buffer()
      const code = await runFleetCli(['diagnose', '--deployed', BASE, '--json'], {
        stdout: out,
        stderr: buffer(),
        env: HERMETIC_ENV,
        diagnoseFetch: stubFetch({ healthz: { status, body: { message: 'gateway timeout page' } } }),
      })

      expect(code).not.toBe(0)
      const report = JSON.parse(out.text()) as { live?: boolean; verdict: string }
      expect(report.live).toBeUndefined()
      expect(report.verdict).toMatch(/cannot tell/iu)
      expect(report.verdict).not.toContain('reports itself not live')
    }
  })
})
