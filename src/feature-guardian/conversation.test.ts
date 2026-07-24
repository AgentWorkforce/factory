import type { WorkforceCtx, WorkforceEvent } from '@agentworkforce/runtime'
import { slackClient, type GithubClient } from '@relayfile/relay-helpers'
import { describe, expect, it, vi } from 'vitest'

import {
  GuardianStateConflictError,
  classifyGuardianResponse,
  createGithubIssueWriter,
  createSdkConversationStore,
  guardianConfirmationPath,
  guardianContentRevision,
  guardianConversationId,
  registerGuardianQuestion,
  remediationMarker,
  runGuardianConversationTurn,
  type ConversationSnapshot,
  type ConversationStore,
  type FeatureGuardianAdapters,
  type GuardianConfirmationRecord,
  type GuardianConversationRecord,
  type GuardianConversationState,
  type GuardianIssuePolicy,
  type GuardianIssueReceipt,
  type GuardianManifestCatalog,
  type GuardianProcedureResult,
  type ImmutableConfirmationStore,
} from './conversation.js'

const CHANNEL = 'C-GUARDIAN'
const ROOT_TS = '1710000000.000100'
const AUTHORIZED = 'U-AUTHORIZED'

const feature = {
  id: 'verification-procedure-routing',
  name: 'Manifest-to-Procedure Routing',
  category: 'release-verification',
  api: 'manifest.yaml#verification.categories',
  description: 'Routes every feature to an exact named procedure.',
  locations: [
    '.agentworkforce/features/manifest.yaml',
    '.agentworkforce/features/verify/procedures.md',
  ],
  procedure: 'release-verification',
  tier: 1,
  criticality: 'critical',
}

const catalog: GuardianManifestCatalog = {
  manifestRevision: guardianContentRevision('manifest-a'),
  manifestVersion: '1.1',
  procedureRevision: guardianContentRevision('procedures-a'),
  features: [feature],
}

const question = {
  feature,
  manifestRevision: catalog.manifestRevision,
  manifestVersion: catalog.manifestVersion,
  procedureRevision: catalog.procedureRevision,
  generation: 7,
  channelId: CHANNEL,
  threadTs: ROOT_TS,
  askedAt: slackTime(ROOT_TS),
}

class MemoryConversationStore implements ConversationStore {
  snapshot: ConversationSnapshot | null = null
  saveCalls = 0
  failSaveCalls = new Set<number>()

  async load(): Promise<ConversationSnapshot | null> {
    return this.snapshot ? structuredClone(this.snapshot) : null
  }

  async save(
    state: GuardianConversationState,
    expected: ConversationSnapshot | null,
  ): Promise<ConversationSnapshot> {
    this.saveCalls += 1
    if (this.failSaveCalls.delete(this.saveCalls)) throw new GuardianStateConflictError()
    if (expected?.revision !== this.snapshot?.revision) throw new GuardianStateConflictError()
    this.snapshot = {
      state: structuredClone(state),
      revision: `memory-${this.saveCalls}`,
    }
    return structuredClone(this.snapshot)
  }

  record(): GuardianConversationRecord {
    const record = this.snapshot?.state.records[0]
    if (!record) throw new Error('missing test conversation')
    return record
  }
}

class MemoryConfirmations implements ImmutableConfirmationStore {
  readonly records = new Map<string, GuardianConfirmationRecord>()
  calls = 0

  async append(record: GuardianConfirmationRecord): Promise<string> {
    this.calls += 1
    const path = guardianConfirmationPath(record)
    const existing = this.records.get(path)
    if (existing && JSON.stringify(existing) !== JSON.stringify(record)) {
      throw new Error('immutable confirmation differs')
    }
    this.records.set(path, structuredClone(record))
    return path
  }
}

class MemorySlack {
  readonly posts: Array<{ text: string; key: string }> = []
  private readonly receipts = new Map<string, string>()

  readonly create = (() => ({
    replies: {
      write: vi.fn(async (_target: unknown, body: { text: string; idempotencyKey: string }) => {
        let ts = this.receipts.get(body.idempotencyKey)
        if (!ts) {
          ts = `1710000099.${String(this.receipts.size + 1).padStart(6, '0')}`
          this.receipts.set(body.idempotencyKey, ts)
          this.posts.push({ text: body.text, key: body.idempotencyKey })
        }
        return {
          path: '/slack/reply.json',
          absolutePath: '/slack/reply.json',
          receipt: { externalId: 'not-a-provider-ts', ts },
        }
      }),
    },
  })) as unknown as typeof slackClient
}

function procedureResult(outcome: GuardianProcedureResult['outcome']): GuardianProcedureResult {
  const result = {
    passed: 'positive',
    failed: 'negative',
    skip: 'skip',
    manual: 'manual',
  } as const
  return {
    source: 'procedure',
    result: result[outcome],
    outcome,
    verifier: 'test-procedure-runner',
    summary: `${feature.procedure} ${outcome}`,
    commands: ['npm run build', 'npm test'],
    positiveAssertions: outcome === 'passed' ? ['command exited 0'] : [],
    negativeAssertions: outcome === 'failed' ? ['command exited 1'] : [],
    tests: { passed: outcome === 'passed' ? 42 : 0, failed: outcome === 'failed' ? 1 : 0 },
    cleanup: ['isolated workspace removed'],
  }
}

function adapters(options: {
  currentCatalog?: GuardianManifestCatalog
  result?: GuardianProcedureResult
  issueWriter?: (policy: GuardianIssuePolicy) => Promise<GuardianIssueReceipt>
} = {}): FeatureGuardianAdapters {
  return {
    loadCatalog: vi.fn(async () => options.currentCatalog ?? catalog),
    resolveProcedure: vi.fn(async () => ({
      name: feature.procedure,
      path: '.agentworkforce/features/verify/procedures.md',
      prerequisites: 'source checkout with installed dependencies',
      body: 'npm run build\nnpm test',
      command: 'npm run build\nnpm test',
    })),
    gateTier: vi.fn(async () => ({ outcome: 'available', reason: 'available' })),
    runProcedure: vi.fn(async () => options.result ?? procedureResult('passed')),
    isAuthorizedConfirmer: (_ctx, actorId) => actorId === AUTHORIZED,
    clarification: (snapshot, procedure, _turn, number) =>
      `Clarification ${number}: test ${snapshot.api} at ${snapshot.locations.join(', ')} with ${procedure.name} tier ${snapshot.tier}.`,
    classifyDefect: (_snapshot, turn) => {
      if (/manifest/iu.test(turn.text)) return 'manifest'
      if (/procedure|runbook/iu.test(turn.text)) return 'procedure'
      if (/docs?|documentation/iu.test(turn.text)) return 'documentation'
      if (/test/iu.test(turn.text)) return 'test'
      return 'implementation'
    },
    isDefectEstablished: (_snapshot, turn) => /concrete evidence/iu.test(turn.text),
    repositoryForFeature: () => 'AgentWorkforce/factory',
    issuePolicy: ({ conversation, defectKind, slackBacklink }) => {
      const dedupeKey = `factory:${feature.id}:${feature.procedure}`
      return {
        repository: 'AgentWorkforce/factory',
        title: `[Feature guardian] ${feature.name}: ${defectKind}`,
        body: `${remediationMarker(dedupeKey)}\n${conversation.manifestRevision}\n${slackBacklink}`,
        labels: ['factory-ready'],
        defectKind,
        dedupeKey,
      }
    },
  }
}

function context(): WorkforceCtx {
  const files = new Map<string, string>()
  return {
    agent: { id: 'sim-agent' },
    deployment: { id: 'sim-deployment' },
    persona: { inputs: { SLACK_CHANNEL: CHANNEL }, inputSpecs: {} },
    credentials: { tryRequire: vi.fn(() => null) },
    files: {
      read: vi.fn(async (path: string) => {
        const value = files.get(path)
        if (value === undefined) throw new Error(`ENOENT: ${path}`)
        return value
      }),
      write: vi.fn(async (path: string, value: string) => {
        files.set(path, value)
      }),
    },
    sandbox: {
      cwd: '/workspace',
      readFile: vi.fn(async (path: string) => {
        throw new Error(`ENOENT: ${path}`)
      }),
    },
    log: vi.fn(),
  } as unknown as WorkforceCtx
}

function slackTime(ts: string): string {
  return new Date(Number(ts) * 1_000).toISOString()
}

function reply(
  envelopeId: string,
  messageTs: string,
  text: string,
  actorId = AUTHORIZED,
  channel = CHANNEL,
): WorkforceEvent {
  return {
    id: envelopeId,
    type: 'slack.message.created',
    occurredAt: slackTime(messageTs),
    resource: { path: `/slack/channels/${channel}/messages/${ROOT_TS.replace('.', '_')}` },
    expand: vi.fn(async () => ({
      data: {
        event: {
          channel,
          ts: messageTs,
          thread_ts: ROOT_TS,
          user: actorId,
          text,
          event_ts: messageTs,
        },
      },
    })),
  } as unknown as WorkforceEvent
}

function reaction(
  envelopeId: string,
  eventTs: string,
  emoji: string,
  actorId = AUTHORIZED,
): WorkforceEvent {
  return {
    id: envelopeId,
    type: 'slack.reaction.added',
    occurredAt: slackTime(eventTs),
    resource: { path: `/slack/channels/${CHANNEL}/messages/${ROOT_TS.replace('.', '_')}` },
    expand: vi.fn(async () => ({
      data: {
        event: {
          item: { channel: CHANNEL, ts: ROOT_TS },
          reaction: emoji,
          user: actorId,
          event_ts: eventTs,
        },
      },
    })),
  } as unknown as WorkforceEvent
}

async function fixture(options: Parameters<typeof adapters>[0] = {}) {
  const store = new MemoryConversationStore()
  const confirmations = new MemoryConfirmations()
  const slack = new MemorySlack()
  const ctx = context()
  await registerGuardianQuestion(ctx, question, { conversationStore: store })
  const issueWrites: GuardianIssuePolicy[] = []
  const issueWriter = {
    upsert: vi.fn(async (policy: GuardianIssuePolicy) => {
      issueWrites.push(policy)
      if (options.issueWriter) return options.issueWriter(policy)
      return {
        repository: policy.repository,
        number: 1700,
        url: 'https://github.com/AgentWorkforce/factory/issues/1700',
        dedupeKey: policy.dedupeKey,
      }
    }),
  }
  const deps = {
    conversationStore: store,
    confirmationStore: confirmations,
    issueWriter,
    createSlackClient: slack.create,
  }
  return { store, confirmations, slack, ctx, deps, issueWriter, issueWrites }
}

describe('feature guardian response classification', () => {
  it.each([
    [{ reaction: 'white_check_mark' }, 'affirmative'],
    [{ reaction: 'wrench' }, 'failure'],
    [{ text: 'untested' }, 'untested'],
    [{ text: 'I looked at it briefly' }, 'ambiguous'],
    [{ text: "tested but it doesn't work" }, 'failure'],
    [{ text: 'not broken' }, 'affirmative'],
    [{ text: 'manual, credentials are unavailable' }, 'deferred'],
  ] as const)('classifies %o as %s', (input, expected) => {
    expect(classifyGuardianResponse(input)).toBe(expected)
  })
})

describe('feature guardian conversation ownership', () => {
  it('holds two clarification turns across restart and stores one exact confirmation', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(f.ctx, reply('env-1', '1710000001.000100', 'maybe'), a, f.deps)
    expect(f.store.record()).toMatchObject({
      status: 'discussing',
      turnCount: 1,
      clarificationCount: 1,
    })
    expect(f.slack.posts[0]?.text).toContain('Clarification 1')

    await runGuardianConversationTurn(
      f.ctx,
      reply('duplicate-envelope', '1710000001.000100', 'maybe'),
      a,
      f.deps,
    )
    expect(f.slack.posts).toHaveLength(1)

    await runGuardianConversationTurn(f.ctx, reply('env-2', '1710000002.000100', 'maybe still'), a, f.deps)
    expect(f.store.record()).toMatchObject({ turnCount: 2, clarificationCount: 2 })
    expect(f.slack.posts[1]?.text).toContain('Clarification 2')

    await runGuardianConversationTurn(
      f.ctx,
      reply('env-3', '1710000003.000100', 'I tested it and confirmed it works as expected'),
      a,
      f.deps,
    )
    expect(f.store.record().status).toBe('confirmed')
    expect(f.confirmations.records).toHaveLength(1)
    const confirmation = [...f.confirmations.records.values()][0]!
    expect(confirmation).toMatchObject({
      featureId: feature.id,
      manifestRevision: catalog.manifestRevision,
      procedureRevision: catalog.procedureRevision,
      generation: 7,
      actor: { id: AUTHORIZED },
      verifier: AUTHORIZED,
      timestamp: slackTime('1710000003.000100'),
      slack: { channelId: CHANNEL, threadTs: ROOT_TS, questionTs: ROOT_TS },
    })
  })

  it('clarifies a wrench, verifies the next failure, and opens one remediation issue', async () => {
    const f = await fixture({ result: procedureResult('failed') })
    const a = adapters({ result: procedureResult('failed') })
    await runGuardianConversationTurn(f.ctx, reaction('wrench-1', '1710000001.000100', 'wrench'), a, f.deps)
    expect(f.store.record().status).toBe('discussing')
    expect(a.runProcedure).not.toHaveBeenCalled()

    const failure = reply('failure-2', '1710000002.000100', 'the implementation is still failing')
    await runGuardianConversationTurn(f.ctx, failure, a, f.deps)
    expect(f.store.record()).toMatchObject({
      status: 'remediation-open',
      issue: { repository: 'AgentWorkforce/factory', number: 1700 },
    })
    expect(f.issueWriter.upsert).toHaveBeenCalledTimes(1)
    expect(f.issueWrites[0]).toMatchObject({
      repository: 'AgentWorkforce/factory',
      labels: ['factory-ready'],
      defectKind: 'implementation',
    })
    expect(f.slack.posts.at(-1)?.text).toContain('/issues/1700')

    await runGuardianConversationTurn(f.ctx, failure, a, f.deps)
    expect(f.issueWriter.upsert).toHaveBeenCalledTimes(1)
  })

  it('runs an untested check only after clarification and records automated evidence', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(f.ctx, reply('untested-1', '1710000001.000100', 'untested'), a, f.deps)
    expect(a.runProcedure).not.toHaveBeenCalled()
    await runGuardianConversationTurn(f.ctx, reply('untested-2', '1710000002.000100', 'still untested'), a, f.deps)
    expect(a.runProcedure).toHaveBeenCalledTimes(1)
    expect(f.store.record().status).toBe('confirmed')
    expect([...f.confirmations.records.values()][0]).toMatchObject({
      verifier: 'test-procedure-runner',
      commands: ['npm run build', 'npm test'],
      tests: { passed: 42, failed: 0 },
    })
  })

  it('requires automation for an unauthorized affirmative actor', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(
      f.ctx,
      reaction('unauthorized-check', '1710000001.000100', 'white_check_mark', 'U-OTHER'),
      a,
      f.deps,
    )
    expect(a.runProcedure).toHaveBeenCalledTimes(1)
    expect([...f.confirmations.records.values()][0]?.verifier).toBe('test-procedure-runner')
  })

  it('clarifies a bare checkmark instead of treating delivery as confirmation evidence', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(
      f.ctx,
      reaction('bare-check', '1710000001.000100', 'white_check_mark'),
      a,
      f.deps,
    )
    expect(f.store.record()).toMatchObject({ status: 'discussing', clarificationCount: 1 })
    expect(f.confirmations.records).toHaveLength(0)
    expect(a.runProcedure).not.toHaveBeenCalled()
  })

  it('preserves exact SKIP and MANUAL outcomes without confirmation', async () => {
    const skip = await fixture()
    const skipAdapters = adapters()
    skipAdapters.gateTier = vi.fn(async () => ({ outcome: 'skip', reason: 'SKIP: no fixture' }))
    await runGuardianConversationTurn(skip.ctx, reply('skip-1', '1710000001.000100', 'untested'), skipAdapters, skip.deps)
    await runGuardianConversationTurn(skip.ctx, reply('skip-2', '1710000002.000100', 'still untested'), skipAdapters, skip.deps)
    expect(skip.store.record().status).toBe('deferred')
    expect(skip.slack.posts.at(-1)?.text).toContain('Result: SKIP')
    expect(skip.confirmations.records).toHaveLength(0)

    const manual = await fixture()
    await runGuardianConversationTurn(
      manual.ctx,
      reply('manual-1', '1710000001.000100', 'manual, credentials are unavailable'),
      adapters(),
      manual.deps,
    )
    expect(manual.slack.posts.at(-1)?.text).toContain('Result: MANUAL')
  })

  it('defers when the manifest or procedure revision changes after the question', async () => {
    const f = await fixture()
    const changed = { ...catalog, manifestRevision: guardianContentRevision('manifest-b') }
    const a = adapters({ currentCatalog: changed })
    await runGuardianConversationTurn(f.ctx, reply('changed-1', '1710000001.000100', 'yes'), a, f.deps)
    expect(f.store.record().status).toBe('deferred')
    expect(f.confirmations.records).toHaveLength(0)
    expect(f.slack.posts.at(-1)?.text).toContain('different revision')
  })

  it('revalidates revision and tier gates before resuming a pending verification', async () => {
    const revision = await fixture()
    let currentCatalog = catalog
    const revisionAdapters = adapters()
    revisionAdapters.loadCatalog = vi.fn(async () => currentCatalog)
    await runGuardianConversationTurn(
      revision.ctx,
      reply('revision-1', '1710000001.000100', 'untested'),
      revisionAdapters,
      revision.deps,
    )
    revision.store.failSaveCalls.add(revision.store.saveCalls + 3)
    const retry = reply('revision-2', '1710000002.000100', 'still untested')
    await expect(
      runGuardianConversationTurn(revision.ctx, retry, revisionAdapters, revision.deps),
    ).rejects.toBeInstanceOf(GuardianStateConflictError)
    expect(revision.store.record()).toMatchObject({ status: 'verifying' })
    currentCatalog = { ...catalog, procedureRevision: guardianContentRevision('procedures-b') }
    await runGuardianConversationTurn(revision.ctx, retry, revisionAdapters, revision.deps)
    expect(revision.store.record().status).toBe('deferred')
    expect(revision.confirmations.records).toHaveLength(0)
    expect(revisionAdapters.runProcedure).toHaveBeenCalledTimes(1)

    const gated = await fixture()
    const gatedAdapters = adapters()
    const originalRun = gatedAdapters.runProcedure
    let runAttempts = 0
    const restartedRun = vi.fn(async (...args: Parameters<typeof originalRun>) => {
      runAttempts += 1
      if (runAttempts === 1) throw new Error('restart now')
      return originalRun(...args)
    })
    gatedAdapters.runProcedure = restartedRun
    await runGuardianConversationTurn(
      gated.ctx,
      reply('gate-1', '1710000001.000100', 'untested'),
      gatedAdapters,
      gated.deps,
    )
    const gatedRetry = reply('gate-2', '1710000002.000100', 'still untested')
    await expect(
      runGuardianConversationTurn(gated.ctx, gatedRetry, gatedAdapters, gated.deps),
    ).rejects.toThrow('restart now')
    gatedAdapters.gateTier = vi.fn(async () => ({ outcome: 'manual', reason: 'MANUAL: opt-in revoked' }))
    await runGuardianConversationTurn(gated.ctx, gatedRetry, gatedAdapters, gated.deps)
    expect(gated.store.record().status).toBe('deferred')
    expect(restartedRun).toHaveBeenCalledTimes(1)
  })

  it('ignores unrelated channels, threads, bots, and delayed older events', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(
      f.ctx,
      reply('wrong-channel', '1710000001.000100', 'yes', AUTHORIZED, 'C-OTHER'),
      a,
      f.deps,
    )
    const wrongThread = reply('wrong-thread', '1710000001.000200', 'yes') as WorkforceEvent & {
      expand: WorkforceEvent['expand']
    }
    wrongThread.expand = vi.fn(async () => ({
      data: { event: { channel: CHANNEL, ts: '1710000001.000200', thread_ts: '1710000000.999999', user: AUTHORIZED, text: 'yes' } },
    }))
    await runGuardianConversationTurn(f.ctx, wrongThread, a, f.deps)
    expect(f.store.record().status).toBe('asked')

    await runGuardianConversationTurn(f.ctx, reply('newer', '1710000002.000100', 'maybe'), a, f.deps)
    await runGuardianConversationTurn(f.ctx, reply('older', '1710000001.000100', 'maybe'), a, f.deps)
    expect(f.store.record()).toMatchObject({ turnCount: 1, clarificationCount: 1 })
    expect(f.slack.posts).toHaveLength(1)
  })

  it('orders same-millisecond reactions by the full provider timestamp', async () => {
    const f = await fixture({ result: procedureResult('failed') })
    const a = adapters({ result: procedureResult('failed') })
    await runGuardianConversationTurn(
      f.ctx,
      reaction('same-ms-1', '1710000001.000100', 'question'),
      a,
      f.deps,
    )
    await runGuardianConversationTurn(
      f.ctx,
      reaction('same-ms-2', '1710000001.000200', 'wrench'),
      a,
      f.deps,
    )
    expect(a.runProcedure).toHaveBeenCalledTimes(1)
    expect(f.store.record().status).toBe('remediation-open')
  })

  it('rejects payload identities that disagree with the mounted resource path', async () => {
    const f = await fixture()
    const a = adapters()
    const mismatched = reply('path-mismatch', '1710000001.000100', 'tested and works') as
      WorkforceEvent & { resource: { path: string } }
    mismatched.resource.path = `/slack/channels/${CHANNEL}/messages/1719999999_000100`
    await runGuardianConversationTurn(f.ctx, mismatched, a, f.deps)
    expect(f.store.record().status).toBe('asked')
    expect(f.confirmations.records).toHaveLength(0)
  })

  it('replays a clarification after a post-before-CAS conflict without duplicating Slack', async () => {
    const f = await fixture()
    const a = adapters()
    f.store.failSaveCalls.add(f.store.saveCalls + 1)
    const event = reply('cas-1', '1710000001.000100', 'maybe')
    await expect(runGuardianConversationTurn(f.ctx, event, a, f.deps)).rejects.toBeInstanceOf(
      GuardianStateConflictError,
    )
    expect(f.slack.posts).toHaveLength(1)
    await runGuardianConversationTurn(f.ctx, event, a, f.deps)
    expect(f.slack.posts).toHaveLength(1)
    expect(f.store.record()).toMatchObject({ status: 'discussing', turnCount: 1 })
  })

  it('rejects an explicitly pending Slack reply even when it carries a stale timestamp', async () => {
    const f = await fixture()
    const pendingSlack = (() => ({
      replies: {
        write: vi.fn(async () => ({
          path: '/slack/pending.json',
          absolutePath: '/slack/pending.json',
          deliveryStatus: 'pending' as const,
          receipt: { ts: '1710000099.000100' },
        })),
      },
    })) as unknown as typeof slackClient
    await expect(
      runGuardianConversationTurn(
        f.ctx,
        reply('pending-slack', '1710000001.000100', 'maybe'),
        adapters(),
        { ...f.deps, createSlackClient: pendingSlack },
      ),
    ).rejects.toThrow('not provider-confirmed')
    expect(f.store.record().status).toBe('asked')
  })

  it('resumes an issue-write failure without rerunning the procedure', async () => {
    let attempts = 0
    const f = await fixture({
      result: procedureResult('failed'),
      issueWriter: async (policy) => {
        attempts += 1
        if (attempts === 1) throw new Error('simulated issue admission failure')
        return {
          repository: policy.repository,
          number: 1701,
          url: 'https://github.com/AgentWorkforce/factory/issues/1701',
          dedupeKey: policy.dedupeKey,
        }
      },
    })
    const a = adapters({ result: procedureResult('failed') })
    await runGuardianConversationTurn(f.ctx, reaction('issue-1', '1710000001.000100', 'wrench'), a, f.deps)
    const event = reply('issue-2', '1710000002.000100', 'implementation is failing')
    await expect(runGuardianConversationTurn(f.ctx, event, a, f.deps)).rejects.toThrow(
      'issue admission failure',
    )
    expect(f.store.record().status).toBe('verifying')
    await runGuardianConversationTurn(f.ctx, event, a, f.deps)
    expect(a.runProcedure).toHaveBeenCalledTimes(1)
    expect(f.store.record().status).toBe('remediation-open')
  })

  it('stores confirmation once when terminal checkpoint retries after Slack delivery', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(f.ctx, reply('confirm-1', '1710000001.000100', 'untested'), a, f.deps)
    // verifying, result, confirmation-recorded, confirmation path, then terminal
    f.store.failSaveCalls.add(f.store.saveCalls + 5)
    const event = reply('confirm-2', '1710000002.000100', 'still untested')
    await expect(runGuardianConversationTurn(f.ctx, event, a, f.deps)).rejects.toBeInstanceOf(
      GuardianStateConflictError,
    )
    expect(f.store.record().status).toBe('confirmation-recorded')
    await runGuardianConversationTurn(f.ctx, event, a, f.deps)
    expect(f.confirmations.records).toHaveLength(1)
    expect(f.store.record().status).toBe('confirmed')
    expect(f.slack.posts.filter((post) => post.text.startsWith('✅'))).toHaveLength(1)
  })

  it('opens remediation for repeated documentation drift even when implementation checks pass', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(
      f.ctx,
      reply('docs-1', '1710000001.000100', 'the documentation is wrong'),
      a,
      f.deps,
    )
    await runGuardianConversationTurn(
      f.ctx,
      reply(
        'docs-2',
        '1710000002.000100',
        'the documentation is wrong; concrete evidence: `docs/guardian.md` says expected enabled, but the command outputs disabled',
      ),
      a,
      f.deps,
    )
    expect(f.store.record().status).toBe('remediation-open')
    expect(f.issueWrites[0]?.defectKind).toBe('documentation')
  })

  it('does not open remediation from a repeated allegation without concrete evidence', async () => {
    const f = await fixture()
    const a = adapters()
    await runGuardianConversationTurn(
      f.ctx,
      reply('unsupported-1', '1710000001.000100', 'the documentation is wrong'),
      a,
      f.deps,
    )
    await runGuardianConversationTurn(
      f.ctx,
      reply('unsupported-2', '1710000002.000100', 'the documentation is still wrong'),
      a,
      f.deps,
    )
    expect(f.store.record().status).toBe('discussing')
    expect(f.issueWriter.upsert).not.toHaveBeenCalled()
  })
})

describe('feature guardian malformed and bounded state', () => {
  it('rejects an impossible pending state from the exact SDK store', async () => {
    const id = guardianConversationId(question)
    const malformed = {
      kind: 'feature-guardian:conversations',
      version: 1,
      records: [{
        ...question,
        id,
        status: 'asked',
        turnCount: 0,
        clarificationCount: 0,
        seenEventIds: [],
        evidence: [],
        pending: {
          eventId: `slack:${'a'.repeat(64)}`,
          eventOrder: `${slackTime('1710000001.000100')}:1710000001.000100:slack:${'a'.repeat(64)}`,
          eventOccurredAt: slackTime('1710000001.000100'),
          actorId: AUTHORIZED,
          response: 'ambiguous',
          text: 'maybe',
        },
        updatedAt: slackTime('1710000001.000100'),
      }],
    }
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({
      path: '/memory/workspace/feature-guardian/conversations.json',
      revision: 'rev-1',
      content: `${JSON.stringify(malformed)}\n`,
      encoding: 'utf-8',
    }), { status: 200, headers: { 'content-type': 'application/json' } })) as typeof fetch
    const store = createSdkConversationStore(
      { url: 'https://relayfile.test', token: 'token', workspaceId: 'workspace' },
      { fetchImpl },
    )
    await expect(store.load()).rejects.toThrow('asked conversation cannot retain a pending turn')
  })

  it('rejects a resumable confirmation without a validated confirmation basis', async () => {
    const id = guardianConversationId(question)
    const eventId = `slack:${'b'.repeat(64)}`
    const order = `${slackTime('1710000001.000100')}:1710000001.000100:${eventId}`
    const malformed = {
      kind: 'feature-guardian:conversations',
      version: 1,
      records: [{
        ...question,
        id,
        status: 'confirmation-recorded',
        turnCount: 1,
        clarificationCount: 0,
        seenEventIds: [],
        evidence: [{ source: 'actor', result: 'positive', summary: 'yes' }],
        pending: {
          eventId,
          eventOrder: order,
          eventOccurredAt: slackTime('1710000001.000100'),
          actorId: 'U-UNTRUSTED',
          response: 'affirmative',
          text: 'yes',
          evidence: { source: 'actor', result: 'positive', summary: 'yes' },
        },
        lastProcessedEventOrder: order,
        updatedAt: slackTime('1710000001.000100'),
      }],
    }
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({
      path: '/memory/workspace/feature-guardian/conversations.json',
      revision: 'rev-1',
      content: `${JSON.stringify(malformed)}\n`,
      encoding: 'utf-8',
    }), { status: 200, headers: { 'content-type': 'application/json' } })) as typeof fetch
    const store = createSdkConversationStore(
      { url: 'https://relayfile.test', token: 'token', workspaceId: 'workspace' },
      { fetchImpl },
    )
    await expect(store.load()).rejects.toThrow('requires a confirmation basis')
  })

  it('prunes oldest terminal records to satisfy count and byte bounds', async () => {
    const store = new MemoryConversationStore()
    const records = Array.from({ length: 512 }, (_, index): GuardianConversationRecord => {
      const ts = `1720${String(index).padStart(6, '0')}.000100`
      const historical = { ...question, threadTs: ts, askedAt: slackTime(ts) }
      return {
        ...historical,
        id: guardianConversationId(historical),
        status: 'confirmed',
        turnCount: 1,
        clarificationCount: 0,
        seenEventIds: [`event-${index}`],
        evidence: [],
        confirmationPath: `/confirmations/${index}.json`,
        finalReplyTs: ts,
        updatedAt: slackTime(ts),
      }
    })
    store.snapshot = {
      state: { kind: 'feature-guardian:conversations', version: 1, records },
      revision: 'memory-seed',
    }
    await registerGuardianQuestion(context(), question, { conversationStore: store })
    expect(store.snapshot?.state.records.length).toBeLessThanOrEqual(512)
    expect(new TextEncoder().encode(JSON.stringify(store.snapshot?.state)).byteLength).toBeLessThanOrEqual(
      256 * 1024,
    )
    expect(store.snapshot?.state.records.some((record) => record.id === records[0]?.id)).toBe(false)
    expect(store.snapshot?.state.records.some((record) => record.id === guardianConversationId(question))).toBe(true)
  })
})

describe('feature guardian GitHub remediation durability', () => {
  const policy: GuardianIssuePolicy = {
    repository: 'AgentWorkforce/factory',
    title: '[Feature guardian] routing defect',
    body: `${remediationMarker(`factory:${feature.id}:${feature.procedure}`)}\ncomplete evidence`,
    labels: ['factory-ready'],
    defectKind: 'implementation',
    dedupeKey: `factory:${feature.id}:${feature.procedure}`,
  }

  function github(overrides: Partial<GithubClient> = {}): GithubClient {
    return {
      issues: { list: vi.fn(async () => []) },
      'issue-comments': { list: vi.fn(async () => []) },
      createIssue: vi.fn(async () => ({
        status: 'confirmed',
        id: '1777',
        url: 'https://github.com/AgentWorkforce/factory/issues/1777',
        path: '/github/issues/draft.json',
        receipt: { externalId: '1777' },
      })),
      comment: vi.fn(),
      ...overrides,
    } as unknown as GithubClient
  }

  it('persists a provider receipt and never creates the issue twice', async () => {
    const ctx = context()
    const client = github()
    const writer = createGithubIssueWriter(ctx, client)
    const first = await writer.upsert(policy)
    const replay = await writer.upsert(policy)
    expect(replay).toEqual(first)
    expect(client.createIssue).toHaveBeenCalledTimes(1)
  })

  it('admits only one concurrent provider create', async () => {
    const ctx = context()
    const createIssue = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5))
      return {
        status: 'confirmed' as const,
        id: '1780',
        url: 'https://github.com/AgentWorkforce/factory/issues/1780',
        path: '/github/issues/draft.json',
        receipt: { externalId: '1780' },
      }
    })
    const writer = createGithubIssueWriter(ctx, github({ createIssue }))
    const results = await Promise.allSettled([writer.upsert(policy), writer.upsert(policy)])
    expect(createIssue).toHaveBeenCalledTimes(1)
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1)
  })

  it('appends changed evidence to the deduplicated issue exactly once', async () => {
    const ctx = context()
    const client = github({
      comment: vi.fn(async () => ({
        status: 'confirmed',
        id: 'comment-1',
        url: 'https://github.com/AgentWorkforce/factory/issues/1777#issuecomment-1',
        path: '/github/issues/1777/comments/draft.json',
      })),
    })
    const writer = createGithubIssueWriter(ctx, client)
    const changedPolicy = { ...policy, body: `${policy.body}\nnew verification evidence` }
    await writer.upsert(policy)
    await writer.upsert(changedPolicy)
    await writer.upsert(changedPolicy)
    expect(client.createIssue).toHaveBeenCalledTimes(1)
    expect(client.comment).toHaveBeenCalledTimes(1)
  })

  it('refuses a second create while an admitted submission is still uncorrelated', async () => {
    const ctx = context()
    const client = github({
      createIssue: vi.fn(async () => ({
        status: 'pending',
        id: '/github/issues/draft.json',
        url: '',
        path: '/github/issues/draft.json',
      })),
    })
    const writer = createGithubIssueWriter(ctx, client)
    await expect(writer.upsert(policy)).rejects.toThrow('pending, not provider-confirmed')
    await expect(writer.upsert(policy)).rejects.toThrow('refusing a duplicate issue')
    expect(client.createIssue).toHaveBeenCalledTimes(1)
  })

  it('retries after a provider positively drops the prior issue submission', async () => {
    const ctx = context()
    let attempt = 0
    const client = github({
      createIssue: vi.fn(async () => {
        attempt += 1
        if (attempt === 1) {
          return {
            status: 'dropped' as const,
            id: '/github/issues/draft.json',
            url: '' as const,
            path: '/github/issues/draft.json',
            reason: 'provider rejected before admission',
          }
        }
        return {
          status: 'confirmed' as const,
          id: '1779',
          url: 'https://github.com/AgentWorkforce/factory/issues/1779',
          path: '/github/issues/draft-2.json',
          receipt: { externalId: '1779' },
        }
      }),
    })
    const writer = createGithubIssueWriter(ctx, client)
    await expect(writer.upsert(policy)).rejects.toThrow('dropped before provider admission')
    await expect(writer.upsert(policy)).resolves.toMatchObject({ number: 1779 })
    expect(client.createIssue).toHaveBeenCalledTimes(2)
  })

  it('finds an existing marker in the canonical nested issue mount', async () => {
    const ctx = context()
    ctx.sandbox.readFile = vi.fn(async (path: string) => {
      if (path.endsWith('/issues/_index.json')) {
        return JSON.stringify([{ id: '1778', number: 1778, title: 'Guardian routing defect' }])
      }
      if (path.endsWith('/1778__guardian-routing-defect/meta.json')) {
        return JSON.stringify({
          payload: {
            number: 1778,
            url: 'https://github.com/AgentWorkforce/factory/issues/1778',
            body: policy.body,
          },
        })
      }
      throw new Error(`ENOENT: ${path}`)
    })
    const client = github()
    const receipt = await createGithubIssueWriter(ctx, client).upsert(policy)
    expect(receipt.number).toBe(1778)
    expect(client.createIssue).not.toHaveBeenCalled()
  })
})
