import { describe, expect, it } from 'vitest'

import { parseWatchStateDocument } from './watch-state-document'

describe('parseWatchStateDocument', () => {
  it.each([
    ['GitHub watch', 'githubIssueCommentWatches'],
    ['Slack thread watch', 'slackThreadWatches'],
    ['waiting clarification', 'waitingClarifications'],
    ['dispatch lifecycle', 'dispatchLifecycles'],
  ])('rejects a malformed %s record during readiness parsing', (_label, collection) => {
    const document = validDocument()
    document.workspaces.workspace[collection] = { broken: { unexpected: true } }

    expect(() => parseWatchStateDocument(document)).toThrow('Factory GitHub watch state file is invalid')
  })

  it('accepts the persisted v3 record shapes used by the document-backed store', () => {
    expect(parseWatchStateDocument(validDocument())).toEqual(validDocument())
  })

  // Triage can label an issue `agent:swarm`, and every persisted decision — the
  // Slack thread watch, the waiting clarification, the dispatch lifecycle — stores
  // that scope verbatim. A validator that does not know the scope rejects the whole
  // document, so one swarm issue costs every watch in the workspace on restart.
  it('accepts a swarm-scope decision everywhere a triage decision is persisted', () => {
    const document = validDocument()
    const swarmDecision = { ...decision(), scope: 'swarm' }
    document.workspaces.workspace.slackThreadWatches.watch = {
      kind: 'terminal-grace',
      issue: issue(),
      decision: swarmDecision,
      threadId: '1780751612.176224',
      retiredAtMs: 1,
      expiresAtMs: 2,
    }
    document.workspaces.workspace.waitingClarifications.clarification.decision = swarmDecision
    document.workspaces.workspace.dispatchLifecycles.lifecycle.decision = swarmDecision

    expect(parseWatchStateDocument(document)).toEqual(document)
  })

  it('migrates legacy conversation history as already acknowledged without acknowledging pending work', () => {
    const document = validDocument()
    document.workspaces.workspace.conversationSessions.legacy = {
      provider: 'slack',
      issue: issue(),
      externalId: '1780751612.176224',
      context: { channelDir: 'factory' },
      agent: { name: 'implementer', sessionRef: 'session-implementer' },
      history: [{ id: 'delivered', text: 'Already delivered.', receivedAtMs: 1_000 }],
      processedMessageIds: ['delivered', 'pending'],
      pending: [{ id: 'pending', text: 'Still pending.', receivedAtMs: 1_001 }],
    }

    expect(parseWatchStateDocument(document).workspaces.workspace?.conversationSessions.legacy)
      .toMatchObject({ acknowledgedMessageIds: ['delivered'] })
  })

  it.each([
    ['preview reference', (document: Record<string, any>) => {
      document.workspaces.workspace.waitingClarifications.clarification.decision.implementers[0].preview = {
        provider: 'tailscale-serve',
      }
    }],
    ['dispatch result role', (document: Record<string, any>) => {
      document.workspaces.workspace.dispatchLifecycles.lifecycle.result = {
        issue: issue(),
        agents: [{ name: 'triage', role: 'triage' }],
        dryRun: false,
      }
    }],
    ['dispatch result optional field', (document: Record<string, any>) => {
      document.workspaces.workspace.dispatchLifecycles.lifecycle.result = {
        issue: issue(),
        agents: [],
        comments: [123],
        dryRun: false,
      }
    }],
    ['nested run cost', (document: Record<string, any>) => {
      document.workspaces.workspace.dispatchLifecycles.lifecycle.cost = {
        runId: 'run-268',
        inputTokens: 1,
        outputTokens: 2,
        usd: null,
        byRole: [{
          role: 'implementer',
          inputTokens: 1,
          outputTokens: 2,
          usd: null,
          byModel: [{ model: 123, inputTokens: 1, outputTokens: 2, usd: null }],
        }],
      }
    }],
  ])('rejects a malformed %s during readiness parsing', (_label, mutate) => {
    const document = validDocument()
    mutate(document)

    expect(() => parseWatchStateDocument(document)).toThrow('Factory GitHub watch state file is invalid')
  })
})

const validDocument = (): Record<string, any> => ({
  version: 3,
  workspaces: {
    workspace: {
      githubIssueCommentWatches: {
        watch: {
          issue: issue(),
          source: { owner: 'AgentWorkforce', repo: 'factory', number: 268, url: 'https://example.invalid/268' },
          pending: [{
            correlationId: 'correlation',
            kind: 'agent-question',
            authorizedAuthor: 'reporter',
          }],
        },
      },
      slackThreadWatches: {},
      waitingClarifications: {
        clarification: {
          issue: issue(),
          decision: decision(),
          dryRun: false,
          askerName: 'implementer',
          question: 'Which path?',
          askedAtMs: 1,
          agents: [{ name: 'implementer', tracked: { spec: agent('implementer') } }],
        },
      },
      babysitterSessions: {},
      babysitterGenerations: {},
      conversationSessions: {},
      dispatchLifecycles: {
        lifecycle: {
          runId: 'run-268',
          issue: issue(),
          decision: decision(),
          dryRun: false,
          phase: 'dispatching',
          agents: [],
          invocationIds: [],
          updatedAtMs: 1,
        },
      },
      discoverySweep: { consecutiveOverloads: 0, backoffUntilMs: 0, lastEpoch: 0 },
    },
  },
})

const issue = (): { uuid: string; key: string; path: string } => ({
  uuid: 'AgentWorkforce/factory#268',
  key: '268',
  path: '/github/issues/AgentWorkforce__factory/268.json',
})

const agent = (role: 'implementer' | 'reviewer'): Record<string, unknown> => ({
  name: role,
  role,
  capability: 'spawn:codex',
  task: role,
  repo: 'AgentWorkforce/factory',
})

const decision = (): Record<string, unknown> => ({
  issue: issue(),
  routes: [{ repo: 'AgentWorkforce/factory', rationale: 'test' }],
  scope: 'single',
  implementers: [agent('implementer')],
  reviewer: agent('reviewer'),
  thin: false,
  confidence: 'high',
  rationale: 'test',
})
