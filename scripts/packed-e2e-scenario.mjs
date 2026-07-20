import assert from 'node:assert/strict'
import { FactoryConfigSchema } from '@agent-relay/factory'
import {
  createHostedFactory,
  InMemoryHostedFactoryStateStore,
} from '@agent-relay/factory/hosted'

const checks = ['public-exports-import']
const workspaceId = 'factory-e2e-workspace'
const issue = {
  uuid: 'factory-e2e-issue-1',
  key: 'AR-E2E-1',
  title: '[factory-e2e] Verify the packaged control plane',
  description: [
    'Implement and verify the complete packaged Factory lifecycle from ready-issue discovery',
    'through deterministic fleet dispatch, duplicate suppression, terminal completion reconciliation,',
    'merge-gate evaluation, and idempotent provider writeback using only public package exports.',
  ].join(' '),
  stateId: 'ready-for-agent',
  state: { name: 'Ready for Agent' },
  labels: ['factory', 'cloud', 'agent:single'],
  team: 'AR',
  path: '/linear/issues/AR-E2E-1__factory-e2e-issue-1.json',
  raw: {
    id: 'factory-e2e-issue-1',
    identifier: 'AR-E2E-1',
    title: '[factory-e2e] Verify the packaged control plane',
    labels: ['factory', 'cloud', 'agent:single'],
    team: { key: 'AR' },
  },
}
const config = FactoryConfigSchema.parse({
  workspaceId,
  batchSize: 2,
  triage: { maxImplementers: 2 },
  repos: {
    byLabel: { cloud: 'AgentWorkforce/cloud' },
    default: 'AgentWorkforce/cloud',
  },
  agentCapabilities: {
    implementer: 'spawn:codex',
    reviewer: 'spawn:codex',
    babysitter: 'spawn:codex',
  },
  safety: {
    requireTitlePrefix: '[factory-e2e]',
    requireLabel: 'factory',
    requireTeamKey: 'AR',
  },
})
let nowMs = Date.parse('2026-07-20T12:00:00.000Z')
const state = new InMemoryHostedFactoryStateStore({ now: () => nowMs })
const spawned = []
const invocationStatus = new Map()
const writebacks = { clarification: [], dispatched: [], completed: [] }
const ports = {
  discovery: { discoverReady: async () => [structuredClone(issue), structuredClone(issue)] },
  state,
  fleet: {
    async spawn(input) {
      assert.ok(input.invocationId, 'hosted fleet spawn must carry a deterministic invocation ID')
      spawned.push(structuredClone(input))
      invocationStatus.set(input.invocationId, { invocationId: input.invocationId, status: 'dispatched' })
      return { name: input.name, invocationId: input.invocationId, sessionRef: `e2e:${input.name}` }
    },
  },
  completions: {
    getInvocation: async ({ invocationId }) => invocationStatus.get(invocationId) ?? null,
  },
  writeback: {
    requestClarification: async (input) => writebacks.clarification.push(structuredClone(input)),
    dispatched: async (input) => writebacks.dispatched.push(structuredClone(input)),
    completed: async (input) => writebacks.completed.push(structuredClone(input)),
  },
  now: () => new Date(nowMs),
}
const factory = createHostedFactory({
  workspaceId,
  ownerId: 'packed-e2e-runner',
  config,
  maxIssuesPerRun: 10,
}, ports)

const dispatched = await factory.runOnce()
assert.deepEqual(dispatched.dispatched, ['AR-E2E-1'])
assert.equal(dispatched.discovered, 1, 'duplicate discovery entries must collapse by issue UUID')
assert.equal(spawned.length, 2, 'single scope must dispatch implementer plus reviewer')
assert.equal(new Set(spawned.map(({ invocationId }) => invocationId)).size, 2)
assert.equal(writebacks.dispatched.length, 1)
checks.push('discover-triage-dispatch-writeback')

const duplicateSweep = await factory.runOnce()
assert.equal(spawned.length, 2, 'repeat discovery must not duplicate fleet spawns')
assert.ok(duplicateSweep.skipped.some(({ issueKey, reason }) =>
  issueKey === issue.key && reason === 'already running'
))
assert.equal(writebacks.dispatched.length, 1, 'dispatch writeback must be idempotent')
checks.push('at-least-once-deduplication')

nowMs += 5_000
for (const spawn of spawned) {
  invocationStatus.set(spawn.invocationId, {
    invocationId: spawn.invocationId,
    status: 'completed',
    completedAt: new Date(nowMs).toISOString(),
    output: `${spawn.name} completed`,
  })
}
const reconciled = await factory.runOnce()
assert.deepEqual(reconciled.reconciled, ['AR-E2E-1'])
const terminal = await state.getIssue(workspaceId, issue.uuid)
assert.equal(terminal?.phase, 'complete')
assert.equal(terminal?.mergeGate?.status, 'ready')
assert.equal(writebacks.completed.length, 1)
checks.push('completion-merge-gate-writeback')

const replayed = await factory.ingestCompletion({
  invocationId: spawned[0].invocationId,
  status: 'completed',
  completedAt: new Date(nowMs).toISOString(),
})
assert.equal(replayed.status, 'terminal')
assert.equal(writebacks.completed.length, 1, 'terminal completion replay must not duplicate writeback')
checks.push('terminal-replay-idempotency')

const fenceState = new InMemoryHostedFactoryStateStore({ now: () => nowMs })
let releaseDiscovery
let discoveryEntered
const entered = new Promise((resolveEntered) => { discoveryEntered = resolveEntered })
const blockedDiscovery = new Promise((resolveDiscovery) => { releaseDiscovery = resolveDiscovery })
const fencePorts = {
  ...ports,
  state: fenceState,
  discovery: {
    async discoverReady() {
      discoveryEntered()
      await blockedDiscovery
      return []
    },
  },
}
const firstHost = createHostedFactory({ workspaceId, ownerId: 'host-a', config }, fencePorts)
const secondHost = createHostedFactory({ workspaceId, ownerId: 'host-b', config }, fencePorts)
const firstRun = firstHost.runOnce()
await entered
assert.equal((await secondHost.runOnce()).status, 'fenced')
releaseDiscovery()
assert.equal((await firstRun).status, 'completed')
checks.push('active-active-fencing')

process.stdout.write(`${JSON.stringify({ result: 'passed', checks })}\n`)
