import { TieredTriage } from '../triage/tiered'
import { isInFactoryScope } from '../safety/factory-scope'
import { createFactoryCloudEventV1 } from '../observability/events'

import type {
  FactoryCloudEventInputV1,
  FactoryCloudReleaseReasonV1,
} from '../observability/events'
import type { AgentSpec } from '../ports/fleet'
import type { RepoMapEntry, TriageDecision } from '../types'
import type {
  HostedFactory,
  HostedFactoryCompletion,
  HostedFactoryCompletionResult,
  HostedFactoryInvocation,
  HostedFactoryIssueRecord,
  HostedFactoryLease,
  HostedFactoryMergeGateResult,
  HostedFactoryOptions,
  HostedFactoryPorts,
  HostedFactoryRunReport,
  HostedFactoryWritebackRecord,
} from './types'

const DEFAULT_LEASE_TTL_MS = 5 * 60_000
const DEFAULT_MAX_ISSUES_PER_RUN = 100
const TERMINAL_INVOCATION_STATUSES = new Set(['completed', 'failed', 'cancelled'])
const ACTIVE_ISSUE_PHASES = new Set(['dispatching', 'running', 'merge-gate'])

class HostedFactoryLeaseLostError extends Error {
  constructor() {
    super('hosted Factory workspace lease was lost')
    this.name = 'HostedFactoryLeaseLostError'
  }
}

export class HostedFactoryLoop implements HostedFactory {
  readonly #options: Required<Pick<HostedFactoryOptions, 'leaseTtlMs' | 'maxIssuesPerRun'>> & HostedFactoryOptions
  readonly #ports: HostedFactoryPorts
  readonly #triage: NonNullable<HostedFactoryPorts['triage']>
  readonly #now: () => Date
  #operationSequence = 0

  constructor(options: HostedFactoryOptions, ports: HostedFactoryPorts) {
    if (!options.workspaceId.trim()) throw new Error('hosted Factory requires a workspaceId')
    if (!options.ownerId.trim()) throw new Error('hosted Factory requires a unique ownerId')
    if (options.config.workspaceId && options.config.workspaceId !== options.workspaceId) {
      throw new Error('hosted Factory workspaceId must match config.workspaceId')
    }
    this.#options = {
      ...options,
      leaseTtlMs: options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS,
      maxIssuesPerRun: options.maxIssuesPerRun ?? DEFAULT_MAX_ISSUES_PER_RUN,
    }
    this.#ports = ports
    this.#triage = ports.triage ?? new TieredTriage()
    this.#now = ports.now ?? (() => new Date())
  }

  async runOnce(): Promise<HostedFactoryRunReport> {
    const report = emptyReport()
    const claim = await this.#claim('sweep')
    if (!claim.acquired) {
      return { ...report, status: 'fenced' }
    }

    let lease = claim.lease
    try {
      lease = await this.#reconcileAll(lease, report)
      lease = await this.#renew(lease)
      const issues = dedupeIssues(await this.#ports.discovery.discoverReady({
        workspaceId: this.#options.workspaceId,
        limit: this.#options.maxIssuesPerRun,
      })).slice(0, this.#options.maxIssuesPerRun)
      report.discovered = issues.length

      let records = await this.#ports.state.listIssues(this.#options.workspaceId)
      let activeCount = records.filter((record) => ACTIVE_ISSUE_PHASES.has(record.phase)).length
      for (const issue of issues) {
        lease = await this.#renew(lease)
        let record = records.find((candidate) => candidate.issue.uuid === issue.uuid)
        if (record?.phase === 'complete' || record?.phase === 'blocked') {
          report.skipped.push({ issueKey: issue.key, reason: `already ${record.phase}` })
          continue
        }
        if (record?.phase === 'running' || record?.phase === 'merge-gate') {
          report.skipped.push({ issueKey: issue.key, reason: `already ${record.phase}` })
          continue
        }
        if (!isInFactoryScope(issue, this.#options.config.safety)) {
          report.skipped.push({ issueKey: issue.key, reason: 'outside configured Factory safety scope' })
          continue
        }

        try {
          if (record?.phase === 'dispatching' && record.decision) {
            lease = await this.#dispatchRecord(record, lease, report)
            records = replaceRecord(records, record)
            activeCount = records.filter((candidate) => ACTIVE_ISSUE_PHASES.has(candidate.phase)).length
            continue
          }

          if (!record && activeCount >= this.#options.config.batchSize) {
            report.skipped.push({ issueKey: issue.key, reason: 'hosted Factory batch is full' })
            continue
          }

          const now = this.#timestamp()
          const previousPhase = record?.phase
          const created = !record
          record = record
            ? { ...record, issue: structuredClone(issue), phase: 'triaging', updatedAt: now }
            : {
                workspaceId: this.#options.workspaceId,
                issue: structuredClone(issue),
                phase: 'triaging',
                invocations: [],
                createdAt: now,
                updatedAt: now,
              }
          await this.#save(record, lease)
          if (created) {
            await this.#reportLifecycle(record, 'run.started')
          }
          const decision = await this.#triage.triage(issue, {
            config: this.#options.config,
            repoMap: repoMapFromConfig(this.#options.config),
          })
          report.triaged.push(issue.key)
          lease = await this.#renew(lease)

          const clarificationReason = triageEscalationReason(decision)
          if (clarificationReason) {
            record = {
              ...record,
              decision: structuredClone(decision),
              phase: 'awaiting-clarification',
              clarificationReason,
              updatedAt: this.#timestamp(),
            }
            await this.#save(record, lease)
            if (previousPhase !== 'awaiting-clarification') {
              await this.#reportLifecycle(record, 'run.phase_changed', {
                previousPhase: previousPhase ?? 'triaging',
              })
              await this.#reportLifecycle(record, 'run.waiting')
              await this.#reportLifecycle(record, 'clarification.requested', {
                component: 'writeback',
                operation: 'request_clarification',
              })
            }
            lease = await this.#ensureWriteback(record, lease, 'clarification')
            report.awaitingClarification.push(issue.key)
            records = replaceRecord(records, record)
            continue
          }

          const invocations = dispatchSpecs(decision).map((spec) => ({
            invocationId: hostedFactoryInvocationId(this.#options.workspaceId, decision, spec),
            spec: structuredClone(spec),
            status: 'pending' as const,
          }))
          if (invocations.length === 0) {
            record = {
              ...record,
              decision: structuredClone(decision),
              phase: 'awaiting-clarification',
              clarificationReason: 'Factory triage produced no dispatchable agent or workflow.',
              invocations,
              updatedAt: this.#timestamp(),
            }
            await this.#save(record, lease)
            if (previousPhase !== 'awaiting-clarification') {
              await this.#reportLifecycle(record, 'run.phase_changed', {
                previousPhase: previousPhase ?? 'triaging',
              })
              await this.#reportLifecycle(record, 'run.waiting')
              await this.#reportLifecycle(record, 'clarification.requested', {
                component: 'writeback',
                operation: 'request_clarification',
              })
            }
            lease = await this.#ensureWriteback(record, lease, 'clarification')
            report.awaitingClarification.push(issue.key)
            records = replaceRecord(records, record)
            continue
          }

          record = {
            ...record,
            decision: structuredClone(decision),
            phase: 'dispatching',
            clarificationReason: undefined,
            invocations,
            updatedAt: this.#timestamp(),
          }
          await this.#save(record, lease)
          await this.#reportLifecycle(record, 'run.phase_changed', {
            previousPhase: previousPhase ?? 'triaging',
          })
          for (const invocation of record.invocations) {
            await this.#reportAgent(record, invocation, 'agent.planned')
          }
          activeCount += 1
          lease = await this.#dispatchRecord(record, lease, report)
          records = replaceRecord(records, record)
        } catch (error) {
          if (error instanceof HostedFactoryLeaseLostError) throw error
          report.errors.push({ issueKey: issue.key, message: errorMessage(error) })
          if (record) {
            await this.#reportLifecycle(record, 'factory.failure', {
              component: 'orchestrator',
              operation: 'issue_sweep',
              errorClass: telemetryErrorClass(error),
              retryable: true,
            })
          }
          this.#ports.logger?.error?.('[factory:hosted] issue sweep failed', {
            workspaceId: this.#options.workspaceId,
            issueKey: issue.key,
            error: errorMessage(error),
          })
        }
      }
      return report
    } catch (error) {
      if (error instanceof HostedFactoryLeaseLostError) {
        return { ...report, status: 'lease-lost' }
      }
      report.errors.push({ message: errorMessage(error) })
      return report
    } finally {
      await this.#release(lease)
    }
  }

  async ingestCompletion(completion: HostedFactoryCompletion): Promise<HostedFactoryCompletionResult> {
    const claim = await this.#claim('completion')
    if (!claim.acquired) {
      return { status: 'fenced', invocationId: completion.invocationId }
    }
    let lease = claim.lease
    let record: HostedFactoryIssueRecord | null = null
    try {
      record = await this.#ports.state.findIssueByInvocation(
        this.#options.workspaceId,
        completion.invocationId,
      )
      if (!record) return { status: 'not-found', invocationId: completion.invocationId }
      if (isTerminalIssue(record)) {
        return {
          status: 'terminal',
          invocationId: completion.invocationId,
          issueKey: record.issue.key,
          phase: record.phase,
        }
      }
      const invocation = record.invocations.find(
        (candidate) => candidate.invocationId === completion.invocationId,
      )
      const wasTerminal = invocation ? isTerminalInvocation(invocation) : false
      applyCompletion(record, completion, this.#timestamp())
      await this.#save(record, lease)
      if (invocation && !wasTerminal && isTerminalInvocation(invocation)) {
        await this.#reportAgent(record, invocation, 'agent.exited')
      }
      if (record.invocations.every(isTerminalInvocation)) {
        lease = await this.#finishRecord(record, lease)
      }
      return {
        status: isTerminalIssue(record) ? 'terminal' : 'updated',
        invocationId: completion.invocationId,
        issueKey: record.issue.key,
        phase: record.phase,
      }
    } catch (error) {
      if (error instanceof HostedFactoryLeaseLostError) {
        return { status: 'fenced', invocationId: completion.invocationId }
      }
      if (record) {
        await this.#reportLifecycle(record, 'factory.failure', {
          component: 'orchestrator',
          operation: 'completion_ingest',
          errorClass: telemetryErrorClass(error),
          retryable: true,
        })
      }
      throw error
    } finally {
      await this.#release(lease)
    }
  }

  async #reconcileAll(
    initialLease: HostedFactoryLease,
    report: HostedFactoryRunReport,
  ): Promise<HostedFactoryLease> {
    let lease = initialLease
    const records = await this.#ports.state.listIssues(this.#options.workspaceId)
    for (const record of records) {
      try {
        if (record.phase === 'awaiting-clarification') {
          lease = await this.#ensureWriteback(record, lease, 'clarification')
          continue
        }
        if (record.phase === 'dispatching' && record.decision) {
          lease = await this.#dispatchRecord(record, lease, report)
          continue
        }
        if (record.phase !== 'running' && record.phase !== 'merge-gate') continue

        lease = await this.#renew(lease)
        if (record.phase === 'running') {
          lease = await this.#ensureWriteback(record, lease, 'dispatch')
        }
        let changed = false
        const exited: HostedFactoryInvocation[] = []
        for (const invocation of record.invocations) {
          if (isTerminalInvocation(invocation)) continue
          const completion = await this.#ports.completions.getInvocation({
            workspaceId: this.#options.workspaceId,
            invocationId: invocation.invocationId,
          })
          if (!completion || completion.status === invocation.status) continue
          applyCompletion(record, completion, this.#timestamp())
          if (isTerminalInvocation(invocation)) exited.push(invocation)
          changed = true
        }
        if (changed) {
          await this.#save(record, lease)
          for (const invocation of exited) {
            await this.#reportAgent(record, invocation, 'agent.exited')
          }
          report.reconciled.push(record.issue.key)
        }
        if (record.invocations.every(isTerminalInvocation)) {
          lease = await this.#finishRecord(record, lease)
        }
      } catch (error) {
        if (error instanceof HostedFactoryLeaseLostError) throw error
        report.errors.push({ issueKey: record.issue.key, message: errorMessage(error) })
        await this.#reportLifecycle(record, 'factory.failure', {
          component: 'orchestrator',
          operation: 'reconciliation',
          errorClass: telemetryErrorClass(error),
          retryable: true,
        })
        this.#ports.logger?.error?.('[factory:hosted] reconciliation failed', {
          workspaceId: this.#options.workspaceId,
          issueKey: record.issue.key,
          error: errorMessage(error),
        })
      }
    }
    return lease
  }

  async #dispatchRecord(
    record: HostedFactoryIssueRecord,
    initialLease: HostedFactoryLease,
    report: HostedFactoryRunReport,
  ): Promise<HostedFactoryLease> {
    let lease = initialLease
    for (const invocation of record.invocations) {
      if (invocation.status !== 'pending') continue
      lease = await this.#renew(lease)
      const result = await this.#ports.fleet.spawn({
        ...structuredClone(invocation.spec),
        invocationId: invocation.invocationId,
      })
      if (result.invocationId && result.invocationId !== invocation.invocationId) {
        throw new Error(
          `fleet changed deterministic invocation ID ${invocation.invocationId} to ${result.invocationId}`,
        )
      }
      invocation.status = 'dispatched'
      invocation.dispatchedAt = this.#timestamp()
      invocation.sessionRef = result.sessionRef
      invocation.node = result.node
      record.updatedAt = this.#timestamp()
      lease = await this.#renew(lease)
      await this.#save(record, lease)
      await this.#reportAgent(record, invocation, 'agent.spawned')
    }
    if (record.invocations.every((invocation) => invocation.status !== 'pending')) {
      const previousPhase = record.phase
      record.phase = 'running'
      record.updatedAt = this.#timestamp()
      await this.#save(record, lease)
      if (previousPhase !== record.phase) {
        await this.#reportLifecycle(record, 'run.phase_changed', { previousPhase })
      }
      lease = await this.#ensureWriteback(record, lease, 'dispatch')
      if (!report.dispatched.includes(record.issue.key)) report.dispatched.push(record.issue.key)
    }
    return lease
  }

  async #finishRecord(
    record: HostedFactoryIssueRecord,
    initialLease: HostedFactoryLease,
  ): Promise<HostedFactoryLease> {
    let lease = await this.#renew(initialLease)
    const previousPhase = record.phase
    record.phase = 'merge-gate'
    record.updatedAt = this.#timestamp()
    await this.#save(record, lease)
    if (previousPhase !== record.phase) {
      await this.#reportLifecycle(record, 'run.phase_changed', { previousPhase })
    }
    const mergeGate = await (this.#ports.mergeGate?.evaluate({
      workspaceId: this.#options.workspaceId,
      record: structuredClone(record),
    }) ?? Promise.resolve(defaultMergeGate(record, this.#timestamp())))
    lease = await this.#renew(lease)
    record.mergeGate = {
      ...structuredClone(mergeGate),
      ...(mergeGate.status === 'pending' ? {} : { decidedAt: mergeGate.decidedAt ?? this.#timestamp() }),
    }
    record.updatedAt = this.#timestamp()
    await this.#save(record, lease)
    if (mergeGate.status === 'pending') return lease

    lease = await this.#ensureWriteback(record, lease, 'completion')
    if (record.completionWriteback?.status === 'posted') {
      record.phase = mergeGate.status === 'ready' ? 'complete' : 'blocked'
      record.updatedAt = this.#timestamp()
      await this.#save(record, lease)
      await this.#reportLifecycle(
        record,
        record.phase === 'complete' ? 'run.succeeded' : 'run.failed',
        { previousPhase: 'merge-gate' },
      )
    }
    return lease
  }

  async #ensureWriteback(
    record: HostedFactoryIssueRecord,
    initialLease: HostedFactoryLease,
    kind: 'clarification' | 'dispatch' | 'completion',
  ): Promise<HostedFactoryLease> {
    const field = writebackField(kind)
    if (record[field]?.status === 'posted') return initialLease
    const attempts = (record[field]?.attempts ?? 0) + 1
    record[field] = { status: 'pending', attempts }
    record.updatedAt = this.#timestamp()
    await this.#save(record, initialLease)
    let lease = await this.#renew(initialLease)
    let applied = false
    try {
      const input = {
        workspaceId: this.#options.workspaceId,
        idempotencyKey: `factory:${this.#options.workspaceId}:${record.issue.uuid}:${kind}`,
        record: structuredClone(record),
      }
      if (kind === 'clarification') {
        await this.#ports.writeback.requestClarification({
          ...input,
          reason: record.clarificationReason ?? 'Factory needs more issue context.',
        })
      } else if (kind === 'dispatch') {
        await this.#ports.writeback.dispatched(input)
      } else {
        if (!record.mergeGate || record.mergeGate.status === 'pending') return lease
        await this.#ports.writeback.completed({ ...input, mergeGate: record.mergeGate })
      }
      lease = await this.#renew(lease)
      record[field] = { status: 'posted', attempts, postedAt: this.#timestamp() }
      applied = true
    } catch (error) {
      record[field] = { status: 'failed', attempts, error: errorMessage(error) }
      this.#ports.logger?.warn?.('[factory:hosted] writeback failed', {
        workspaceId: this.#options.workspaceId,
        issueKey: record.issue.key,
        kind,
        error: errorMessage(error),
      })
    }
    record.updatedAt = this.#timestamp()
    await this.#save(record, lease)
    if (applied) {
      await this.#reportLifecycle(record, 'writeback.applied', {
        component: 'writeback',
        operation: kind,
      })
    }
    return lease
  }

  async #report(
    input: Parameters<typeof createFactoryCloudEventV1>[0],
  ): Promise<void> {
    if (!this.#ports.reporter) return
    try {
      await this.#ports.reporter.report(createFactoryCloudEventV1(input, {
        now: this.#now,
      }))
    } catch (error) {
      this.#ports.logger?.warn?.('[factory:hosted] progress reporter rejected an event', {
        eventType: input.type,
        errorClass: telemetryErrorClass(error),
      })
    }
  }

  async #reportLifecycle(
    record: HostedFactoryIssueRecord,
    type: FactoryCloudEventInputV1['type'],
    options: {
      level?: FactoryCloudEventInputV1['level']
      previousPhase?: HostedFactoryIssueRecord['phase']
      component?: string
      operation?: string
      errorClass?: string
      errorCode?: string
      retryable?: boolean
    } = {},
  ): Promise<void> {
    await this.#report({
      type,
      level: options.level ?? (
        type === 'run.failed' || type === 'factory.failure' ? 'error' : 'info'
      ),
      runId: record.runId ?? hostedFactoryRunId(record.workspaceId, record.issue.uuid),
      phase: record.phase,
      status: hostedTelemetryRunStatus(record.phase, type),
      run: hostedTelemetryRun(record),
      attributes: {
        backend: 'hosted',
        component: options.component ?? 'orchestrator',
        operation: options.operation ?? 'save_lifecycle',
        previousPhase: options.previousPhase,
        errorClass: options.errorClass,
        errorCode: options.errorCode,
        retryable: options.retryable,
        trackedAgents: record.invocations.length,
      },
    })
  }

  async #reportAgent(
    record: HostedFactoryIssueRecord,
    invocation: HostedFactoryInvocation,
    type: 'agent.planned' | 'agent.spawned' | 'agent.exited',
  ): Promise<void> {
    await this.#report({
      type,
      level: invocation.status === 'failed' ? 'error' : 'info',
      runId: record.runId ?? hostedFactoryRunId(record.workspaceId, record.issue.uuid),
      phase: record.phase,
      status: hostedTelemetryRunStatus(record.phase, type),
      run: {
        ...hostedTelemetryRun(record),
        repository: invocation.spec.repo,
      },
      attributes: {
        backend: 'hosted',
        component: 'fleet',
        operation: type.slice('agent.'.length),
        agentRole: invocation.spec.role,
        invocationId: invocation.invocationId,
        nodeId: invocation.node,
        releaseReason: hostedInvocationReleaseReason(invocation),
        errorCode: invocation.status === 'failed'
          ? 'agent_failed'
          : invocation.status === 'cancelled'
            ? 'agent_cancelled'
            : undefined,
      },
    })
  }

  async #claim(operation: string) {
    this.#operationSequence += 1
    return await this.#ports.state.claimRunLease(
      this.#options.workspaceId,
      `${this.#options.ownerId}:${operation}:${this.#operationSequence}`,
      this.#options.leaseTtlMs,
    )
  }

  async #renew(lease: HostedFactoryLease): Promise<HostedFactoryLease> {
    const renewed = await this.#ports.state.renewRunLease(lease, this.#options.leaseTtlMs)
    if (!renewed) throw new HostedFactoryLeaseLostError()
    return renewed
  }

  async #save(record: HostedFactoryIssueRecord, lease: HostedFactoryLease): Promise<void> {
    record.runId ??= hostedFactoryRunId(record.workspaceId, record.issue.uuid)
    if (!await this.#ports.state.saveIssue(record, lease)) {
      throw new HostedFactoryLeaseLostError()
    }
  }

  async #release(lease: HostedFactoryLease): Promise<void> {
    try {
      await this.#ports.state.releaseRunLease(lease)
    } catch (error) {
      this.#ports.logger?.warn?.('[factory:hosted] workspace lease release failed', {
        workspaceId: this.#options.workspaceId,
        error: errorMessage(error),
      })
    }
  }

  #timestamp(): string {
    return this.#now().toISOString()
  }
}

export function createHostedFactory(
  options: HostedFactoryOptions,
  ports: HostedFactoryPorts,
): HostedFactory {
  return new HostedFactoryLoop(options, ports)
}

export function hostedFactoryInvocationId(
  workspaceId: string,
  decision: TriageDecision,
  spec: AgentSpec,
): string {
  const identity = [
    workspaceId,
    decision.issue.uuid,
    spec.role,
    spec.name,
    spec.repo,
    spec.workflow ?? '',
  ].join(':')
  return `factory:${stableHash(identity)}`
}

export function hostedFactoryRunId(workspaceId: string, issueUuid: string): string {
  return `factory:hosted:${stableHash(JSON.stringify([
    'agent-relay.factory.hosted-run.v1',
    workspaceId,
    issueUuid,
  ]))}`
}

function defaultMergeGate(
  record: HostedFactoryIssueRecord,
  decidedAt: string,
): HostedFactoryMergeGateResult {
  const failure = record.invocations.find((invocation) => invocation.status !== 'completed')
  return failure
    ? {
        status: 'blocked',
        reason: `${failure.spec.name} ended with ${failure.status}.`,
        decidedAt,
      }
    : {
        status: 'ready',
        reason: 'All hosted Factory invocations completed successfully.',
        decidedAt,
      }
}

function applyCompletion(
  record: HostedFactoryIssueRecord,
  completion: HostedFactoryCompletion,
  updatedAt: string,
): void {
  const invocation = record.invocations.find(
    (candidate) => candidate.invocationId === completion.invocationId,
  )
  if (!invocation) return
  // Invocation delivery is at-least-once and can arrive out of order. Once a
  // terminal observation wins, a late dispatched/failed/completed event must
  // not rewrite the merge-gate input.
  if (isTerminalInvocation(invocation)) return
  invocation.status = completion.status
  invocation.completedAt = completion.completedAt ?? (
    TERMINAL_INVOCATION_STATUSES.has(completion.status) ? updatedAt : undefined
  )
  invocation.output = completion.output
  invocation.error = completion.error
  record.updatedAt = updatedAt
}

function dispatchSpecs(decision: TriageDecision): AgentSpec[] {
  return decision.scope === 'workflow'
    ? (decision.workflow ? [decision.workflow] : [])
    : [...decision.implementers, decision.reviewer]
}

function triageEscalationReason(decision: TriageDecision): string | undefined {
  const reasons = [
    decision.confidence === 'low' ? 'low-confidence triage' : undefined,
    decision.thin ? 'thin issue context' : undefined,
  ].filter((reason): reason is string => Boolean(reason))
  return reasons.length > 0
    ? `${reasons.join(' and ')}${decision.rationale ? `: ${decision.rationale}` : ''}`
    : undefined
}

function repoMapFromConfig(config: HostedFactoryOptions['config']): RepoMapEntry[] {
  const entries: RepoMapEntry[] = []
  for (const [key, repo] of Object.entries(config.repos.byLabel)) {
    entries.push({ repo, clonePath: config.repos.clonePaths[repo], source: 'label', key })
  }
  for (const [key, repo] of Object.entries(config.repos.byProject)) {
    entries.push({ repo, clonePath: config.repos.clonePaths[repo], source: 'project', key })
  }
  for (const rule of config.repos.keywordRules) {
    entries.push({ repo: rule.repo, clonePath: config.repos.clonePaths[rule.repo], source: 'keyword', key: rule.pattern })
  }
  if (config.repos.default) {
    entries.push({
      repo: config.repos.default,
      clonePath: config.repos.clonePaths[config.repos.default],
      source: 'default',
    })
  }
  return entries
}

function writebackField(kind: 'clarification' | 'dispatch' | 'completion'):
  'clarificationWriteback' | 'dispatchWriteback' | 'completionWriteback' {
  if (kind === 'clarification') return 'clarificationWriteback'
  if (kind === 'dispatch') return 'dispatchWriteback'
  return 'completionWriteback'
}

function isTerminalInvocation(invocation: HostedFactoryInvocation): boolean {
  return TERMINAL_INVOCATION_STATUSES.has(invocation.status)
}

function isTerminalIssue(record: HostedFactoryIssueRecord): boolean {
  return record.phase === 'complete' || record.phase === 'blocked'
}

function hostedTelemetryRun(
  record: HostedFactoryIssueRecord,
): NonNullable<FactoryCloudEventInputV1['run']> {
  return {
    source: 'linear',
    repository: record.decision?.routes[0]?.repo ?? record.invocations[0]?.spec.repo,
    issueKey: record.issue.key,
    recipe: record.decision?.scope,
  }
}

function hostedTelemetryRunStatus(
  phase: HostedFactoryIssueRecord['phase'],
  type: FactoryCloudEventInputV1['type'],
): NonNullable<FactoryCloudEventInputV1['status']> {
  if (type === 'run.failed') return 'failed'
  if (type === 'run.succeeded') return 'succeeded'
  switch (phase) {
    case 'awaiting-clarification':
      return 'waiting'
    case 'blocked':
      return 'blocked'
    case 'complete':
      return 'succeeded'
    default:
      return 'running'
  }
}

function hostedInvocationReleaseReason(
  invocation: HostedFactoryInvocation,
): FactoryCloudReleaseReasonV1 | undefined {
  if (invocation.status === 'completed') return 'completed'
  if (invocation.status === 'failed' || invocation.status === 'cancelled') return 'other'
  return undefined
}

function telemetryErrorClass(error: unknown): string {
  const name = error instanceof Error ? error.name : ''
  return /^[A-Za-z][A-Za-z0-9]{0,63}(?:Error|Exception)$/u.test(name) ? name : 'Error'
}

function dedupeIssues<T extends { uuid: string }>(issues: T[]): T[] {
  return [...new Map(issues.map((issue) => [issue.uuid, issue])).values()]
}

function replaceRecord(
  records: HostedFactoryIssueRecord[],
  record: HostedFactoryIssueRecord,
): HostedFactoryIssueRecord[] {
  return [...records.filter((candidate) => candidate.issue.uuid !== record.issue.uuid), record]
}

function stableHash(input: string): string {
  let hash = 0xcbf29ce484222325n
  for (let index = 0; index < input.length; index += 1) {
    hash ^= BigInt(input.charCodeAt(index))
    hash = BigInt.asUintN(64, hash * 0x100000001b3n)
  }
  return hash.toString(36)
}

function emptyReport(): HostedFactoryRunReport {
  return {
    status: 'completed',
    discovered: 0,
    triaged: [],
    dispatched: [],
    reconciled: [],
    awaitingClarification: [],
    skipped: [],
    errors: [],
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
