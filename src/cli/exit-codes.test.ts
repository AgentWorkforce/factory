import { describe, expect, it } from 'vitest'

import { MountAuthScopeError } from '../mount/mount-auth-error'
import type { DispatchResult, IterationReport } from '../types'
import {
  FACTORY_EXIT,
  exitCodeForDispatchResult,
  exitCodeForError,
  exitCodeForIterationReport,
  exitCodeForLoopReports,
  exitCodeForRelayDispatch,
} from './exit-codes'

const issue = { key: 'AR-77', uuid: 'uuid-77', path: '/linear/issues/AR-77__uuid-77.json' }

const dispatchResult = (overrides: Partial<DispatchResult> = {}): DispatchResult => ({
  issue,
  agents: [],
  dryRun: false,
  ...overrides,
})

const iterationReport = (overrides: Partial<IterationReport> = {}): IterationReport => ({
  pulled: [],
  triaged: [],
  dispatched: [],
  skipped: [],
  dryRun: false,
  ...overrides,
})

// A deliberate LOOK-ALIKE: a plain Error wearing the real class's name and
// message. `LiveDispatchStateChangedError` is exported, so the CLI could match
// it by type; this fixture exists to prove that it does, and that a forgeable
// `error.name` is not enough to claim a retryable exit.
const liveStateChangedError = (): Error => {
  const error = new Error(`Live state changed before writeback for ${issue.key}`)
  error.name = 'LiveDispatchStateChangedError'
  return error
}

describe('exitCodeForError', () => {
  it('classifies a mount filesystem-scope shortfall as a refusal', () => {
    expect(exitCodeForError(new MountAuthScopeError('needs fs:read', { missingScope: 'fs:read' })))
      .toBe(FACTORY_EXIT.REFUSED)
  })

  it('classifies any other failure as a generic failure', () => {
    expect(exitCodeForError(new Error('gh unavailable'))).toBe(FACTORY_EXIT.FAILED)
    expect(exitCodeForError('not even an Error')).toBe(FACTORY_EXIT.FAILED)
    expect(exitCodeForError(undefined)).toBe(FACTORY_EXIT.FAILED)
  })

  it('does not classify a look-alike by name alone', () => {
    // A plain Error wearing the same `name` is not the orchestrator's typed
    // race. Matching on the name would let any caller forge a retryable exit.
    expect(exitCodeForError(liveStateChangedError())).toBe(FACTORY_EXIT.FAILED)
  })

  it('never returns OK — a thrown error always leaves the action unperformed', () => {
    for (const error of [new Error('boom'), new MountAuthScopeError('scope'), liveStateChangedError()]) {
      expect(exitCodeForError(error)).not.toBe(FACTORY_EXIT.OK)
    }
  })
})

describe('exitCodeForDispatchResult', () => {
  it('is OK when agents were actually placed', () => {
    expect(exitCodeForDispatchResult(dispatchResult({
      agents: [{ name: 'ar-77-impl', role: 'implementer' }],
    }))).toBe(FACTORY_EXIT.OK)
  })

  it('is OK for a dry run, whose requested action is the dry run itself', () => {
    expect(exitCodeForDispatchResult(dispatchResult({ dryRun: true }))).toBe(FACTORY_EXIT.OK)
  })

  it('is retryable for a capacity or dependency hold', () => {
    expect(exitCodeForDispatchResult(dispatchResult({ hold: { kind: 'capacity' } })))
      .toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForDispatchResult(dispatchResult({
      hold: { kind: 'dependency', blockers: ['AR-70'] },
    }))).toBe(FACTORY_EXIT.RETRYABLE)
  })

  it('is a refusal for a dependency cycle, which no amount of waiting clears', () => {
    expect(exitCodeForDispatchResult(dispatchResult({
      hold: { kind: 'dependency-cycle', cycle: ['AR-77', 'AR-78', 'AR-77'] },
    }))).toBe(FACTORY_EXIT.REFUSED)
  })

  it('is a refusal when nothing was dispatched and nothing is holding it', () => {
    // Queued or escalated: the issue is exactly where it started, and the
    // caller's dispatch did not happen.
    expect(exitCodeForDispatchResult(dispatchResult())).toBe(FACTORY_EXIT.REFUSED)
  })
})

describe('exitCodeForRelayDispatch', () => {
  const heldOnCapacity = dispatchResult({ hold: { kind: 'capacity' } })

  it('is OK when the run reached a complete terminal phase', () => {
    // The pre-wait result is an empty capacity hold, which on its own classifies
    // as retryable. The durable retry then dispatched and completed, so the run
    // DID perform the action — classifying the stale result would report
    // failure for a successful run.
    expect(exitCodeForDispatchResult(heldOnCapacity)).toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForRelayDispatch(heldOnCapacity, 'complete')).toBe(FACTORY_EXIT.OK)
  })

  it('fails when the run was abandoned', () => {
    expect(exitCodeForRelayDispatch(dispatchResult({
      agents: [{ name: 'ar-77-impl', role: 'implementer' }],
    }), 'abandoned')).toBe(FACTORY_EXIT.FAILED)
  })

  it('falls back to the dispatch result when no terminal phase was observed', () => {
    expect(exitCodeForRelayDispatch(heldOnCapacity, undefined)).toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForRelayDispatch(dispatchResult({
      agents: [{ name: 'ar-77-impl', role: 'implementer' }],
    }), undefined)).toBe(FACTORY_EXIT.OK)
  })
})

describe('exitCodeForIterationReport', () => {
  it('is OK for a clean sweep, including one that dispatched nothing', () => {
    expect(exitCodeForIterationReport(iterationReport())).toBe(FACTORY_EXIT.OK)
    expect(exitCodeForIterationReport(iterationReport({
      skipped: [{ issue, reason: 'queued or escalated' }],
    }))).toBe(FACTORY_EXIT.OK)
  })

  it('is a failure when the cycle recorded an error', () => {
    expect(exitCodeForIterationReport(iterationReport({ error: { message: 'discovery failed' } })))
      .toBe(FACTORY_EXIT.FAILED)
  })

  it('is retryable when another owner was already sweeping', () => {
    expect(exitCodeForIterationReport(iterationReport({ discoveryDeferred: 'sweep-in-flight' })))
      .toBe(FACTORY_EXIT.RETRYABLE)
  })

  it('is retryable when discovery failed and was absorbed (#406)', () => {
    // The sweep survived -- no `error` is recorded, because absorbing the
    // fault is what keeps one bad repository from aborting the pass -- so
    // without this the caller reads exit 0 for a sweep that enumerated
    // nothing. FAILED would overstate it: nothing refused, and a backend
    // fault clears on its own.
    expect(exitCodeForIterationReport(iterationReport({ discoveryFailed: 'issue-listing-failed' })))
      .toBe(FACTORY_EXIT.RETRYABLE)
    // Control: an ordinary empty sweep is still OK, so this is scoped to the
    // failure and is not "zero candidates now means retry".
    expect(exitCodeForIterationReport(iterationReport())).toBe(FACTORY_EXIT.OK)
    // Control: a recorded error still outranks it.
    expect(exitCodeForIterationReport(iterationReport({
      discoveryFailed: 'issue-listing-failed',
      error: { message: 'boom' },
    }))).toBe(FACTORY_EXIT.FAILED)
  })

  it('is retryable only when EVERY iteration failed discovery (#406)', () => {
    expect(exitCodeForLoopReports([
      iterationReport({ discoveryFailed: 'issue-listing-failed' }),
      iterationReport({ discoveryFailed: 'issue-listing-failed' }),
    ])).toBe(FACTORY_EXIT.RETRYABLE)
    // Mixed with a deferral: the loop still enumerated nothing.
    expect(exitCodeForLoopReports([
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
      iterationReport({ discoveryFailed: 'issue-listing-failed' }),
    ])).toBe(FACTORY_EXIT.RETRYABLE)
    // Control: one failed sweep among healthy ones is ordinary, not a retry.
    expect(exitCodeForLoopReports([
      iterationReport(),
      iterationReport({ discoveryFailed: 'issue-listing-failed' }),
    ])).toBe(FACTORY_EXIT.OK)
  })

  it('fails a bounded loop when any iteration recorded an error', () => {
    expect(exitCodeForLoopReports([])).toBe(FACTORY_EXIT.OK)
    expect(exitCodeForLoopReports([iterationReport(), iterationReport()])).toBe(FACTORY_EXIT.OK)
    expect(exitCodeForLoopReports([
      iterationReport(),
      iterationReport({ error: { message: 'boom' } }),
    ])).toBe(FACTORY_EXIT.FAILED)
  })

  it('is retryable when EVERY iteration was deferred and nothing ran', () => {
    // One deferral out of several is contention; all of them means the loop
    // performed nothing, which must not read as success.
    expect(exitCodeForLoopReports([
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
    ])).toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForLoopReports([iterationReport({ discoveryDeferred: 'sweep-in-flight' })]))
      .toBe(FACTORY_EXIT.RETRYABLE)
  })

  it('does not fail a bounded loop when only SOME iterations were deferred', () => {
    // `run-once` was asked for ONE sweep, so a deferral means it never ran.
    // A loop was asked for several, and losing one to another owner is
    // ordinary contention. Escalating it would make a healthy loop exit
    // non-zero — the same defect as exiting 0 on a refusal, pointed the
    // other way.
    expect(exitCodeForIterationReport(iterationReport({ discoveryDeferred: 'sweep-in-flight' })))
      .toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForLoopReports([
      iterationReport(),
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
    ])).toBe(FACTORY_EXIT.OK)
  })
})
