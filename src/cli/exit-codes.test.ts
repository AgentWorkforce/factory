import { describe, expect, it } from 'vitest'

import { MountAuthScopeError } from '../mount/mount-auth-error'
import type { DispatchResult, IterationReport } from '../types'
import {
  FACTORY_EXIT,
  exitCodeForDispatchResult,
  exitCodeForError,
  exitCodeForIterationReport,
  exitCodeForIterationReports,
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

// The claim race is thrown from deep inside the orchestrator and is not
// exported as a class. Reproduce it the way the CLI actually receives it:
// whatever `factory.dispatch()` rejected with.
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

  it('keeps the most severe code across a loop of reports', () => {
    expect(exitCodeForIterationReports([])).toBe(FACTORY_EXIT.OK)
    expect(exitCodeForIterationReports([iterationReport(), iterationReport()])).toBe(FACTORY_EXIT.OK)
    expect(exitCodeForIterationReports([
      iterationReport(),
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
    ])).toBe(FACTORY_EXIT.RETRYABLE)
    expect(exitCodeForIterationReports([
      iterationReport({ discoveryDeferred: 'sweep-in-flight' }),
      iterationReport({ error: { message: 'boom' } }),
      iterationReport(),
    ])).toBe(FACTORY_EXIT.FAILED)
  })
})
