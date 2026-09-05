#!/usr/bin/env node
import { readFile } from 'node:fs/promises'

function reject(reasons) {
  for (const reason of reasons) process.stderr.write(`relayfile flush canary NOT PROVEN: ${reason}\n`)
  process.exitCode = 1
}

async function readInput(path) {
  if (path) return readFile(path, 'utf8')
  let body = ''
  process.stdin.setEncoding('utf8')
  for await (const chunk of process.stdin) body += chunk
  return body
}

let evidence
try {
  evidence = JSON.parse(await readInput(process.argv[2]))
} catch (error) {
  reject([`evidence is not JSON (${error.message})`])
  process.exit()
}

// Support both a direct step record and the envelopes used by /evidence.
const step = evidence.step ?? evidence.lastStep ?? evidence.stepMetadata ?? evidence
const rawStatus = step.stepStatus
const status = typeof rawStatus === 'string' ? rawStatus : rawStatus?.status
const statusError = typeof rawStatus === 'object' && rawStatus !== null ? rawStatus.error : undefined
const lastError = step.lastError
const lastErrorType = typeof lastError === 'object' && lastError !== null ? lastError.type : undefined
const lastErrorMessage = typeof lastError === 'string' ? lastError : lastError?.message
const combinedError = [statusError, step.error, lastErrorMessage].filter(Boolean).join(' ')
const reasons = []

if (status !== 'failed') reasons.push(`stepStatus is ${JSON.stringify(status)}, expected "failed"`)
if (lastErrorType !== 'RelayfileFlushError' && !combinedError.includes('relayfile flush failed after command exit 0')) {
  reasons.push('lastError does not identify RelayfileFlushError')
}
if (!/(payload_too_large|Failed to flush)/i.test(combinedError)) {
  reasons.push('step error contains neither payload_too_large nor Failed to flush')
}

const readiness = evidence.readinessReconcile ?? evidence.status?.readinessReconcile
if (readiness?.state === 'stalled' && readiness?.lastError == null) {
  reasons.push('readinessReconcile is silently stalled with lastError: null')
}

// When timestamps and cadence are present, prove the sweep observed the result
// within one cycle. Older evidence without these optional fields still proves
// the three fatal-error invariants above.
const failureAt = step.failedAtMs ?? step.completedAtMs ?? step.endedAtMs
const observedAt = readiness?.lastFailureAtMs ?? readiness?.lastCompletedAtMs
const intervalMs = readiness?.intervalMs
if ([failureAt, observedAt, intervalMs].every(Number.isFinite) && observedAt - failureAt > intervalMs) {
  reasons.push(`sweep observed failure after ${observedAt - failureAt}ms, exceeding one ${intervalMs}ms cycle`)
}

if (reasons.length) reject(reasons)
else process.stdout.write('relayfile flush canary PROVEN\n')
