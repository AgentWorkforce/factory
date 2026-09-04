#!/usr/bin/env node
import { readFile } from 'node:fs/promises'

function fail(message) {
  process.stderr.write(`flush-failure canary NOT PROVEN: ${message}\n`)
  process.exitCode = 1
}

const input = process.argv[2]
  ? await readFile(process.argv[2], 'utf8')
  : await new Promise((resolve, reject) => {
      let body = ''
      process.stdin.setEncoding('utf8')
      process.stdin.on('data', (chunk) => { body += chunk })
      process.stdin.on('end', () => resolve(body))
      process.stdin.on('error', reject)
    })

let evidence
try {
  evidence = JSON.parse(input)
} catch (error) {
  fail(`evidence is not JSON (${error.message})`)
  process.exit()
}

// Accept both the direct step object and an /evidence envelope containing it.
const step = evidence.step ?? evidence.lastStep ?? evidence
const status = typeof step.stepStatus === 'string' ? step.stepStatus : step.stepStatus?.status
const statusError = typeof step.stepStatus === 'object' ? step.stepStatus?.error : step.error
const lastErrorType = step.lastError?.type
const lastErrorMessage = typeof step.lastError === 'string' ? step.lastError : step.lastError?.message
const combinedError = [statusError, step.error, lastErrorMessage].filter(Boolean).join(' ')

if (status !== 'failed') fail(`stepStatus is ${JSON.stringify(status)}, expected "failed"`)
if (lastErrorType !== 'RelayfileFlushError' && !combinedError.includes('relayfile flush failed after command exit 0')) {
  fail(`lastError does not identify RelayfileFlushError`)
}
if (!/(payload_too_large|Failed to flush)/i.test(combinedError)) {
  fail(`step error contains neither payload_too_large nor Failed to flush`)
}

const readiness = evidence.readinessReconcile
if (readiness?.state === 'stalled' && readiness?.lastError == null) {
  fail('readinessReconcile is silently stalled with lastError: null')
}

if (!process.exitCode) process.stdout.write('flush-failure canary PROVEN\n')
