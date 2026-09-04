import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'

import { describe, expect, it } from 'vitest'

const ROOT = join(import.meta.dirname, '../..')
const STAGE = join(ROOT, 'scripts/relayfile-flush-failure-canary.sh')
const VERIFY = join(ROOT, 'scripts/verify-flush-failure-evidence.mjs')

describe('Relayfile flush-failure live canary', () => {
  it('stages exactly 12 MiB beneath one unique mount directory', () => {
    const mount = mkdtempSync(join(tmpdir(), 'flush-canary-'))
    const result = spawnSync('bash', [STAGE], {
      encoding: 'utf8',
      env: { ...process.env, RELAYFILE_MOUNT: mount },
    })
    expect(result.status).toBe(0)
    expect(result.stdout).toContain('12582912 incompressible bytes')
    const found = spawnSync('find', [mount, '-name', 'payload.bin', '-type', 'f'], { encoding: 'utf8' })
    const payloads = found.stdout.trim().split('\n').filter(Boolean)
    expect(payloads).toHaveLength(1)
    expect(readFileSync(payloads[0])).toHaveLength(12 * 1024 * 1024)
  })

  it('accepts the fatal flush evidence required by Fix B', () => {
    const dir = mkdtempSync(join(tmpdir(), 'flush-evidence-'))
    const path = join(dir, 'evidence.json')
    writeFileSync(path, JSON.stringify({
      step: {
        stepStatus: { status: 'failed', error: 'Failed to flush: payload_too_large' },
        lastError: {
          type: 'RelayfileFlushError',
          message: 'relayfile flush failed after command exit 0: http 413 payload_too_large',
        },
      },
      readinessReconcile: { state: 'retrying', lastError: 'RelayfileFlushError' },
    }))
    chmodSync(VERIFY, 0o755)
    const result = spawnSync(process.execPath, [VERIFY, path], { encoding: 'utf8' })
    expect(result.status).toBe(0)
    expect(result.stdout).toContain('canary PROVEN')
  })

  it.each([
    ['a successful step', { stepStatus: 'succeeded', lastError: null, error: '' }],
    ['a silent stall', {
      step: {
        stepStatus: { status: 'failed', error: 'Failed to flush' },
        lastError: { type: 'RelayfileFlushError' },
      },
      readinessReconcile: { state: 'stalled', lastError: null },
    }],
  ])('rejects %s', (_case, evidence) => {
    const result = spawnSync(process.execPath, [VERIFY], {
      encoding: 'utf8',
      input: JSON.stringify(evidence),
    })
    expect(result.status).not.toBe(0)
    expect(result.stderr).toContain('NOT PROVEN')
  })
})
