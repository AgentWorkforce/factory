import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'

import { describe, expect, it } from 'vitest'

const ROOT = join(import.meta.dirname, '../..')
const STAGE = join(ROOT, 'scripts/relayfile-flush-failure-canary.sh')
const VERIFY = join(ROOT, 'scripts/verify-relayfile-flush-failure-evidence.mjs')
const PAYLOAD_BYTES = 12_582_912

function stubPayloadWriter(root: string): string {
  const bin = join(root, 'bin')
  spawnSync('mkdir', ['-p', bin])
  writeFileSync(join(bin, 'head'), `#!/usr/bin/env bash\n/usr/bin/head -c "$2" /dev/zero\n`)
  chmodSync(join(bin, 'head'), 0o755)
  return bin
}

function verify(evidence: unknown) {
  return spawnSync(process.execPath, [VERIFY], {
    encoding: 'utf8',
    input: JSON.stringify(evidence),
  })
}

describe('Relayfile flush-failure live canary', () => {
  it('fails closed without a provider mount', () => {
    const result = spawnSync('bash', [STAGE], {
      encoding: 'utf8',
      env: { ...process.env, RELAYFILE_MOUNT: '' },
    })
    expect(result.status).toBe(2)
    expect(result.stderr).toContain('RELAYFILE_MOUNT is not set')
  })

  it('stages exactly 12 MiB beneath the provider mount', () => {
    const root = mkdtempSync(join(tmpdir(), 'flush-canary-'))
    const mount = join(root, 'mount')
    spawnSync('mkdir', ['-p', mount])
    const result = spawnSync('bash', [STAGE], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${stubPayloadWriter(root)}:${process.env.PATH}`, RELAYFILE_MOUNT: mount },
    })
    const found = spawnSync('find', [mount, '-name', 'payload.bin', '-type', 'f'], { encoding: 'utf8' })
    const [payload] = found.stdout.trim().split('\n').filter(Boolean)
    expect(result.status).toBe(0)
    expect(readFileSync(payload)).toHaveLength(PAYLOAD_BYTES)
    expect(result.stdout).toContain(`${PAYLOAD_BYTES} incompressible bytes`)
    expect(readFileSync(STAGE, 'utf8')).toContain('/dev/urandom')
  })

  it('accepts fatal flush evidence observed within one sweep cycle', () => {
    const result = verify({
      step: {
        stepStatus: { status: 'failed', error: 'Failed to flush: payload_too_large' },
        lastError: { type: 'RelayfileFlushError' },
        failedAtMs: 1_000,
      },
      readinessReconcile: { state: 'retrying', lastError: 'RelayfileFlushError', lastFailureAtMs: 60_000, intervalMs: 60_000 },
    })
    expect(result.status).toBe(0)
    expect(result.stdout).toContain('canary PROVEN')
  })

  it.each([
    ['successful step', { stepStatus: 'succeeded', error: '' }],
    ['silent stall', {
      step: { stepStatus: { status: 'failed', error: 'Failed to flush' }, lastError: { type: 'RelayfileFlushError' } },
      readinessReconcile: { state: 'stalled', lastError: null },
    }],
    ['late sweep', {
      step: { stepStatus: { status: 'failed', error: 'payload_too_large' }, lastError: { type: 'RelayfileFlushError' }, failedAtMs: 1_000 },
      readinessReconcile: { state: 'failed', lastError: 'RelayfileFlushError', lastFailureAtMs: 61_001, intervalMs: 60_000 },
    }],
  ])('rejects %s evidence', (_case, evidence) => {
    const result = verify(evidence)
    expect(result.status).not.toBe(0)
    expect(result.stderr).toContain('NOT PROVEN')
  })
})
