import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { describe, expect, it } from 'vitest'

import {
  classifyMountAuthError,
  MountAuthScopeError,
  mountAuthRemediation,
  readMountAuthErrorFromState,
} from './mount-auth-error'

async function withTempStateFile(
  lastError: unknown,
  fn: (stateFilePath: string) => Promise<void> | void,
): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), 'mount-auth-error-test-'))
  try {
    const stateDir = join(dir, '.relay')
    await mkdir(stateDir, { recursive: true })
    const stateFilePath = join(stateDir, 'state.json')
    await writeFile(
      stateFilePath,
      JSON.stringify({ workspaceId: 'rw_test', lastReconcileAt: new Date().toISOString(), lastError }),
      'utf8',
    )
    await fn(stateFilePath)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}

describe('classifyMountAuthError', () => {
  it('extracts the missing scope from a 403 message', () => {
    const details = classifyMountAuthError('http 403 forbidden: missing required scope: fs:read')
    expect(details?.missingScope).toBe('fs:read')
  })

  it('matches a stalled bootstrap that reports forbidden without a named scope', () => {
    const details = classifyMountAuthError('bootstrap stalled for 200 cycles: 403 forbidden')
    expect(details).toBeDefined()
    expect(details?.missingScope).toBeUndefined()
  })

  it('handles the real bootstrap_stall_cycle_limit signature verbatim', () => {
    const details = classifyMountAuthError(
      'bootstrap stalled for 200 consecutive checkpoint-stable cycles (limit 20, cursor ""): http 403 forbidden: missing required scope: fs:read',
    )
    expect(details?.missingScope).toBe('fs:read')
  })

  it('does not misclassify a transient/unrelated failure', () => {
    expect(classifyMountAuthError('context deadline exceeded')).toBeUndefined()
    expect(classifyMountAuthError('relayfile mount did not become ready within 60000ms')).toBeUndefined()
    expect(classifyMountAuthError(undefined)).toBeUndefined()
    expect(classifyMountAuthError('')).toBeUndefined()
  })

  it('does not treat a bare 403 (no stall, no scope) as a scope failure', () => {
    // A one-off 403 without the stall signature or a named scope is ambiguous;
    // classification must not fire and trigger a terminal abort.
    expect(classifyMountAuthError('request returned 403')).toBeUndefined()
  })
})

describe('readMountAuthErrorFromState', () => {
  it('detects a scope shortfall recorded in state.json lastError.message', async () => {
    await withTempStateFile(
      {
        kind: 'bootstrap_stalled',
        code: 'bootstrap_stall_cycle_limit',
        message: 'http 403 forbidden: missing required scope: fs:read',
      },
      (stateFilePath) => {
        const details = readMountAuthErrorFromState(stateFilePath)
        expect(details?.missingScope).toBe('fs:read')
      },
    )
  })

  it('returns undefined when lastError is unrelated', async () => {
    await withTempStateFile(
      { kind: 'reconcile_error', message: 'context deadline exceeded' },
      (stateFilePath) => {
        expect(readMountAuthErrorFromState(stateFilePath)).toBeUndefined()
      },
    )
  })

  it('returns undefined when there is no lastError', async () => {
    await withTempStateFile(undefined, (stateFilePath) => {
      expect(readMountAuthErrorFromState(stateFilePath)).toBeUndefined()
    })
  })

  it('returns undefined for a missing state file', () => {
    expect(readMountAuthErrorFromState('/nonexistent/path/state.json')).toBeUndefined()
  })
})

describe('mountAuthRemediation', () => {
  it('names the missing scope when known', () => {
    expect(mountAuthRemediation({ missingScope: 'fs:read', detail: 'x' })).toContain('missing fs:read')
  })

  it('is still actionable without a scope', () => {
    const msg = mountAuthRemediation()
    expect(msg).toContain('Re-authenticate')
  })
})

describe('MountAuthScopeError', () => {
  it('carries the missing scope and is an Error', () => {
    const err = new MountAuthScopeError('boom', { missingScope: 'fs:read' })
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe('MountAuthScopeError')
    expect(err.missingScope).toBe('fs:read')
  })
})
