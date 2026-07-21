import { spawnSync } from 'node:child_process'
import { resolve } from 'node:path'

import { describe, expect, it } from 'vitest'

describe('packed E2E attestation', () => {
  it('rejects an attestation head that differs from the tested checkout', () => {
    const root = resolve(import.meta.dirname, '..')
    const result = spawnSync(process.execPath, [resolve(root, 'scripts/verify-packed-e2e.mjs')], {
      cwd: root,
      encoding: 'utf8',
      env: {
        ...process.env,
        FACTORY_E2E_HEAD_SHA: '0000000000000000000000000000000000000000',
      },
    })

    expect(result.status).not.toBe(0)
    expect(result.stderr).toContain(
      'FACTORY_E2E_HEAD_SHA must match the checkout being packed and tested',
    )
  })
})
