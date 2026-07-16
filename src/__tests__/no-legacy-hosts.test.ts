import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

// The pre-agentrelay.com gateway/api hosts are dead. The engine host is
// cast.agentrelay.com and lives in the SDK default, never in factory code.
describe('legacy host invariant', () => {
  it('no source file references a legacy relaycast host', () => {
    // Assembled so this file does not trip its own invariant.
    const legacyHostSuffix = ['relaycast', 'dev'].join('.')
    const offenders: string[] = []
    for (const file of walk(join(import.meta.dirname, '..'))) {
      if (readFileSync(file, 'utf8').includes(legacyHostSuffix)) {
        offenders.push(file)
      }
    }
    expect(offenders).toEqual([])
  })
})

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) {
      yield* walk(path)
    } else if (/\.(ts|mts|cts|js|mjs|cjs|json)$/.test(entry.name)) {
      yield path
    }
  }
}
