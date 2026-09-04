import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'

import { describe, expect, it } from 'vitest'

const SCRIPT = join(import.meta.dirname, '../../scripts/relayfile-flush-failure-canary.sh')
const PAYLOAD_BYTES = 12_582_912

function stubCommands(root: string): string {
  const bin = join(root, 'bin')
  const mkdir = spawnSync('mkdir', ['-p', bin])
  if (mkdir.status !== 0) throw new Error(`failed to create stub directory: ${mkdir.stderr}`)

  writeFileSync(join(bin, 'date'), '#!/usr/bin/env bash\nprintf 1700000000\n')
  writeFileSync(
    join(bin, 'head'),
    `#!/usr/bin/env bash
if [[ "$1" != "-c" || "$2" != "${PAYLOAD_BYTES}" || "$3" != "/dev/urandom" ]]; then
  echo "unexpected head arguments: $*" >&2
  exit 64
fi
/usr/bin/head -c "$2" /dev/zero
`,
  )
  chmodSync(join(bin, 'date'), 0o755)
  chmodSync(join(bin, 'head'), 0o755)
  return bin
}

describe('relayfile flush-failure canary', () => {
  it('fails closed when it is not running with a provider mount', () => {
    const result = spawnSync('bash', [SCRIPT], {
      encoding: 'utf8',
      env: { ...process.env, RELAYFILE_MOUNT: '' },
    })

    expect(result.status).toBe(2)
    expect(result.stderr).toContain('RELAYFILE_MOUNT is not set')
  })

  it('stages exactly 12 MiB under one timestamped relayfile path', () => {
    const root = mkdtempSync(join(tmpdir(), 'relayfile-flush-canary-'))
    const mount = join(root, 'mount')
    const setup = spawnSync('mkdir', ['-p', mount])
    expect(setup.status).toBe(0)

    const result = spawnSync('bash', [SCRIPT], {
      encoding: 'utf8',
      env: {
        ...process.env,
        PATH: `${stubCommands(root)}:${process.env.PATH ?? ''}`,
        RELAYFILE_MOUNT: mount,
      },
    })

    const payload = join(mount, 'factory/canary/flush-oversize/1700000000/payload.bin')
    expect(result.status).toBe(0)
    expect(readFileSync(payload)).toHaveLength(PAYLOAD_BYTES)
    expect(result.stdout).toContain(`staged ${PAYLOAD_BYTES} incompressible bytes`)
    expect(result.stdout).toContain('executor must now report the expected flush failure')
  })

  it('keeps /dev/urandom as the payload source so the body cannot compress below the cap', () => {
    const source = readFileSync(SCRIPT, 'utf8')
    expect(source).toContain(`head -c "\${PAYLOAD_BYTES}" /dev/urandom > payload.bin`)
    expect(source).not.toMatch(/rm .*payload\.bin/)
  })
})
