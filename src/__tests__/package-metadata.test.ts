import { readFile } from 'node:fs/promises'

import { describe, expect, it } from 'vitest'

describe('published package metadata', () => {
  it('names the canonical GitHub repository used by npm provenance', async () => {
    const manifest = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'))

    expect(manifest.repository).toEqual({
      type: 'git',
      url: 'git+https://github.com/AgentWorkforce/software-garden.git',
    })
    expect(manifest.homepage).toBe('https://github.com/AgentWorkforce/software-garden#readme')
    expect(manifest.bugs).toEqual({ url: 'https://github.com/AgentWorkforce/software-garden/issues' })
  })
})
