import { describe, expect, it, vi } from 'vitest'

import {
  countVersionsBehind,
  isMeaningfullyBehind,
  readFactoryVersionInfo,
} from './version-info'

describe('Factory version information', () => {
  it('counts stable published releases newer than the running artifact', () => {
    const versions = Array.from({ length: 59 }, (_, patch) => `0.1.${patch}`)
    versions.push('0.1.59-beta.1')

    expect(countVersionsBehind('0.1.20', '0.1.58', versions)).toBe(38)
  })

  it('warns for meaningful drift without nagging for one patch', () => {
    const installedAt = '2026-08-14T12:00:00.000Z'
    expect(isMeaningfullyBehind({
      version: '0.1.20',
      latestVersion: '0.1.58',
      versionsBehind: 38,
      installedAt,
    })).toBe(true)
    expect(isMeaningfullyBehind({
      version: '0.1.57',
      latestVersion: '0.1.58',
      versionsBehind: 1,
      installedAt,
    })).toBe(false)
    expect(isMeaningfullyBehind({
      version: '0.1.58',
      latestVersion: '0.1.58',
      versionsBehind: 0,
      installedAt,
    })).toBe(false)
  })

  it('degrades to installed metadata when the registry is unreachable', async () => {
    const fetch = vi.fn(async () => {
      throw new Error('offline')
    }) as unknown as typeof globalThis.fetch

    await expect(readFactoryVersionInfo({ fetch, registryTimeoutMs: 10 })).resolves.toMatchObject({
      version: expect.stringMatching(/^\d+\.\d+\.\d+$/u),
      installedAt: expect.any(String),
    })
    expect(fetch).toHaveBeenCalledTimes(1)
  })
})
