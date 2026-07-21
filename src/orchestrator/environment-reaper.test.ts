import { describe, expect, it } from 'vitest'

import { FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION } from '../environments/kubernetes-environment'
import { reapFactoryEnvironmentsOnce } from './reaper'

describe('reapFactoryEnvironmentsOnce', () => {
  it('deletes expired managed namespaces and preserves live or unleased ones', async () => {
    const calls: string[][] = []
    const report = await reapFactoryEnvironmentsOnce({
      nowMs: Date.parse('2026-07-21T12:00:00.000Z'),
      runner: async (args) => {
        calls.push(args)
        if (args.includes('get')) {
          return {
            stderr: '',
            stdout: JSON.stringify({
              items: [
                namespace('expired', '2026-07-21T11:59:59.000Z'),
                namespace('live', '2026-07-21T12:00:01.000Z'),
                namespace('legacy'),
              ],
            }),
          }
        }
        return { stdout: '', stderr: '' }
      },
    })

    expect(report).toEqual({
      reaped: ['expired'],
      retained: [
        { namespace: 'live', expiresAt: '2026-07-21T12:00:01.000Z', reason: 'lease has not expired' },
        { namespace: 'legacy', reason: 'missing or invalid expiration lease' },
      ],
    })
    expect(calls[1]).toEqual([
      'delete', 'namespace', 'expired', '--ignore-not-found=true', '--wait=false',
    ])
  })
})

const namespace = (name: string, expiresAt?: string) => ({
  metadata: {
    name,
    annotations: expiresAt ? { [FACTORY_ENVIRONMENT_EXPIRES_ANNOTATION]: expiresAt } : {},
  },
})
