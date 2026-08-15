import { describe, expect, it } from 'vitest'

import {
  renderTrajectoryPointer,
  resolvableTrajectorySessionRef,
  stripTrajectoryPointers,
  trajectorySessionRefFromBody,
} from './trajectory'

describe('trajectory pointer', () => {
  it('renders the ruled format with a resolvable session reference', () => {
    expect(renderTrajectoryPointer({
      workUnitId: 'AgentWorkforce/factory#260',
      workUnitSurface: 'github',
      sessionRef: '0198b179-c6c2-7e63-9177-4ef52f56c192',
    })).toBe(
      '<!-- trajectory: work_unit_id=AgentWorkforce/factory#260 work_unit_surface=github session_ref=0198b179-c6c2-7e63-9177-4ef52f56c192 -->',
    )
  })

  it.each([
    undefined,
    '',
    'unknown-session-v3b',
    'placeholder',
    'missing',
    'unsafe --> comment',
  ])('discloses an unresolved ref instead of shipping placeholder %j', (sessionRef) => {
    expect(resolvableTrajectorySessionRef(sessionRef)).toBeUndefined()
    expect(renderTrajectoryPointer({
      workUnitId: 'AR-260',
      workUnitSurface: 'linear',
      sessionRef,
    })).toContain('session_ref=missing -->')
  })

  it('resolves only one unambiguous real ref and strips inherited pointers', () => {
    const old = '<!-- trajectory: work_unit_id=AgentWorkforce/relay#1476 work_unit_surface=github session_ref=unknown-session-v3b -->'
    const current = '<!-- trajectory: work_unit_id=AgentWorkforce/factory#260 work_unit_surface=github session_ref=0198b179-c6c2-7e63-9177-4ef52f56c192 -->'
    expect(trajectorySessionRefFromBody(`body\n\n${old}\n${current}`))
      .toBe('0198b179-c6c2-7e63-9177-4ef52f56c192')
    expect(stripTrajectoryPointers(`body\n\n${old}\n${current}`)).toBe('body')
  })

  it('refuses conflicting real refs during babysitter adoption', () => {
    const first = renderTrajectoryPointer({
      workUnitId: 'AR-1',
      workUnitSurface: 'linear',
      sessionRef: 'session-one',
    })
    const second = renderTrajectoryPointer({
      workUnitId: 'AR-1',
      workUnitSurface: 'linear',
      sessionRef: 'session-two',
    })
    expect(trajectorySessionRefFromBody(`${first}\n${second}`)).toBeUndefined()
  })
})
