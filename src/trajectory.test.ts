import { describe, expect, it } from 'vitest'

import {
  canonicalTrajectorySessionRef,
  renderTrajectoryPointer,
  stripTrajectoryPointers,
  trajectoryPointerFromBody,
  trajectorySessionRefFromBody,
} from './trajectory'

describe('trajectory replay pointer', () => {
  const sessionRef = '0198b179-c6c2-7e63-9177-4ef52f56c192'

  it('renders and parses the ruled three-key pointer for a canonical resolver input', () => {
    const rendered = renderTrajectoryPointer({
      workUnitId: 'AgentWorkforce/factory#260',
      workUnitSurface: 'github',
      sessionRef,
    })

    expect(rendered).toBe(
      `<!-- trajectory: work_unit_id=AgentWorkforce/factory#260 work_unit_surface=github session_ref=${sessionRef} -->`,
    )
    expect(trajectoryPointerFromBody(rendered)).toEqual({
      workUnitId: 'AgentWorkforce/factory#260',
      workUnitSurface: 'github',
      sessionRef,
    })
    expect(trajectorySessionRefFromBody(rendered)).toBe(sessionRef)
  })

  it.each([
    undefined,
    '',
    'unknown-session-v3b',
    'missing',
    'ar-260-impl-factory',
    '00000000-0000-0000-0000-000000000000',
    'unsafe --> comment',
  ])('does not render unavailable input %j as replayable', (unavailableRef) => {
    const rendered = renderTrajectoryPointer({
      workUnitId: 'AR-260',
      workUnitSurface: 'linear',
      sessionRef: unavailableRef,
    })

    expect(canonicalTrajectorySessionRef(unavailableRef)).toBeUndefined()
    expect(rendered).toContain('session_ref=missing -->')
    expect(trajectoryPointerFromBody(rendered)).toBeUndefined()
    expect(rendered).not.toContain('relay session replay')
  })

  it('never bakes a replay availability or retention claim into the PR body', () => {
    const rendered = renderTrajectoryPointer({
      workUnitId: 'AR-260',
      workUnitSurface: 'linear',
      sessionRef,
    })

    expect(rendered).not.toContain('relay session replay')
    expect(rendered).not.toMatch(/retained|retention|expires|available/iu)
  })

  it('refuses conflicting pointers and strips inherited markers', () => {
    const first = renderTrajectoryPointer({
      workUnitId: 'AR-1',
      workUnitSurface: 'linear',
      sessionRef,
    })
    const second = renderTrajectoryPointer({
      workUnitId: 'AR-1',
      workUnitSurface: 'linear',
      sessionRef: '0198b179-c6c2-7e63-9177-4ef52f56c197',
    })

    expect(trajectoryPointerFromBody(`${first}\n${second}`)).toBeUndefined()
    expect(stripTrajectoryPointers(`body\n\n${first}\n${second}`)).toBe('body')
  })

  it('rejects an unsafe work-unit token before emitting an HTML comment', () => {
    expect(() => renderTrajectoryPointer({
      workUnitId: 'AR-1 --> leaked',
      workUnitSurface: 'linear',
      sessionRef,
    })).toThrow(/comment-safe token/u)
  })
})
