import { describe, expect, it } from 'vitest'

import { isAgentAlreadyGoneOnRelease } from './release-error'

describe('isAgentAlreadyGoneOnRelease', () => {
  it('recognises the canonical Relay 404 agent_not_found shape', () => {
    const relayError = {
      name: 'RelayError',
      message: 'Agent "ar-9-impl-sandbox" not found',
      code: 'not_found',
      retryable: false,
      statusCode: 404,
      rawCode: 'agent_not_found',
      status: 404,
    }
    expect(isAgentAlreadyGoneOnRelease(relayError)).toBe(true)
  })

  it('accepts the fallback shape (404 + code=not_found without rawCode)', () => {
    // A middleware that re-throws with the HTTP status preserved but drops
    // the SDK-specific rawCode must still classify — otherwise the fix
    // depends on which layer surfaces the failure.
    const rethrown = { statusCode: 404, code: 'not_found', message: 'not found' }
    expect(isAgentAlreadyGoneOnRelease(rethrown)).toBe(true)
  })

  it('rejects a 404 on a different code (e.g. workspace_not_found)', () => {
    // 404 alone is not sufficient — a not-found response for a different
    // resource must not silently succeed a release.
    const workspaceMissing = {
      statusCode: 404,
      code: 'workspace_not_found',
      rawCode: 'workspace_not_found',
    }
    expect(isAgentAlreadyGoneOnRelease(workspaceMissing)).toBe(false)
  })

  it('rejects a 503 host-unavailable (retryable path)', () => {
    // The ar-1540-babysit-relay case: retryable transport failure. Must NOT
    // classify as gone — the caller should keep retrying with backoff.
    const hostUnavailable = {
      name: 'RelayError',
      code: 'transport_error',
      retryable: true,
      statusCode: 503,
      rawCode: 'agent_host_unavailable',
    }
    expect(isAgentAlreadyGoneOnRelease(hostUnavailable)).toBe(false)
  })

  it('rejects a generic 500 without the not_found code', () => {
    expect(
      isAgentAlreadyGoneOnRelease({ statusCode: 500, code: 'internal_error' }),
    ).toBe(false)
  })

  it('rejects null / undefined / primitive errors safely', () => {
    expect(isAgentAlreadyGoneOnRelease(null)).toBe(false)
    expect(isAgentAlreadyGoneOnRelease(undefined)).toBe(false)
    expect(isAgentAlreadyGoneOnRelease('string error')).toBe(false)
    expect(isAgentAlreadyGoneOnRelease(0)).toBe(false)
    expect(isAgentAlreadyGoneOnRelease(new Error('bare error'))).toBe(false)
  })

  it('rejects an object without any of the discriminator fields', () => {
    expect(isAgentAlreadyGoneOnRelease({ message: 'oops' })).toBe(false)
    expect(isAgentAlreadyGoneOnRelease({ statusCode: 404 })).toBe(false)
    expect(isAgentAlreadyGoneOnRelease({ code: 'not_found' })).toBe(false)
  })
})
