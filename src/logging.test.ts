import { describe, expect, it, vi } from 'vitest'

import { normalizeLogger, normalizeLogValue, stringifyLogValue } from './logging'

describe('log metadata normalization', () => {
  it('serializes a plain Error with its non-enumerable diagnostic fields', () => {
    const error = new Error('plain failure')

    expect(normalizeLogValue(error)).toMatchObject({
      name: 'Error',
      message: 'plain failure',
      stack: expect.stringContaining('Error: plain failure'),
    })
    expect(stringifyLogValue(error)).not.toBe('{}')
  })

  it('preserves safe structured error fields while redacting sensitive metadata and omitting causes', () => {
    class ProtocolError extends Error {
      name = 'HarnessDriverProtocolError'
      code = 'http_500'
      status = 500
      retryable = true
      data = { error: 'internal reply dropped', accessToken: 'do-not-log' }
      apiKey = 'do-not-log'
    }
    const error = new ProtocolError('broker request failed', { cause: new Error('secret upstream response') })
    Object.defineProperty(error, 'cause', { value: error.cause, enumerable: true })

    expect(normalizeLogValue(error)).toMatchObject({
      name: 'HarnessDriverProtocolError',
      message: 'broker request failed',
      stack: expect.stringContaining('ProtocolError: broker request failed'),
      code: 'http_500',
      status: 500,
      retryable: true,
      data: {
        error: 'internal reply dropped',
        accessToken: '[Redacted]',
      },
      apiKey: '[Redacted]',
    })
    expect(normalizeLogValue(error)).not.toHaveProperty('cause')
  })

  it('contains nested errors, cycles, BigInts, getters, and throwing toJSON hooks', () => {
    const metadata: Record<string, unknown> = {
      error: new Error('nested failure'),
      count: 12n,
      sibling: 'still visible',
      toJSON: () => {
        throw new Error('must not run')
      },
    }
    Object.defineProperty(metadata, 'broken', {
      enumerable: true,
      get: () => {
        throw new Error('must not run')
      },
    })
    metadata.self = metadata

    const normalized = normalizeLogValue(metadata)
    expect(normalized).toMatchObject({
      error: {
        name: 'Error',
        message: 'nested failure',
        stack: expect.stringContaining('Error: nested failure'),
      },
      count: '12n',
      sibling: 'still visible',
      broken: '[Getter]',
      self: '[Circular]',
    })
    expect(() => stringifyLogValue(metadata)).not.toThrow()
    expect(stringifyLogValue(metadata)).toContain('still visible')
  })

  it('does not invoke accessor-backed Error fields or array elements', () => {
    let errorGets = 0
    const error = new Error()
    for (const key of ['name', 'message', 'code']) {
      Object.defineProperty(error, key, {
        configurable: true,
        enumerable: true,
        get: () => {
          errorGets += 1
          return `getter-${key}`
        },
      })
    }
    const stackError = new Error('safe message')
    Object.defineProperty(stackError, 'stack', {
      configurable: true,
      get: () => {
        errorGets += 1
        return 'getter-stack'
      },
    })
    let arrayGets = 0
    const array: unknown[] = []
    Object.defineProperty(array, '0', {
      configurable: true,
      enumerable: true,
      get: () => {
        arrayGets += 1
        return 'getter-item'
      },
    })

    expect(normalizeLogValue(error)).toEqual({ name: 'Error', message: 'Error' })
    expect(normalizeLogValue(stackError)).toEqual({ name: 'Error', message: 'safe message' })
    expect(normalizeLogValue(array)).toEqual(['[Getter]'])
    expect(errorGets).toBe(0)
    expect(arrayGets).toBe(0)
  })

  it('uses intrinsic special-object conversion without calling instance hooks', () => {
    let dateCalls = 0
    const date = new Date(0)
    Object.defineProperty(date, 'toISOString', {
      value: () => {
        dateCalls += 1
        return 'unsafe date'
      },
    })
    let regexpCalls = 0
    const regexp = /factory/giu
    Object.defineProperty(regexp, 'toString', {
      value: () => {
        regexpCalls += 1
        return 'unsafe regexp'
      },
    })
    Object.defineProperty(regexp, 'source', {
      configurable: true,
      get: () => {
        regexpCalls += 1
        return 'unsafe-source'
      },
    })
    Object.defineProperty(regexp, 'flags', {
      configurable: true,
      get: () => {
        regexpCalls += 1
        return 'i'
      },
    })
    let functionNameGets = 0
    const fn = () => undefined
    Object.defineProperty(fn, 'name', {
      configurable: true,
      get: () => {
        functionNameGets += 1
        return 'unsafe function name'
      },
    })

    expect(normalizeLogValue(date)).toBe('1970-01-01T00:00:00.000Z')
    expect(normalizeLogValue(regexp)).toBe('/factory/giu')
    expect(normalizeLogValue(fn)).toBe('[Function]')
    expect({ dateCalls, regexpCalls, functionNameGets }).toEqual({
      dateCalls: 0,
      regexpCalls: 0,
      functionNameGets: 0,
    })
  })

  it('does not materialize a lazy stack through a custom Error.prepareStackTrace hook', () => {
    const error = new Error('hooked stack')
    const original = Object.getOwnPropertyDescriptor(Error, 'prepareStackTrace')
    let calls = 0
    Object.defineProperty(Error, 'prepareStackTrace', {
      configurable: true,
      value: () => {
        calls += 1
        return 'unsafe stack hook output'
      },
    })
    try {
      expect(normalizeLogValue(error)).toEqual({
        name: 'Error',
        message: 'hooked stack',
        stack: 'Error: hooked stack',
      })
      expect(calls).toBe(0)
    } finally {
      if (original) Object.defineProperty(Error, 'prepareStackTrace', original)
      else delete (Error as ErrorConstructor & { prepareStackTrace?: unknown }).prepareStackTrace
    }
  })

  it('applies the global field budget to branching nested arrays', () => {
    const branch = (depth: number): unknown => depth === 0
      ? 'leaf'
      : Array.from({ length: 5 }, () => branch(depth - 1))
    const normalized = normalizeLogValue(branch(6))
    const countNodes = (value: unknown): number => Array.isArray(value)
      ? 1 + value.reduce((count, child) => count + countNodes(child), 0)
      : 1

    expect(countNodes(normalized)).toBeLessThanOrEqual(210)
    expect(JSON.stringify(normalized)).toContain('[FieldLimit]')
  })

  it('normalizes every argument passed through an injected logger', () => {
    const warn = vi.fn()
    const logger = normalizeLogger({ warn })

    logger.warn?.('representative warning', new Error('raw failure'), {
      nested: new Error('nested failure'),
    })

    expect(warn).toHaveBeenCalledWith(
      'representative warning',
      expect.objectContaining({ message: 'raw failure', stack: expect.any(String) }),
      { nested: expect.objectContaining({ message: 'nested failure', stack: expect.any(String) }) },
    )
  })

  it('keeps the default console logger callable through the adapter', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    try {
      normalizeLogger(console).error?.('default console error', new Error('console failure'))

      expect(error).toHaveBeenCalledWith(
        'default console error',
        expect.objectContaining({ message: 'console failure', stack: expect.any(String) }),
      )
    } finally {
      error.mockRestore()
    }
  })
})
