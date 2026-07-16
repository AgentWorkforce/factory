import type { Logger } from './ports/system'

const REDACTED = '[Redacted]'
const CIRCULAR = '[Circular]'
const UNSERIALIZABLE = '[Unserializable]'
const MAX_DEPTH = 6
const MAX_FIELDS = 100
const MAX_ARRAY_ITEMS = 100
const MAX_STRING_LENGTH = 32_000
const SAFE_ERROR_STACKS = new WeakMap<Error, string>()
const NATIVE_REGEXP_SOURCE_GETTER = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')?.get
const NATIVE_REGEXP_FLAG_GETTERS = [
  ['d', Object.getOwnPropertyDescriptor(RegExp.prototype, 'hasIndices')?.get],
  ['g', Object.getOwnPropertyDescriptor(RegExp.prototype, 'global')?.get],
  ['i', Object.getOwnPropertyDescriptor(RegExp.prototype, 'ignoreCase')?.get],
  ['m', Object.getOwnPropertyDescriptor(RegExp.prototype, 'multiline')?.get],
  ['s', Object.getOwnPropertyDescriptor(RegExp.prototype, 'dotAll')?.get],
  ['u', Object.getOwnPropertyDescriptor(RegExp.prototype, 'unicode')?.get],
  ['v', Object.getOwnPropertyDescriptor(RegExp.prototype, 'unicodeSets')?.get],
  ['y', Object.getOwnPropertyDescriptor(RegExp.prototype, 'sticky')?.get],
] as const

const SENSITIVE_FIELD = /authorization|cookie|credential|password|passwd|privatekey|secret|token|apikey/iu

interface NormalizationState {
  readonly seen: WeakSet<object>
  fieldsRemaining: number
}

/**
 * Convert logger metadata to a bounded JSON-safe value.
 *
 * Error's useful built-in fields are non-enumerable, so they are copied
 * deliberately. Other objects retain only their enumerable data properties,
 * matching JSON.stringify's normal visibility without invoking getters or
 * toJSON hooks. Sensitive-looking fields and Error causes are not exposed.
 */
export function normalizeLogValue(value: unknown): unknown {
  try {
    return normalizeValue(value, 0, {
      seen: new WeakSet<object>(),
      fieldsRemaining: MAX_FIELDS,
    })
  } catch {
    return UNSERIALIZABLE
  }
}

/** Wrap a Logger so every metadata argument is safe for structured sinks. */
export function normalizeLogger(logger: Logger): Logger {
  const wrap = (level: keyof Logger): Logger[typeof level] => {
    let sink: Logger[typeof level]
    try {
      sink = logger[level]
    } catch {
      return undefined
    }
    if (typeof sink !== 'function') return undefined
    return (message: string, ...args: unknown[]) => {
      sink.call(logger, message, ...args.map(normalizeLogValue))
    }
  }

  return {
    debug: wrap('debug'),
    info: wrap('info'),
    warn: wrap('warn'),
    error: wrap('error'),
  }
}

/** Serialize one log value without allowing it to throw or disappear. */
export function stringifyLogValue(value: unknown): string {
  try {
    return JSON.stringify(normalizeLogValue(value)) ?? UNSERIALIZABLE
  } catch {
    return UNSERIALIZABLE
  }
}

/** Attach a stack assembled from already-normalized diagnostics. */
export function setSafeErrorStack(error: Error, stack: string): void {
  Object.defineProperty(error, 'stack', {
    configurable: true,
    writable: true,
    value: stack,
  })
  SAFE_ERROR_STACKS.set(error, stack)
}

function normalizeValue(value: unknown, depth: number, state: NormalizationState): unknown {
  if (typeof value === 'string') return boundedString(value)
  if (value === null || typeof value === 'boolean') return value
  if (typeof value === 'number') return Number.isFinite(value) ? value : String(value)
  if (typeof value === 'bigint') return `${value}n`
  if (typeof value === 'undefined') return '[undefined]'
  if (typeof value === 'symbol') return `[${String(value)}]`
  if (typeof value === 'function') return '[Function]'
  if (typeof value !== 'object') return String(value)

  if (state.seen.has(value)) return CIRCULAR
  if (depth >= MAX_DEPTH) return '[MaxDepth]'
  state.seen.add(value)

  if (value instanceof Error) return normalizeError(value, depth, state)
  if (value instanceof Date) {
    try {
      return Date.prototype.toISOString.call(value)
    } catch {
      return '[Invalid Date]'
    }
  }
  if (value instanceof RegExp) {
    try {
      if (!NATIVE_REGEXP_SOURCE_GETTER) return '[RegExp]'
      const source = NATIVE_REGEXP_SOURCE_GETTER.call(value)
      const flags = NATIVE_REGEXP_FLAG_GETTERS
        .filter(([, getter]) => getter?.call(value))
        .map(([flag]) => flag)
        .join('')
      return `/${source}/${flags}`
    } catch {
      return '[RegExp]'
    }
  }

  if (Array.isArray(value)) return normalizeArray(value, depth, state)

  return normalizeObject(value, depth, state)
}

function normalizeError(error: Error, depth: number, state: NormalizationState): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  let descriptors: PropertyDescriptorMap
  try {
    descriptors = getOwnErrorDescriptors(error)
  } catch {
    return { name: 'Error', message: 'Error' }
  }
  const name = safeDataField(error, descriptors, 'name')
  const message = safeDataField(error, descriptors, 'message')
  const displayFieldHasAccessor = hasAccessorField(error, descriptors, 'name') || hasAccessorField(error, descriptors, 'message')
  const markedSafeStack = SAFE_ERROR_STACKS.get(error)
  const stack = markedSafeStack ?? (
    displayFieldHasAccessor || hasCustomPrepareStackTrace()
      ? undefined
      : safeStackField(error)
  )
  const code = safeDataField(error, descriptors, 'code')

  result.name = typeof name === 'string' && name ? boundedString(name) : 'Error'
  result.message = typeof message === 'string' && message ? boundedString(message) : result.name
  if (typeof stack === 'string' && stack) result.stack = boundedString(stack)
  else if (!displayFieldHasAccessor) {
    // Stack descriptors are runtime-specific: inspecting one can materialize
    // V8's lazy stack and execute Error.prepareStackTrace on Node 20. Keep a
    // safe diagnostic header whenever the full stack cannot be read without
    // invoking user code.
    result.stack = `${result.name}: ${result.message}`
  }
  if (isSafeErrorCode(code)) result.code = code

  appendEnumerableProperties(result, descriptors, depth, state, new Set(['name', 'message', 'stack', 'code', 'cause']))
  return result
}

function getOwnErrorDescriptors(error: Error): PropertyDescriptorMap {
  const descriptors: PropertyDescriptorMap = {}
  for (const key of Reflect.ownKeys(error)) {
    if (typeof key !== 'string' || key === 'stack') continue
    const descriptor = Object.getOwnPropertyDescriptor(error, key)
    if (descriptor) descriptors[key] = descriptor
  }
  return descriptors
}

function safeStackField(error: Error): unknown {
  let descriptor: PropertyDescriptor | undefined
  try {
    descriptor = Object.getOwnPropertyDescriptor(error, 'stack')
  } catch {
    return undefined
  }
  if (!descriptor) return undefined
  if ('value' in descriptor) return descriptor.value

  const nativeGetter = getNativeErrorStackGetter()
  if (!nativeGetter || descriptor.get !== nativeGetter) return undefined
  try {
    return nativeGetter.call(error)
  } catch {
    return undefined
  }
}

let nativeErrorStackGetter: (() => string) | undefined
let nativeErrorStackGetterResolved = false

function getNativeErrorStackGetter(): (() => string) | undefined {
  if (hasCustomPrepareStackTrace()) return undefined
  if (nativeErrorStackGetterResolved) return nativeErrorStackGetter
  nativeErrorStackGetterResolved = true
  try {
    nativeErrorStackGetter = Object.getOwnPropertyDescriptor(new Error(), 'stack')?.get
  } catch {
    nativeErrorStackGetter = undefined
  }
  return nativeErrorStackGetter
}

function normalizeArray(value: unknown[], depth: number, state: NormalizationState): unknown[] | string {
  let descriptors: PropertyDescriptorMap
  try {
    descriptors = Object.getOwnPropertyDescriptors(value) as unknown as PropertyDescriptorMap
  } catch {
    return UNSERIALIZABLE
  }
  const rawLength = descriptors.length && 'value' in descriptors.length ? descriptors.length.value : 0
  const length = typeof rawLength === 'number' && Number.isSafeInteger(rawLength) && rawLength >= 0 ? rawLength : 0
  const visibleLength = Math.min(length, MAX_ARRAY_ITEMS)
  const items: unknown[] = []
  for (let index = 0; index < visibleLength; index += 1) {
    if (state.fieldsRemaining <= 0) {
      items.push('[FieldLimit]')
      return items
    }
    state.fieldsRemaining -= 1
    const descriptor = descriptors[String(index)]
    if (!descriptor) {
      items.push(null)
      continue
    }
    if (!('value' in descriptor)) {
      items.push('[Getter]')
    } else {
      items.push(normalizeValue(descriptor.value, depth + 1, state))
    }
  }
  if (length > MAX_ARRAY_ITEMS) items.push(`[${length - MAX_ARRAY_ITEMS} more items]`)
  return items
}

function normalizeObject(value: object, depth: number, state: NormalizationState): Record<string, unknown> | string {
  let descriptors: PropertyDescriptorMap
  try {
    descriptors = Object.getOwnPropertyDescriptors(value)
  } catch {
    return UNSERIALIZABLE
  }
  const result: Record<string, unknown> = {}
  appendEnumerableProperties(result, descriptors, depth, state, new Set(['toJSON']))
  return result
}

function appendEnumerableProperties(
  target: Record<string, unknown>,
  descriptors: PropertyDescriptorMap,
  depth: number,
  state: NormalizationState,
  excluded: ReadonlySet<string>,
): void {
  for (const [key, descriptor] of Object.entries(descriptors)) {
    if (!descriptor.enumerable || excluded.has(key)) continue
    // Match JSON object semantics for values it normally omits. Top-level
    // logger arguments still receive explicit placeholders from normalizeValue.
    if (
      'value' in descriptor &&
      (typeof descriptor.value === 'undefined' || typeof descriptor.value === 'symbol' || typeof descriptor.value === 'function')
    ) continue
    if (state.fieldsRemaining <= 0) {
      target.__truncated__ = '[FieldLimit]'
      break
    }
    state.fieldsRemaining -= 1
    if (isSensitiveField(key)) {
      target[key] = REDACTED
    } else if (!('value' in descriptor)) {
      target[key] = '[Getter]'
    } else {
      target[key] = normalizeValue(descriptor.value, depth + 1, state)
    }
  }
}

function safeDataField(error: Error, ownDescriptors: PropertyDescriptorMap, key: string): unknown {
  const descriptor = findPropertyDescriptor(error, ownDescriptors, key)
  if (!descriptor) return undefined
  if ('value' in descriptor) return descriptor.value
  return undefined
}

function hasCustomPrepareStackTrace(): boolean {
  let current: object | null = Error
  for (let depth = 0; current && depth < MAX_DEPTH; depth += 1) {
    try {
      const descriptor = Object.getOwnPropertyDescriptor(current, 'prepareStackTrace')
      if (descriptor) return !('value' in descriptor) || descriptor.value !== undefined
      current = Object.getPrototypeOf(current)
    } catch {
      return true
    }
  }
  return false
}

function hasAccessorField(error: Error, ownDescriptors: PropertyDescriptorMap, key: string): boolean {
  const descriptor = findPropertyDescriptor(error, ownDescriptors, key)
  return Boolean(descriptor && !('value' in descriptor))
}

function findPropertyDescriptor(
  error: Error,
  ownDescriptors: PropertyDescriptorMap,
  key: string,
): PropertyDescriptor | undefined {
  const own = ownDescriptors[key]
  if (own) return own

  let current: object | null
  try {
    current = Object.getPrototypeOf(error)
  } catch {
    return undefined
  }
  for (let depth = 0; current && depth < MAX_DEPTH; depth += 1) {
    try {
      const descriptor = Object.getOwnPropertyDescriptor(current, key)
      if (descriptor) return descriptor
      current = Object.getPrototypeOf(current)
    } catch {
      return undefined
    }
  }
  return undefined
}

function isSafeErrorCode(value: unknown): value is string | number {
  return typeof value === 'string' || (typeof value === 'number' && Number.isFinite(value))
}

function isSensitiveField(key: string): boolean {
  return SENSITIVE_FIELD.test(key.replace(/[^a-z0-9]/giu, '').toLowerCase())
}

function boundedString(value: string): string {
  if (value.length <= MAX_STRING_LENGTH) return value
  return `${value.slice(0, MAX_STRING_LENGTH)}...[truncated ${value.length - MAX_STRING_LENGTH} chars]`
}
