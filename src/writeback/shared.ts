export const parseJsonContent = (content: unknown): unknown => {
  if (typeof content !== 'string') {
    return content
  }

  return JSON.parse(content)
}

export const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined

export const wrappedPayload = (content: unknown): Record<string, unknown> => {
  const record = asRecord(parseJsonContent(content)) ?? {}
  return asRecord(record.payload) ?? record
}

export const stableHash = (input: string): string => {
  let hash = 0x811c9dc5
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }

  return (hash >>> 0).toString(36)
}

export const safePathSegment = (input: string): string =>
  input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    || 'factory'

export const trimToLines = (text: string, maxLines: number): string =>
  text.split(/\r?\n/).slice(0, maxLines).join('\n')
