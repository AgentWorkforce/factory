export function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
}

export function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((entry): entry is string => typeof entry === 'string' && entry.trim().length > 0)
}

export function scopeStringList(scope: Record<string, unknown>, key: string): string[] {
  return stringList(scope[key])
}

export function dedupeStrings(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean))).sort()
}

export function sameStringList(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}

export function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.trim()
    return message || error.name || error.constructor.name || 'Error'
  }
  if (typeof error === 'string') {
    const message = error.trim()
    return message || 'empty string error'
  }
  if (isRecord(error)) {
    const message = error.message
    if (typeof message === 'string' && message.trim()) return message.trim()
    const reason = error.reason
    if (typeof reason === 'string' && reason.trim()) return reason.trim()
    const parts = [
      typeof error.name === 'string' && error.name.trim() ? error.name.trim() : null,
      typeof error.type === 'string' && error.type.trim() ? `type=${error.type.trim()}` : null,
      typeof error.code === 'string' && error.code.trim() ? `code=${error.code.trim()}` : typeof error.code === 'number' ? `code=${error.code}` : null,
      typeof error.status === 'string' && error.status.trim() ? `status=${error.status.trim()}` : typeof error.status === 'number' ? `status=${error.status}` : null,
      typeof error.statusCode === 'string' && error.statusCode.trim() ? `statusCode=${error.statusCode.trim()}` : typeof error.statusCode === 'number' ? `statusCode=${error.statusCode}` : null,
      typeof error.httpStatus === 'string' && error.httpStatus.trim() ? `httpStatus=${error.httpStatus.trim()}` : typeof error.httpStatus === 'number' ? `httpStatus=${error.httpStatus}` : null,
    ].filter((entry): entry is string => entry !== null)
    if (parts.length > 0) return parts.join(' ')
    const constructorName = error.constructor?.name
    if (constructorName && constructorName !== 'Object') return constructorName

    try {
      const json = JSON.stringify(error)
      if (json && json !== '{}') return json
    } catch {
      // Fall through to String(error).
    }
  }
  const text = String(error).trim()
  return text && text !== '[object Object]' ? text : 'unknown error'
}
