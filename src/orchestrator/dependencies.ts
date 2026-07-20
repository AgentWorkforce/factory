export interface DeclaredDependency {
  repo?: string
  number: number
  raw: string
}

export interface ResolvedDependency extends DeclaredDependency {
  repo: string
  identity: string
  label: string
}

const REPO = '[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+'
const REFERENCE = `(?:${REPO})?#(?:[1-9]\\d*)`
const BLOCKED_BY_LINE = new RegExp(`^Blocked by:[ \\t]*(${REFERENCE}(?:[ \\t]*,[ \\t]*${REFERENCE})*)[ \\t]*$`, 'u')
const REFERENCE_PARTS = new RegExp(`^(?:(${REPO}))?#([1-9]\\d*)$`, 'u')

/**
 * Parse only the explicit, line-oriented v1 dependency convention. A malformed
 * Blocked by line is ignored as a whole so incidental issue prose never gates
 * dispatch.
 */
export function parseBlockedBy(text: string | null | undefined): DeclaredDependency[] {
  if (!text) return []
  const dependencies = new Map<string, DeclaredDependency>()
  for (const line of text.split(/\r?\n/u)) {
    const match = BLOCKED_BY_LINE.exec(line)
    if (!match?.[1]) continue
    for (const rawReference of match[1].split(',')) {
      const raw = rawReference.trim()
      const parts = REFERENCE_PARTS.exec(raw)
      if (!parts) continue
      const number = Number(parts[2])
      if (!Number.isSafeInteger(number) || number <= 0) continue
      const repo = parts[1]
      const key = `${repo?.toLowerCase() ?? ''}#${number}`
      dependencies.set(key, { ...(repo ? { repo } : {}), number, raw })
    }
  }
  return [...dependencies.values()]
}

export function resolveDependency(
  dependency: DeclaredDependency,
  currentRepo: string | undefined,
): ResolvedDependency | undefined {
  const repo = dependency.repo ?? currentRepo
  if (!repo) return undefined
  return {
    ...dependency,
    repo,
    identity: dependencyIdentity(repo, dependency.number),
    label: `${repo}#${dependency.number}`,
  }
}

export const dependencyIdentity = (repo: string, number: number): string =>
  `${repo.toLowerCase()}#${number}`

/** Return the first dependency cycle reachable from start, including its closing node. */
export function findDependencyCycle(
  start: string,
  graph: ReadonlyMap<string, readonly string[]>,
): string[] | undefined {
  const visited = new Set<string>()
  const active = new Map<string, number>()
  const path: string[] = []

  const visit = (node: string): string[] | undefined => {
    const cycleStart = active.get(node)
    if (cycleStart !== undefined) return [...path.slice(cycleStart), node]
    if (visited.has(node)) return undefined
    visited.add(node)
    active.set(node, path.length)
    path.push(node)
    for (const dependency of graph.get(node) ?? []) {
      const cycle = visit(dependency)
      if (cycle) return cycle
    }
    path.pop()
    active.delete(node)
    return undefined
  }

  return visit(start)
}
