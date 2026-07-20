type GithubRepositoryIdentity = {
  owner?: string
  name: string
}

/**
 * Compare a configured GitHub repository route with a canonical PR repository.
 * Bare route names inherit the deployment's configured organization elsewhere,
 * so they may match the repository component of an owner/name identity. When
 * both identities include owners, the owners must also match.
 */
export const githubRepositoriesMatch = (left: string, right: string): boolean => {
  const leftIdentity = githubRepositoryIdentity(left)
  const rightIdentity = githubRepositoryIdentity(right)
  if (!leftIdentity || !rightIdentity || leftIdentity.name !== rightIdentity.name) return false
  return !leftIdentity.owner || !rightIdentity.owner || leftIdentity.owner === rightIdentity.owner
}

const githubRepositoryIdentity = (value: string): GithubRepositoryIdentity | undefined => {
  const normalized = value.trim().replace(/\.git$/iu, '').toLowerCase()
  if (!normalized) return undefined
  const slash = normalized.match(/^([^/]+)\/([^/]+)$/u)
  if (slash) return { owner: slash[1], name: slash[2]! }
  const flat = normalized.match(/^(.+)__(.+)$/u)
  if (flat) return { owner: flat[1], name: flat[2]! }
  if (normalized.includes('/')) return undefined
  return { name: normalized }
}
