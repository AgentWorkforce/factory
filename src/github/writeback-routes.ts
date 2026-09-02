/**
 * The writeback paths Relayfile's server-side GitHub adapter actually routes.
 *
 * Transcribed from the adapter's own route table, `resources` in
 * relayfile-adapters `packages/github/src/resources.ts`, which Factory does
 * not depend on at build time. A draft whose path matches none of these is
 * rejected by `buildGitHubWritebackRequest` with `Unsupported GitHub
 * writeback path` before any request reaches GitHub.
 *
 * Factory only ever authors the handful of shapes in
 * `RelayfileGithubConnectionWrite`, so this table cannot lag a route Factory
 * uses: a new adapter route is unusable until this surface writes a path for
 * it, and that change belongs in the same commit as the entry here.
 *
 * This exists because the alternative is what shipped. `#431`/`#434`: two
 * label paths the adapter has never routed were authored for months, accepted
 * by every local check, and refused remotely — so no dispatch ever labelled an
 * issue and several lanes read the label-less issues as "dispatch never
 * started".
 */
export const GITHUB_WRITEBACK_ROUTES: ReadonlyArray<{ name: string; pattern: RegExp }> = [
  { name: 'issues', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/issues(?:\/[^/]+(?:\.json)?)?$/u },
  {
    name: 'issue-comments',
    pattern: /^\/github\/repos\/[^/]+\/[^/]+\/issues\/[^/]+\/comments(?:\/[^/]+(?:\.json|\/meta\.json)?)?$/u,
  },
  { name: 'reviews', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/reviews(?:\/[^/]+(?:\.json)?)?$/u },
  { name: 'pull-requests', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/pull-requests(?:\/[^/]+(?:\.json)?)?$/u },
  { name: 'refs', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/refs(?:\/[^/]+(?:\.json)?)?$/u },
  { name: 'close-pull-request', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/close\.json$/u },
  { name: 'merge', pattern: /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[1-9]\d*(?:__[^/]+)?\/merge\.json$/u },
  {
    name: 'replies',
    pattern: /^\/github\/repos\/[^/]+\/[^/]+\/pulls\/[^/]+\/review-comments\/[^/]+\/replies(?:\/[^/]+(?:\.json)?)?$/u,
  },
]

/**
 * The adapter resource that would handle `path`, or `undefined` when nothing
 * routes it. Mirrors the adapter's own `findResourceByPath` normalisation: a
 * path that does not end in `.json` is matched with any trailing slash removed.
 */
export const githubWritebackRoute = (path: string): string | undefined => {
  const normalized = path.endsWith('.json') ? path : path.replace(/\/$/u, '')
  return GITHUB_WRITEBACK_ROUTES.find((route) => route.pattern.test(normalized))?.name
}

/**
 * Refuse an unroutable draft here, where the caller that built the path is
 * still on the stack, rather than 90 seconds later as an opaque confirmation
 * failure that the lifecycle retries forever as if it were transient.
 */
export const assertRoutedGithubWritebackPath = (path: string): void => {
  if (githubWritebackRoute(path)) return
  throw new Error(
    `Refusing to author an unroutable GitHub writeback path: ${path}. ` +
    `Relayfile's GitHub adapter routes only ${GITHUB_WRITEBACK_ROUTES.map((route) => route.name).join(', ')}.`,
  )
}
