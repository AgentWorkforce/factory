import type { FactoryConfig } from '../config/schema'
import type { AgentSpec } from '../ports'
import type { IssueRef, LinearIssue, RepoMapEntry, TriageContext, TriageDecision, TriageEngine } from '../types'

type RouteSource = RepoMapEntry['source']
type Route = TriageDecision['routes'][number]

const DEFAULT_THIN_DESCRIPTION_LENGTH = 140
const MAX_IMPLEMENTERS = 2

const SURFACE_BUCKETS: Array<{ name: string; patterns: RegExp[] }> = [
  { name: 'ui', patterns: [/\bui\b/i, /\brenderer\b/i, /\bfrontend\b/i, /\breact\b/i, /\bxterm\b/i] },
  { name: 'main', patterns: [/\bmain\b/i, /\bbroker\b/i, /\bipc\b/i, /\bdaemon\b/i, /\bpty\b/i] },
  { name: 'slack', patterns: [/\bslack\b/i] },
  { name: 'linear', patterns: [/\blinear\b/i, /\bissue\b/i, /\bstate\b/i] },
  { name: 'github', patterns: [/\bgithub\b/i, /\bpr\b/i, /\bpull request\b/i, /\bchecks?\b/i] },
  { name: 'cloud', patterns: [/\bcloud\b/i, /\bdeploy\b/i, /\bserver\b/i, /\bapi\b/i] },
  { name: 'docs', patterns: [/\bdocs?\b/i, /\breadme\b/i, /\bguide\b/i, /\bdocumentation\b/i] },
]

const ACCEPTANCE_SIGNAL_PATTERNS = [
  /\b(fix|add|update|remove|implement|verify|test|ensure|support|handle|prevent|preserve|route|dispatch)\b/i,
  /\b(error|exception|fail(?:ure|ing|ed)?|bug|regression|timeout|crash)\b/i,
  /(?:^|\s)[\w./-]+\.(?:ts|tsx|js|jsx|json|md|css|scss|html|yml|yaml)\b/i,
  /`[^`]+`/,
]

export interface HeuristicTriageOptions {
  thinDescriptionLength?: number
}

export class HeuristicTriage implements TriageEngine {
  readonly #thinDescriptionLength: number

  constructor(opts: HeuristicTriageOptions = {}) {
    this.#thinDescriptionLength = opts.thinDescriptionLength ?? DEFAULT_THIN_DESCRIPTION_LENGTH
  }

  async triage(issue: LinearIssue, ctx: TriageContext): Promise<TriageDecision> {
    const routed = routeIssue(issue, ctx)
    const thin = isThinIssue(issue, this.#thinDescriptionLength)
    const surfaces = detectSurfaceBuckets(issue)
    const scope = routed.routes.length >= 2 || surfaces.length >= 2 ? 'team' : 'single'
    const confidence = routed.routes.length === 0 ? 'low' : 'high'

    return buildDecision({
      issue,
      config: ctx.config,
      routes: routed.routes,
      scope,
      thin,
      confidence,
      rationale: routed.rationale,
      scopeSlugs: routed.routes.length >= 2
        ? routed.routes.map((route) => slugFromRepo(route.repo))
        : surfaces,
    })
  }
}

export function buildDecision(input: {
  issue: LinearIssue
  config: FactoryConfig
  routes: Route[]
  scope: 'single' | 'team'
  thin: boolean
  confidence: 'high' | 'low'
  rationale: string
  scopeSlugs?: string[]
}): TriageDecision {
  const issueRef = issueRefFor(input.issue)
  const routes = dedupeRoutes(input.routes).slice(0, MAX_IMPLEMENTERS)
  const scope = routes.length >= 2 || input.scope === 'team' ? 'team' : 'single'
  const slugs = input.scopeSlugs ?? routes.map((route) => slugFromRepo(route.repo))
  const implementerAssignments = input.confidence === 'low' && routes.length === 0
    ? []
    : implementationAssignments(routes, scope, slugs)
  const implementers = implementerAssignments.map(({ route, slug }) => implementerSpec({
    issue: input.issue,
    config: input.config,
    route,
    scope,
    slug,
  }))

  return {
    issue: issueRef,
    routes,
    scope,
    implementers,
    reviewer: reviewerSpec(input.issue, input.config, routes[0]),
    thin: input.thin,
    confidence: input.confidence,
    rationale: input.rationale,
  }
}

export function routeIssue(issue: LinearIssue, ctx: TriageContext): { routes: Route[]; rationale: string } {
  const labelRoutes = routeByLabels(issue, ctx)
  if (labelRoutes.length > 0) {
    return { routes: labelRoutes, rationale: 'Matched repository from Linear label.' }
  }

  const projectRoutes = routeByProject(issue, ctx)
  if (projectRoutes.length > 0) {
    return { routes: projectRoutes, rationale: 'Matched repository from Linear project.' }
  }

  const keywordRoutes = routeByKeywords(issue, ctx)
  if (keywordRoutes.length > 0) {
    return { routes: keywordRoutes, rationale: 'Matched repository from keyword rule.' }
  }

  if (ctx.config.repos.default) {
    return {
      routes: [routeForRepo(ctx.config.repos.default, ctx, 'default', 'Default repository route.')],
      rationale: 'Used configured default repository.',
    }
  }

  return { routes: [], rationale: 'No repository route matched; escalate for human routing.' }
}

export function isThinIssue(issue: LinearIssue, minDescriptionLength = DEFAULT_THIN_DESCRIPTION_LENGTH): boolean {
  const description = issue.description.trim()
  return description.length < minDescriptionLength || !hasAcceptanceSignal(issue)
}

export function detectSurfaceBuckets(issue: LinearIssue): string[] {
  const haystack = issue.description
  return SURFACE_BUCKETS
    .filter((bucket) => bucket.patterns.some((pattern) => pattern.test(haystack)))
    .map((bucket) => bucket.name)
}

export function normalizeDecision(decision: TriageDecision, issue: LinearIssue, config: FactoryConfig): TriageDecision {
  return buildDecision({
    issue,
    config,
    routes: decision.routes.map((route) => ({
      ...route,
      clonePath: route.clonePath ?? config.repos.clonePaths[route.repo],
    })),
    scope: decision.scope,
    thin: decision.thin,
    confidence: decision.routes.length === 0 ? 'low' : decision.confidence,
    rationale: decision.rationale,
    scopeSlugs: decision.implementers.map((implementer) => implementer.name.replace(/^ar-\d+-impl-?/, '')).filter(Boolean),
  })
}

function routeByLabels(issue: LinearIssue, ctx: TriageContext): Route[] {
  const routes = issue.labels
    .map((label) => {
      const repo = findCaseInsensitive(ctx.config.repos.byLabel, label)
      return repo ? routeForRepo(repo, ctx, 'label', `Label "${label}" routes to ${repo}.`) : null
    })
    .filter((route): route is Route => route !== null)

  return dedupeRoutes(routes)
}

function routeByProject(issue: LinearIssue, ctx: TriageContext): Route[] {
  if (!issue.project) {
    return []
  }

  const repo = findCaseInsensitive(ctx.config.repos.byProject, issue.project)
  return repo ? [routeForRepo(repo, ctx, 'project', `Project "${issue.project}" routes to ${repo}.`)] : []
}

function routeByKeywords(issue: LinearIssue, ctx: TriageContext): Route[] {
  const haystack = `${issue.title}\n${issue.description}`
  const routes = ctx.config.repos.keywordRules
    .map((rule) => {
      const pattern = new RegExp(rule.pattern, 'i')
      return pattern.test(haystack)
        ? routeForRepo(rule.repo, ctx, 'keyword', `Keyword /${rule.pattern}/ routes to ${rule.repo}.`)
        : null
    })
    .filter((route): route is Route => route !== null)

  return dedupeRoutes(routes)
}

function routeForRepo(repo: string, ctx: TriageContext, source: RouteSource, rationale: string): Route {
  return {
    repo,
    clonePath: ctx.config.repos.clonePaths[repo] ?? ctx.repoMap.find((entry) => entry.repo === repo && entry.source === source)?.clonePath
      ?? ctx.repoMap.find((entry) => entry.repo === repo)?.clonePath,
    rationale,
  }
}

function findCaseInsensitive(map: Record<string, string>, key: string): string | undefined {
  const exact = map[key]
  if (exact) {
    return exact
  }

  const normalized = key.toLowerCase()
  const entry = Object.entries(map).find(([candidate]) => candidate.toLowerCase() === normalized)
  return entry?.[1]
}

function dedupeRoutes(routes: Route[]): Route[] {
  const seen = new Set<string>()
  const deduped: Route[] = []
  for (const route of routes) {
    if (seen.has(route.repo)) {
      continue
    }

    seen.add(route.repo)
    deduped.push(route)
  }

  return deduped
}

function implementationAssignments(
  routes: Route[],
  scope: 'single' | 'team',
  slugs: string[],
): Array<{ route: Route; slug: string }> {
  if (scope === 'single') {
    const route = routes[0]
    return route ? [{ route, slug: slugFromRepo(route.repo) }] : []
  }

  if (routes.length >= 2) {
    return routes.slice(0, MAX_IMPLEMENTERS).map((route, index) => ({
      route,
      slug: slugs[index] ?? slugFromRepo(route.repo),
    }))
  }

  const route = routes[0]
  if (!route) {
    return []
  }

  const teamSlugs = slugs.length >= 2 ? slugs : [slugFromRepo(route.repo), 'scope']
  return teamSlugs.slice(0, MAX_IMPLEMENTERS).map((slug) => ({ route, slug }))
}

function hasAcceptanceSignal(issue: LinearIssue): boolean {
  return ACCEPTANCE_SIGNAL_PATTERNS.some((pattern) => pattern.test(issue.description))
}

function issueRefFor(issue: LinearIssue): IssueRef {
  return { uuid: issue.uuid, key: issue.key, path: issue.path }
}

function implementerSpec(input: {
  issue: LinearIssue
  config: FactoryConfig
  route: Route
  scope: 'single' | 'team'
  slug: string
}): AgentSpec {
  const base = agentBaseName(input.issue)
  const name = input.scope === 'team' ? `${base}-impl-${sanitizeSlug(input.slug)}` : `${base}-impl`
  return {
    name,
    role: 'implementer',
    capability: 'spawn:codex',
    model: input.config.models.implementer,
    task: taskFor(input.issue, input.route, 'implementer'),
    repo: input.route.repo,
    clonePath: input.route.clonePath,
    node: 'self',
  }
}

function reviewerSpec(issue: LinearIssue, config: FactoryConfig, route?: Route): AgentSpec {
  const repo = route?.repo ?? config.repos.default ?? 'unroutable'
  return {
    name: `${agentBaseName(issue)}-review`,
    role: 'reviewer',
    capability: 'spawn:claude',
    model: config.models.reviewer,
    task: taskFor(issue, route ?? { repo, clonePath: config.repos.clonePaths[repo], rationale: 'Review escalated triage decision.' }, 'reviewer'),
    repo,
    clonePath: route?.clonePath ?? config.repos.clonePaths[repo],
    node: 'self',
  }
}

function taskFor(issue: LinearIssue, route: Route, role: 'implementer' | 'reviewer'): string {
  const verb = role === 'implementer' ? 'Implement' : 'Review'
  return [
    `${verb} ${issue.key}: ${issue.title}`,
    `Repo: ${route.repo}`,
    `Route rationale: ${route.rationale}`,
    issue.description,
  ].join('\n\n')
}

function agentBaseName(issue: LinearIssue): string {
  const number = issue.key.match(/\d+/)?.[0] ?? sanitizeSlug(issue.key)
  return `ar-${number}`
}

function slugFromRepo(repo: string): string {
  return sanitizeSlug(repo.split('/').at(-1) ?? repo)
}

function sanitizeSlug(slug: string): string {
  return slug.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'scope'
}
