import type { FactoryConfig } from '../config/schema'
import type { AgentSpec } from '../ports'
import type { IssueRef, LinearIssue, RepoMapEntry, TriageContext, TriageDecision, TriageEngine } from '../types'
import { agentBaseName, agentNameForRole, repoSlugFromName } from './agent-names'

type RouteSource = RepoMapEntry['source']
type Route = TriageDecision['routes'][number]
type Scope = TriageDecision['scope']

const DEFAULT_THIN_DESCRIPTION_LENGTH = 140
const DEFAULT_MAX_IMPLEMENTERS = 2
const DEFAULT_WORKFLOW_PATH = 'workflows/factory/linear-issue.ts'
const SHAPE_LABELS: Record<string, Scope> = {
  'agent:single': 'single',
  'agent:workflow': 'workflow',
  'agent:team': 'team',
  'agent:swarm': 'swarm',
}

/** First slug is always the swarm lead; the rest are workers, in spawn order. */
export function swarmMemberSlugs(maxImplementers: number): string[] {
  const workerCount = Math.max(maxImplementers - 1, 0)
  const slugs = ['lead', ...Array.from({ length: workerCount }, (_, index) => `worker-${index + 1}`)]
  return slugs.slice(0, Math.max(maxImplementers, 0))
}

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
    const explicitScope = scopeFromLabels(issue.labels)
    const scope = explicitScope ?? (routed.routes.length >= 2 || surfaces.length >= 2 ? 'team' : 'single')
    const confidence = routed.routes.length === 0 ? 'low' : 'high'

    return buildDecision({
      issue,
      config: ctx.config,
      routes: routed.routes,
      scope,
      scopeSource: explicitScope ? 'label' : 'inference',
      thin,
      confidence,
      rationale: explicitScope
        ? `${routed.rationale} Scope selected from Linear label agent:${explicitScope}.`
        : routed.rationale,
      scopeSlugs: routed.routes.length >= 2
        ? routed.routes.map((route) => repoSlugFromName(route.repo) ?? 'scope')
        : surfaces,
    })
  }
}

export function buildDecision(input: {
  issue: LinearIssue
  config: FactoryConfig
  routes: Route[]
  scope: Scope
  scopeSource?: 'label' | 'inference'
  thin: boolean
  confidence: 'high' | 'low'
  rationale: string
  scopeSlugs?: string[]
}): TriageDecision {
  const issueRef = issueRefFor(input.issue)
  const maxImplementers = input.config.triage?.maxImplementers ?? DEFAULT_MAX_IMPLEMENTERS
  const routes = dedupeRoutes(input.routes).slice(0, maxImplementers)
  const scope = input.scopeSource === 'label'
    ? input.scope
    : routes.length >= 2 || input.scope === 'team'
      ? 'team'
      : input.scope === 'workflow'
        ? 'workflow'
        : 'single'
  const slugs = input.scopeSlugs ?? routes.map((route) => repoSlugFromName(route.repo) ?? 'scope')
  const implementerAssignments = scope === 'workflow' || input.confidence === 'low' && routes.length === 0
    ? []
    : implementationAssignments(routes, scope, slugs, maxImplementers)
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
    workflow: scope === 'workflow' && routes.length > 0 ? workflowSpec(input.issue, input.config, routes) : undefined,
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
    scopeSource: scopeFromLabels(issue.labels) ? 'label' : 'inference',
    thin: decision.thin,
    confidence: decision.routes.length === 0 ? 'low' : decision.confidence,
    rationale: decision.rationale,
    scopeSlugs: decision.implementers.map((implementer) => implementer.name.replace(/^ar-\d+-impl-?/, '')).filter(Boolean),
  })
}

function routeByLabels(issue: LinearIssue, ctx: TriageContext): Route[] {
  const routes = issue.labels
    .filter((label) => !isShapeLabel(label))
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
  scope: Scope,
  slugs: string[],
  maxImplementers: number,
): Array<{ route: Route; slug: string }> {
  if (scope === 'single' || scope === 'workflow') {
    const route = routes[0]
    return route ? [{ route, slug: repoSlugFromName(route.repo) ?? 'scope' }] : []
  }

  if (scope === 'swarm') {
    // Swarm always shares one checkout (lead + workers collaborate live over a
    // shared relay channel) — unlike team, extra matched routes are not fanned
    // out, only the first is used.
    const route = routes[0]
    return route ? swarmMemberSlugs(maxImplementers).map((slug) => ({ route, slug })) : []
  }

  if (routes.length >= 2) {
    return routes.slice(0, maxImplementers).map((route, index) => ({
      route,
      slug: slugs[index] ?? repoSlugFromName(route.repo) ?? 'scope',
    }))
  }

  const route = routes[0]
  if (!route) {
    return []
  }

  const teamSlugs = slugs.length >= 2 ? slugs : [repoSlugFromName(route.repo) ?? 'scope', 'scope']
  return teamSlugs.slice(0, maxImplementers).map((slug) => ({ route, slug }))
}

function hasAcceptanceSignal(issue: LinearIssue): boolean {
  const taskText = `${issue.title}\n${issue.description}`
  return ACCEPTANCE_SIGNAL_PATTERNS.some((pattern) => pattern.test(taskText))
}

function issueRefFor(issue: LinearIssue): IssueRef {
  return { uuid: issue.uuid, key: issue.key, path: issue.path }
}

function implementerSpec(input: {
  issue: LinearIssue
  config: FactoryConfig
  route: Route
  scope: Scope
  slug: string
}): AgentSpec {
  const name = agentNameForRole(input.issue, 'impl', {
    repo: input.route.repo,
    discriminator: input.scope === 'team' || input.scope === 'swarm' ? input.slug : undefined,
  })
  const channel = input.scope === 'swarm' ? swarmChannel(input.issue) : undefined
  return {
    name,
    role: 'implementer',
    capability: input.config.agentCapabilities.implementer,
    model: input.config.models.implementer,
    task: channel
      ? swarmTaskFor(input.issue, input.route, input.slug, channel)
      : taskFor(input.issue, input.route, 'implementer'),
    repo: input.route.repo,
    clonePath: input.route.clonePath,
    ...(channel ? { channel, swarmRole: input.slug === 'lead' ? 'lead' as const : 'worker' as const } : {}),
    node: 'self',
  }
}

/** Shared relay channel every lead/worker for one issue's swarm joins live. */
export function swarmChannel(issue: LinearIssue): string {
  return `swarm-${agentBaseName(issue)}`
}

export function swarmTaskFor(issue: LinearIssue, route: Route, slug: string, channel: string): string {
  const base = taskFor(issue, route, 'implementer')
  const roleBriefing = slug === 'lead'
    ? [
      `You are the SWARM LEAD for this task. Worker agents are collaborating with you in the same checkout, in real time.`,
      `Coordinate over the shared relay channel #${channel}: break the work into subtasks, assign them to workers by name, and check their progress before you finish.`,
      `Integrate everyone's changes into one coherent result before handing off to review. Do not finish until you've confirmed on #${channel} that every worker is done or blocked.`,
    ]
    : [
      `You are a SWARM WORKER (${slug}) for this task, collaborating with a lead and other workers in the same checkout, in real time.`,
      `Watch the shared relay channel #${channel} for direction from the lead. Announce what you're starting, ask there if scope is ambiguous, and post when a subtask is done or blocked.`,
      `Do not open your own pull request — the lead integrates and finishes the work.`,
    ]
  return [base, ...roleBriefing].join('\n\n')
}

function workflowSpec(issue: LinearIssue, _config: FactoryConfig, routes: Route[]): AgentSpec {
  const primaryRoute = routes[0]
  // NOTE: this derives repoLabels from raw label strings, whereas the dispatch
  // path (routeWorkflowSpec in factory.ts) rebuilds inputs from route slugs. The
  // dispatch-path rebuild is authoritative — the live workflow spawn never
  // consumes this version, so the two can diverge without an observable bug.
  const repoLabels = issue.labels.filter((label) => !isShapeLabel(label))
  return {
    name: agentNameForRole(issue, 'workflow', { repo: primaryRoute.repo }),
    role: 'workflow',
    capability: 'workflow:run',
    task: taskFor(issue, primaryRoute, 'workflow'),
    workflow: DEFAULT_WORKFLOW_PATH,
    inputs: {
      issue: issueRefFor(issue),
      title: issue.title,
      description: issue.description,
      labels: issue.labels,
      repoLabels,
      routes,
    },
    repo: primaryRoute.repo,
    clonePath: primaryRoute.clonePath,
    node: 'self',
  }
}

function reviewerSpec(issue: LinearIssue, config: FactoryConfig, route?: Route): AgentSpec {
  const repo = route?.repo ?? config.repos.default ?? 'unroutable'
  return {
    name: agentNameForRole(issue, 'review', { repo }),
    role: 'reviewer',
    capability: config.agentCapabilities.reviewer,
    model: config.models.reviewer,
    task: taskFor(issue, route ?? { repo, clonePath: config.repos.clonePaths[repo], rationale: 'Review escalated triage decision.' }, 'reviewer'),
    repo,
    clonePath: route?.clonePath ?? config.repos.clonePaths[repo],
    node: 'self',
  }
}

/**
 * Build the babysitter agent spec for an issue. Unlike the implementer/reviewer,
 * this is NOT spawned at dispatch time — the orchestrator spawns it lazily once
 * the PR opens (so it carries the original spec) and overwrites `.task` with the
 * full babysitter prompt via renderAgentTask. Defaults to sonnet (models.babysitter).
 */
export function babysitterSpec(issue: LinearIssue, config: FactoryConfig, route?: Route): AgentSpec {
  const repo = route?.repo ?? config.repos.default ?? 'unroutable'
  return {
    name: agentNameForRole(issue, 'babysit', { repo }),
    role: 'babysitter',
    capability: config.agentCapabilities.babysitter,
    model: config.models.babysitter,
    task: taskFor(issue, route ?? { repo, clonePath: config.repos.clonePaths[repo], rationale: 'Babysit the open PR to Human Review.' }, 'babysitter'),
    repo,
    clonePath: route?.clonePath ?? config.repos.clonePaths[repo],
    node: 'self',
  }
}

function taskFor(issue: LinearIssue, route: Route, role: AgentSpec['role']): string {
  const verb = role === 'implementer'
    ? 'Implement'
    : role === 'babysitter'
      ? 'Babysit the PR for'
      : role === 'workflow'
        ? 'Run workflow for'
        : 'Review'
  return [
    `${verb} ${issue.key}: ${issue.title}`,
    `Repo: ${route.repo}`,
    `Route rationale: ${route.rationale}`,
    issue.description,
  ].join('\n\n')
}

export function scopeFromLabels(labels: string[]): Scope | undefined {
  const normalized = new Set(labels.map((label) => label.trim().toLowerCase()))
  if (normalized.has('agent:swarm')) return 'swarm'
  if (normalized.has('agent:team')) return 'team'
  if (normalized.has('agent:workflow')) return 'workflow'
  if (normalized.has('agent:single')) return 'single'
  return undefined
}

export function isShapeLabel(label: string): boolean {
  return SHAPE_LABELS[label.trim().toLowerCase()] !== undefined
}
