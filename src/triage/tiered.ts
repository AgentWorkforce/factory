import type { LinearIssue, TriageContext, TriageDecision, TriageEngine } from '../types'
import { buildDecision, HeuristicTriage, normalizeDecision } from './heuristic'

export class TieredTriage implements TriageEngine {
  readonly #heuristic: TriageEngine
  readonly #llm?: TriageEngine

  constructor(heuristic: TriageEngine = new HeuristicTriage(), llm?: TriageEngine) {
    this.#heuristic = heuristic
    this.#llm = llm
  }

  async triage(issue: LinearIssue, ctx: TriageContext): Promise<TriageDecision> {
    const heuristicDecision = await this.#heuristic.triage(issue, ctx)
    if (!heuristicDecision.thin && heuristicDecision.confidence !== 'low') {
      return heuristicDecision
    }

    if (!this.#llm) {
      return { ...heuristicDecision, confidence: 'low' }
    }

    try {
      const llmDecision = await this.#llm.triage(issue, ctx)
      return mergeDecisions(heuristicDecision, llmDecision, issue, ctx)
    } catch {
      return { ...heuristicDecision, confidence: 'low' }
    }
  }
}

export function mergeDecisions(
  heuristicDecision: TriageDecision,
  llmDecision: TriageDecision,
  issue: LinearIssue,
  ctx: TriageContext,
): TriageDecision {
  const mergedRoutes = llmDecision.routes.length > 0 ? llmDecision.routes : heuristicDecision.routes
  const mergedScope = mergedRoutes.length >= 2 || llmDecision.scope === 'team' || heuristicDecision.scope === 'team' ? 'team' : 'single'

  return normalizeDecision(buildDecision({
    issue,
    config: ctx.config,
    routes: mergedRoutes,
    scope: mergedScope,
    thin: llmDecision.thin && heuristicDecision.thin,
    confidence: mergedRoutes.length === 0 ? 'low' : llmDecision.confidence,
    rationale: llmDecision.rationale || heuristicDecision.rationale,
    scopeSlugs: llmDecision.implementers.map((implementer) => implementer.name.replace(/^ar-\d+-impl-?/, '')).filter(Boolean),
  }), issue, ctx.config)
}
