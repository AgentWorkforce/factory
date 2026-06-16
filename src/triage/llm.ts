import type { LinearIssue, TriageContext, TriageDecision, TriageEngine } from '../types'
import { TriageDecisionSchema } from './schema'

const DEFAULT_TIMEOUT_MS = 30_000

export interface LlmTriageOptions {
  timeoutMs?: number
}

export class LlmTriage implements TriageEngine {
  readonly #complete: (prompt: string) => Promise<string>
  readonly #timeoutMs: number

  constructor(complete: (prompt: string) => Promise<string>, opts: LlmTriageOptions = {}) {
    this.#complete = complete
    this.#timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS
  }

  async triage(issue: LinearIssue, ctx: TriageContext): Promise<TriageDecision> {
    const response = await withTimeout(this.#complete(buildPrompt(issue, ctx)), this.#timeoutMs)
    return TriageDecisionSchema.parse(JSON.parse(response)) as TriageDecision
  }
}

export function buildPrompt(issue: LinearIssue, ctx: TriageContext): string {
  return [
    'You are the Pear factory triage brain.',
    'Apply these deterministic heuristics before proposing any route:',
    '- Repo routing precedence: repos.byLabel, then repos.byProject, then keywordRules regex, then repos.default, else low-confidence escalation. Never guess.',
    '- Thin issue: description under 140 characters or no acceptance signal.',
    '- Team scope iff two or more surface buckets are named, or two or more routes are needed. Use at most two implementers.',
    '- Preserve agent naming: ar-{n}-impl, ar-{n}-impl-{scope-or-repo}, ar-{n}-review.',
    'Return only JSON matching the TriageDecision schema.',
    '',
    `Config repos: ${JSON.stringify(ctx.config.repos)}`,
    `Repo map: ${JSON.stringify(ctx.repoMap)}`,
    `Issue: ${JSON.stringify(issue)}`,
  ].join('\n')
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`LlmTriage timed out after ${timeoutMs}ms`))
    }, timeoutMs)
  })

  try {
    return await Promise.race([promise, timeoutPromise])
  } finally {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}
