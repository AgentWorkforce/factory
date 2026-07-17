import { z } from 'zod'

export const AgentSpecSchema = z.object({
  name: z.string(),
  role: z.enum(['implementer', 'reviewer', 'babysitter', 'workflow']),
  capability: z.enum(['spawn:codex', 'spawn:claude', 'workflow:run']),
  model: z.string().optional(),
  task: z.string(),
  workflow: z.string().optional(),
  inputs: z.record(z.unknown()).optional(),
  repo: z.string(),
  clonePath: z.string().optional(),
  channel: z.string().optional(),
  node: z.string().optional(),
  sessionRef: z.string().optional(),
  invocationId: z.string().optional(),
  restartPolicy: z.unknown().optional(),
  ownedPullRequest: z.object({
    repo: z.string(),
    number: z.number().int().positive(),
    path: z.string().optional(),
  }).optional(),
  pendingPullRequestWake: z.object({
    repo: z.string(),
    number: z.number().int().positive(),
    kinds: z.array(z.string()),
  }).optional(),
})

export const TriageDecisionSchema = z.object({
  issue: z.object({
    uuid: z.string(),
    key: z.string(),
    path: z.string(),
  }),
  routes: z.array(z.object({
    repo: z.string(),
    clonePath: z.string().optional(),
    rationale: z.string(),
  })),
  scope: z.enum(['single', 'workflow', 'team']),
  implementers: z.array(AgentSpecSchema),
  workflow: AgentSpecSchema.optional(),
  reviewer: AgentSpecSchema,
  thin: z.boolean(),
  confidence: z.enum(['high', 'low']),
  rationale: z.string(),
})
