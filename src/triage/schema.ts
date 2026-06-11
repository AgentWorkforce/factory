import { z } from 'zod'

export const AgentSpecSchema = z.object({
  name: z.string(),
  role: z.enum(['implementer', 'reviewer']),
  capability: z.enum(['spawn:codex', 'spawn:claude']),
  model: z.string().optional(),
  task: z.string(),
  repo: z.string(),
  clonePath: z.string().optional(),
  channel: z.string().optional(),
  node: z.string().optional(),
  sessionRef: z.string().optional(),
  invocationId: z.string().optional(),
  restartPolicy: z.unknown().optional(),
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
  scope: z.enum(['single', 'team']),
  implementers: z.array(AgentSpecSchema),
  reviewer: AgentSpecSchema,
  thin: z.boolean(),
  confidence: z.enum(['high', 'low']),
  rationale: z.string(),
})
