import { z } from 'zod'

export const AgentSpecSchema = z.object({
  name: z.string(),
  role: z.enum(['implementer', 'reviewer', 'babysitter', 'workflow']),
  principal: z.string().optional(),
  owner: z.string().optional(),
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
})

export const TriageDecisionSchema = z.object({
  issue: z.object({
    uuid: z.string(),
    key: z.string(),
    path: z.string(),
  }),
  issueResolution: z.object({
    source: z.enum(['relayfile-projection', 'github-api-fallback']),
    repo: z.string().optional(),
    detail: z.string(),
    projection: z.object({
      outcome: z.enum(['matched', 'no-match']),
      localMountDegraded: z.boolean().optional(),
      localMountDegradedReason: z.string().optional(),
      eventListener: z.object({
        state: z.enum(['starting', 'subscribed', 'polling', 'not-listening', 'unknown']),
        reason: z.string().optional(),
      }).optional(),
      githubConnection: z.object({
        ready: z.boolean(),
        state: z.string().optional(),
        initialSyncState: z.string().optional(),
      }).optional(),
    }),
  }).optional(),
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
