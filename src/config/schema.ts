import { z } from 'zod'

import { LINEAR_STATE_IDS } from '../constants/linear'

export const FactoryConfigSchema = z.object({
  workspaceId: z.string(),
  subscription: z.object({
    teams: z.array(z.string()).default([]),
    projects: z.array(z.string()).default([]),
    labels: z.array(z.string()).default([]),
    assignees: z.array(z.string()).default([]),
  }).default({}),
  repos: z.object({
    byLabel: z.record(z.string(), z.string()),
    byProject: z.record(z.string(), z.string()).default({}),
    keywordRules: z.array(z.object({ pattern: z.string(), repo: z.string() })).default([]),
    clonePaths: z.record(z.string(), z.string()).default({}),
    default: z.string().optional(),
  }),
  batchSize: z.number().int().min(1).max(5).default(5),
  models: z.object({
    implementer: z.string().default('codex'),
    reviewer: z.string().default('claude'),
    triage: z.string().optional(),
  }).default({}),
  slack: z.object({
    channel: z.string(),
    style: z.literal('threaded-summarized').default('threaded-summarized'),
  }).optional(),
  mergePolicy: z.enum(['never', 'on-green-with-review']).default('never'),
  stateIds: z.object({
    readyForAgent: z.string(),
    agentImplementing: z.string(),
    done: z.string(),
    inPlanning: z.string(),
  }).default(LINEAR_STATE_IDS),
  dryRun: z.boolean().default(false),
})

export type FactoryConfig = z.infer<typeof FactoryConfigSchema>
