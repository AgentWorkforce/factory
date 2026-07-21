import { readFile } from 'node:fs/promises'

import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

describe('reusable Cloudflare verification workflow', () => {
  it('runs the infra up/suite/down contract in Miniflare and real Cloudflare modes', async () => {
    const text = await readFile('.github/workflows/cloudflare-verification.yml', 'utf8')
    const workflow = parse(text) as {
      on: { workflow_call: unknown }
      jobs: Record<string, { if?: string; steps: Array<Record<string, unknown>> }>
    }

    expect(workflow.on.workflow_call).toBeDefined()
    expect(Object.keys(workflow.jobs)).toEqual(['miniflare', 'cloudflare'])
    expect(workflow.jobs.cloudflare.if).toBe('inputs.run_real')

    for (const [jobName, mode] of [['miniflare', 'miniflare'], ['cloudflare', 'cloudflare']] as const) {
      const job = workflow.jobs[jobName]
      const commands = job.steps.flatMap((step) => typeof step.run === 'string' ? [step.run] : [])
      expect(commands).toContain('bash ci/verification-env/up.sh')
      expect(commands).toContain('bash ci/verification-env/run-suite.sh')
      expect(commands).toContain('bash ci/verification-env/down.sh')

      const lifecycle = job.steps.filter((step) => (
        typeof step.run === 'string' && step.run.startsWith('bash ci/verification-env/')
      ))
      expect(lifecycle).toHaveLength(3)
      expect(lifecycle.every((step) => (
        (step.env as Record<string, string>).FACTORY_VERIFICATION_MODE === mode
      ))).toBe(true)
      expect(lifecycle.find((step) => step.run === 'bash ci/verification-env/down.sh')?.if).toBe('always()')
    }

    const realSteps = workflow.jobs.cloudflare.steps
    expect(realSteps.some((step) => step.name === 'Require real Cloudflare resource secret')).toBe(true)
    expect(realSteps.some((step) => step.name === 'Upload real-environment evidence' && step.if === 'always()')).toBe(true)
  })
})
