/**
 * Factory build — p7 (Phase 2)
 * Spec: factory/planning/linear-issue-factory-cloud-p7-label-scope.md
 * Logic in @agent-relay/factory (factory repo); consumed by cloud.
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p7',
    slug: 'label-scope',
    description: 'Widen TriageDecision.scope to single|workflow|team; map issue labels -> scope with tiered inference fallback.',
    repo: 'factory',
    branch: 'ricky/factory-p7-label-scope',
    specFile: 'linear-issue-factory-cloud-p7-label-scope.md',
    fileTargets: [
      'src/triage',
      'src/types.ts',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -40 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Widen TriageDecision.scope from single|team to single|workflow|team across the triage engine and orchestrator dispatch. Map explicit labels (agent:single/agent:workflow/agent:team) deterministically; otherwise TieredTriage (heuristic -> LLM) emits scope + confidence and routes low-confidence issues to Slack clarification before spawning. Define the workflow-scope spec so it maps onto cloud\'s existing workflow execution path (no new dialect). One knob, not three code paths. Unit-test all three labels + inference + clarification branches.',
    prTitle: '[factory] p7: scope single|workflow|team driven by issue labels',
    prSummary: 'Three-value scope knob bound to agent:single/workflow/team labels, with tiered inference + Slack clarification fallback. Emits specs into cloud\'s existing execution paths.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
