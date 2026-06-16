/**
 * Factory build — p2 (Phase 1 extraction prep #2)
 * Spec: factory/planning/linear-issue-factory-extract-p2-config-split.md
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p2',
    slug: 'config-split',
    description: 'Split FactoryConfig into WorkspaceConfig (cloud policy) + NodeConfig (local execution).',
    repo: 'pear',
    branch: 'ricky/factory-p2-config-split',
    specFile: 'linear-issue-factory-extract-p2-config-split.md',
    fileTargets: [
      'packages/factory-sdk/src/config/schema.ts',
    ],
    acceptanceCmd: 'npm run build -w @pear/factory-sdk 2>&1 | tail -40 && npm test -w @pear/factory-sdk 2>&1 | tail -40',
    tier: 'standard',
    task: 'REQUIRES pear#368 merged first (it rewrites config/schema.ts with dynamic per-team Linear states). Split FactoryConfig into two Zod schemas: WorkspaceConfig (subscription, repos, batchSize, mergePolicy, safety, slack, AND pear#368 per-team Linear state mapping — linear.states, linear.statesByTeam, plus the stateIds UUID fallback — all cloud/workspace policy) and NodeConfig (workspaceId, capabilities, repoPaths — local execution). The loader must accept BOTH the legacy combined shape and the new split shape so nothing regresses. Build on pear#368 post-merge schema; do NOT revert its dynamic state resolution. Behavior-preserving.',
    prTitle: '[factory] p2: split FactoryConfig into WorkspaceConfig + NodeConfig',
    prSummary: 'Split the config schema into cloud-policy (WorkspaceConfig) and local-execution (NodeConfig) halves; loader accepts legacy + split shapes.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
