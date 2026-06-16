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
    task: 'BUILD ON pear#368 (LANDED — dynamic per-team Linear states) AND pear#369 (LANDED — compact repos: org/cloneRoot/names/overrides + Zod .transform deriving byLabel/clonePaths/labels). Both config/schema.ts PRs are merged. Split FactoryConfig into two Zod schemas, splitting #369\'s compact form along the workspace/node seam: WorkspaceConfig = subscription + repos identity (org, names, overrides, byLabel, default) + batchSize + mergePolicy + safety + slack + #368 Linear states (linear.states, linear.statesByTeam, stateIds) — all cloud/workspace policy. NodeConfig = workspaceId + capabilities + #369\'s cloneRoot + clonePaths (THIS is the per-machine repoPaths — keep #369\'s derivation: clonePaths[repo] = cloneRoot/repoName). The loader must accept the legacy combined shape, #369\'s compact form, AND the new split shape so nothing regresses; preserve #369\'s .transform derivation and #368\'s dynamic state resolution. Behavior-preserving.',
    prTitle: '[factory] p2: split FactoryConfig into WorkspaceConfig + NodeConfig',
    prSummary: 'Split the config schema into cloud-policy (WorkspaceConfig) and local-execution (NodeConfig) halves; loader accepts legacy + split shapes.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
