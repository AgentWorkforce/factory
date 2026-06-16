/**
 * Factory build — p9 (Phase 2) — THE CRUX abstraction
 * Spec: factory/planning/linear-issue-factory-cloud-p9-dispatch-target.md
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p9',
    slug: 'dispatch-target',
    description: 'Make proactive-runtime dispatch target pluggable (daytona | fleet-node).',
    repo: 'cloud',
    branch: 'ricky/factory-p9-dispatch-target',
    specFile: 'linear-issue-factory-cloud-p9-dispatch-target.md',
    fileTargets: [
      'packages/web/lib/proactive-runtime',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -60 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Introduce a DispatchTarget interface (spawn/poll/cancel; SpawnRequest carries scope, repo, capability, payload) in proactive-runtime. Refactor the CURRENT Daytona path (deliverDeploymentTrigger / createDeploymentSandboxRuntime / pollDeploymentTriggerRun) to implement it with ZERO behavior change (regression-test against existing flows). Add a targetKind: daytona|fleet-node selector resolved from WorkspaceConfig, defaulting to daytona so nothing regresses. Leave the fleet-node branch as a clear unimplemented stub (p10 implements it).',
    prTitle: '[factory] p9: pluggable dispatch target (daytona | fleet-node)',
    prSummary: 'Abstract proactive-runtime dispatch behind DispatchTarget; refactor Daytona behind it (no behavior change); add fleet-node stub for p10. This is the seam that lets cloud place spawns onto a user\'s laptop.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
