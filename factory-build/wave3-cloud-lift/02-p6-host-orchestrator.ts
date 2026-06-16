/**
 * Factory build — p6 (Phase 2 cloud lift)
 * Spec: factory/planning/linear-issue-factory-cloud-p6-host-orchestrator.md
 * Depends on p1 (StateStore port) + p4 (published package).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p6',
    slug: 'host-orchestrator',
    description: 'Phase 2 cloud lift: factory brain in the cloud worker; triage selects a recipe and EMITS spawns into the fleet; StateStore on a Cloudflare DO.',
    repo: 'cloud',
    branch: 'ricky/factory-p6-cloud-lift',
    specFile: 'linear-issue-factory-phase-2-cloud-lift.md',
    fileTargets: [
      'packages/web/lib/proactive-runtime',
      'packages/web/lib/teams',
      'packages/web/lib/db/schema.ts',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -60 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Lift the factory brain into the cloud worker per epic v2 Phase 2. Import @agent-relay/factory (triage + merge-gate + state) and run watch -> triage -> recipe -> EMIT, multi-tenant by workspaceId, driven by the EXISTING watch dispatcher (integration-watch-dispatcher.ts:1523) + relaycron sweep. Triage selects a recipe (single|workflow|team) and EMITS spawn{capability,...} invocations into the relay fleet via RelayFleetClient (Phase 3) — it does NOT execute or place (placement is fleet-side, RFC §6). Reconstruct cloud/.../teams/spawn-team.ts (Daytona team-provisioning; MEMBER_LOCAL_ROOT line 55, launchMember) as the `team` recipe (N implementer spawns + reviewer); DELETE the Daytona local-root + launch-member callback. StateStore: durable Cloudflare DO behind the p1 port; reuse integration_watch_deliveries for the delivery lifecycle where it fits. No factory-side execution/placement code.',
    prTitle: '[factory] p6: Phase 2 cloud lift — emit spawns into the fleet, team recipe, DO StateStore',
    prSummary: 'Factory brain in cloud, multi-tenant by workspaceId. Triage -> recipe -> emit spawn into the fleet (no execution/placement). spawn-team becomes the team recipe; StateStore on a Cloudflare DO.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
