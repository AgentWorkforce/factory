/**
 * Factory build — proactive-runtime fleet unification (sibling to the factory phases)
 * Spec: factory/planning/linear-issue-cloud-proactive-runtime-fleet-unification.md
 * Independent of the factory phases — same architectural principle, different cloud system.
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'proactive',
    slug: 'fleet-unification',
    description: 'Proactive-runtime stops hardcoding Daytona; emits spawns into the fleet (Relaycast places).',
    repo: 'cloud',
    branch: 'ricky/factory-proactive-fleet-unification',
    specFile: 'linear-issue-cloud-proactive-runtime-fleet-unification.md',
    fileTargets: [
      'packages/web/lib/proactive-runtime/team-launch-n1.ts',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -60 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Reframe proactive spawns onto the fleet primitive per the unified-node model. team-launch-n1.ts hardcodes Daytona: MEMBER_LOCAL_ROOT = "/home/daytona/workspace" (line 245), resolveServerDaytonaAuthParams() (line 803) spread into the credential bundle (line 829), via dispatchTeamLaunchN1 (lines 453-498). Change dispatchTeamLaunchN1 to EMIT spawn{capability, persona} into the relay fleet (same RelayFleetClient pattern as Phase 3) instead of building Daytona launch options. Relaycast places it (RFC §6 least-loaded) on any live eligible node; if none + workspace has autospawn permission, Relaycast spins an ephemeral Daytona node — the proactive runtime does NOT branch on this. Delete the Daytona local-root + resolveServerDaytonaAuthParams injection from the proactive path. Personas resolve from AgentWorkforce/agents/<name>/persona.ts (same registry as factory team recipe).',
    prTitle: '[factory] proactive-runtime fleet unification — emit spawns, drop hardcoded Daytona',
    prSummary: 'team-launch-n1 emits a fleet spawn instead of a Daytona launch; placement is fleet-side with ephemeral-Daytona autospawn fallback. Same principle as the factory extraction.',
    crossRepoNote: 'Reuses the RelayFleetClient pattern from Phase 3 (p10). Consumes the relay fleet protocol (relay#1056). Personas from ../agents.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
