/**
 * Factory build — p10 (Phase 3 — fleet client)
 * Spec: factory/planning/linear-issue-factory-phase-3-fleet-client.md
 * Depends on p4 (published package) + the relay fleet protocol (RFC).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p10',
    slug: 'fleet-client',
    description: 'Implement RelayFleetClient as a thin client of the relay fleet protocol (replaces the relay#1056 stub).',
    repo: 'factory',
    branch: 'ricky/factory-p10-fleet-client',
    specFile: 'linear-issue-factory-phase-3-fleet-client.md',
    fileTargets: [
      'src/fleet',
      'src/ports/fleet.ts',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Implement RelayFleetClient in @agent-relay/factory (src/fleet/relay-fleet-client.ts) as a THIN CLIENT of the relay fleet protocol (RFC §6 spawn/placement, §7 invocationId lifecycle, §9 control surface), replacing the stub that throws relay#1056. It emits spawn{capability,...} and observes pending -> dispatched -> completed; PLACEMENT IS FLEET-SIDE (the factory targets no node — there is no daytona-vs-fleet-node branch; there is only the fleet). An agent:single spawn from cloud triage round-trips the fleet, lands on whichever eligible node is live, completion observed via the invocationId, Linear writeback fires. Extend SpawnInput.capability (src/ports/fleet.ts) to include workflow:run; define the workflow:run handler contract (the node shells out to `relayflows run <workflow>` — see Phase 4). Node-loss mid-spawn reschedules the same invocationId with no double-spawn.',
    prTitle: '[factory] p10: RelayFleetClient — thin client of the relay fleet protocol',
    prSummary: 'RelayFleetClient consumes the fleet protocol (RFC §6/§7/§9), replacing the relay#1056 stub. Emits spawns; placement + reconcile are fleet-side. Adds the workflow:run capability.',
    crossRepoNote: 'Consumes the relay fleet protocol (relay#1056 / ../relay). The cloud emit path is Phase 2 (p6). No DispatchTarget, no daytona|fleet-node branch — there is only the fleet.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
