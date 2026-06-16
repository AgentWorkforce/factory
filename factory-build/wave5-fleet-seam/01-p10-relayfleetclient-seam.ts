/**
 * Factory build — p10 (Phase 3) — connects the crux to the fabric
 * Spec: factory/planning/linear-issue-factory-fleet-p10-relayfleetclient-seam.md
 * Depends on p9 (DispatchTarget) + p4 (published package).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p10',
    slug: 'relayfleetclient-seam',
    description: 'Implement RelayFleetClient (replace the relay#1056 stub) and wire the cloud fleet-node DispatchTarget branch to agent.create.',
    repo: 'factory',
    branch: 'ricky/factory-p10-relayfleetclient-seam',
    specFile: 'linear-issue-factory-fleet-p10-relayfleetclient-seam.md',
    fileTargets: [
      'src/fleet',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Implement RelayFleetClient in @agent-relay/factory against the relay SDK messaging API (agent.create / agent.release, nodes.list({capability}), deliveries.*), replacing the stub that throws relay#1056. A targetKind: fleet-node workspace must dispatch an agent:single issue to a connected node\'s local checkout; agent exit/messages must reach the cloud orchestrator so merge-gate + writeback proceed. Capability mismatch fails cleanly.',
    prTitle: '[factory] p10: implement RelayFleetClient + cloud fleet-node dispatch seam',
    prSummary: 'RelayFleetClient round-trips a real spawn over relaycast; the cloud fleet-node DispatchTarget branch drives it. Cloud can now place a spawn onto a user\'s laptop.',
    crossRepoNote: 'Also wire the fleet-node branch of DispatchTarget in cloud at ../cloud/packages/web/lib/proactive-runtime (the p9 stub). RelayFleetClient uses the relay SDK at ../relay/packages/sdk. Open the cloud-side change as a separate cloud PR and note it in the signoff; smart placement/multi-node is p12 (single-node assumption is fine here).',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
