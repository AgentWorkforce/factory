/**
 * Factory build — p12 (Phase 3)
 * Spec: factory/planning/linear-issue-factory-fleet-p12-node-placement.md
 * Depends on p10 (RelayFleetClient) + p11 (heartbeat/roster).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p12',
    slug: 'node-placement',
    description: 'Node-targeted placement (by name/self) + reject-and-reconcile unmapped repos. Minimal relay#1056 §6 slice.',
    repo: 'relay',
    branch: 'ricky/factory-p12-node-placement',
    specFile: 'linear-issue-factory-fleet-p12-node-placement.md',
    fileTargets: [
      'packages/sdk/src/messaging',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Implement minimal placement: spawn { capability, node } places onto a named eligible live node; else pick any live eligible node for the workspace (NO least-loaded scheduling). The node pushes NodeConfig.repoPaths keys up on registration; cloud only places work for mapped repos. A placement for an unmapped repo or missing capability is rejected by the node and reconciled (re-queued bounded, or surfaced to Slack) with a log line — never silently dropped. None eligible -> bounded-queue then fail (reuse the durable mailbox TTL).',
    prTitle: '[factory] p12: node-targeted placement + reject-and-reconcile',
    prSummary: 'Targeted placement by node name/self, repo-path-aware eligibility, reject-and-reconcile for unmapped repos. Minimal #1056 §6 slice; no least-loaded scheduler.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
