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
    description: 'Host the factory orchestrator in cloud proactive-runtime; StateStore backed by existing Postgres.',
    repo: 'cloud',
    branch: 'ricky/factory-p6-host-orchestrator',
    specFile: 'linear-issue-factory-cloud-p6-host-orchestrator.md',
    fileTargets: [
      'packages/web/lib/proactive-runtime',
      'packages/web/lib/db/schema.ts',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -60 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Import @agent-relay/factory (orchestrator + triage + github + state) into the cloud web worker and run its watch->triage->dispatch->merge loop per workspaceId, driven by the EXISTING watch dispatcher (integration-watch-dispatcher.ts) + relaycron sweep — no polling. Implement a PostgresStateStore against the EXISTING tables (integration_watch_deliveries, integration_watch_issue_dispatch_dedup, proactive_continuations); add columns/tables only where a field has no home. Multi-tenant by workspaceId, no state bleed. NO new datastore/binding (no DynamoDB/DO).',
    prTitle: '[factory] p6: host factory orchestrator in proactive-runtime (Postgres StateStore)',
    prSummary: 'Run the factory brain in cloud, multi-tenant by workspaceId, reusing the existing watch dispatcher + relaycron + Postgres. No new infrastructure.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
