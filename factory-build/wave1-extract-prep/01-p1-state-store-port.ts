/**
 * Factory build — p1 (Phase 1 extraction prep #1)
 * Spec: factory/planning/linear-issue-factory-extract-p1-state-store-port.md
 * Run:  relayflows run <abs path to this file>
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p1',
    slug: 'state-store-port',
    description: 'Define StateStore port + InMemoryStateStore; route BatchTracker / InFlightRegistry / clarification through it.',
    repo: 'pear',
    branch: 'ricky/factory-p1-state-store-port',
    specFile: 'linear-issue-factory-extract-p1-state-store-port.md',
    fileTargets: [
      'packages/factory-sdk/src/ports/state.ts',
      'packages/factory-sdk/src/state/in-memory-state-store.ts',
      'packages/factory-sdk/src/orchestrator/factory.ts',
      'packages/factory-sdk/src/types.ts',
    ],
    acceptanceCmd: 'npm run build -w @pear/factory-sdk 2>&1 | tail -40 && npm test -w @pear/factory-sdk 2>&1 | tail -40',
    tier: 'standard',
    task: 'REQUIRES pear#368 merged first (it rewrites orchestrator/factory.ts + types.ts). Introduce a StateStore port (ports/state.ts) and an InMemoryStateStore (state/in-memory-state-store.ts), then route the factory RUNTIME state machine (BatchTracker, InFlightRegistry, clarification records) through the port. IMPORTANT: "StateStore" here is factory runtime state — it is NOT the Linear workflow-state resolver (resolveFactoryStates / linear/state-resolver.ts) that pear#368 added; leave that alone and route only runtime state through the port. Pure, behavior-preserving refactor. The cloud Postgres-backed StateStore (Phase 2) will swap in behind the same port, so keep the interface free of pear/electron imports.',
    prTitle: '[factory] p1: StateStore port + InMemoryStateStore',
    prSummary: 'Extract a StateStore port and route the factory state machine through it (behavior-preserving). Enables the Phase 2 cloud-backed store to swap in behind the same interface.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
