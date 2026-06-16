/**
 * Factory build — p13 (Phase 4 — node registration)
 * Spec: factory/planning/linear-issue-factory-phase-4-node-registration.md
 * Depends on p10 (fleet client), p11 (broker heartbeat), p12 (placement).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p13',
    slug: 'factory-node-definition',
    description: 'Ship the factory node-definition that `agent-relay fleet serve <def>` runs — reads NodeConfig, advertises, executes cloud-placed spawns locally.',
    repo: 'factory',
    branch: 'ricky/factory-p13-factory-node-definition',
    specFile: 'linear-issue-factory-phase-4-node-registration.md',
    fileTargets: [
      'src/fleet',
      'src/node',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'standard',
    task: 'Node registration per epic v2 Phase 4. Ship a factory node-definition (defineNode({...})) in @agent-relay/factory that registers THIS machine as a fleet node from NodeConfig (workspaceId; capabilities spawn:claude/spawn:codex/workflow:run; repo cloneRoot/clonePaths). Implement the RFC §9 control surface: node.register/heartbeat/deregister/inventory.sync. Handle action.invoke from Relaycast — spawn:claude/spawn:codex run the harness in the mapped checkout (InternalFleetClient/harness-driver reference); workflow:run shells out to `relayflows run <workflow>` in the checkout. Emit action.result/agent.register/delivery.ack back. NO orchestration logic (triage/merge/state are cloud, Phase 2). One command: `agent-relay fleet serve ./factory.node.ts` — fleet serve already auto-starts the broker (no separate `agent-relay up`).',
    prTitle: '[factory] p13: factory node-definition for `agent-relay fleet serve`',
    prSummary: 'One command (`agent-relay fleet serve <factory-node-def>`) registers a node from NodeConfig and executes cloud-placed spawns locally. fleet serve already auto-starts the broker, so this is genuinely zero-infra for the user.',
    crossRepoNote: 'IMPORTANT — `agent-relay fleet serve` ALREADY auto-starts the broker: relay/packages/cli/src/cli/commands/fleet.ts calls startBrokerWithPortFallback before serving the node, so NO separate `agent-relay up --background` is needed. Acceptance MUST include a cold-broker proof: from a machine with no running broker, `agent-relay fleet serve <factory-node-def>` brings the broker up and registers the node in `agent-relay fleet nodes`. Verify against relay/packages/cli/src/cli/commands/fleet.ts.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
