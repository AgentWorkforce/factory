/**
 * Factory build — p13 (Phase 4) — the one-command UX
 * Spec: factory/planning/linear-issue-factory-node-p13-node-definition.md
 * Depends on p9, p10, p11, p12.
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p13',
    slug: 'factory-node-definition',
    description: 'Ship the factory node-definition that `agent-relay fleet serve <def>` runs — reads NodeConfig, advertises, executes cloud-placed spawns locally.',
    repo: 'factory',
    branch: 'ricky/factory-p13-factory-node-definition',
    specFile: 'linear-issue-factory-node-p13-node-definition.md',
    fileTargets: [
      'src/fleet',
      'src/node',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Ship a factory node-definition (defineNode({...})) in @agent-relay/factory that reads NodeConfig (workspaceId, capabilities, repoPaths), advertises spawn:claude/spawn:codex, pushes repoPaths keys up on registration (for p12 placement), runs cloud-placed spawns locally via the existing harness path (InternalFleetClient / harness-driver) in the mapped checkout, and streams exit/messages back. Document the one-command flow: `agent-relay fleet serve ./factory.node.ts`. NO new CLI command — fleet serve is the entry point.',
    prTitle: '[factory] p13: factory node-definition for `agent-relay fleet serve`',
    prSummary: 'One command (`agent-relay fleet serve <factory-node-def>`) registers a node from NodeConfig and executes cloud-placed spawns locally. fleet serve already auto-starts the broker, so this is genuinely zero-infra for the user.',
    crossRepoNote: 'IMPORTANT — `agent-relay fleet serve` ALREADY auto-starts the broker: relay/packages/cli/src/cli/commands/fleet.ts calls startBrokerWithPortFallback before serving the node, so NO separate `agent-relay up --background` is needed. Acceptance MUST include a cold-broker proof: from a machine with no running broker, `agent-relay fleet serve <factory-node-def>` brings the broker up and registers the node in `agent-relay fleet nodes`. Verify against relay/packages/cli/src/cli/commands/fleet.ts.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
