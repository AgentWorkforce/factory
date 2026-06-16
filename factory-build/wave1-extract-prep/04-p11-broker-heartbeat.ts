/**
 * Factory build — p11 (Phase 3 fleet slice; independent — only needs the shipped relay#1056 fabric)
 * Spec: factory/planning/linear-issue-factory-fleet-p11-broker-heartbeat.md
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p11',
    slug: 'broker-heartbeat',
    description: 'Broker outbound heartbeat/liveness + reconnect inventory sync so placement + the durable mailbox survive blips.',
    repo: 'relay',
    branch: 'ricky/factory-p11-broker-heartbeat',
    specFile: 'linear-issue-factory-fleet-p11-broker-heartbeat.md',
    fileTargets: [
      'packages/sdk/src/messaging/types.ts',
      'packages/sdk/src/messaging/relaycast.ts',
    ],
    acceptanceCmd: 'npm run build --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Add node outbound heartbeat (~10-15s: name, capabilities, activeAgents, load, lastHeartbeatAt) populating the RelayNode roster fields, reconnect inventory sync (re-announce live agents for reconciliation; first-to-completed wins), and graceful deregister. Mark a node offline when heartbeats lapse; the bounded-durable mailbox holds messages until TTL.',
    prTitle: '[factory] p11: broker heartbeat + reconnect inventory sync',
    prSummary: 'Outbound liveness/load heartbeat, reconnect re-announce/reconcile, graceful deregister. The load-bearing piece for laptop-closed -> queue -> drain-on-reopen.',
    crossRepoNote: 'The broker runtime is Rust at ../relay-broker/src/relaycast_ws.rs — the heartbeat emit + reconnect re-announce live there; build/test it with `cd ../relay-broker && cargo build && cargo test`. The RelayNode roster types/SDK live in THIS repo (packages/sdk/src/messaging). Open the Rust-side changes as a separate relay-broker PR and note it in the signoff.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
