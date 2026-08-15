# Cloud-node Factory dispatch

This runbook moves the Factory CLI control plane from an operator laptop to a
cloud host without changing the dispatch contract or briefly running two
dispatchers. It is a migration, not a cutover: the laptop installation remains
intact as the rollback target.

## Architecture boundary

The Factory control plane and the nodes that execute agents are different
lifecycles:

- The control-plane host runs `factory start --mode live --backend relay` with
  one resolved config and durable node-local state. It does not advertise
  `spawn:*` capabilities and does not execute implementation agents itself.
- Agent execution uses a fresh sandbox for each agent. Cloud's
  `provisionFleetSandboxNode()` remains the provisioning primitive for that
  lifecycle; callers must select the force-provision path, wait for the new
  node to become live, target one spawn to it, and destroy it after the agent
  reaches a terminal state.

`provisionFleetSandboxNode()` is not the control-plane-host provisioner. It
creates and enrolls an agent execution node, starts `relay node up`, and returns
without installing or supervising Factory. Reusing it for the dispatcher would
turn the dispatcher into an eligible execution node and recreate the stale
persistent-sandbox model. A cloud platform may use the function for each agent;
it should provision the small control-plane VM/container through its ordinary
service mechanism.

This split also makes the legacy identity-reclaim gate irrelevant to normal
agent startup: every agent sandbox receives a fresh sandbox ID, node identity,
and unique node name. Identity reclaim remains relevant only if an operator
chooses to restart an old, previously enrolled node, which this runbook never
does. In particular, do not restart `daytona-fleet-proof-0811`.

## Prerequisites

Do not start the cloud dispatcher until all of these are merged, released, and
deployed in the target workspace:

1. Relayfile delegated credential re-mint uses the SDK and does not interpret
   `AGENT_RELAY_BIN` as the Node CLI. Relayfile 0.10.42 contains that fix; this
   package pins `@relayfile/sdk` 0.10.43.
2. `relayfile-adapters#263` supplies labels in the issue index.
3. `relayfile-cloud#155` restores GitHub App writeback.
4. `factory#267` bounds the readiness fallback and surfaces reconciliation
   degradation.

The deployment must retain `mergePolicy: "never"`. Do not work around a failed
GitHub App publication with another identity; report it as
`relayfile-cloud#155` evidence.

## Prepare the node-local contract

Copy the committed principal config to the cloud host as an input, build this
checkout, then create a new resolved copy. All four paths below are paths on the
cloud host:

```bash
npm ci
npm run build

node bin/factory.mjs cloud-node prepare \
  --config /srv/chief/factory.khaliq.config.json \
  --output /etc/agent-relay/factory.khaliq.config.json \
  --clone-root /srv/agent-workforce \
  --runtime-root /var/lib/agent-relay/factory \
  --workspace rw_7ccfea89 \
  --instance-name factory-khaliq-cloud
```

The command refuses to overwrite an existing output. Review or remove the old
copy explicitly before regenerating it. The generated config:

- retains the issue source, repository routes, safety gates, recipes,
  babysitter policy, and `mergePolicy`;
- replaces every laptop checkout path with the cloud clone root;
- pins the Relay workspace rather than depending on ambient selection;
- gives heartbeat, registry, reporting outbox, and preview state their own
  cloud-host paths; and
- emits the exact status, dry-run, and live-start argument arrays. Every one
  includes the absolute `--config` path.

The config is created mode `0600`; its parent directory is created mode `0700`
when absent. It contains policy, not credentials. Supply the cloud session or
workspace credentials through the host's secret manager. Do not set
`AGENT_RELAY_BIN` for Relayfile. Factory's mount path uses the Relayfile SDK and
the canonical rotating Cloud session.

## Stop/start handover

There is no supported cross-host active/active CLI topology. The laptop and
cloud host have independent file state stores, so the handover order is a
safety invariant rather than an optimization.

1. On the cloud host, while the laptop daemon is still active, run only the
   required non-dispatching checks:

   ```bash
   node bin/factory.mjs status --config /etc/agent-relay/factory.khaliq.config.json
   node bin/factory.mjs run-once --config /etc/agent-relay/factory.khaliq.config.json --dry-run
   ```

2. Stop the laptop daemon with its service manager or `SIGTERM`. Confirm the
   process has exited and its `loop-status` is stopped/stale before continuing.
   Record timestamps and process identities in the migration evidence.
3. Start the cloud daemon, with the exact config path, only after step 2:

   ```bash
   node bin/factory.mjs start --mode live --backend relay \
     --config /etc/agent-relay/factory.khaliq.config.json
   ```

4. Confirm the cloud heartbeat and Cloud instance identity
   `factory-khaliq-cloud`. The recorded laptop-stop time must precede the
   cloud-start time. A negative or missing gap fails the handover.
5. Keep the laptop service disabled but installed. Rollback uses the reverse
   order: stop and prove the cloud process absent, then start the laptop.

## Acceptance evidence for ephemeral agents

The old 24-hour continuous-node-uptime criterion is retired. It measured a
persistent execution node, which is not the supported model. A release passes
only with evidence for all of the following:

1. **Dispatcher independence:** with the laptop asleep or offline, the cloud
   Factory discovers one disposable opted-in issue, spawns its agent(s), and
   opens a PR. Capture the required built-CLI status and dry-run output plus the
   live dispatch, node-placement receipt, and PR receipt.
2. **Fresh execution:** every implementation/review agent receives a newly
   provisioned sandbox and unique node identity. Capture sandbox creation, the
   node becoming `online`/`live`, the agent process on that target host, terminal
   agent state, and sandbox destruction. No sandbox ID may be reused by another
   agent in the proof.
3. **Credential rotation:** force the staging access credential to expire while
   the cloud daemon remains alive. Capture a successful SDK refresh, advancing
   Relayfile reconciliation, another built-CLI `status`, and a subsequent
   dry-run or dispatch. Merely observing a long expiry time does not pass.
4. **Single dispatcher:** capture the laptop stop proof before cloud start and
   show that only `factory-khaliq-cloud` emits dispatch events during the live
   issue. Also exercise the reverse-order rollback once with a dry run.
5. **Publication:** the live agent pushes its issue-owned branch and the
   connected GitHub App opens the PR. If publication returns the known 403,
   stop and attach it to `relayfile-cloud#155`; do not fall back to `gh` or a
   personal identity.

An execution node that stays online for 24 hours does not satisfy any of these
criteria. A provision response without target-host process evidence also does
not prove execution.
