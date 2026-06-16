Title: [factory] relay: Phase 3 — node-targeted placement; reject-and-reconcile unmapped repos

Team: AR
Suggested status: Ready for Agent
Repo label: relay
Project: Factory (f97660a3-a08c-4157-998f-e2d91951f3e7)
Depends on: p10 (RelayFleetClient), p11 (heartbeat/roster)
Epic: factory-cloud-watches-local-node-linear-issue.md §6, §8 (Phase 3); relay#1056 §6

---

## Context

p10 dispatches to "the workspace's single live node." Real placement needs to target the right node by name and reject work a node can't service (repo it has no local checkout for). This is the minimal relay#1056 §6 placement slice — NOT the full least-loaded scheduler.

## Goal

`spawn { capability, node }` places onto a named eligible node; if the node can't map the target repo, it rejects and the request is reconciled (re-queued or surfaced), never silently dropped.

## Scope

- Placement: `eligible = nodes where capability ∈ node.capabilities ∧ node.live`. Target by name when given; else pick any live eligible node for the workspace. (Least-loaded selection is out of scope — single/any is fine for v1.)
- Node pushes its `NodeConfig.repoPaths` keys up on registration; cloud only places work for repos the node maps.
- **Reject-and-reconcile**: a placement for an unmapped repo or missing capability is refused by the node; the orchestrator re-queues (bounded) or surfaces to Slack — defined explicitly, not a silent drop.
- None eligible → bounded-queue then fail (reuse the durable mailbox TTL semantics from p11).

## Acceptance criteria

1. A spawn targeted at a named node lands there; capability mismatch → clean hard fail.
2. A spawn for a repo the node doesn't map is rejected and reconciled (re-queued/surfaced), with a log line — never silently lost.
3. No eligible node → bounded-queue, drains when a node comes online, fails after TTL.
4. Two nodes in one workspace: work lands on a live eligible one without bleed.

## Out of scope

- Least-loaded / weighted scheduling, tags/fuzzy targeting, access control (#1056 §10).
- Cross-node migration / resume stickiness.
