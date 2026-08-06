# Notion as a first-class Factory ticket feeder

Date: 2026-08-06

## Decision summary

Notion should remain a separate, explicit intake command that normalizes
repository tasks into GitHub lifecycle issues or dispatches exact-path fleet
work. It should not become a third `issueSource` until Factory has both native
Notion discovery semantics and a guarded lifecycle-writeback adapter. The
immediate correctness gap is cross-dispatcher claim durability.

## 1. Claim durability

**Assessment:** defect; fix now.

The prior `.factory/notion-intake-state.json`-style receipt was protected only
by a same-filesystem lock. Two machines using different state paths could both
observe no receipt and dispatch the same stable `notion:<page-id>:<target>` work
unit. The stable source identity and digest were already correct; the authority
was on the wrong surface.

**Recommendation and implementation:** use one immutable Agent Relay claim
channel per hashed source key. Channel-name uniqueness in the shared workspace
is the atomic cross-dispatcher gate. The winner must receive an acknowledged,
digest-bound claim record before GitHub issue creation, lifecycle publication,
agent spawn, or redispatch. A second dispatcher observes the record and cannot
spawn. A failed or ambiguous record write leaves an incomplete claim channel
and blocks subsequent dispatch, favoring an operator-visible stranded claim
over duplicate real work.

The manifest `statePath` remains a local receipt cache for reconciliation and
migration. It is no longer the authority. Agent Relay message idempotency was
not used as the claim primitive because it is sender-scoped and time-bounded;
workspace-global channel uniqueness supplies the required durable exclusion.

## 2. Lifecycle reconciliation to Notion

**Assessment:** deliberate boundary worth keeping now, not a defect to paper
over in Factory.

Relayfile currently presents the Notion page as a read-only execution contract.
Factory has guarded writeback adapters and lifecycle mappings for Linear,
Slack, and GitHub, but no Notion property/comment writeback contract. A native
Notion lifecycle would also need an operator-selected mapping for accepted,
dispatched, PR opened, blocked, and completed states. Inventing that mapping or
assuming a writable provider behind a read-only mount would weaken the existing
fail-closed model.

**Recommendation:** keep the page immutable and reconcile repository work on
the generated GitHub issue. Treat native Notion lifecycle as a separate product
decision requiring a real provider capability, guarded acknowledgement/readback
semantics, and a configured database/property mapping.

## 3. `issueSource` and routing-contract visibility

**Assessment:** terminology/documentation ambiguity; do not add `notion` to the
enum yet.

`issueSource` selects the orchestrator's discovery and lifecycle-writeback
adapter. Notion intake is intentionally an upstream normalization step, not an
adapter implementing that contract. Accepting `issueSource: "notion"` today
would promise discovery, triage, state transitions, comments, and terminal
reconciliation that do not exist.

**Recommendation:** retain `linear | github`, explicitly define the field as a
lifecycle issue source, and document that `factory intake notion` normalizes
into GitHub lifecycle or exact-path fleet work. A future native Notion adapter
can extend the enum only when it satisfies the whole lifecycle port.

## Contract resolution

Factory resolves exactly one config: the path supplied with `--config`, or
`./factory.config.json` in the command's current working directory. It does not
search the target repository, walk to a clone root, or merge configs. Notion
intake does not create an implicit fallback layer.

## Proof and current operational limitation

Automated coverage proves both repository publication and exact-path spawn are
preceded by the shared claim, claim-write failure prevents the external action,
and two manifests with independent local state paths produce only one issue or
one spawn.

Chief also has prior live evidence: the mounted Notion benchmark page produced
agent `notion-9a84f582-8fc3bc47` on `kjg-laptop`, with a durable fleet invocation
record and a portable Relay delivery receipt. Repository pages produced labeled
GitHub lifecycle issues (for example Cloud #2935 and Relay #1433).

A fresh production dispatch was intentionally not created during this change:
every page in Chief's active manifest already has a receipt, so another task
would be duplicate real work. `relayfile status` reports the Notion provider as
lagging with no sync cursor/watermark and the Chief snapshots are dated August
5. The remote page is currently readable and its digest matches the local
snapshot checked, but that does not establish fresh provider ingress. A new-page
live proof remains blocked until a fresh Notion sync watermark is observable.
