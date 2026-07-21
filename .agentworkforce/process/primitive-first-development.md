# Primitive-first development

How new orchestration capability gets built in this repo, starting with the
current backlog of feature-map, dependency-ordering, and supervision work
(#128-#133 and their follow-ups). This formalizes what the existing
verification tiers in `.agentworkforce/features/verify/procedures.md` already
imply — Tier 1 (nothing but a checkout) and Tier 2 (fixture-backed full cycle)
are not just verification checkpoints, they're a build order.

## The two phases

### Phase A — hardened, standalone primitive

A pure module with **no dependency on `src/orchestrator/factory.ts`,
`harness-driver`, `FleetClient`, or any live provider**. Data in, data out.
Unit-tested in isolation the same way Tier 1 requires: no config, no fixture,
no network, no fleet.

- Lands as its own PR, reviewed and merged on its own, before any orchestrator
  wiring exists for it.
- The unit suite must cover the edge cases the feature exists to handle, not
  just the happy path — a graph primitive needs a cycle test, not just a
  linear-chain test; a classifier needs the boundary case that decides which
  branch it takes, not just an obviously-one-sided example.
- If a Phase A primitive turns out to need something from the orchestrator to
  be useful, that's a sign the boundary was drawn in the wrong place — redraw
  it rather than importing orchestrator state into the primitive.

### Phase B — wire-in, verified against a full example

Only starts once its Phase A primitive is already merged.

- Calls the primitive from the real dispatch path.
- Adds an **extended fixture scenario** to `test/fixtures/factory.config.json`
  (the same `/tmp/factory-tier2-config.json` pattern documented in
  `verify/procedures.md`'s Tier 2 section) that exercises this feature
  end-to-end, including its specific failure modes — not just "does it
  dispatch," the actual edge cases named in the issue.
- Does not merge until both the Phase A unit suite and the Phase B fixture
  scenario pass. Neither substitutes for the other: a primitive can be
  correct in isolation and still be wired in wrong, and a fixture scenario
  passing by accident (because the primitive was never really exercised) is
  the failure mode this whole split exists to prevent.

## Referencing this from an issue

Don't restate this document's contents in every issue body. Reference it —
`Process: see .agentworkforce/process/primitive-first-development.md` — and
in the issue itself only name the two concrete things that are specific to
that feature:

1. What the Phase A primitive is (module path, function signature, the edge
   cases its unit suite must cover).
2. What the Phase B fixture scenario must prove end-to-end.

## When this doesn't apply

Small, self-contained bug fixes and one-off operational scripts don't need a
Phase A/B split — this is for new orchestration primitives (graph/dependency
logic, checkpointing, classifiers, scope checks) where getting the boundary
and the edge-case coverage wrong is expensive to unwind after the fact. If
it's not obvious which category a piece of work falls into, default to
applying the split — the cost of an unnecessary primitive extraction is much
lower than the cost of an orchestrator change that turns out to be
untestable in isolation.
