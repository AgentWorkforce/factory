# Completed-session replay pointers on Factory PRs

Every pull request Factory opens ends with the ruled, reference-only trajectory
marker:

```html
<!-- trajectory: work_unit_id=AgentWorkforce/factory#260 work_unit_surface=github session_ref=<relay-session-uuid> -->
```

The three keys are the contract. `session_ref` is the existing opaque UUID
emitted by Relay; Factory does not mint a replay id or add a fourth key. Linear
work uses its issue key, GitHub work uses `owner/repo#number`, and work without a
provider ticket uses a Factory-synthesized work-unit id.

Factory deliberately does not write `relay session replay <session_ref>` or a
retention claim into the PR body. Replay availability changes after publication
as the workspace's pricing-tier retention window advances. An authenticated
resolver reads this marker, obtains the workspace's live `retained-since` or
never-prune boundary, and only then renders the copyable replay command. If the
conversation has aged out, the resolver must show incomplete coverage and must
not render it as replayable.

When Factory has no canonical non-nil session UUID, the marker says
`session_ref=missing`. The SDK parser does not return that marker as resolver
input. Parsing a UUID does not itself establish replay availability. Factory
also strips inherited trajectory markers from issue text before
appending its single canonical marker.

The PR carries only a reference. Conversation payload and access control remain
at resolution time, and replay of completed work stays distinct from attaching
to or relocating a running session.
