# Factory PR trajectory references

Factory-created pull requests end with the ruled reference-only pointer:

```html
<!-- trajectory: work_unit_id=AgentWorkforce/factory#260 work_unit_surface=github session_ref=<ai-hist-session-id> -->
```

`work_unit_surface` makes the identifier namespace explicit: Linear work uses
the issue key, GitHub work uses `owner/repo#number`, and work without a provider
ticket uses a Factory-synthesized id. Factory removes inherited pointer comments
from issue text before appending this single canonical pointer.

The session reference is captured when dispatch receives the originating prompt
(`dispatch(..., { sessionRef })`, falling back to `RELAY_ATTEST_SESSION_ID`). It
is stored in the existing durable dispatch lifecycle before any worker spawn,
then copied into implementer, reviewer, and babysitter prompts. Worker spawn and
resume session ids never replace it. A restart therefore recovers the same
originating reference before reconciling or publishing the PR.

The same originating prompt reference is passed to the existing attestation
grant write, replacing the former per-implementer value so SOC-2 traceability
and trajectory replay use one lineage substrate.

If the value is absent, not a canonical non-nil ai-hist session UUID, or unsafe,
Factory writes
`session_ref=missing`, logs an error after the PR is opened, increments
`trajectorySessionRefErrors`, and reports the affected PR under
`factory status` → `trajectoryErrors`. This is an explicit coverage failure,
not a resolvable reference.

The pointer is a reference, never conversation payload. Factory neither copies
conversation slices into Relayfile nor reads relayhistory. This is important for
cross-node trajectories: relayhistory is node-local, so reconstructing from it
would be silently incomplete after a handoff. Resolution belongs to the
relay-side session service, which uses the durable ai-hist id and applies access
control at read time; expiring relaycast message ids are not the primary key.
