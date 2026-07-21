# Live preview provider decision

Factory uses **Tailscale Serve** for its first live-preview integration. This is
an integration with the provider's existing node agent (`tailscaled`), not a new
sandbox or port-proxy runtime.

## Requirements

The provider must run on the same machine as a local or relay-placed fleet node,
outlive any individual implementer/reviewer/babysitter process, return a URL fit
for a Slack thread and pull-request description, and support guarded teardown
plus startup orphan recovery. A URL must never be surfaced without an explicit
access-control guarantee.

## Options considered

| Provider | Fleet-node and handoff fit | URL/access-control story | Lifecycle/API fit | Decision |
| --- | --- | --- | --- | --- |
| Tailscale Serve | The existing Tailscale node daemon proxies a node-local port. `tailscale serve --bg` persists independently of an agent process, so implementer → reviewer → babysitter handoffs keep the same route. | Serve is available only inside the configured tailnet; it is not Funnel. Normal tailnet grants/ACLs apply. Slack and PR recipients therefore authenticate through tailnet membership and policy. | The CLI provides background create, JSON status, and exact-port `off`. Factory records only routes it created and verifies the live HTTPS port and upstream target before disabling one. | **Chosen.** It has the smallest control plane and strongest fail-closed local contract for Factory's existing fleet-node model. |
| Cloudflare Tunnel + Access | `cloudflared` runs well on a fleet node and the tunnel can outlive workers. | Access offers excellent browser-native IdP policies for self-hosted applications. | A safe per-preview implementation must also create/reconcile DNS hostnames, tunnel ingress, Access applications, and policies through Cloudflare's control-plane APIs. That is materially more credentialed lifecycle state than Factory needs for the first provider. | Not selected initially. Revisit when reviewers must access previews without a tailnet client. |
| ngrok Agent Endpoints | The agent runs beside the checkout and its local API can start/stop endpoints dynamically. | Traffic Policies support OAuth/OIDC or basic authentication. | Endpoint lifetime is tied to a separately supervised ngrok agent session. Factory would need to own/recover that daemon as well as endpoint policy and identity metadata. | Not selected initially. The local API is attractive, but it introduces another supervised session lifecycle. |

## Factory contract

`preview.services.<repo>` declares a preferred local HTTP port, a required
bootstrap-plus-foreground dev command, an optional allocation span, and an optional stable tailnet HTTPS port. The
node advertises `preview:tailscale-serve`. Factory asks the selected node to create the route,
stores the returned reference on `AgentSpec`, and reuses it across the issue's
agents. The route lifetime is the issue's in-flight lifetime—not an agent PID.
The adapter reserves a distinct target port for concurrent issues using the
same repository. Because provisioning precedes agent startup, the command must
install any ignored dependencies needed by a fresh worktree before starting the
foreground server. The node starts it in the selected issue checkout
with `PORT` set, waits for a local HTTP response, and records the wrapper's PID,
start time, command line, checkout, and deterministic marker. The detached,
identity-checked process and `tailscaled` route both outlive any one agent and
are recovered together across handoffs and daemon restarts. The command gets a
minimal execution environment rather than inheriting Factory, Relay, or
provider credentials from the node process; repository-specific settings must
come from an intentional checkout-local environment mechanism.

Before publishing or recovering a URL, Factory resolves the local listener on
Linux (`/proc`) or macOS (`lsof`) and confirms its parent chain reaches the
exact supervised wrapper. An unrelated or indeterminate listener fails closed;
an active sweep also disables the exact Factory route if ownership changes.
This is a point-in-time guard. Eliminating the final listener-check/route-use
race would require socket activation or an intermediary proxy, which is outside
this integration's no-bespoke-runtime boundary.

At Human Review or Done, Factory removes the exact route and terminates only the
matching managed process tree before committing the
dispatch lifecycle as complete. On daemon startup it sends every non-terminal
dispatch identity to preview-capable nodes. Each node reaps registry entries
whose owner is absent in that Factory workspace namespace. Route intent is
persisted before either resource is started so a crash in the process-spawn or
provider API gap is recoverable. A periodic sweep retries nodes that were
offline during daemon startup. Teardown and sweeping are fail-closed: a route is touched only
when the Factory registry marker, workspace namespace, preview ID/owner, HTTPS
port, and live upstream target all agree. Operators should reserve
`preview.httpsPortRange` exclusively for Factory previews; pre-existing TCP,
Serve, or Funnel listeners are treated as occupied and are never adopted.

## Access-control guarantee

Factory accepts and surfaces only references whose access mode is exactly
`tailnet`. The provider adapter invokes `tailscale serve`, never
`tailscale funnel`, verifies the exact live proxy target, and rejects or removes
any candidate route whose Serve status has `AllowFunnel` enabled. The rendered
task, Slack message, and PR description all state that tailnet membership and
the tailnet policy are required. If the node does not return a credential-free
HTTPS URL with that guarantee, dispatch fails and removes the route before a URL
is posted.

Provider references: [Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve),
[Tailscale Serve CLI](https://tailscale.com/docs/reference/tailscale-cli/serve),
[Cloudflare Access policies](https://developers.cloudflare.com/cloudflare-one/access-controls/policies/),
[ngrok Agent API](https://ngrok.com/docs/agent/api), and
[ngrok OAuth Traffic Policy](https://ngrok.com/docs/traffic-policy/examples/oauth-protection).
