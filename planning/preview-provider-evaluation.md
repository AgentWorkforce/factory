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

`preview.services.<repo>` declares the local HTTP port and optional dev command
and stable tailnet HTTPS port. The node advertises
`preview:tailscale-serve`. Factory asks the selected node to create the route,
stores the returned reference on `AgentSpec`, and reuses it across the issue's
agents. The route lifetime is the issue's in-flight lifetime—not an agent PID.

At Human Review or Done, Factory removes the exact route before committing the
dispatch lifecycle as complete. On daemon startup it sends every non-terminal
dispatch identity to preview-capable nodes. Each node reaps registry entries
whose owner is absent. Teardown and sweeping are fail-closed: a route is touched
only when the Factory registry marker, preview ID/owner, HTTPS port, and live
upstream target all agree.

## Access-control guarantee

Factory accepts and surfaces only references whose access mode is exactly
`tailnet`. The provider adapter invokes `tailscale serve`, never
`tailscale funnel`, and the rendered task, Slack message, and PR description all
state that tailnet membership and the tailnet policy are required. If the node
does not return that guarantee, dispatch fails before a URL is posted.

Provider references: [Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve),
[Tailscale Serve CLI](https://tailscale.com/docs/reference/tailscale-cli/serve),
[Cloudflare Access policies](https://developers.cloudflare.com/cloudflare-one/access-controls/policies/),
[ngrok Agent API](https://ngrok.com/docs/agent/api), and
[ngrok OAuth Traffic Policy](https://ngrok.com/docs/traffic-policy/examples/oauth-protection).
