# Cloudflare durable Factory state

Factory defaults to its existing file-backed state. A Cloudflare Container must
select the durable backend explicitly:

```text
FACTORY_STATE_BACKEND=cloudflare-do
FACTORY_STATE_URL=http://factory-state.do/<stable-object-name>/v1/document
```

`FACTORY_STATE_URL` is an internal virtual hostname handled by the Container's
outbound Worker bridge. Factory does not fall back to a file or to an empty
document when that URL is unreadable. CLI startup reads and validates the
durable document before constructing `Factory`, so discovery and dispatch do
not run on unknown state. Once the gate succeeds, `factory status` includes
`"stateBackend": "cloudflare-do"` for the deployment supervisor.

## Worker and Durable Object wiring

The deployment Worker needs one SQLite-backed Durable Object binding:

```toml
[[durable_objects.bindings]]
name = "FACTORY_STATE"
class_name = "FactoryStateDurableObject"

[exports.FactoryStateDurableObject]
type = "durable-object"
storage = "sqlite"
```

For a Worker still using legacy migrations rather than declarative `exports`,
declare the class once with `new_sqlite_classes`. Do not configure both forms.

The Durable Object class delegates storage and protocol handling to the
worker-safe `@agent-relay/factory/hosted` entrypoint:

```ts
import { DurableObject } from 'cloudflare:workers'
import {
  DurableObjectWatchStateService,
  SqliteDurableDocumentPersistence,
} from '@agent-relay/factory/hosted'

export class FactoryStateDurableObject extends DurableObject<Env> {
  readonly service: DurableObjectWatchStateService

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.service = new DurableObjectWatchStateService(
      new SqliteDurableDocumentPersistence(ctx.storage),
    )
  }

  fetch(request: Request): Promise<Response> {
    return this.service.fetch(request)
  }
}
```

The Container outbound handler for `factory-state.do` extracts the stable
object name from the first URL path component, resolves
`env.FACTORY_STATE.getByName(name)`, and forwards the request to that stub. It
must not expose a public state route. The object name must remain stable across
Container restarts and deployments.

## Storage and concurrency contract

The DO stores one metadata row and chunked logical records. Map-backed state is
one logical record per existing key. Discovery lease/backoff metadata is
separate from checkpoint metadata and from each tree prefix, so a lease renewal
does not rewrite the large discovery snapshot. Chunks remain far below
Cloudflare's 2 MB SQLite row/BLOB/string limit.

Clients use a numeric revision as an ETag. Writes carry the revision read by the
mutation and the DO applies the complete logical-record patch in one SQLite
transaction. A conflict returns HTTP 409; Factory reloads and retries the pure
state transition. This is the cross-process/cross-node atomicity boundary.

## One-time cutover import

A new object is deliberately uninitialized. `GET` returns HTTP 503 until an
explicit, create-only `PUT` imports the current validated v3 document. A second
initialization returns HTTP 409 and cannot overwrite live state.

Use `CloudflareWatchStateDocumentStore.initialize(document)` from a one-time
importer that has a remote service binding to the production DO namespace. The
cutover sequence is:

1. Stop the laptop Factory and verify its exit codes.
2. Hash and measure the current state file, then parse it as v3.
3. Initialize the stable object exactly once through the private binding.
4. Read the document back and verify the hash-equivalent parsed content.
5. Start the Container with the two environment variables above.
6. Delete the Container's local state directory, restart it, and verify the
   existing claim still prevents redispatch.

Never make ordinary Container startup initialize the object. Treating an
unreachable or uninitialized backend as an empty document is the catastrophic
duplicate-dispatch failure this backend exists to prevent.
