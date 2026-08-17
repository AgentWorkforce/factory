# Document state-store port

`DocumentStateStore` owns Factory's existing claim, lease, lifecycle,
conversation, babysitter, and discovery behavior. It delegates only persistence
and serialization to `WatchStateDocumentStore`:

```ts
interface WatchStateDocumentStore {
  read(): Promise<WatchStateDocument>
  write(document: WatchStateDocument): Promise<void>
  runMutation<T>(operation: () => Promise<T>): Promise<T>
  assertReady(): Promise<void>
}
```

`runMutation` must serialize a complete read/modify/write callback against all
other writers that share the backend. A compare-and-set backend may retry that
callback after a conflict. It must never translate an unreadable or
uninitialized backend into `{ version: 3, workspaces: {} }`.

`FileStateStore` remains the default. Its adapter keeps the existing advisory
file lock, private temporary file, file sync, atomic rename, parent-directory
sync, pretty JSON bytes, and missing-file behavior.

## Embedded CLI adapter

Hosts that need a different persistence implementation can use the public
`@agent-relay/factory/cli` entrypoint:

```ts
import { runFleetCli } from '@agent-relay/factory/cli'
import { DocumentStateStore } from '@agent-relay/factory'

const exitCode = await runFleetCli(process.argv.slice(2), {
  stateStoreFactory: (config) => new DocumentStateStore({
    batchSize: config.batchSize,
    backend: 'host-defined-backend',
    documentStore,
  }),
})
```

The CLI invokes `assertReady()` on every injected store before it constructs
`Factory`. A failed readiness check exits nonzero and prevents discovery and
dispatch. If the adapter supplies a `backend` identifier, status JSON exposes
it as `stateStore.backend`; Factory does not interpret that host-defined value.
