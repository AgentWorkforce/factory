# Cloudflare verification environments

`CloudflareEnvironmentProvider` is Factory's Cloudflare-first implementation of
the substrate-neutral `EnvironmentProvider` port. It deliberately owns only the
Factory-side lifecycle. The dedicated private `AgentWorkforce/factory-test-infra`
repository owns account resources, the dispatch Worker, Container definitions,
usage monitoring, the scheduled reaper, and real-environment CI scripts.

## Credential boundary

Cloudflare credentials must not be placed in `factory.config.json` or read from
`process.env`. Configuration contains only a logical resource name and
source-control-safe policy:

```json
{
  "environments": {
    "cloudflare": {
      "resource": "FactoryTestInfra",
      "dispatchNamespacePrefix": "factory-verification",
      "maxConcurrentEnvironments": 2,
      "defaultTtlMs": 900000,
      "maxTtlMs": 3600000,
      "maxRunCostUsd": 1,
      "workerLimits": { "cpuMs": 100, "subrequests": 100 },
      "container": { "instanceType": "lite", "maxInstances": 3 }
    }
  }
}
```

At runtime, pass the linked SST `Resource.*`-style object explicitly:

```ts
import { CloudflareEnvironmentProvider } from '@agent-relay/factory/environments'

const provider = CloudflareEnvironmentProvider.fromResource({
  config: factoryConfig.environments.cloudflare,
  resources: {
    FactoryTestInfra: Resource.FactoryTestInfra,
  },
})
```

The linked resource has this shape:

```ts
{
  accountId: string
  apiToken: string
  dispatcherUrlTemplate?: 'https://.../{namespace}/{script}'
}
```

The API token needs the narrow Workers Scripts read/write permissions used by
the Workers for Platforms dispatch namespace API. Never log, persist, return in
an `Environment`, or copy that object into Factory configuration.

## Lifecycle and isolation

Each provision call:

1. Validates TTL, requested cost reservation, Container instance count, and the
   max-concurrent namespace cap before creating anything.
2. Creates a new dispatch namespace. A namespace reported as `trusted` is
   rejected and immediately removed.
3. Uploads an inert `factory-environment` Worker containing ownership, creation,
   expiration, and guardrail bindings. User workloads never receive the account
   token or bindings belonging to another namespace.
4. Returns the namespace identity and non-secret limits through `Environment`.

Destroy and reaper paths read the metadata Worker's binding lease and require
its `FACTORY_ENVIRONMENT_ID` to equal the namespace name before deletion. A
missing, incomplete, expired in the wrong direction, or mismatched lease fails
closed. The provider reaper handles TTL and optional owner-liveness checks; the
separate infra Cron Trigger performs the same check independently.

Cloudflare documents that Workers for Platforms user Workers are untrusted by
default, which isolates their cache and request metadata. Binding isolation is
still a deployment responsibility: the infra renderer must create or select
per-environment D1/KV/R2/Queue/Hyperdrive resources and attach only that
environment's identifiers to scripts in its dispatch namespace.

## Guardrail contract

The sentinel Worker exposes these non-secret bindings for the infra control
plane and scheduled reaper:

- `FACTORY_ENVIRONMENT_ID`, `FACTORY_OWNER_ID`, `FACTORY_CUSTOMER_ID`
- `FACTORY_REPOSITORY`, `FACTORY_CREATED_AT`, `FACTORY_EXPIRES_AT`
- `FACTORY_MAX_RUN_COST_USD`, `FACTORY_CONTAINER_MAX_INSTANCES`
- `FACTORY_WORKER_CPU_MS`, `FACTORY_WORKER_SUBREQUESTS`

Worker CPU and subrequest limits are also attached to the metadata Worker at
upload. Cloudflare Container deployments must copy `maxInstances` to Wrangler's
production-enforced `containers[].max_instances`. The infra usage monitor must
abort and tear down a run when elapsed time or metered cost reaches its binding;
the Factory provider's TTL reaper and the Cron Trigger remain independent
backstops.

## Verification workflow

`.github/workflows/cloudflare-verification.yml` is reusable through
`workflow_call`. It checks out Factory and the private infra repository, runs the
infra repository's exact `ci/verification-env/up.sh`, `run-suite.sh`, and
`down.sh` contract against Miniflare and a real Cloudflare account, and always
runs teardown. The real job consumes a single JSON resource secret through a
temporary mode-0600 file and uploads guardrail evidence even when an assertion
fails.

The real job is intentionally not part of ordinary fork PR execution. A trusted
caller supplies the private-repository checkout token and Cloudflare resource
secret. The scripts must fail loudly for isolation, quota/cost refusal,
concurrency refusal, orphan reaping, and k6 load absorption; a successful HTTP
smoke test alone is not an acceptable result.

Primary Cloudflare references:

- <https://developers.cloudflare.com/api/resources/workers_for_platforms/subresources/dispatch/>
- <https://developers.cloudflare.com/workers/platform/infrastructure-as-code/>
- <https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/how-workers-for-platforms-works/>
- <https://developers.cloudflare.com/containers/get-started/>
- <https://developers.cloudflare.com/workers/testing/vitest-integration/>
