# In-cluster load harness

`runLoad(environment, profile)` creates a k6 Job in the environment's Kubernetes
namespace, waits for it to complete, captures its summary, and returns a
structured SLO verdict. The environment endpoint map should contain URLs that
are reachable from a pod; Kubernetes Service DNS names are preferred when the
target stack runs in the same cluster.

```ts
import { runLoad } from '@agent-relay/factory'

const result = await runLoad(
  {
    id: 'verify-123',
    namespace: 'verify-123',
    endpoints: { api: 'http://api.verify-123.svc.cluster.local:8080' },
  },
  {
    name: 'api-load',
    targets: [
      { name: 'read', endpoint: 'api', path: '/items', weight: 4 },
      {
        name: 'write',
        endpoint: 'api',
        path: '/items',
        method: 'POST',
        body: { value: 'example' },
      },
    ],
    vus: 50,
    duration: '30s',
    ramp: { up: '5s', down: '2s' },
    thresholds: {
      maxP95LatencyMs: 2_000,
      maxP99LatencyMs: 5_000,
      maxErrorRate: 0.01,
      minThroughputRps: 10,
    },
  },
  { evidencePath: 'artifacts/load.json' },
)

if (!result.passed) {
  console.error(result.violations)
}
```

Set `rps` to use an arrival-rate scenario; `vus` then controls preallocated
workers and `maxVus` caps scaling. Without `rps`, k6 runs a VU-based scenario.
Profiles can also be checked in as JSON or YAML and loaded with
`loadLoadProfile(path)`.

The returned `evidence` and `evidenceJson` include request count, p95 and p99,
error rate, throughput, a cumulative latency histogram, thresholds, and every
violated metric. Request headers and bodies are deliberately omitted from
evidence. The ephemeral ConfigMap and Job are deleted by default; pass
`cleanup: false` when a surrounding environment teardown owns cleanup.
The gate also fails closed with a `requestCount` violation when k6 completes
without sending any requests, even if the profile omitted a throughput floor.

Run the real kind-cluster proof with:

```bash
kind create cluster --name factory-load-e2e
npm run test:e2e:load
kind delete cluster --name factory-load-e2e
```

The E2E deploys a healthy service, drives a 50-VU load by default, records its
evidence, asserts a lenient SLO passes, and asserts a strict p95 SLO fails.
