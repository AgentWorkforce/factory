/**
 * Factory build — p8 (Phase 2)
 * Spec: factory/planning/linear-issue-factory-cloud-p8-linear-webhook.md
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p8',
    slug: 'linear-webhook',
    description: 'Linear webhook ingress + Linear watch-match config so issue label/state changes drive triage.',
    repo: 'cloud',
    branch: 'ricky/factory-p8-linear-webhook',
    specFile: 'linear-issue-factory-cloud-p8-linear-webhook.md',
    fileTargets: [
      'packages/web/app/api/v1/webhooks/linear',
      'packages/web/lib/proactive-runtime',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -60 && npm test --if-present 2>&1 | tail -40',
    tier: 'deep',
    task: 'Add POST /api/v1/webhooks/linear mirroring the existing GitHub webhook route (signature validation, workspace resolution, normalizeWebhook, write-through to relayfile /linear/issues/**, then dispatchIntegrationWatchEvent). Extend watch-match config so factory subscriptions match Linear paths + label conditions reusing watch_globs/watch_rules. Respect the existing issue-level cooldown to coalesce label churn. FIRST verify whether relayfile already delivers Linear events through a shared /webhooks/{provider} pipe — if so this is just the watch-match config, not a new route.',
    prTitle: '[factory] p8: Linear webhook ingress + watch-match config',
    prSummary: 'Linear issue events reach cloud and drive the factory orchestrator like GitHub events already do, with cooldown-coalesced dispatch.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
