/**
 * Factory build — p3 (Phase 1 extraction prep #3)
 * Spec: factory/planning/linear-issue-factory-extract-p3-publish-prep.md
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p3',
    slug: 'publish-prep',
    description: 'Make @pear/factory-sdk publishable as @agent-relay/factory: dist build, drop private, bin rename.',
    repo: 'pear',
    branch: 'ricky/factory-p3-publish-prep',
    specFile: 'linear-issue-factory-extract-p3-publish-prep.md',
    fileTargets: [
      'packages/factory-sdk/package.json',
      'packages/factory-sdk/tsconfig.json',
      'packages/factory-sdk/tsconfig.build.json',
      'packages/factory-sdk/bin',
    ],
    acceptanceCmd: 'npm run build -w @pear/factory-sdk 2>&1 | tail -40 && npm pack --dry-run -w @pear/factory-sdk 2>&1 | tail -30',
    tier: 'standard',
    task: 'Make the package publishable as @agent-relay/factory: build with tsc into dist/, point exports at dist/, drop private:true, set npm public access, and rename the bin fleet -> factory. Pear continues consuming the local workspace package (no behavior change) until p4 swaps the dep. npm pack --dry-run must list only dist/, bin/, package.json, README.md.',
    prTitle: '[factory] p3: publishable build + bin rename (fleet -> factory)',
    prSummary: 'tsc dist build, exports -> dist, drop private, public access, bin rename. Prepares the package for extraction + npm publish (p4).',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
