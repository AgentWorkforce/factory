/**
 * Factory build — p5 (Phase 1.5 Pear teardown)
 * Spec: factory/planning/linear-issue-factory-extract-p5-pear-teardown.md
 * Depends on p4 (pear consumes published @agent-relay/factory).
 */
import { runFactoryWorkflow } from '../lib/factory-build-lib.ts';

async function main() {
  await runFactoryWorkflow({
    id: 'p5',
    slug: 'pear-teardown',
    description: 'Delete the in-Pear factory daemon model; reduce Pear to a read-only cloud view.',
    repo: 'pear',
    branch: 'ricky/factory-p5-pear-teardown',
    specFile: 'linear-issue-factory-extract-p5-pear-teardown.md',
    fileTargets: [
      'src/main/factory-manager.ts',
      'bin/pear.mjs',
      'src/shared/types/ipc.ts',
      'src/renderer/src/components/factory/FactoryPage.tsx',
      'src/renderer/src/App.tsx',
      'src/renderer/src/stores/ui-store.ts',
      'src/renderer/src/components/common/AppTopBar.tsx',
      'src/renderer/src/components/common/CommandMenu.tsx',
      'src/renderer/src/components/settings/AccountSettings.tsx',
      'src/renderer/src/lib/ipc-mock.ts',
    ],
    acceptanceCmd: 'npm run typecheck --if-present 2>&1 | tail -50 && npm test --if-present 2>&1 | tail -30',
    tier: 'standard',
    task: 'Delete the local-daemon process model: remove src/main/factory-manager.ts (FactoryManager + factoryManager singleton) and the `pear factory <action>` passthrough in bin/pear.mjs. In the factory IPC namespace (src/shared/types/ipc.ts) drop start/stop, repoint status/onEvent at cloud state, and scope readConfig/saveConfig to NodeConfig only. Convert FactoryPage.tsx from a control panel to a read-only cloud-backed status view. Update the renderer touchpoints. Grep `factory` across src/ to catch dangling imports. Nothing factory-specific may remain in Pear main.',
    prTitle: '[factory] p5: tear down in-Pear factory daemon; read-only cloud view',
    prSummary: 'Remove the Electron factory daemon (factory-manager, pear factory passthrough), drop start/stop IPC, repoint status at cloud, FactoryPage -> read-only. Pear becomes a consumer + viewer only.',
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
