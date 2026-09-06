import { describe, expect, it } from 'vitest'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { FactoryConfigSchema, NodeConfigSchema, loadFactoryConfig } from './schema'
import { routedPrRepos } from '../github/routed-pr-babysitter'

describe('safety.requireTitlePrefix', () => {
  it('accepts null as an explicit opt-out while omission keeps the default', () => {
    const labelOnly = FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/hoopsheet' },
      safety: { requireTitlePrefix: null, requireLabel: 'garden-ready' },
    })
    const defaulted = FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/factory' },
    })

    expect(labelOnly.safety.requireTitlePrefix).toBeNull()
    expect(defaulted.safety.requireTitlePrefix).toBe('[garden-e2e]')
    expect(FactoryConfigSchema.safeParse({
      repos: { default: 'AgentWorkforce/hoopsheet' },
      safety: { requireTitlePrefix: '', requireLabel: 'garden-ready' },
    }).success).toBe(false)
  })
})

describe('relay.agentName', () => {
  it('keeps an explicit relay agent name', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      relay: { agentName: 'factory-cloud' },
    })

    expect(parsed.relay.agentName).toBe('factory-cloud')
  })

  it('trims surrounding whitespace from an otherwise valid name', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      relay: { agentName: '  factory-cloud  ' },
    })

    expect(parsed.relay.agentName).toBe('factory-cloud')
  })

  // An identity that silently falls back to the default is how a workspace
  // registration collision hides: the operator believes they pinned a name.
  it.each([
    ['empty', ''],
    ['whitespace-only', '   '],
    ['a tab', '\t'],
  ])('rejects %s relay.agentName at config load instead of defaulting it', (_label, agentName) => {
    const result = FactoryConfigSchema.safeParse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      relay: { agentName },
    })

    expect(result.success).toBe(false)
    // Asserted by path and code, not by message text: the point is that the
    // trimmed value failed the length check under `relay.agentName`, rather
    // than some unrelated issue happening to mention the field.
    const issue = result.success ? undefined : result.error.issues.find(
      (candidate) => candidate.path.join('.') === 'relay.agentName',
    )
    expect(issue?.code).toBe('too_small')
  })

  // Split configs are how a cloud deployment and another deployment share one
  // workspace. If the identity could only live on the shared half, every
  // deployment would register the same name -- the collision this prevents.
  it('takes the relay agent name from the node-local half', () => {
    const loaded = loadFactoryConfig({
      workspaceConfig: { repos: { default: 'AgentWorkforce/factory' } },
      nodeConfig: { relay: { agentName: 'factory-cloud' } },
    })

    expect(loaded.factoryConfig.relay.agentName).toBe('factory-cloud')
    // The node-local half must carry it back too, or a per-host identity is
    // reflected as workspace-shared configuration.
    expect(loaded.nodeConfig.relay.agentName).toBe('factory-cloud')
  })

  it('lets the node-local relay agent name override the workspace default', () => {
    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { default: 'AgentWorkforce/factory' },
        relay: { agentName: 'factory-shared' },
      },
      nodeConfig: { relay: { agentName: 'factory-cloud' } },
    })

    expect(loaded.factoryConfig.relay.agentName).toBe('factory-cloud')
  })

  it('still applies a workspace-half relay agent name when the node half sets none', () => {
    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { default: 'AgentWorkforce/factory' },
        relay: { agentName: 'factory-shared' },
      },
      nodeConfig: {},
    })

    expect(loaded.factoryConfig.relay.agentName).toBe('factory-shared')
  })

  // Both halves are validated before the merge, so a node override cannot
  // launder an invalid shared value into a clean load on the hosts that
  // override it.
  it.each([
    ['workspaceConfig', { relay: { agentName: '   ' } }, { relay: { agentName: 'factory-cloud' } }],
    ['nodeConfig', { relay: { agentName: 'factory-shared' } }, { relay: { agentName: '   ' } }],
  ])('rejects an invalid relay agent name in the %s half even when the other half is valid', (field, workspaceRelay, nodeRelay) => {
    expect(() => loadFactoryConfig({
      workspaceConfig: { repos: { default: 'AgentWorkforce/factory' }, ...workspaceRelay },
      nodeConfig: nodeRelay,
    })).toThrow(new RegExp(`${field} has an invalid relay config`))
  })

  // Pins existing `normalizeLoadedConfig` semantics rather than asserting an
  // intent: both views are projected from the *merged* config, so the
  // workspace-shaped view reports the effective identity, exactly as it already
  // does for preview, cloneRoot, and clonePaths. Nothing serializes
  // `workspaceConfig` back to a shared file today; this test fails loudly if
  // that projection ever changes underneath a caller who starts to.
  it('reports the effective identity on the workspace-shaped view after a node override', () => {
    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { default: 'AgentWorkforce/factory' },
        relay: { agentName: 'factory-shared' },
      },
      nodeConfig: { relay: { agentName: 'factory-cloud' } },
    })

    expect(loaded.workspaceConfig.relay.agentName).toBe('factory-cloud')
    expect(loaded.nodeConfig.relay.agentName).toBe('factory-cloud')
  })

  it('rejects an unknown key under relay rather than ignoring a typo', () => {
    expect(() => FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      relay: { agentname: 'factory-cloud' },
    })).toThrow(/unrecognized key/i)
  })
})

describe('FactoryConfigSchema', () => {
  it('parses a valid config and applies defaults', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      slack: {
        channel: 'C123',
      },
    })

    expect(parsed.subscription).toEqual({
      teams: [],
      projects: [],
      labels: [],
      assignees: [],
    })
    expect(parsed.repos.byProject).toEqual({})
    expect(parsed.repos.keywordRules).toEqual([])
    expect(parsed.repos.clonePaths).toEqual({})
    expect(parsed.batchSize).toBe(1)
    expect(parsed.dispatch).toEqual({
      errorCooldownMs: 60_000,
      maxAttempts: 2,
      // Much shorter than the placed-agent hold: a slot-occupying lifecycle
      // that never placed an agent has nothing that can move it (#303).
      agentlessHoldTimeoutMs: 30 * 60_000,
      capacityWaitWarnMs: 30 * 60_000,
      agentHoldTimeoutMs: 4 * 60 * 60_000,
    })
    expect(parsed.fleetHealth).toEqual({
      rosterTimeoutMs: 5_000,
      failureThreshold: 2,
      resetTimeoutMs: 60_000,
      requireDedicatedBroker: false,
    })
    // Absent, not defaulted in the schema: RelayFleetClient owns the
    // `factory` fallback, so there is exactly one place the default lives.
    expect(parsed.relay).toEqual({})
    expect(parsed.models).toEqual({ babysitter: 'sonnet' })
    // Agent CLI per role defaults to today's behavior: codex implements, claude
    // reviews/babysits — so existing configs are unaffected unless set.
    expect(parsed.agentCapabilities).toEqual({
      implementer: 'spawn:codex',
      reviewer: 'spawn:claude',
      babysitter: 'spawn:claude',
    })
    expect(parsed.babysitter).toEqual({
      enabled: false,
      mode: 'factory-created',
      excludeLabels: ['garden:skip-babysitter'],
      excludePullRequests: [],
      notifyHumans: false,
    })
    expect(parsed.terminalState).toBe('human-review')
    expect(parsed.stateIds.humanReview).toBeUndefined()
    expect(parsed.loop.registryPath).toBe('/tmp/factory-run/factory-loop-registry.json')
    expect(parsed.loop.maxConsecutiveFailures).toBe(3)
    expect(parsed.reporting).toEqual({
      enabled: true,
      batchSize: 100,
      requestTimeoutMs: 15_000,
    })
    expect(parsed.github).toEqual({ identity: 'auto' })
    expect(parsed.verification).toEqual({
      enabled: true,
      descriptorPath: '.factory/verification-stack.yaml',
      maxConcurrentEnvironments: 2,
      maxRunTimeoutMs: 30 * 60_000,
      maxEnvironmentTtlMs: 60 * 60_000,
      maxTeardownTimeoutMs: 5 * 60_000,
    })
    expect(parsed.slack).toEqual({
      channel: 'C123',
      style: 'threaded-summarized',
      botUserId: 'U0B2596R7EZ',
      stakeholderUserIds: [],
      staleAfterMs: 10 * 60_000,
      conversationCoalesceMs: 750,
    })
    expect(parsed.mergePolicy).toBe('never')
    // No hardcoded state defaults: omitted stateIds resolve to {} and are filled
    // at runtime from linear.states (by name) or explicit stateIds.
    expect(parsed.stateIds).toEqual({})
    // The factory ships its workflow-state NAME conventions as defaults (so a
    // consumer needn't configure them); statesByTeam stays empty.
    expect(parsed.linear).toEqual({
      states: {
        readyForAgent: 'Ready for Agent',
        agentImplementing: 'Agent Implementing',
        done: 'Done',
        inPlanning: 'In Planning',
        humanReview: 'In Human Review',
      },
      statesByTeam: {},
      teamIds: {},
    })
    expect(parsed.safety).toEqual({
      requireTitlePrefix: '[garden-e2e]',
      requireLabel: 'garden',
      requireTeamKey: 'AR',
      neverAutoCloseLabels: ['incident', 'outage', 'sev1', 'sev2'],
      agentQuestionAuthors: [],
      agentQuestionRequireAllowlist: false,
    })
    expect(parsed.dryRun).toBe(false)
    expect(parsed.environments).toEqual({})
  })

  it('rejects ticket-dispatch notification targets that cannot deliver', () => {
    const withTarget = (target: Record<string, unknown>) => FactoryConfigSchema.safeParse({
      repos: { default: 'AgentWorkforce/factory' },
      hooks: { onTicketDispatch: { notify: [target] } },
    })

    expect(withTarget({ surface: 'slack' }).success).toBe(false)
    expect(withTarget({ surface: 'linear', commentOnIssue: false }).success).toBe(false)
    expect(withTarget({ surface: 'slack', dm: 'U123' }).success).toBe(true)
    expect(withTarget({ surface: 'linear', commentOnIssue: true }).success).toBe(true)
  })

  it('accepts secret-reference-only Kubernetes BYOC and managed connections', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/factory' },
      environments: {
        kubernetes: {
          connections: [
            {
              id: 'customer-eks',
              target: 'byoc',
              customers: ['customer-a'],
              repositories: ['AgentWorkforce/factory'],
              credential: { kind: 'irsa', secretRef: 'aws-sm:customer-a/verification-role' },
              protectedNamespaces: ['payments-prod'],
            },
            {
              id: 'factory-managed',
              target: 'managed',
              credential: { kind: 'kubeconfig', secretRef: 'env:FACTORY_MANAGED_KUBECONFIG' },
            },
          ],
        },
      },
    })

    expect(parsed.environments.kubernetes?.connections[0].credential.secretRef)
      .toBe('aws-sm:customer-a/verification-role')
    expect(parsed.environments.kubernetes?.connections[1].fidelityCaveat).toContain('may differ')
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      environments: {
        kubernetes: {
          connections: [{
            id: 'unsafe',
            credential: { kind: 'kubeconfig', secretRef: 'apiVersion: v1\nclusters: []' },
          }],
        },
      },
    })).toThrow(/never inline kubeconfig/iu)
  })

  it('preserves explicit model overrides', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      models: {
        implementer: 'gpt-5-codex',
        reviewer: 'claude-opus-4-1',
      },
    })

    expect(parsed.models).toMatchObject({
      implementer: 'gpt-5-codex',
      reviewer: 'claude-opus-4-1',
    })
  })

  it('bounds the Slack conversation coalescing window', () => {
    expect(FactoryConfigSchema.parse({ repos: {}, slack: { channel: 'C123', conversationCoalesceMs: 0 } })
      .slack?.conversationCoalesceMs).toBe(0)
    expect(() => FactoryConfigSchema.parse({
      repos: {}, slack: { channel: 'C123', conversationCoalesceMs: -1 },
    })).toThrow()
    expect(() => FactoryConfigSchema.parse({
      repos: {}, slack: { channel: 'C123', conversationCoalesceMs: 60_001 },
    })).toThrow()
  })

  it('accepts a bounded agent hold timeout and rejects an unbounded disable value', () => {
    expect(FactoryConfigSchema.parse({ repos: {}, dispatch: { agentHoldTimeoutMs: 2_500 } })
      .dispatch.agentHoldTimeoutMs).toBe(2_500)
    expect(() => FactoryConfigSchema.parse({ repos: {}, dispatch: { agentHoldTimeoutMs: 0 } })).toThrow()
    expect(() => FactoryConfigSchema.parse({
      repos: {}, dispatch: { agentHoldTimeoutMs: 7 * 24 * 60 * 60_000 + 1 },
    })).toThrow()
  })

  it('trims and validates an explicit reporting instance name', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: '  Oslo Factory  ' },
    })

    expect(parsed.reporting.instanceName).toBe('Oslo Factory')
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: '   ' },
    })).toThrow()
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      reporting: { instanceName: 'x'.repeat(257) },
    })).toThrow()
  })

  it('honors an explicit per-role agent CLI override and rejects unwired capabilities', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      // Swap the implementer CLI to claude to route around a codex-specific
      // failure; reviewer/babysitter fall back to their defaults.
      agentCapabilities: { implementer: 'spawn:claude' },
    })

    expect(parsed.agentCapabilities).toEqual({
      implementer: 'spawn:claude',
      reviewer: 'spawn:claude',
      babysitter: 'spawn:claude',
    })

    // spawn:opencode / spawn:gemini have no capabilityCli mapping yet, so the
    // schema refuses them rather than resolving to an undefined CLI at spawn.
    expect(() => FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/pear' },
      agentCapabilities: { implementer: 'spawn:opencode' },
    })).toThrow()
  })

  it('honors explicit babysitter, terminalState, and humanReview config', () => {
    const parsed = FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      babysitter: { enabled: true },
      terminalState: 'done',
      models: { babysitter: 'claude-sonnet-4-6' },
      stateIds: {
        readyForAgent: 'state-ready',
        agentImplementing: 'state-impl',
        done: 'state-done',
        inPlanning: 'state-plan',
        humanReview: 'state-human-review',
      },
    })

    expect(parsed.babysitter.enabled).toBe(true)
    expect(parsed.terminalState).toBe('done')
    expect(parsed.models.babysitter).toBe('claude-sonnet-4-6')
    expect(parsed.stateIds.humanReview).toBe('state-human-review')
  })

  it('supports an explicit GitHub issue source while leaving source auto-detection enabled by default', () => {
    const auto = FactoryConfigSchema.parse({ repos: { default: 'AgentWorkforce/factory' } })
    const github = FactoryConfigSchema.parse({
      issueSource: 'github',
      repos: { default: 'AgentWorkforce/factory' },
    })

    expect(auto.issueSource).toBeUndefined()
    expect(github.issueSource).toBe('github')
  })

  it('normalizes an explicit workspace mirror root and rejects unsafe values', () => {
    const parsed = FactoryConfigSchema.parse({
      localMountRoot: '  /work/chief/.integrations  ',
      repos: { default: 'AgentWorkforce/factory' },
    })

    expect(parsed.localMountRoot).toBe('/work/chief/.integrations')
    for (const localMountRoot of ['', '   ', './.integrations', 'relative/mirror']) {
      expect(() => FactoryConfigSchema.parse({
        localMountRoot,
        repos: { default: 'AgentWorkforce/factory' },
      })).toThrow()
    }
  })

  it('keeps Notion on the separate intake path instead of accepting it as a lifecycle issue source', () => {
    expect(() => FactoryConfigSchema.parse({
      issueSource: 'notion',
      repos: { default: 'AgentWorkforce/factory' },
    })).toThrow()
  })

  it.each(['app', 'user', 'auto'] as const)('accepts github.identity %s', (identity) => {
    const parsed = FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/factory' },
      github: { identity },
    })

    expect(parsed.github.identity).toBe(identity)
  })

  it('rejects an unsupported GitHub PR identity', () => {
    expect(() => FactoryConfigSchema.parse({
      repos: { default: 'AgentWorkforce/factory' },
      github: { identity: 'installation-owner' },
    })).toThrow()
  })

  it('parses dynamic per-team Linear state name mappings', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: { byLabel: { pear: 'AgentWorkforce/pear' } },
      linear: {
        states: { readyForAgent: 'Ready for Agent', done: 'Done' },
        teamIds: { AR: 'team-ar' },
        statesByTeam: {
          ENG: { readyForAgent: 'To Do', done: 'Shipped' },
        },
      },
    })

    expect(parsed.linear.states.readyForAgent).toBe('Ready for Agent')
    expect(parsed.linear.teamIds.AR).toBe('team-ar')
    expect(parsed.linear.statesByTeam.ENG).toEqual({ readyForAgent: 'To Do', done: 'Shipped' })
    expect(parsed.stateIds).toEqual({})
  })

  it('derives byLabel, clonePaths, and subscription.labels from a compact repos config', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        org: 'AgentWorkforce',
        cloneRoot: '/work/AgentWorkforce/',
        names: ['pear', 'cloud', 'agentswarm'],
        overrides: { agentswarm: 'AgentWorkforce/AgentSwarm' },
        default: 'pear',
      },
    })

    expect(parsed.repos.byLabel).toEqual({
      pear: 'AgentWorkforce/pear',
      cloud: 'AgentWorkforce/cloud',
      agentswarm: 'AgentWorkforce/AgentSwarm',
    })
    expect(parsed.repos.clonePaths).toEqual({
      'AgentWorkforce/pear': '/work/AgentWorkforce/pear',
      'AgentWorkforce/cloud': '/work/AgentWorkforce/cloud',
      'AgentWorkforce/AgentSwarm': '/work/AgentWorkforce/AgentSwarm',
    })
    // subscription.labels defaults to the repo names
    expect(parsed.subscription.labels).toEqual(['pear', 'cloud', 'agentswarm'])
    expect(parsed.repos.default).toBe('pear')
    expect(parsed.repos.org).toBe('AgentWorkforce')
    expect(parsed.repos.names).toEqual(['pear', 'cloud', 'agentswarm'])
  })

  it('requires explicit routed repositories for routed PR babysitting', () => {
    expect(() => FactoryConfigSchema.parse({
      babysitter: { enabled: true, mode: 'routed-open-prs' },
      repos: {},
    })).toThrow(/repos\.names must contain at least one repository/u)

    expect(FactoryConfigSchema.parse({
      babysitter: { enabled: true, mode: 'factory-created' },
      repos: {},
    }).babysitter.mode).toBe('factory-created')

    expect(() => FactoryConfigSchema.parse({
      babysitter: { enabled: true, mode: 'routed-open-prs' },
      repos: { names: ['pear'] },
    })).toThrow(/must resolve at least one owner\/repository route/u)
  })

  it('does not reject a byLabel bare-name entry that org would successfully resolve at runtime', () => {
    // requireRoutedBabysitterRepos runs in a superRefine BEFORE the
    // normalizeFactoryConfig transform, so it only sees the raw pre-merge
    // repos.byLabel the user wrote -- it previously replicated the org-prefix
    // fallback by hand, but only for names with no byLabel/overrides entry at
    // all. An explicit byLabel value that itself lacks a slash (a common
    // shorthand: map a label to a bare repo name, let `org` supply the
    // owner) got no such rescue and was wrongly rejected here, even though
    // routedPrRepos applies exactly that rescue to any resolved value
    // lacking a slash, regardless of where it came from.
    const parsed = FactoryConfigSchema.parse({
      babysitter: { enabled: true, mode: 'routed-open-prs' },
      repos: { names: ['pear'], org: 'AgentWorkforce', byLabel: { pear: 'pear-fork' } },
    })
    expect(parsed.repos.byLabel.pear).toBe('pear-fork')
    expect(routedPrRepos(parsed)).toEqual(['AgentWorkforce/pear-fork'])
  })

  it('accepts one-character repository opt-out identities', () => {
    const parsed = FactoryConfigSchema.parse({
      babysitter: {
        enabled: true,
        excludePullRequests: ['owner/r#1'],
      },
      repos: {},
    })

    expect(parsed.babysitter.excludePullRequests).toEqual(['owner/r#1'])
  })

  it('mirrors the owner-segment length rule onto the repo segment', () => {
    const withRepo = (repo: string) => FactoryConfigSchema.parse({
      babysitter: { enabled: true, excludePullRequests: [`owner/${repo}#1`] },
      repos: {},
    })

    expect(() => withRepo('')).toThrow(/expected owner\/repo#number/u)
    expect(withRepo('r').babysitter.excludePullRequests).toEqual(['owner/r#1'])
    expect(withRepo('r'.repeat(100)).babysitter.excludePullRequests).toEqual([`owner/${'r'.repeat(100)}#1`])
    expect(() => withRepo('r'.repeat(101))).toThrow(/expected owner\/repo#number/u)
  })

  it('lets explicit byLabel/clonePaths/labels override the derived ones', () => {
    const parsed = FactoryConfigSchema.parse({
      subscription: { labels: ['pear'] },
      repos: {
        org: 'AgentWorkforce',
        cloneRoot: '/work',
        names: ['pear', 'cloud'],
        byLabel: { cloud: 'Other/cloud-fork' },
        clonePaths: { 'AgentWorkforce/pear': '/custom/pear' },
      },
    })

    expect(parsed.repos.byLabel.cloud).toBe('Other/cloud-fork')
    expect(parsed.repos.byLabel.pear).toBe('AgentWorkforce/pear')
    expect(parsed.repos.clonePaths['AgentWorkforce/pear']).toBe('/custom/pear')
    expect(parsed.repos.clonePaths['Other/cloud-fork']).toBe('/work/cloud-fork')
    // explicit subscription.labels is preserved (not overwritten by names)
    expect(parsed.subscription.labels).toEqual(['pear'])
  })

  it('expands exact ~ and ~/ in cloneRoot and explicit clonePaths while preserving precedence', () => {
    const parsed = FactoryConfigSchema.parse({
      cloneRoot: '~/top-level',
      clonePaths: { 'AgentWorkforce/pear': '~/top-level-explicit' },
      repos: {
        org: 'AgentWorkforce',
        names: ['pear', 'cloud'],
        cloneRoot: '~/legacy-root',
        clonePaths: {
          'AgentWorkforce/pear': '~/legacy-explicit',
          'AgentWorkforce/cloud': '~',
        },
      },
    })

    expect(parsed.cloneRoot).toBe(join(homedir(), 'top-level'))
    expect(parsed.clonePaths).toEqual({
      'AgentWorkforce/pear': join(homedir(), 'top-level-explicit'),
      'AgentWorkforce/cloud': homedir(),
    })
    expect(parsed.repos.clonePaths).toEqual(parsed.clonePaths)
  })

  it('derives clone paths from an exact ~ legacy cloneRoot', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        org: 'AgentWorkforce',
        names: ['factory'],
        cloneRoot: '~',
      },
    })

    expect(parsed.cloneRoot).toBe(homedir())
    expect(parsed.clonePaths).toEqual({
      'AgentWorkforce/factory': join(homedir(), 'factory'),
    })
  })

  it('expands node-only and split clone paths consistently', () => {
    expect(NodeConfigSchema.parse({
      cloneRoot: '~',
      clonePaths: { 'AgentWorkforce/factory': '~/Projects/factory' },
    })).toMatchObject({
      cloneRoot: homedir(),
      clonePaths: { 'AgentWorkforce/factory': join(homedir(), 'Projects/factory') },
    })

    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { org: 'AgentWorkforce', names: ['factory'] },
      },
      nodeConfig: {
        cloneRoot: '~/Projects/AgentWorkforce',
        clonePaths: { 'AgentWorkforce/factory': '~' },
      },
    })
    expect(loaded.factoryConfig.cloneRoot).toBe(join(homedir(), 'Projects/AgentWorkforce'))
    expect(loaded.factoryConfig.clonePaths).toEqual({ 'AgentWorkforce/factory': homedir() })
    expect(loaded.factoryConfig.repos.clonePaths).toEqual(loaded.factoryConfig.clonePaths)
    expect(loaded.nodeConfig.cloneRoot).toBe(join(homedir(), 'Projects/AgentWorkforce'))
    expect(loaded.nodeConfig.clonePaths).toEqual(loaded.factoryConfig.clonePaths)
  })

  it('merges split preview services and enforces a tailnet-only provider', () => {
    const loaded = loadFactoryConfig({
      workspaceConfig: {
        repos: { default: 'AgentWorkforce/factory' },
        preview: {
          services: { factory: { port: 3_000, portSpan: 25, startCommand: 'npm run dev' } },
        },
      },
      nodeConfig: {
        preview: {
          services: { pear: { port: 4_173, startCommand: 'npm run dev' } },
          registryPath: '~/.factory/test-previews.json',
        },
      },
    })

    expect(loaded.factoryConfig.preview).toEqual({
      provider: 'tailscale-serve',
      access: 'tailnet',
      services: {
        factory: { port: 3_000, portSpan: 25, startCommand: 'npm run dev' },
        pear: { port: 4_173, startCommand: 'npm run dev' },
      },
      tailscaleBinary: 'tailscale',
      registryPath: join(homedir(), '.factory/test-previews.json'),
      httpsPortRange: [10_000, 10_999],
    })
    expect(loaded.nodeConfig.preview).toEqual(loaded.factoryConfig.preview)
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      preview: { provider: 'tailscale-funnel', access: 'public', services: {} },
    })).toThrow()
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      preview: { services: {}, httpsPortRange: [11_000, 10_000] },
    })).toThrow('preview.httpsPortRange start must be less than or equal to end')
    expect(() => FactoryConfigSchema.parse({
      repos: {},
      preview: { services: { factory: { port: 65_500, startCommand: 'npm run dev' } } },
    })).toThrow('preview service port range must end at or below 65535')
  })

  it('does not rewrite embedded tildes', () => {
    const parsed = FactoryConfigSchema.parse({
      cloneRoot: '/work/~shared',
      repos: {
        byLabel: { pear: 'AgentWorkforce/pear' },
        clonePaths: { 'AgentWorkforce/pear': '/work/~shared/pear' },
      },
    })

    expect(parsed.cloneRoot).toBe('/work/~shared')
    expect(parsed.clonePaths['AgentWorkforce/pear']).toBe('/work/~shared/pear')
  })

  it.each([
    { input: { cloneRoot: '~other', repos: {} }, field: 'cloneRoot' },
    { input: { repos: { cloneRoot: '~other/projects' } }, field: 'cloneRoot' },
    {
      input: { repos: { clonePaths: { 'AgentWorkforce/pear': '~other/pear' } } },
      field: 'repos.clonePaths["AgentWorkforce/pear"]',
    },
  ])('rejects unsupported ~user syntax in $field', ({ input, field }) => {
    expect(() => FactoryConfigSchema.parse(input)).toThrow(`${field} does not support ~user expansion`)
  })

  it('rejects ~user syntax even when a higher-precedence value overrides it', () => {
    expect(() => FactoryConfigSchema.parse({
      cloneRoot: '/top-level',
      clonePaths: { 'AgentWorkforce/pear': '/top-level/pear' },
      repos: {
        cloneRoot: '~other/legacy',
        clonePaths: { 'AgentWorkforce/pear': '~other/pear' },
      },
    })).toThrow('repos.cloneRoot does not support ~user expansion')

    expect(() => loadFactoryConfig({
      workspaceConfig: {
        repos: { cloneRoot: '~other/legacy' },
      },
      nodeConfig: { cloneRoot: '/node-root' },
    })).toThrow('workspaceConfig.repos.cloneRoot does not support ~user expansion')
  })

  it('still accepts the legacy explicit-only repos form', () => {
    const parsed = FactoryConfigSchema.parse({
      repos: {
        byLabel: { pear: 'AgentWorkforce/pear' },
        clonePaths: { 'AgentWorkforce/pear': '/work/pear' },
        default: 'AgentWorkforce/pear',
      },
    })

    expect(parsed.repos.byLabel).toEqual({ pear: 'AgentWorkforce/pear' })
    expect(parsed.repos.clonePaths).toEqual({ 'AgentWorkforce/pear': '/work/pear' })
  })

  it('rejects batch sizes over the ceiling', () => {
    expect(() => FactoryConfigSchema.parse({
      workspaceId: 'ws_123',
      repos: {
        byLabel: {
          pear: 'AgentWorkforce/pear',
        },
      },
      batchSize: 26,
    })).toThrow()
  })
})
