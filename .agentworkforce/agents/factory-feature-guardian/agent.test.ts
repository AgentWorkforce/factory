import { readFileSync } from 'node:fs';
import type { WorkforceCtx } from '@agentworkforce/runtime';
import { parsePersonaSpec } from '@agentworkforce/persona-kit';
import {
  bindPreviewTransport,
  slackClient,
  type RelayTransport,
  type RelayTransportRequest,
  type RelayTransportWriteRequest,
  type WritebackResult,
} from '@relayfile/relay-helpers';
import { describe, expect, it, vi } from 'vitest';
import guardian, {
  CYCLE_STATE_PATH,
  ProgressStateConflictError,
  SLACK_WRITEBACK_POLL_MS,
  SLACK_WRITEBACK_TIMEOUT_MS,
  createSdkProgressStore,
  deliveredSlackTs,
  factoryFeatureGuardianAdapters,
  featurePostIdempotencyKey,
  gateFactoryGuardianTier,
  loadFactoryGuardianCatalog,
  parseFactoryGuardianTestCounts,
  parseManifestFeatures,
  resolveManifestPath,
  resolveFactoryGuardianProcedure,
  runGuardian,
  runFactoryGuardianProcedure,
  type ProgressState,
} from './agent.js';

const persona = JSON.parse(readFileSync(new URL('./persona.json', import.meta.url), 'utf8')) as {
  harness?: string;
  model?: string;
  useSubscription?: boolean;
  integrations: Record<
    string,
    {
      optional?: boolean;
      enabledByInput?: string;
      relayfileMount?: { requiredReadPaths?: unknown; writeOnlyPaths?: unknown };
    }
  >;
  inputs: { SLACK_CHANNEL: { default: string } };
  memory: { enabled: boolean; scopes: string[]; ttlDays: number };
};

const manifestFeatures = [
  {
    id: 'broker-up',
    name: 'Start Broker',
    cli: 'relay node up',
    description: 'Starts the local broker.',
    tier: 1,
  },
  {
    id: 'broker-down',
    name: 'Stop Broker',
    cli: 'relay node down',
    description: 'Stops the local broker.',
    tier: 2,
  },
  {
    id: 'broker-status',
    name: 'Broker Status',
    cli: 'relay node status',
    description: 'Shows broker status.',
    tier: 1,
  },
  ...Array.from({ length: 119 }, (_, index) => ({
    id: `feature-${index + 4}`,
    name: `Feature ${index + 4}`,
    cli: `relay feature-${index + 4}`,
    description: `Checks feature ${index + 4}.`,
    tier: 6,
  })),
];

function renderManifest(features: typeof manifestFeatures): string {
  const tierCounts = new Map<number, number>()
  for (let tier = 1; tier <= 6; tier += 1) tierCounts.set(tier, 0)
  for (const feature of features) {
    tierCounts.set(feature.tier, (tierCounts.get(feature.tier) ?? 0) + 1)
  }
  return [
    "version: '1.1'",
    'catalog:',
    '  category_count: 1',
    `  feature_count: ${features.length}`,
    '  tier_counts:',
    ...Array.from({ length: 6 }, (_, index) =>
      `    ${index + 1}: ${tierCounts.get(index + 1) ?? 0}`),
    'verification:',
    '  document: .agentworkforce/features/verify/procedures.md',
    '  categories:',
    '    core: cli-and-package',
    'categories:',
    '  core:',
    '    name: Core',
    '    criticality: critical',
    '    features:',
    ...features.flatMap((feature) => [
      `      - id: ${feature.id}`,
      `        name: ${feature.name}`,
      `        cli: ${feature.cli}`,
      `        description: ${feature.description}`,
      '        location: src/index.ts',
      `        verify_tier: ${feature.tier}`,
    ]),
  ].join('\n');
}

const manifest = renderManifest(manifestFeatures);

const cycleStatePath = '/memory/workspace/factory-feature-guardian/cycle-state.json';

const storeFeatures = manifestFeatures.map((feature) => ({
  id: feature.id,
  name: feature.name,
  cli: feature.cli,
  desc: feature.description,
  category: 'core',
  location: 'src/index.ts',
  procedure: 'cli-and-package',
  tier: feature.tier,
  criticality: 'critical' as const,
}));

const orderedManifestFeatures = [...manifestFeatures].sort((a, b) => a.tier - b.tier);

function progressState(checkedCount: number, generation = 1): ProgressState {
  const checkedIds = orderedManifestFeatures.slice(0, checkedCount).map((feature) => feature.id);
  return {
    kind: 'factory-feature-guardian:progress',
    version: 3,
    generation,
    checkedIds,
    cycleStartedAt: generation === 1 ? '2026-07-18T10:26:47.981Z' : '2026-07-18T11:26:47.981Z',
    totalFeatures: 122,
    ...(checkedCount > 0
      ? {
          lastPost: {
            featureId: checkedIds.at(-1) as string,
            ts: `1784370${checkedCount}.029509`,
          },
        }
      : {}),
  };
}

class RelayfileStateServer {
  content: string | null;
  revision = 1;
  failGetStatus = 0;
  hangMethod: 'GET' | 'PUT' | null = null;
  corruptReadBack = false;
  invalidFileResponse = false;
  invalidOperationResponse = false;
  invalidQueuedResponse = false;
  readonly requests: Array<{ method: string; ifMatch: string | null }> = [];

  constructor(seed: ProgressState | null) {
    this.content = seed ? `${JSON.stringify(seed)}\n` : null;
  }

  readonly fetch = (async (input: URL | RequestInfo, init: RequestInit = {}) => {
    const method = init.method ?? 'GET';
    const headers = new Headers(init.headers);
    this.requests.push({ method, ifMatch: headers.get('if-match') });
    const url = new URL(input instanceof URL ? input : typeof input === 'string' ? input : input.url);
    expect(headers.get('authorization')).toBe('Bearer relayfile-token');
    expect(headers.get('x-correlation-id')).toMatch(/^guardian-state-/);

    if (url.pathname.includes('/ops/')) {
      if (this.invalidOperationResponse) return jsonResponse(null);
      return jsonResponse({
        opId: url.pathname.split('/').at(-1),
        path: CYCLE_STATE_PATH,
        revision: `rev-${this.revision}`,
        action: 'file_upsert',
        status: 'succeeded',
        attemptCount: 1,
      });
    }

    expect(url.searchParams.get('path')).toBe(CYCLE_STATE_PATH);

    if (this.hangMethod === method) return new Promise<Response>(() => undefined);
    if (method === 'GET') {
      if (this.failGetStatus) {
        return jsonResponse({
          code: 'read_denied',
          message: `cycle state GET failed with HTTP ${this.failGetStatus}`,
        }, this.failGetStatus);
      }
      if (this.content === null) return jsonResponse({}, 404);
      if (this.invalidFileResponse) return jsonResponse(null);
      const content = this.corruptReadBack ? `${JSON.stringify(progressState(1))}\n` : this.content;
      return jsonResponse({
        path: CYCLE_STATE_PATH,
        revision: `rev-${this.revision}`,
        content,
        encoding: 'utf-8',
      });
    }

    expect(method).toBe('PUT');
    const ifMatch = headers.get('if-match');
    const expected = this.content === null ? '0' : `rev-${this.revision}`;
    if (ifMatch !== expected) {
      return jsonResponse({
        code: 'revision_conflict',
        message: 'Revision conflict',
        expectedRevision: ifMatch,
        currentRevision: expected,
      }, 409);
    }
    const body = JSON.parse(String(init.body)) as { content?: unknown };
    expect(body).toMatchObject({ contentType: 'application/json', encoding: 'utf-8' });
    this.content = String(body.content);
    this.revision += 1;
    if (this.invalidQueuedResponse) return jsonResponse(null, 202);
    return jsonResponse({
      opId: `op-${this.revision}`,
      status: 'queued',
      targetRevision: `rev-${this.revision}`,
    }, 202);
  }) as typeof fetch;

  state(): ProgressState {
    return JSON.parse(this.content ?? '{}') as ProgressState;
  }
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

class IdempotentSlackTransport implements RelayTransport {
  readonly attempts: RelayTransportWriteRequest[] = [];
  providerCreates = 0;
  private readonly results = new Map<string, WritebackResult>();

  async read<T = unknown>(_request: RelayTransportRequest): Promise<T> {
    return undefined as T;
  }

  async list<T = unknown>(_request: RelayTransportRequest): Promise<T[]> {
    return [];
  }

  async write(request: RelayTransportWriteRequest): Promise<WritebackResult> {
    this.attempts.push(request);
    const body = request.body as { idempotencyKey?: string };
    const key = body.idempotencyKey ?? `unkeyed:${this.attempts.length}`;
    const replay = this.results.get(key);
    if (replay) return replay;

    this.providerCreates += 1;
    const ts = `171000000${this.providerCreates}.000100`;
    const path = `${request.path}/message-${this.providerCreates}.json`;
    const result: WritebackResult = {
      path,
      absolutePath: path,
      receipt: { externalId: ts, ts },
    };
    this.results.set(key, result);
    return result;
  }
}

class DelayedSlackTransport extends IdempotentSlackTransport {
  constructor(private readonly delayMs: number) {
    super();
  }

  override async write(request: RelayTransportWriteRequest): Promise<WritebackResult> {
    await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    const result = await super.write(request);
    return {
      ...result,
      receipt: { externalId: '   ', ts: ` ${deliveredSlackTs(result)} ` },
    };
  }
}

class ReceiptlessSlackTransport extends IdempotentSlackTransport {
  override async write(request: RelayTransportWriteRequest): Promise<WritebackResult> {
    this.attempts.push(request);
    return {
      path: '/slack/draft.json',
      absolutePath: '/slack/draft.json',
      receipt: { id: 'mountcmd-without-provider-ts' },
    };
  }
}

function guardianContext(failWriteCall: number): {
  ctx: WorkforceCtx;
  files: Map<string, string>;
} {
  const files = new Map<string, string>();
  let writeCalls = 0;
  const ctx = {
    agent: { id: 'sim-agent' },
    deployment: { id: 'sim-deployment' },
    persona: {
      inputs: { SLACK_CHANNEL: 'C0BHWJSF309' },
      inputSpecs: {},
    },
    sandbox: {
      cwd: '/home/daytona/workspace',
      readFile: vi.fn(async () => manifest),
      exec: vi.fn(async () => ({ output: '', exitCode: 0 })),
    },
    files: {
      read: vi.fn(async (path: string) => {
        const contents = files.get(path);
        if (contents === undefined) throw new Error(`ENOENT: ${path}`);
        return contents;
      }),
      write: vi.fn(async (path: string, contents: string) => {
        writeCalls += 1;
        if (writeCalls === failWriteCall) throw new Error('simulated exact-state write failure');
        files.set(path, contents);
      }),
    },
    credentials: {
      tryRequire: vi.fn(() => null),
    },
    llm: {
      complete: vi.fn(async () => 'Is this feature working as expected?'),
    },
    memory: {
      recall: vi.fn(async () => []),
      save: vi.fn(async () => undefined),
    },
    log: vi.fn(),
  } as unknown as WorkforceCtx;
  return { ctx, files };
}

function exactStateContext(
  seed: string,
  manifestText = manifest
): {
  ctx: WorkforceCtx;
  files: Map<string, string>;
} {
  const files = new Map([[cycleStatePath, seed]]);
  const ctx = {
    agent: { id: 'sim-agent' },
    deployment: { id: 'sim-deployment' },
    persona: {
      inputs: { SLACK_CHANNEL: 'C0BHWJSF309' },
      inputSpecs: {},
    },
    sandbox: {
      cwd: '/home/daytona/workspace',
      readFile: vi.fn(async () => manifestText),
      exec: vi.fn(async () => ({ output: '', exitCode: 0 })),
    },
    files: {
      read: vi.fn(async (path: string) => {
        const contents = files.get(path);
        if (contents === undefined) throw new Error(`ENOENT: ${path}`);
        return contents;
      }),
      write: vi.fn(async (path: string, contents: string) => {
        files.set(path, contents);
      }),
    },
    credentials: {
      tryRequire: vi.fn(() => null),
    },
    llm: {
      complete: vi.fn(async () => 'Is this feature working as expected?'),
    },
    memory: {
      recall: vi.fn(async () => []),
      save: vi.fn(async () => undefined),
    },
    log: vi.fn(),
  } as unknown as WorkforceCtx;
  return { ctx, files };
}

describe('factory-feature-guardian runtime paths', () => {
  it('reads the manifest from the cloned Factory repository', () => {
    expect(resolveManifestPath('/home/daytona/workspace')).toBe(
      '/home/daytona/workspace/github/repos/AgentWorkforce/factory/.agentworkforce/features/manifest.yaml'
    );
  });

  it('fails closed on missing or escaping manifest locations', async () => {
    const missing = exactStateContext(JSON.stringify(progressState(1)));
    missing.ctx.sandbox.exec = vi.fn(async () => ({ output: 'src/index.ts', exitCode: 1 }));
    await expect(loadFactoryGuardianCatalog(missing.ctx)).rejects.toThrow(
      'missing location: src/index.ts',
    );

    const escaping = exactStateContext(
      JSON.stringify(progressState(1)),
      manifest.replace('        location: src/index.ts', '        location: ../outside'),
    );
    await expect(loadFactoryGuardianCatalog(escaping.ctx)).rejects.toThrow(
      'path escapes the repository',
    );
  });

  it('defaults delivery to the factory feature-check channel', () => {
    expect(persona.inputs.SLACK_CHANNEL.default).toBe('C0BHWJSF309');
  });

  it('is a deployable relay-orchestrator persona', () => {
    expect(() => parsePersonaSpec(persona, 'relay-orchestrator')).not.toThrow();
  });

  it('falls back with every declared Factory surface when quiz generation fails', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const apiOnlyManifest = manifest.replace(
      '        cli: relay node up',
      '        api: Factory.runOnce()',
    );
    const { ctx } = exactStateContext(JSON.stringify(progressState(0)), apiOnlyManifest);
    ctx.llm.complete = vi.fn(async () => {
      throw new Error('simulated quiz model failure');
    });

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      const text = (transport.attempts[0]?.body as { text: string }).text;
      expect(text).toContain('API/config: Factory.runOnce()');
      expect(text).toContain('Source: src/index.ts');
      expect(text).toContain('Procedure: cli-and-package');
      expect(text).not.toContain('CLI command:');
    } finally {
      restore();
    }
  });

  it('uses a dedicated low-reasoning model path instead of shared subscription quota', () => {
    expect(persona).toMatchObject({
      harness: 'opencode',
      model: 'deepseek-v4-flash-free',
    });
    expect(persona).not.toHaveProperty('useSubscription');
  });

  it('declares bounded manifest and memory reads plus configured Slack output', () => {
    expect(persona.integrations.github?.relayfileMount).toEqual({
      requiredReadPaths: ['/github/repos/AgentWorkforce/factory/**'],
      writeOnlyPaths: ['/github/repos/AgentWorkforce/factory/issues/**'],
    });
    expect(persona.memory).toEqual({
      enabled: true,
      scopes: ['workspace'],
      ttlDays: 14,
    });
    expect(persona.integrations.slack).toMatchObject({
      optional: true,
      enabledByInput: 'SLACK_CHANNEL',
      relayfileMount: {
        requiredReadPaths: ['/slack/channels/${SLACK_CHANNEL}/messages/**'],
        writeOnlyPaths: ['/slack/channels/${SLACK_CHANNEL}/messages/**'],
      },
    });
  });

  it('deduplicates an ambiguous post retry and advances after a saved receipt', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = guardianContext(2);

    try {
      // Run 1: the provider delivers Start Broker, then the exact progress
      // checkpoint fails. Only the pre-post empty cycle remains.
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.providerCreates).toBe(1);
      expect(JSON.parse(files.get(cycleStatePath) ?? '{}').checkedIds).toEqual([]);

      // Run 2: the same feature uses the same deterministic key, so the
      // provider receipt is replayed rather than creating a duplicate post.
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.providerCreates).toBe(1);
      expect(transport.attempts).toHaveLength(2);
      const firstBody = transport.attempts[0].body as { idempotencyKey: string };
      const retryBody = transport.attempts[1].body as { idempotencyKey: string };
      expect(retryBody.idempotencyKey).toBe(firstBody.idempotencyKey);

      const completed = JSON.parse(files.get(cycleStatePath) ?? '{}') as {
        checkedIds: string[];
        lastPost?: { featureId: string; ts: string };
      };
      expect(completed.checkedIds).toEqual(['broker-up']);
      expect(completed.lastPost).toEqual({
        featureId: 'broker-up',
        ts: '1710000001.000100',
      });

      // Run 3: persisted progress selects the next feature in the cycle.
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.providerCreates).toBe(2);
      const advanced = JSON.parse(files.get(cycleStatePath) ?? '{}') as {
        checkedIds: string[];
      };
      expect(advanced.checkedIds).toEqual(['broker-up', 'broker-status']);
    } finally {
      restore();
    }
  });

  it('does not post when the initial cycle checkpoint has no receipt', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = guardianContext(1);

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.providerCreates).toBe(0);
      expect(files.size).toBe(0);
      expect(ctx.log).toHaveBeenCalledWith(
        'error',
        'factory-feature-guardian.cycle-checkpoint-failed',
        expect.objectContaining({ err: expect.stringContaining('simulated exact-state write failure') })
      );
    } finally {
      restore();
    }
  });

  it('loads exact cycle state and advances 2/122 then 3/122 across fresh runs', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = exactStateContext(
      JSON.stringify({
        kind: 'factory-feature-guardian:progress',
        version: 3,
        generation: 1,
        checkedIds: ['broker-up'],
        cycleStartedAt: '2026-07-18T10:26:47.981Z',
        totalFeatures: 122,
        lastPost: { featureId: 'broker-up', ts: '1784370419.029509' },
      })
    );

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(1);
      expect((transport.attempts[0].body as { text: string }).text).toContain(
        'factory feature check · 2/122 · 120 remaining in cycle'
      );
      expect(JSON.parse(files.get(cycleStatePath) ?? '{}').checkedIds).toEqual([
        'broker-up',
        'broker-status',
      ]);

      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(2);
      expect((transport.attempts[1].body as { text: string }).text).toContain(
        'factory feature check · 3/122 · 119 remaining in cycle'
      );
      expect(JSON.parse(files.get(cycleStatePath) ?? '{}').checkedIds).toEqual([
        'broker-up',
        'broker-status',
        'broker-down',
      ]);
      expect(ctx.memory.recall).not.toHaveBeenCalled();
      expect(ctx.memory.save).not.toHaveBeenCalled();
    } finally {
      restore();
    }
  });

  it('preserves progress when the manifest adds features', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const staleTotal = { ...progressState(1), totalFeatures: 121 };
    const { ctx, files } = exactStateContext(JSON.stringify(staleTotal));
    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      const state = JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}') as ProgressState;
      expect(state.totalFeatures).toBe(122);
      expect(state.checkedIds).toEqual(['broker-up', 'broker-status']);
      expect(state.lastPost?.featureId).toBe('broker-status');
    } finally {
      restore();
    }
  });

  it('reconciles one unchecked feature retirement without resetting the generation', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const staleTotal = { ...progressState(1), totalFeatures: 123 };
    const { ctx, files } = exactStateContext(JSON.stringify(staleTotal));

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(1);
      expect((transport.attempts[0].body as { text: string }).text).toContain(
        'factory feature check · 2/122 · 120 remaining in cycle'
      );
      const state = JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}') as ProgressState;
      expect(state.generation).toBe(1);
      expect(state.totalFeatures).toBe(122);
      expect(state.checkedIds).toEqual(['broker-up', 'broker-status']);
      expect(state.lastPost).toEqual({
        featureId: 'broker-status',
        ts: '1710000001.000100',
      });
      expect(ctx.files.write).toHaveBeenCalledTimes(3);
    } finally {
      restore();
    }
  });

  it('resets a generation under CAS when the manifest retires a checked feature', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = exactStateContext(
      JSON.stringify({
        kind: 'factory-feature-guardian:progress',
        version: 3,
        generation: 7,
        checkedIds: ['broker-up', 'retired-feature'],
        cycleStartedAt: '2026-07-18T10:26:47.981Z',
        totalFeatures: 123,
        lastPost: { featureId: 'retired-feature', ts: '1784370419.029509' },
      })
    );

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(1);
      expect((transport.attempts[0].body as { text: string }).text).toContain(
        'factory feature check · 1/122 · 121 remaining in cycle'
      );
      const state = JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}') as ProgressState;
      expect(state.generation).toBe(8);
      expect(state.totalFeatures).toBe(122);
      expect(state.checkedIds).toEqual(['broker-up']);
      expect(state.lastPost).toEqual({
        featureId: 'broker-up',
        ts: '1710000001.000100',
      });
    } finally {
      restore();
    }
  });

  it('preserves exact progress when a suspiciously partial manifest omits checked features', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const seed = JSON.stringify({
      kind: 'factory-feature-guardian:progress',
      version: 3,
      generation: 7,
      checkedIds: ['broker-up', 'retired-feature'],
      cycleStartedAt: '2026-07-18T10:26:47.981Z',
      totalFeatures: 123,
      lastPost: { featureId: 'retired-feature', ts: '1784370419.029509' },
    });
    const { ctx, files } = exactStateContext(seed, renderManifest(manifestFeatures.slice(0, 2)));

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(0);
      expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}')).toEqual(JSON.parse(seed));
      expect(ctx.log).toHaveBeenCalledWith(
        'error',
        'factory-feature-guardian.progress-reconcile-failed',
        expect.objectContaining({
          err: expect.stringContaining('refusing a partial-manifest reset'),
          previousTotal: 123,
          currentTotal: 2,
        })
      );
    } finally {
      restore();
    }
  });

  it('preserves exact progress when multiple checked feature IDs disappear at the same total', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const seed = JSON.stringify({
      kind: 'factory-feature-guardian:progress',
      version: 3,
      generation: 7,
      checkedIds: ['broker-up', 'retired-feature-a', 'retired-feature-b'],
      cycleStartedAt: '2026-07-18T10:26:47.981Z',
      totalFeatures: 122,
      lastPost: { featureId: 'retired-feature-b', ts: '1784370419.029509' },
    });
    const { ctx, files } = exactStateContext(seed);

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(0);
      expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}')).toEqual(JSON.parse(seed));
      expect(ctx.log).toHaveBeenCalledWith(
        'error',
        'factory-feature-guardian.progress-reconcile-failed',
        expect.objectContaining({
          err: expect.stringContaining('multiple checked feature ids disappeared'),
          previousTotal: 122,
          currentTotal: 122,
          retiredIds: ['retired-feature-a', 'retired-feature-b'],
        })
      );
    } finally {
      restore();
    }
  });

  it('preserves exact progress when the manifest read fails', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const seed = JSON.stringify(progressState(2));
    const { ctx, files } = exactStateContext(seed);
    ctx.sandbox.readFile = vi.fn(async () => {
      throw new Error('simulated manifest read failure');
    });

    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}')).toEqual(JSON.parse(seed));
      expect(ctx.log).toHaveBeenCalledWith(
        'error',
        'factory-feature-guardian.manifest-load-failed',
        expect.objectContaining({ err: expect.stringContaining('simulated manifest read failure') })
      );
    } finally {
      restore();
    }
  });

  it('uses messages.write for manifest failure notifications', async () => {
    const { ctx } = exactStateContext(JSON.stringify(progressState(2)));
    ctx.sandbox.readFile = vi.fn(async () => {
      throw new Error('simulated manifest read failure');
    });
    const write = vi.fn(async () => ({
      path: '/slack/draft.json',
      absolutePath: '/slack/draft.json',
      receipt: { ts: '1710000001.000100' },
    }));
    const createSlackClient = (() => ({ messages: { write } })) as unknown as typeof slackClient;

    await runGuardian(ctx, { type: 'cron.tick' } as never, { createSlackClient });

    expect(write).toHaveBeenCalledWith(
      { channelId: 'C0BHWJSF309' },
      { text: expect.stringContaining('failed to load the feature manifest') }
    );
  });

  it('fails closed outside invoke simulation when Relayfile credentials are absent', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    (ctx.agent as { id: string }).id = 'deployed-agent';
    (ctx.deployment as { id: string }).id = 'deployed-run';
    try {
      await guardian.handler(ctx, { type: 'cron.tick' } as never);
      expect(transport.attempts).toHaveLength(0);
      expect(ctx.log).toHaveBeenCalledWith(
        'error',
        'factory-feature-guardian.progress-load-failed',
        expect.objectContaining({ err: expect.stringContaining('exact Relayfile credentials') })
      );
    } finally {
      restore();
    }
  });

  it('scopes provider idempotency to an exact feature revision and generation', () => {
    const revision = { manifestRevision: 'manifest-a', procedureRevision: 'procedures-a', generation: 1 };
    const key = featurePostIdempotencyKey('cycle-a', 'start-broker', revision);
    expect(key).toBe(featurePostIdempotencyKey('cycle-a', 'start-broker', revision));
    expect(key).not.toBe(featurePostIdempotencyKey('cycle-b', 'start-broker', revision));
    expect(key).not.toBe(featurePostIdempotencyKey('cycle-a', 'start-broker', {
      ...revision,
      manifestRevision: 'manifest-b',
    }));
    expect(key).not.toBe(featurePostIdempotencyKey('cycle-a', 'start-broker', {
      ...revision,
      procedureRevision: 'procedures-b',
    }));
    expect(key).not.toBe(featurePostIdempotencyKey('cycle-a', 'start-broker', {
      ...revision,
      generation: 2,
    }));
  });

  it('requires a delivered Slack ts instead of a draft receipt id', () => {
    expect(deliveredSlackTs(undefined)).toBe('');
    expect(deliveredSlackTs(null)).toBe('');
    expect(
      deliveredSlackTs({
        path: '/draft.json',
        absolutePath: '/draft.json',
        receipt: { id: 'mountcmd-draft', created: 'mountcmd-draft' },
      })
    ).toBe('');
    expect(
      deliveredSlackTs({
        path: '/delivered.json',
        absolutePath: '/delivered.json',
        receipt: { externalId: '1710000001.000100' },
      })
    ).toBe('1710000001.000100');
    expect(
      deliveredSlackTs({
        path: '/delivered-via-ts.json',
        absolutePath: '/delivered-via-ts.json',
        receipt: { externalId: '   ', ts: '1710000002.000200' },
      })
    ).toBe('1710000002.000200');
    expect(
      deliveredSlackTs({
        path: '/invalid-external-id.json',
        absolutePath: '/invalid-external-id.json',
        receipt: { externalId: 'mountcmd-not-a-ts', ts: '1710000003.000300' },
      })
    ).toBe('1710000003.000300');
    expect(
      deliveredSlackTs({
        path: '/pending.json',
        absolutePath: '/pending.json',
        deliveryStatus: 'pending',
        receipt: { ts: '1710000004.000400' },
      })
    ).toBe('');
    expect(
      deliveredSlackTs({
        path: '/dropped.json',
        absolutePath: '/dropped.json',
        deliveryStatus: 'dropped',
        receipt: { ts: '1710000005.000500' },
      })
    ).toBe('');
  });
});

describe('factory-feature-guardian exact SDK state', () => {
  const credentials = {
    url: 'https://relayfile.test',
    token: 'relayfile-token',
    workspaceId: 'rw_guardian',
  };

  it('bootstraps create-only and updates with the exact loaded revision', async () => {
    const server = new RelayfileStateServer(null);
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });

    expect(await store.load(storeFeatures)).toBeNull();
    const initial = await store.save(progressState(0), null, storeFeatures);
    const completed = await store.save(progressState(1), initial, storeFeatures);

    expect(completed.state.checkedIds).toEqual(['broker-up']);
    expect(server.requests.filter((request) => request.method === 'PUT')).toEqual([
      { method: 'PUT', ifMatch: '0' },
      { method: 'PUT', ifMatch: initial.revision },
    ]);
  });

  it('rejects a stale writer after newer runs checkpoint positions 2 and 3', async () => {
    const server = new RelayfileStateServer(progressState(1));
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    const staleA = await store.load(storeFeatures);
    const runB = await store.load(storeFeatures);
    expect(staleA).not.toBeNull();
    expect(runB).not.toBeNull();

    const position2 = await store.save(progressState(2), runB, storeFeatures);
    const runC = await store.load(storeFeatures);
    const position3 = await store.save(progressState(3), runC, storeFeatures);
    await expect(store.save(progressState(2), staleA, storeFeatures)).rejects.toBeInstanceOf(
      ProgressStateConflictError
    );

    expect(position2.state.lastPost?.featureId).toBe('broker-status');
    expect(position3.state.lastPost?.featureId).toBe('broker-down');
    expect(server.state()).toEqual(progressState(3));
  });

  it('uses the loaded revision when resetting a genuinely retired feature', async () => {
    const retiredState: ProgressState = {
      kind: 'factory-feature-guardian:progress',
      version: 3,
      generation: 7,
      checkedIds: ['broker-up', 'retired-feature'],
      cycleStartedAt: '2026-07-18T10:26:47.981Z',
      totalFeatures: 123,
      lastPost: { featureId: 'retired-feature', ts: '1784370419.029509' },
    };
    const server = new RelayfileStateServer(retiredState);
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    const loaded = await store.load(storeFeatures);
    expect(loaded).not.toBeNull();

    const reset: ProgressState = {
      kind: 'factory-feature-guardian:progress',
      version: 3,
      generation: 8,
      checkedIds: [],
      cycleStartedAt: '2026-07-18T11:26:47.981Z',
      totalFeatures: 122,
    };
    const saved = await store.save(reset, loaded, storeFeatures);

    expect(saved.state).toEqual(reset);
    expect(server.requests.filter((request) => request.method === 'PUT')).toEqual([
      { method: 'PUT', ifMatch: loaded?.revision ?? '' },
    ]);
  });

  it('uses the loaded revision for a bounded downward total reconciliation', async () => {
    const previousState = { ...progressState(1), totalFeatures: 123 };
    const server = new RelayfileStateServer(previousState);
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    const loaded = await store.load(storeFeatures);
    expect(loaded).not.toBeNull();

    const reconciled = { ...previousState, totalFeatures: 122 };
    const saved = await store.save(reconciled, loaded, storeFeatures);

    expect(saved.state).toEqual(reconciled);
    expect(server.requests.filter((request) => request.method === 'PUT')).toEqual([
      { method: 'PUT', ifMatch: loaded?.revision ?? '' },
    ]);
  });

  it('does not let an old completed generation overwrite its CAS reset', async () => {
    const server = new RelayfileStateServer(progressState(122));
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    const completedGeneration = await store.load(storeFeatures);
    expect(completedGeneration).not.toBeNull();

    const reset = await store.save(progressState(0, 2), completedGeneration, storeFeatures);
    await expect(store.save(progressState(122), completedGeneration, storeFeatures)).rejects.toBeInstanceOf(
      ProgressStateConflictError
    );

    expect(reset.state.generation).toBe(2);
    expect(server.state()).toEqual(progressState(0, 2));
  });

  it('treats only 404 as absent and fails closed on authorization errors', async () => {
    const absent = new RelayfileStateServer(null);
    const absentStore = createSdkProgressStore(credentials, { fetchImpl: absent.fetch });
    expect(await absentStore.load(storeFeatures)).toBeNull();

    const forbidden = new RelayfileStateServer(progressState(1));
    forbidden.failGetStatus = 403;
    const forbiddenStore = createSdkProgressStore(credentials, { fetchImpl: forbidden.fetch });
    await expect(forbiddenStore.load(storeFeatures)).rejects.toThrow('HTTP 403');
  });

  it('bounds a hung GET and a hung PUT with one state transaction deadline', async () => {
    vi.useFakeTimers();
    try {
      const hungGet = new RelayfileStateServer(progressState(1));
      hungGet.hangMethod = 'GET';
      const getStore = createSdkProgressStore(credentials, {
        fetchImpl: hungGet.fetch,
        timeoutMs: 25,
      });
      const getResult = getStore.load(storeFeatures);
      const getRejection = expect(getResult).rejects.toThrow('cycle state load timed out after 25ms');
      await vi.advanceTimersByTimeAsync(26);
      await getRejection;

      const hungPut = new RelayfileStateServer(null);
      hungPut.hangMethod = 'PUT';
      const putStore = createSdkProgressStore(credentials, {
        fetchImpl: hungPut.fetch,
        timeoutMs: 25,
      });
      const putResult = putStore.save(progressState(0), null, storeFeatures);
      const putRejection = expect(putResult).rejects.toThrow('cycle state save timed out after 25ms');
      await vi.advanceTimersByTimeAsync(26);
      await putRejection;
    } finally {
      vi.useRealTimers();
    }
  });

  it('fails closed when the exact GET read-back does not match the PUT', async () => {
    const server = new RelayfileStateServer(null);
    server.corruptReadBack = true;
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    await expect(store.save(progressState(0), null, storeFeatures)).rejects.toThrow(
      'read-back did not match'
    );
  });

  it('fails closed on malformed SDK file, queued-write, and operation responses', async () => {
    const invalidFile = new RelayfileStateServer(progressState(1));
    invalidFile.invalidFileResponse = true;
    await expect(
      createSdkProgressStore(credentials, { fetchImpl: invalidFile.fetch }).load(storeFeatures)
    ).rejects.toThrow('cycle state SDK read returned an invalid file');

    const invalidQueued = new RelayfileStateServer(null);
    invalidQueued.invalidQueuedResponse = true;
    await expect(
      createSdkProgressStore(credentials, { fetchImpl: invalidQueued.fetch }).save(
        progressState(0),
        null,
        storeFeatures
      )
    ).rejects.toThrow('cycle state SDK write did not return a valid operation ID');

    const invalidOperation = new RelayfileStateServer(null);
    invalidOperation.invalidOperationResponse = true;
    await expect(
      createSdkProgressStore(credentials, { fetchImpl: invalidOperation.fetch }).save(
        progressState(0),
        null,
        storeFeatures
      )
    ).rejects.toThrow('cycle state SDK write returned an invalid operation response');
  });

  it('rejects oversized UTF-8 state before issuing a PUT', async () => {
    const featureIds = Array.from({ length: 300 }, (_, index) => `feature-${index}-${'x'.repeat(220)}`);
    const features = featureIds.map((id, index) => ({
      id,
      name: `Feature ${index}`,
      cli: `relay feature-${index}`,
      desc: `Checks feature ${index}.`,
      category: 'core',
      location: 'src/index.ts',
      procedure: 'cli-and-package',
      tier: 1,
      criticality: 'critical' as const,
    }));
    const state = (checkedIds: string[]): ProgressState => ({
      kind: 'factory-feature-guardian:progress',
      version: 3,
      generation: 1,
      checkedIds,
      cycleStartedAt: '2026-07-18T10:26:47.981Z',
      totalFeatures: features.length,
      ...(checkedIds.length > 0
        ? {
            lastPost: {
              featureId: checkedIds.at(-1) as string,
              ts: '1784370419.029509',
            },
          }
        : {}),
    });
    const previousState = state(featureIds.slice(0, -1));
    const server = new RelayfileStateServer(null);
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });

    await expect(
      Promise.resolve().then(() =>
        store.save(state(featureIds), { state: previousState, revision: '7' }, features)
      )
    ).rejects.toThrow('cycle state exceeds size limit');
    expect(server.requests).toEqual([]);
  });

  it.each([
    ['duplicate ids', { ...progressState(2), checkedIds: ['broker-up', 'broker-up'] }],
    ['malformed historical id', { ...progressState(1), checkedIds: [''] }],
    ['invalid ts', { ...progressState(1), lastPost: { featureId: 'broker-up', ts: 'not-a-slack-ts' } }],
    ['invalid cycle time', { ...progressState(1), cycleStartedAt: 'yesterday' }],
  ])('rejects bounded v3 state with %s', async (_label, invalid) => {
    const server = new RelayfileStateServer(null);
    server.content = `${JSON.stringify(invalid)}\n`;
    const store = createSdkProgressStore(credentials, { fetchImpl: server.fetch });
    await expect(store.load(storeFeatures)).rejects.toThrow(/cycle state/);
  });
});

describe('factory-feature-guardian delayed Slack receipts', () => {
  it('rejects the run when the Slack post fails so the runner records handler.error', async () => {
    const failure = new Error('simulated Slack writeback failure');
    const { ctx, files } = exactStateContext(JSON.stringify(progressState(1)));
    const createSlackClient = (() => ({
      messages: {
        write: vi.fn(async () => {
          throw failure;
        }),
      },
    })) as unknown as typeof slackClient;

    await expect(runGuardian(ctx, { type: 'cron.tick' } as never, { createSlackClient })).rejects.toBe(
      failure
    );

    expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}').checkedIds).toEqual(['broker-up']);
    expect(ctx.log).toHaveBeenCalledWith('error', 'factory-feature-guardian.post-failed', {
      channel: 'C0BHWJSF309',
      feature: 'broker-status',
      err: String(failure),
    });
  });

  it('passes the production receipt deadline and poll interval to the Slack helper', async () => {
    const transport = new IdempotentSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    const observedOptions: unknown[] = [];
    const createSlackClient = ((options?: Parameters<typeof slackClient>[0]) => {
      observedOptions.push(options);
      return slackClient(options);
    }) as typeof slackClient;

    try {
      await runGuardian(ctx, { type: 'cron.tick' } as never, { createSlackClient });
      expect(observedOptions).toContainEqual({
        writebackTimeoutMs: 15_000,
        writebackPollMs: 250,
      });
    } finally {
      restore();
    }
  });

  it('waits beyond the old 3s window and checkpoints only a real trimmed ts', async () => {
    expect(SLACK_WRITEBACK_TIMEOUT_MS).toBe(15_000);
    expect(SLACK_WRITEBACK_POLL_MS).toBe(250);
    vi.useFakeTimers();
    const transport = new DelayedSlackTransport(3_500);
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = exactStateContext(JSON.stringify(progressState(1)));
    try {
      const run = guardian.handler(ctx, { type: 'cron.tick' } as never);
      await vi.advanceTimersByTimeAsync(3_001);
      expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}').checkedIds).toEqual(['broker-up']);
      await vi.advanceTimersByTimeAsync(500);
      await run;

      const state = JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}') as ProgressState;
      expect(state.checkedIds).toEqual(['broker-up', 'broker-status']);
      expect(state.lastPost?.ts).toBe('1710000001.000100');
    } finally {
      restore();
      vi.useRealTimers();
    }
  });

  it('rejects and never advances on a receipt-shaped draft without provider ts', async () => {
    const transport = new ReceiptlessSlackTransport();
    const restore = bindPreviewTransport(transport);
    const { ctx, files } = exactStateContext(JSON.stringify(progressState(1)));
    try {
      await expect(guardian.handler(ctx, { type: 'cron.tick' } as never)).rejects.toThrow(
        'Slack post failed: no timestamp returned for feature broker-status'
      );
      expect(JSON.parse(files.get(CYCLE_STATE_PATH) ?? '{}').checkedIds).toEqual(['broker-up']);
      expect(ctx.log).toHaveBeenCalledWith('error', 'factory-feature-guardian.post-failed', {
        channel: 'C0BHWJSF309',
        feature: 'broker-status',
      });
    } finally {
      restore();
    }
  });
});

describe('Factory feature guardian conversation adapters', () => {
  const snapshot = {
    id: 'verification-procedure-routing',
    name: 'Manifest-to-Procedure Routing',
    category: 'release-verification',
    api: 'manifest.yaml#verification.categories',
    description: 'Routes features to exact procedures.',
    locations: ['.agentworkforce/features/manifest.yaml'],
    procedure: 'release-verification',
    tier: 1,
    criticality: 'critical',
  };

  it.each([
    ['all passing', ' Test Files  2 passed (2)\n      Tests  42 passed (42)', { passed: 42, failed: 0 }],
    ['mixed', ' Test Files  1 failed | 2 passed (3)\n      Tests  3 failed | 39 passed (42)', { passed: 39, failed: 3 }],
    ['build only', 'compiled successfully', { passed: 0, failed: 0 }],
    ['ANSI summary', '\u001b[32m      Tests  7 passed (7)\u001b[39m', { passed: 7, failed: 0 }],
  ])('parses %s test evidence without inventing failures', (_label, output, expected) => {
    expect(parseFactoryGuardianTestCounts(output)).toEqual(expected);
  });

  it('fails confirmation authority closed until a configured actor matches', () => {
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    expect(factoryFeatureGuardianAdapters.isAuthorizedConfirmer(ctx, 'U-ANY')).toBe(false);
    (ctx.persona.inputs as Record<string, string>).SLACK_USER_KHALIQ = 'U-KHALIQ';
    expect(factoryFeatureGuardianAdapters.isAuthorizedConfirmer(ctx, 'U-KHALIQ')).toBe(true);
    expect(factoryFeatureGuardianAdapters.isAuthorizedConfirmer(ctx, 'U-ANY')).toBe(false);
  });

  it('reports missing installed dependencies as SKIP instead of a defect', async () => {
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    ctx.sandbox.exec = vi.fn(async () => ({ output: '', exitCode: 1 }));
    const gate = await gateFactoryGuardianTier(ctx, snapshot, {
      name: 'cli-and-package',
      path: '.agentworkforce/features/verify/procedures.md',
      prerequisites: 'source checkout and npm ci',
      body: 'npm run build',
      command: 'npm run build',
    });
    expect(gate).toEqual({
      outcome: 'skip',
      reason: expect.stringContaining('installed Node dependencies'),
    });
  });

  it('gates provider, fleet, cloud, and live-work procedures without turning absence into confirmation', async () => {
    const procedure = {
      name: 'provider-discovery',
      path: '.agentworkforce/features/verify/procedures.md',
      prerequisites: 'a disposable provider issue and config',
      body: 'npm run build',
      command: 'npm run build',
    };
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    ctx.sandbox.exec = vi.fn(async () => ({ output: '', exitCode: 0 }));
    (ctx.persona.inputs as Record<string, string>).GUARDIAN_LIVE_VERIFY_OPT_IN =
      'verification-procedure-routing,provider-discovery';

    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 2 }, procedure)).resolves
      .toMatchObject({ outcome: 'skip', reason: expect.stringContaining('provider issue or config') });

    Object.assign(ctx.persona.inputs as Record<string, string>, {
      FACTORY_VERIFY_CANARY_ISSUE: 'AR-VERIFY',
      FACTORY_VERIFY_CONFIG: '/tmp/factory-guardian.json',
    });
    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 2 }, procedure)).resolves
      .toMatchObject({ outcome: 'available' });

    const fixtureProcedure = { ...procedure, name: 'fleet-execution' };
    (ctx.persona.inputs as Record<string, string>).GUARDIAN_VERIFY_MAX_TIER = '6';
    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 4 }, fixtureProcedure)).resolves
      .toMatchObject({ outcome: 'skip', reason: expect.stringContaining('disposable fleet') });
    (ctx.persona.inputs as Record<string, string>).FACTORY_GUARDIAN_FLEET_OPT_IN = 'true';
    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 4 }, fixtureProcedure)).resolves
      .toMatchObject({ outcome: 'manual', reason: expect.stringContaining('live fleet lifecycle') });

    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 5 }, fixtureProcedure)).resolves
      .toMatchObject({ outcome: 'skip', reason: expect.stringContaining('cloud credentials') });
    await expect(gateFactoryGuardianTier(ctx, { ...snapshot, tier: 6 }, fixtureProcedure)).resolves
      .toMatchObject({ outcome: 'manual', reason: expect.stringContaining('tier 6') });
  });

  it('selects the complete documented command and runs it in a disposable checkout', async () => {
    const procedures = readFileSync(
      new URL('../../features/verify/procedures.md', import.meta.url),
      'utf8',
    );
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    ctx.sandbox.readFile = vi.fn(async () => procedures);
    const exec = vi.fn(async () => ({
      output: ' Test Files  2 passed (2)\n      Tests  42 passed (42)\n__FACTORY_GUARDIAN_CLEANUP_OK__\n',
      exitCode: 0,
    }));
    ctx.sandbox.exec = exec;
    const procedure = await resolveFactoryGuardianProcedure(ctx, snapshot);
    const result = await runFactoryGuardianProcedure(ctx, snapshot, procedure, 'adapter-test');

    expect(result).toMatchObject({
      outcome: 'passed',
      result: 'positive',
      tests: { passed: 42, failed: 0 },
      cleanup: ['Observed in-process and out-of-band removal of the unique temporary checkout.'],
    });
    const script = String(exec.mock.calls[0]?.[0]);
    expect(script).toContain('tar --exclude=.git --exclude=node_modules')
    expect(script).toContain('cd "$TMP/repo"')
    expect(script).toContain('node bin/factory.mjs --help')
    expect(script).toContain('npm run featuremap:check')
  });

  it('keeps every allowlisted command equal to the procedure first Bash block', async () => {
    const procedures = readFileSync(
      new URL('../../features/verify/procedures.md', import.meta.url),
      'utf8',
    );
    const manifestText = readFileSync(
      new URL('../../features/manifest.yaml', import.meta.url),
      'utf8',
    );
    const features = parseManifestFeatures(manifestText);
    const firstByProcedure = new Map(
      features.map((feature) => [feature.procedure as string, feature]),
    );
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    ctx.sandbox.readFile = vi.fn(async () => procedures);
    ctx.sandbox.exec = vi.fn(async () => ({
      output: '      Tests  42 passed (42)\n__FACTORY_GUARDIAN_CLEANUP_OK__\n',
      exitCode: 0,
    }));

    for (const feature of firstByProcedure.values()) {
      const featureSnapshot = {
        id: feature.id,
        name: feature.name,
        category: feature.category,
        ...(feature.cli ? { cli: feature.cli } : {}),
        ...(feature.api ? { api: feature.api } : {}),
        description: feature.desc,
        locations: feature.location.split(',').map((value) => value.trim()),
        procedure: feature.procedure as string,
        tier: feature.tier,
        criticality: feature.criticality,
      };
      const procedure = await resolveFactoryGuardianProcedure(ctx, featureSnapshot);
      await expect(
        runFactoryGuardianProcedure(ctx, featureSnapshot, procedure, `all-${feature.procedure}`),
      ).resolves.toMatchObject({ outcome: 'passed' });
    }
  });

  it('fails closed when test output reports failures despite a zero shell exit', async () => {
    const procedures = readFileSync(
      new URL('../../features/verify/procedures.md', import.meta.url),
      'utf8',
    );
    const { ctx } = exactStateContext(JSON.stringify(progressState(1)));
    ctx.sandbox.readFile = vi.fn(async () => procedures);
    ctx.sandbox.exec = vi.fn(async () => ({
      output: '      Tests  3 failed | 39 passed (42)\n__FACTORY_GUARDIAN_CLEANUP_OK__\n',
      exitCode: 0,
    }));
    const procedure = await resolveFactoryGuardianProcedure(ctx, {
      ...snapshot,
      procedure: 'cli-and-package',
    });
    await expect(
      runFactoryGuardianProcedure(ctx, snapshot, procedure, 'failed-counts'),
    ).resolves.toMatchObject({
      outcome: 'failed',
      result: 'negative',
      tests: { passed: 39, failed: 3 },
      negativeAssertions: expect.arrayContaining([expect.stringContaining('3 failed tests')]),
    });
  });

  it('routes every remediation only to the Factory repository', () => {
    expect(factoryFeatureGuardianAdapters.repositoryForFeature(snapshot)).toBe(
      'AgentWorkforce/factory',
    );
  });
});
