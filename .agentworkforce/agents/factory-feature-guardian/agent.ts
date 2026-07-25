/**
 * factory-feature-guardian handler.
 *
 * Hourly cron tick:
 *   1. Read the feature list from the cloned Factory repository
 *   2. Load feature progress from an exact, revisioned Relayfile record
 *   3. Pick the next unchecked feature (ordered by criticality then tier)
 *   4. Generate concise verification evidence via ctx.llm
 *   5. Persist updated progress
 *   6. Record success as structured log-only evidence
 *
 * After the full manifest is covered, the cycle resets.
 */
import {
  type RelayfileCredentials,
  type WorkforceCtx,
  type WorkforceEvent,
} from '@agentworkforce/runtime';
import {
  RelayFileApiError,
  RelayFileClient,
  RevisionConflictError,
  type OperationStatusResponse,
} from '@relayfile/sdk';
import { randomUUID } from 'node:crypto';
import {
  parseManifestFeatures,
  featureLocations,
  validateFeatureManifest,
  type FeatureCriticality as Criticality,
  type ManifestFeature,
} from '../../../src/featuremap/validate.js';
import {
  defineFeatureGuardianAgent,
  guardianContentRevision,
  remediationMarker,
  runGuardianConversationTurn,
  type FeatureGuardianAdapters,
  type GuardianConversationDependencies,
  type GuardianDefectKind,
  type GuardianFeatureSnapshot,
  type GuardianIssuePolicy,
  type GuardianManifestCatalog,
  type GuardianProcedure,
  type GuardianProcedureResult,
} from '../../../src/feature-guardian/conversation.js';

export { parseManifestFeatures };

// ── feature (flattened view used by this agent) ───────────────────────────────

type Feature = ManifestFeature;

function factoryFeatureSnapshot(feature: Feature): GuardianFeatureSnapshot {
  if (!feature.procedure) {
    throw new Error(`Factory manifest feature ${feature.id} is missing its named procedure`);
  }
  return {
    id: feature.id,
    name: feature.name,
    category: feature.category,
    ...(feature.cli ? { cli: feature.cli } : {}),
    ...(feature.api ? { api: feature.api } : {}),
    description: feature.desc,
    locations: featureLocations(feature),
    procedure: feature.procedure,
    tier: feature.tier,
    criticality: feature.criticality,
  };
}

function snapshotManifestFeature(feature: GuardianFeatureSnapshot): Feature {
  return {
    id: feature.id,
    name: feature.name,
    category: feature.category,
    ...(feature.cli ? { cli: feature.cli } : {}),
    ...(feature.api ? { api: feature.api } : {}),
    desc: feature.description,
    location: feature.locations.join(', '),
    tier: feature.tier,
    criticality: feature.criticality as Criticality,
    procedure: feature.procedure,
  };
}

const MANIFEST_RELPATH = '.agentworkforce/features/manifest.yaml';
const PROCEDURES_RELPATH = '.agentworkforce/features/verify/procedures.md';
const FACTORY_REPO_RELPATH = 'github/repos/AgentWorkforce/factory';

/**
 * GitHub-scoped repositories are cloned beneath the proactive workspace root.
 * WorkforceCtx currently exposes that workspace root (`sandbox.cwd`), but not
 * an integration-specific repository directory, so derive the clone path from
 * the documented `/github/repos/{owner}/{repo}` layout.
 */
export function resolveManifestPath(workspaceDir: string): string {
  return `${workspaceDir}/${FACTORY_REPO_RELPATH}/${MANIFEST_RELPATH}`;
}

export function resolveProceduresPath(workspaceDir: string): string {
  return `${workspaceDir}/${FACTORY_REPO_RELPATH}/${PROCEDURES_RELPATH}`;
}

/** Load the exact manifest/procedure revisions from the scoped Factory checkout. */
export async function loadFactoryGuardianCatalog(
  ctx: WorkforceCtx,
): Promise<GuardianManifestCatalog> {
  const absPath = resolveManifestPath(ctx.sandbox.cwd);
  const [raw, procedures] = await Promise.all([
    ctx.sandbox.readFile(absPath),
    ctx.sandbox.readFile(resolveProceduresPath(ctx.sandbox.cwd)),
  ]);
  const validated = validateFeatureManifest(raw);
  if (validated.verificationDocument !== PROCEDURES_RELPATH) {
    throw new Error(
      `Factory guardian requires verification.document to be ${PROCEDURES_RELPATH}`,
    );
  }
  const repository = `${ctx.sandbox.cwd}/${FACTORY_REPO_RELPATH}`;
  const declaredPaths = [
    validated.verificationDocument,
    ...validated.features.flatMap(featureLocations),
  ];
  for (const path of declaredPaths) {
    if (!isSafeFactoryRepositoryPath(path)) {
      throw new Error(`Factory guardian catalog path escapes the repository: ${path}`);
    }
  }
  const uniquePaths = [...new Set(declaredPaths)];
  const pathProbe = await ctx.sandbox.exec(
    ['set -eu', ...uniquePaths.map((path) =>
      `test -e ${shellQuote(path)} || { printf '%s\\n' ${shellQuote(path)}; exit 1; }`),
    ].join('\n'),
    { cwd: repository, timeoutMs: 30_000 },
  );
  if (pathProbe.exitCode !== 0) {
    throw new Error(
      `Factory guardian catalog contains a missing location: ${boundedText(pathProbe.output.trim(), 500)}`,
    );
  }
  return {
    manifestRevision: guardianContentRevision(raw),
    manifestVersion: validated.version,
    procedureRevision: guardianContentRevision(procedures),
    features: validated.features.map(factoryFeatureSnapshot),
  };
}

function isSafeFactoryRepositoryPath(path: string): boolean {
  if (!path || path.startsWith('/') || path.includes('\\')) return false;
  const pathWithoutTrailingSlash = path.endsWith('/') ? path.slice(0, -1) : path;
  if (!pathWithoutTrailingSlash) return false;
  return !pathWithoutTrailingSlash
    .split('/')
    .some((segment) => segment === '' || segment === '.' || segment === '..');
}

/** Compatibility loader retained for the scheduled feature ordering path. */
async function loadFeatures(ctx: WorkforceCtx): Promise<Feature[]> {
  const catalog = await loadFactoryGuardianCatalog(ctx);
  return catalog.features.map(snapshotManifestFeature);
}

// ── progress tracking ─────────────────────────────────────────────────────────

export interface ProgressState {
  kind: 'factory-feature-guardian:progress';
  version: 4;
  generation: number;
  checkedIds: string[];
  cycleStartedAt: string;
  totalFeatures: number;
  lastCheck?: {
    featureId: string;
    checkedAt: string;
    evidence: 'legacy-slack' | 'log-only';
  };
}

export const CYCLE_STATE_PATH = '/memory/workspace/factory-feature-guardian/cycle-state.json';
export const STATE_IO_TIMEOUT_MS = 5_000;
export const STATE_OPERATION_POLL_MS = 250;

const MAX_STATE_BYTES = 64 * 1024;
const MAX_SAFE_MANIFEST_SHRINK = 1;
const UTF8_ENCODER = new TextEncoder();

export interface ProgressSnapshot {
  state: ProgressState;
  revision: string;
}

export interface ProgressStore {
  load(features: Feature[]): Promise<ProgressSnapshot | null>;
  save(
    state: ProgressState,
    expected: ProgressSnapshot | null,
    features: Feature[]
  ): Promise<ProgressSnapshot>;
}

type FetchLike = typeof fetch;

/** Signals that the exact Relayfile revision changed before a state write completed. */
export class ProgressStateConflictError extends Error {
  constructor(message = 'factory-feature-guardian cycle state revision conflict') {
    super(message);
    this.name = 'ProgressStateConflictError';
  }
}

/** Return whether an unknown value is a non-array object record. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** Return whether a value is a canonical ISO-8601 timestamp. */
function isCanonicalIsoTimestamp(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  const parsed = new Date(value);
  return Number.isFinite(parsed.valueOf()) && parsed.toISOString() === value;
}

/** Return whether a value has Slack's provider timestamp format. */
function isSlackTs(value: unknown): value is string {
  return typeof value === 'string' && /^\d+\.\d+$/.test(value.trim());
}

/** Convert a legacy Slack provider timestamp into canonical log evidence time. */
function slackTsToIso(value: string): string {
  const seconds = Number(value.trim().split('.')[0]);
  if (!Number.isSafeInteger(seconds)) throw new Error('cycle state lastPost is invalid');
  const timestamp = new Date(seconds * 1_000);
  if (!Number.isFinite(timestamp.valueOf())) throw new Error('cycle state lastPost is invalid');
  return timestamp.toISOString();
}

/** Reject cycle state that exceeds the bounded Relayfile record size. */
function assertStateSize(content: string): void {
  if (UTF8_ENCODER.encode(content).byteLength > MAX_STATE_BYTES) {
    throw new Error('cycle state exceeds size limit');
  }
}

/** Parse untrusted persisted state into the exact current progress-state contract. */
function parseProgressState(
  value: unknown,
  features: Feature[],
  options: { allowHistoricalIds?: boolean } = {}
): ProgressState {
  if (!isRecord(value)) throw new Error('cycle state must be an object');
  if (
    value.kind !== 'factory-feature-guardian:progress' ||
    (value.version !== 3 && value.version !== 4)
  ) {
    throw new Error('cycle state kind/version is invalid');
  }
  if (!Number.isSafeInteger(value.generation) || (value.generation as number) < 1) {
    throw new Error('cycle state generation is invalid');
  }
  if (!isCanonicalIsoTimestamp(value.cycleStartedAt)) {
    throw new Error('cycle state cycleStartedAt is invalid');
  }
  if (
    !Number.isSafeInteger(value.totalFeatures) ||
    (value.totalFeatures as number) < 1 ||
    (value.totalFeatures as number) > 10_000
  ) {
    throw new Error('cycle state totalFeatures is invalid');
  }
  if (!Array.isArray(value.checkedIds) || value.checkedIds.length > (value.totalFeatures as number)) {
    throw new Error('cycle state checkedIds is invalid');
  }

  const knownIds = new Set(features.map((feature) => feature.id));
  const checkedIds = value.checkedIds.map((id) => {
    if (
      typeof id !== 'string' ||
      id.length === 0 ||
      id.length > 256 ||
      (!options.allowHistoricalIds && !knownIds.has(id))
    ) {
      throw new Error('cycle state contains an unknown feature id');
    }
    return id;
  });
  if (new Set(checkedIds).size !== checkedIds.length) {
    throw new Error('cycle state contains duplicate feature ids');
  }

  let lastCheck: ProgressState['lastCheck'];
  if (value.version === 3 && value.lastPost !== undefined) {
    if (
      !isRecord(value.lastPost) ||
      typeof value.lastPost.featureId !== 'string' ||
      !checkedIds.includes(value.lastPost.featureId) ||
      !isSlackTs(value.lastPost.ts)
    ) {
      throw new Error('cycle state lastPost is invalid');
    }
    lastCheck = {
      featureId: value.lastPost.featureId,
      checkedAt: slackTsToIso(value.lastPost.ts),
      evidence: 'legacy-slack',
    };
  } else if (value.version === 4 && value.lastCheck !== undefined) {
    if (
      !isRecord(value.lastCheck) ||
      typeof value.lastCheck.featureId !== 'string' ||
      !checkedIds.includes(value.lastCheck.featureId) ||
      !isCanonicalIsoTimestamp(value.lastCheck.checkedAt) ||
      (value.lastCheck.evidence !== 'legacy-slack' && value.lastCheck.evidence !== 'log-only')
    ) {
      throw new Error('cycle state lastCheck is invalid');
    }
    lastCheck = {
      featureId: value.lastCheck.featureId,
      checkedAt: value.lastCheck.checkedAt,
      evidence: value.lastCheck.evidence,
    };
  }
  if (checkedIds.length > 0 && !lastCheck) {
    throw new Error('cycle state with progress requires lastCheck');
  }
  if (lastCheck && lastCheck.featureId !== checkedIds.at(-1)) {
    throw new Error('cycle state lastCheck must describe the latest checked feature');
  }

  return {
    kind: 'factory-feature-guardian:progress',
    version: 4,
    generation: value.generation as number,
    checkedIds,
    cycleStartedAt: value.cycleStartedAt,
    totalFeatures: value.totalFeatures as number,
    ...(lastCheck ? { lastCheck } : {}),
  };
}

/** Return checked feature IDs that no longer exist in the current manifest. */
function retiredFeatureIds(state: ProgressState, features: Feature[]): string[] {
  const currentIds = new Set(features.map((feature) => feature.id));
  return state.checkedIds.filter((id) => !currentIds.has(id));
}

type ManifestDelta = {
  kind: 'preserve' | 'reset-checked-retirement' | 'unsafe';
  retiredIds: string[];
  removedCount: number;
  reason?: string;
};

/**
 * Complete manifest-delta matrix:
 * - additions and one unchecked removal preserve the current generation;
 * - one checked retirement/rename resets the generation under CAS;
 * - larger shrink or multiple missing checked IDs is suspicious and fail-closed;
 * - manifest read failures never reach this classifier.
 */
function classifyManifestDelta(state: ProgressState, features: Feature[]): ManifestDelta {
  const removedCount = Math.max(0, state.totalFeatures - features.length);
  const retiredIds = retiredFeatureIds(state, features);
  if (removedCount > MAX_SAFE_MANIFEST_SHRINK) {
    return {
      kind: 'unsafe',
      retiredIds,
      removedCount,
      reason: 'manifest feature count shrank by more than one; refusing a partial-manifest reset',
    };
  }
  if (retiredIds.length > 1) {
    return {
      kind: 'unsafe',
      retiredIds,
      removedCount,
      reason: 'multiple checked feature ids disappeared; refusing an ambiguous manifest reset',
    };
  }
  return {
    kind: retiredIds.length === 1 ? 'reset-checked-retirement' : 'preserve',
    retiredIds,
    removedCount,
  };
}

/** Enforce append-only checkpoints and explicit generation-reset invariants. */
function assertValidTransition(
  previous: ProgressSnapshot | null,
  next: ProgressState,
  features: Feature[]
): void {
  if (!previous) {
    if (next.generation !== 1 || next.checkedIds.length !== 0 || next.lastCheck) {
      throw new Error('new cycle state must bootstrap an empty generation 1');
    }
    return;
  }

  const prior = previous.state;
  if (next.generation === prior.generation) {
    if (next.cycleStartedAt !== prior.cycleStartedAt) {
      throw new Error('cycleStartedAt is immutable within a generation');
    }
    if (
      next.checkedIds.length < prior.checkedIds.length ||
      next.checkedIds.length > prior.checkedIds.length + 1 ||
      !prior.checkedIds.every((id, index) => next.checkedIds[index] === id)
    ) {
      throw new Error('cycle progress cannot regress or skip a checkpoint');
    }
    if (next.checkedIds.length === prior.checkedIds.length) {
      if (JSON.stringify(next.lastCheck) !== JSON.stringify(prior.lastCheck)) {
        throw new Error('reconciliation cannot overwrite lastCheck');
      }
    } else if (next.lastCheck?.featureId !== next.checkedIds.at(-1)) {
      throw new Error('new progress must checkpoint the newly checked feature');
    }
    return;
  }

  const manifestDelta = classifyManifestDelta(prior, features);
  const retirementResetAllowed = manifestDelta.kind === 'reset-checked-retirement';
  const allCurrentFeaturesChecked = features.every((feature) => prior.checkedIds.includes(feature.id));
  if (
    next.generation !== prior.generation + 1 ||
    (!allCurrentFeaturesChecked && !retirementResetAllowed) ||
    next.checkedIds.length !== 0 ||
    next.lastCheck ||
    new Date(next.cycleStartedAt) <= new Date(prior.cycleStartedAt)
  ) {
    throw new Error('cycle generation reset is invalid');
  }
}

/** Run an abort-aware asynchronous operation with a hard deadline. */
async function withDeadline<T>(
  label: string,
  timeoutMs: number,
  run: (signal: AbortSignal) => Promise<T>
): Promise<T> {
  const controller = new AbortController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      controller.abort();
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([run(controller.signal), expired]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/** Read, validate, and revision-bind one guardian state snapshot through the public SDK. */
async function readSdkSnapshot(
  client: RelayFileClient,
  workspaceId: string,
  features: Feature[],
  signal: AbortSignal,
  correlationId: string,
  allowHistoricalIds = false
): Promise<ProgressSnapshot | null> {
  let file;
  try {
    file = await client.readFile(workspaceId, CYCLE_STATE_PATH, correlationId, signal);
  } catch (error) {
    if (error instanceof RelayFileApiError && error.status === 404) return null;
    throw error;
  }
  if (!file || file.path !== CYCLE_STATE_PATH || typeof file.content !== 'string') {
    throw new Error('cycle state SDK read returned an invalid file');
  }
  assertStateSize(file.content);
  const revision = typeof file.revision === 'string' ? file.revision.trim() : '';
  if (!revision) throw new Error('cycle state SDK read returned an invalid revision');

  let parsed: unknown;
  try {
    parsed = JSON.parse(file.content);
  } catch {
    throw new Error('cycle state contains invalid JSON');
  }
  return {
    state: parseProgressState(parsed, features, { allowHistoricalIds }),
    revision,
  };
}

/** Require the SDK-queued compare-and-set write to reach a terminal success. */
async function waitForSdkWrite(
  client: RelayFileClient,
  workspaceId: string,
  opId: string,
  signal: AbortSignal,
  correlationId: string
): Promise<OperationStatusResponse> {
  for (;;) {
    const operation = await client.getOp(workspaceId, opId, correlationId, signal);
    const status = operation?.status;
    if (!status || !['pending', 'running', 'succeeded', 'failed', 'dead_lettered', 'canceled'].includes(status)) {
      throw new Error('cycle state SDK write returned an invalid operation response');
    }
    if (status === 'succeeded') return operation;
    if (['failed', 'dead_lettered', 'canceled'].includes(status)) {
      throw new Error(
        `cycle state SDK write ${status}${operation.lastError ? `: ${operation.lastError}` : ''}`
      );
    }
    await new Promise<void>((resolve, reject) => {
      const onAbort = () => {
        clearTimeout(timeout);
        signal.removeEventListener('abort', onAbort);
        reject(signal.reason);
      };
      const timeout = setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, STATE_OPERATION_POLL_MS);
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
  }
}

/** Create a compare-and-set progress store backed only by public Relayfile SDK surfaces. */
export function createSdkProgressStore(
  credentials: RelayfileCredentials,
  options: { fetchImpl?: FetchLike; timeoutMs?: number } = {}
): ProgressStore {
  const client = new RelayFileClient({
    baseUrl: credentials.url,
    token: credentials.token,
    fetchImpl: options.fetchImpl,
    readCache: false,
    retry: { maxRetries: 0 },
  });
  const timeoutMs = options.timeoutMs ?? STATE_IO_TIMEOUT_MS;
  return {
    load: (features) =>
      withDeadline('cycle state load', timeoutMs, (signal) =>
        readSdkSnapshot(
          client,
          credentials.workspaceId,
          features,
          signal,
          `guardian-state-load-${randomUUID()}`,
          true
        )
      ),
    save: (state, expected, features) => {
      const canonical = parseProgressState(state, features);
      if (canonical.totalFeatures !== features.length) {
        throw new Error('cycle state totalFeatures must match the current manifest');
      }
      const content = `${JSON.stringify(canonical)}\n`;
      assertStateSize(content);
      assertValidTransition(expected, canonical, features);
      return withDeadline('cycle state save', timeoutMs, async (signal) => {
        const correlationId = `guardian-state-save-${randomUUID()}`;
        let queued;
        try {
          queued = await client.writeFile({
            workspaceId: credentials.workspaceId,
            path: CYCLE_STATE_PATH,
            baseRevision: expected?.revision ?? '0',
            content,
            contentType: 'application/json',
            encoding: 'utf-8',
            correlationId,
            signal,
          });
        } catch (error) {
          if (error instanceof RevisionConflictError ||
              (error instanceof RelayFileApiError && error.status === 409)) {
            throw new ProgressStateConflictError();
          }
          throw error;
        }
        const opId =
          queued && typeof queued.opId === 'string'
            ? queued.opId.trim()
            : '';
        // Hosted synchronous acknowledgements can omit the queued-operation
        // shape. The exact read-back below remains mandatory in both branches.
        if (opId) {
          await waitForSdkWrite(client, credentials.workspaceId, opId, signal, correlationId);
        }
        const readBack = await readSdkSnapshot(
          client,
          credentials.workspaceId,
          features,
          signal,
          correlationId
        );
        if (!readBack || JSON.stringify(readBack.state) !== JSON.stringify(canonical)) {
          throw new Error('cycle state read-back did not match the saved state');
        }
        if (expected && readBack.revision === expected.revision) {
          throw new Error('cycle state revision did not advance');
        }
        return readBack;
      });
    },
  };
}

/** Derive a deterministic in-preview revision from canonical state. */
function previewRevision(state: ProgressState): string {
  return `preview:${JSON.stringify(state)}`;
}

/** Create the simulation-only progress store backed by the preview file API. */
function createPreviewProgressStore(ctx: WorkforceCtx): ProgressStore {
  const read = async (features: Feature[], allowHistoricalIds = false): Promise<ProgressSnapshot | null> => {
    let content: string;
    try {
      content = await ctx.files.read(CYCLE_STATE_PATH);
    } catch (error) {
      if (String(error).includes('ENOENT')) return null;
      throw error;
    }
    assertStateSize(content);
    const state = parseProgressState(JSON.parse(content) as unknown, features, {
      allowHistoricalIds,
    });
    return { state, revision: previewRevision(state) };
  };
  return {
    load: (features) => read(features, true),
    save: async (state, expected, features) => {
      const canonical = parseProgressState(state, features);
      if (canonical.totalFeatures !== features.length) {
        throw new Error('cycle state totalFeatures must match the current manifest');
      }
      const content = `${JSON.stringify(canonical)}\n`;
      assertStateSize(content);
      assertValidTransition(expected, canonical, features);
      const current = await read(features, true);
      if (current?.revision !== expected?.revision) throw new ProgressStateConflictError();
      await ctx.files.write(CYCLE_STATE_PATH, content);
      const readBack = await read(features);
      if (!readBack || JSON.stringify(readBack.state) !== JSON.stringify(canonical)) {
        throw new Error('cycle state read-back did not match the saved state');
      }
      return readBack;
    },
  };
}

/** Select the exact production store or the explicitly constrained simulation store. */
function createProgressStore(ctx: WorkforceCtx): ProgressStore {
  const credentials = ctx.credentials.tryRequire();
  if (credentials) return createSdkProgressStore(credentials.relayfile);
  if (ctx.agent.id === 'sim-agent' && ctx.deployment.id === 'sim-deployment') {
    return createPreviewProgressStore(ctx);
  }
  throw new Error('exact Relayfile credentials are required for guardian cycle state');
}

// ── feature selection ─────────────────────────────────────────────────────────

/** Select the highest-priority unchecked feature using criticality and tier ordering. */
function pickNextFeature(features: Feature[], checkedIds: Set<string>): Feature | null {
  const critOrder: Record<Criticality, number> = { critical: 0, hot: 1, standard: 2 };
  const ordered = [...features].sort((a, b) => {
    const critDiff = critOrder[a.criticality] - critOrder[b.criticality];
    if (critDiff !== 0) return critDiff;
    return a.tier - b.tier;
  });
  return ordered.find((f) => !checkedIds.has(f.id)) ?? null;
}

// ── check evidence generation ─────────────────────────────────────────────────

/** Generate log-only evidence for one scheduled catalog check. */
async function generateCheckEvidence(ctx: WorkforceCtx, feature: Feature): Promise<string> {
  const surface = [
    feature.cli ? `CLI command: ${feature.cli}` : null,
    feature.api ? `API/config: ${feature.api}` : null,
    `Source: ${feature.location}`,
    feature.procedure ? `Procedure: ${feature.procedure}` : null,
  ]
    .filter((entry): entry is string => entry !== null)
    .join('\n');
  const tierLabel =
    ({
      1: 'installed package or source checkout only',
      2: 'valid factory.config.json; fixture mode is acceptable',
      3: 'reachable Linear or GitHub ticket provider',
      4: 'internal broker or hosted Relay fleet available',
      5: 'cloud auth and writable Relayfile mount',
      6: 'manual check with a live issue or pull request',
    } as Record<number, string>)[feature.tier] ?? 'see feature procedure';

  const prompt = [
    'You are the Factory Feature Guardian.',
    'Write concise structured evidence (3-5 sentences, no markdown header) for an internal success log.',
    'Name the feature and include its CLI/API/config surface, expected behavior, source path, named procedure, and verification prerequisite.',
    'Preserve stated safety and topology boundaries exactly. Never imply that cross-host active/active control-plane ownership is supported.',
    'Do not address people, ask for reactions, or claim an external provider check ran.',
    '',
    `Feature: ${feature.name}`,
    surface,
    `What it should do: ${feature.desc}`,
    `Verify tier: ${feature.tier} (${tierLabel})`,
    `Criticality: ${feature.criticality}`,
  ].join('\n');

  try {
    const output = (await ctx.llm.complete(prompt, { maxTokens: 300 })).trim();
    if (output) return output;
    ctx.log('warn', 'factory-feature-guardian.evidence-fallback', {
      feature: feature.id,
      reason: 'model returned empty output',
    });
  } catch (err) {
    ctx.log('warn', 'factory-feature-guardian.evidence-fallback', {
      feature: feature.id,
      reason: String(err),
    });
  }
  return [
    `Catalog feature: ${feature.name} (${feature.id}).`,
    surface,
    `Expected behavior: ${feature.desc}`,
    `Verification tier: ${feature.tier} (${tierLabel}); criticality: ${feature.criticality}.`,
  ].join('\n');
}

// ── agent definition ──────────────────────────────────────────────────────────

/** Execute one fail-closed guardian tick through a durable log-only checkpoint. */
export async function runGuardian(
  ctx: WorkforceCtx,
  _event: WorkforceEvent
): Promise<void> {
  // Load the live feature list from the manifest
  let catalog: GuardianManifestCatalog;
  let features: Feature[];
  try {
    catalog = await loadFactoryGuardianCatalog(ctx);
    features = catalog.features.map(snapshotManifestFeature);
  } catch (err) {
    const absPath = resolveManifestPath(ctx.sandbox.cwd);
    ctx.log('error', 'factory-feature-guardian.manifest-load-failed', { path: absPath, err: String(err) });
    throw err;
  }
  ctx.log('info', 'factory-feature-guardian.manifest-loaded', {
    path: resolveManifestPath(ctx.sandbox.cwd),
    features: features.length,
  });
  if (features.length === 0) {
    ctx.log('error', 'factory-feature-guardian.no-features', { reason: 'manifest parsed but empty' });
    throw new Error('factory-feature-guardian loaded an empty feature manifest');
  }

  const totalFeatures = features.length;
  let store: ProgressStore;
  let progress: ProgressSnapshot | null;
  try {
    store = createProgressStore(ctx);
    progress = await store.load(features);
  } catch (err) {
    ctx.log('error', 'factory-feature-guardian.progress-load-failed', { err: String(err) });
    throw err;
  }

  // A missing exact record is the only bootstrap case. Persist and read back
  // the empty generation before the feature exercise.
  if (!progress) {
    const initial: ProgressState = {
      kind: 'factory-feature-guardian:progress',
      version: 4,
      generation: 1,
      checkedIds: [],
      cycleStartedAt: new Date().toISOString(),
      totalFeatures,
    };
    try {
      progress = await store.save(initial, null, features);
    } catch (err) {
      ctx.log('error', 'factory-feature-guardian.cycle-checkpoint-failed', { err: String(err) });
      throw err;
    }
  }

  const manifestDelta = classifyManifestDelta(progress.state, features);
  const { retiredIds } = manifestDelta;
  if (manifestDelta.kind === 'unsafe') {
    ctx.log('error', 'factory-feature-guardian.progress-reconcile-failed', {
      err: manifestDelta.reason,
      previousTotal: progress.state.totalFeatures,
      currentTotal: totalFeatures,
      retiredIds,
    });
    throw new Error(manifestDelta.reason);
  }

  // A successfully parsed manifest can retire a feature mid-cycle. Historical
  // IDs are accepted only at load; reset them under exact-revision CAS before
  // the feature exercise so the persisted state is canonical again.
  if (manifestDelta.kind === 'reset-checked-retirement') {
    const previousStart = new Date(progress.state.cycleStartedAt).valueOf();
    const reset: ProgressState = {
      kind: 'factory-feature-guardian:progress',
      version: 4,
      generation: progress.state.generation + 1,
      checkedIds: [],
      cycleStartedAt: new Date(Math.max(Date.now(), previousStart + 1)).toISOString(),
      totalFeatures,
    };
    try {
      progress = await store.save(reset, progress, features);
      ctx.log('info', 'factory-feature-guardian.manifest-retirement-reset', {
        retiredIds,
        generation: progress.state.generation,
        total: totalFeatures,
      });
    } catch (err) {
      ctx.log('error', 'factory-feature-guardian.progress-reconcile-failed', { err: String(err) });
      throw err;
    }
  } else if (progress.state.totalFeatures !== totalFeatures) {
    // Manifest additions change the denominator but must never discard already
    // checked feature ids or overwrite the latest check evidence.
    const reconciled: ProgressState = {
      ...progress.state,
      totalFeatures,
    };
    try {
      progress = await store.save(reconciled, progress, features);
    } catch (err) {
      ctx.log('error', 'factory-feature-guardian.progress-reconcile-failed', { err: String(err) });
      throw err;
    }
  }

  let checkedIds = new Set(progress.state.checkedIds);

  // Pick the next unchecked feature; reset if the cycle is complete
  let feature = pickNextFeature(features, checkedIds);
  if (!feature) {
    ctx.log('info', 'factory-feature-guardian.cycle-complete', { total: totalFeatures });
    const previousStart = new Date(progress.state.cycleStartedAt).valueOf();
    const reset: ProgressState = {
      kind: 'factory-feature-guardian:progress',
      version: 4,
      generation: progress.state.generation + 1,
      checkedIds: [],
      cycleStartedAt: new Date(Math.max(Date.now(), previousStart + 1)).toISOString(),
      totalFeatures,
    };
    try {
      progress = await store.save(reset, progress, features);
    } catch (err) {
      ctx.log('error', 'factory-feature-guardian.cycle-checkpoint-failed', { err: String(err) });
      throw err;
    }
    checkedIds = new Set();
    feature = pickNextFeature(features, checkedIds);
  }
  if (!feature) throw new Error('factory-feature-guardian could not select a feature');

  // Exercise the catalog + model chain without writing to a human channel.
  const evidence = await generateCheckEvidence(ctx, feature);
  const remaining = totalFeatures - checkedIds.size - 1;
  const checkedAt = new Date().toISOString();
  checkedIds.add(feature.id);
  const completed: ProgressState = {
    kind: 'factory-feature-guardian:progress',
    version: 4,
    generation: progress.state.generation,
    checkedIds: [...checkedIds],
    cycleStartedAt: progress.state.cycleStartedAt,
    totalFeatures,
    lastCheck: { featureId: feature.id, checkedAt, evidence: 'log-only' },
  };
  let checkpoint: ProgressSnapshot;
  try {
    checkpoint = await store.save(completed, progress, features);
  } catch (err) {
    ctx.log('error', 'factory-feature-guardian.progress-checkpoint-failed', {
      feature: feature.id,
      err: String(err),
    });
    throw err;
  }
  ctx.log('info', 'factory-feature-guardian.catalog-traversal-passed', {
    feature: feature.id,
    checkedAt,
    evidence,
    index: checkedIds.size,
    total: totalFeatures,
    remaining,
    manifestRevision: catalog.manifestRevision,
    procedureRevision: catalog.procedureRevision,
    checkpointRevision: checkpoint.revision,
  });
}

const FACTORY_PROCEDURE_COMMANDS: Record<string, string> = {
  'cli-and-package': [
    'npm run build',
    'npm test -- --run src/cli/fleet.test.ts src/featuremap/validate.test.ts',
    'node bin/factory.mjs --help | tee "$TMP/help.txt"',
    'node bin/factory.mjs --version | tee "$TMP/version.txt"',
    'node bin/factory.mjs featuremap check | tee "$TMP/featuremap.json"',
    'node -e \'const p=require("./package.json"); if (!p.version) process.exit(1)\'',
    "grep -Fq 'featuremap check' \"$TMP/help.txt\"",
    "grep -Eq '^[0-9]+\\.[0-9]+\\.[0-9]+' \"$TMP/version.txt\"",
    'node -e \'const r=require(process.argv[1]); if (!r.ok || r.featureCount < 1) process.exit(1)\' "$TMP/featuremap.json"',
  ].join('\n'),
  'fleet-execution': [
    'npm run build',
    'npx vitest run \\',
    '  src/cli/fleet.test.ts \\',
    '  src/fleet/create-fleet.test.ts \\',
    '  src/fleet/ensure-relay-broker.test.ts \\',
    '  src/fleet/internal-fleet-client.test.ts \\',
    '  src/fleet/relay-fleet-client.test.ts \\',
    '  src/node/factory-node.test.ts',
  ].join('\n'),
  'provider-discovery': [
    'npm run build',
    'node bin/factory.mjs triage "$FACTORY_VERIFY_CANARY_ISSUE" --config "$CONFIG" | tee "$TMP/triage.json"',
    'node bin/factory.mjs canary "$FACTORY_VERIFY_CANARY_ISSUE" --config "$CONFIG" | tee "$TMP/canary.json"',
    'node bin/factory.mjs run-once --config "$CONFIG" --dry-run | tee "$TMP/discovery.json"',
    "node -e 'const r=require(process.argv[1]); if (!r.ok) process.exit(1)' \"$TMP/canary.json\"",
  ].join('\n'),
  'triage-and-configuration': [
    'npx vitest run \\',
    '  src/config/schema.test.ts \\',
    '  src/config/local-clone-paths.test.ts \\',
    '  src/triage/triage.test.ts \\',
    '  src/safety/factory-scope.test.ts',
  ].join('\n'),
  'issue-dispatch-lifecycle': [
    'npx vitest run \\',
    '  src/orchestrator/batch-tracker.test.ts \\',
    '  src/orchestrator/factory.test.ts \\',
    '  src/dispatch/templates.test.ts \\',
    '  src/git/agent-worktree.test.ts \\',
    '  src/state/file-state-store.test.ts \\',
    '  src/state/github-lifecycle-identity.test.ts',
  ].join('\n'),
  'human-clarification': [
    'npx vitest run \\',
    '  src/config/schema.test.ts \\',
    '  src/orchestrator/coalesced-task-queue.test.ts \\',
    '  src/orchestrator/factory.test.ts \\',
    '  src/state/file-state-store.test.ts',
  ].join('\n'),
  'pull-request-lifecycle': [
    'npx vitest run \\',
    '  src/github/merge-gate.test.ts \\',
    '  src/github/probe-closer.test.ts \\',
    '  src/github/standalone-babysitter.test.ts \\',
    '  src/orchestrator/factory.test.ts \\',
    '  src/state/file-state-store.test.ts',
  ].join('\n'),
  'safety-boundaries': [
    'npx vitest run \\',
    '  src/safety/factory-scope.test.ts \\',
    '  src/github/merge-gate.test.ts \\',
    '  src/github/probe-closer.test.ts \\',
    '  src/node/factory-node.test.ts \\',
    '  src/__tests__/mount-delete-callsite-invariant.test.ts \\',
    '  src/__tests__/writefile-callsite-invariant.test.ts',
  ].join('\n'),
  'integrations-and-writeback': [
    'npx vitest run \\',
    '  src/mount/local-mount-preflight.test.ts \\',
    '  src/mount/relayfile-binary.test.ts \\',
    '  src/mount/relayfile-cloud-mount-client.test.ts \\',
    '  src/mount/relayfile-github-connection-write.test.ts \\',
    '  src/mount/relayfile-integration-preflight.test.ts \\',
    '  src/writeback/writeback.test.ts',
  ].join('\n'),
  'event-intake': [
    'npx vitest run \\',
    '  src/webhook/handler.test.ts \\',
    '  src/webhook/registrar.test.ts \\',
    '  src/subscriptions/__tests__/globs.test.ts \\',
    '  src/subscriptions/__tests__/linear-filter.test.ts \\',
    '  src/subscriptions/__tests__/slack-filter.test.ts \\',
    '  src/subscriptions/__tests__/specs.test.ts \\',
    '  src/subscriptions/__tests__/event-client.test.ts',
  ].join('\n'),
  'public-api': [
    'npm run build',
    'npx vitest run src/__tests__/dist-entrypoints.test.ts',
    'npm pack --pack-destination "$TMP"',
    'mkdir "$TMP/consumer" && cd "$TMP/consumer"',
    'npm init -y >/dev/null',
    'npm install --ignore-scripts "$TMP"/*.tgz >/dev/null',
    "node --input-type=module <<'NODE'",
    "for (const subpath of ['', '/observability', '/telemetry', '/testing', '/writeback', '/featuremap', '/feature-guardian', '/hosted', '/environments']) {",
    '  const mod = await import(`@agent-relay/factory${subpath}`)',
    "  if (Object.keys(mod).length === 0) throw new Error(`empty export ${subpath || '/'}`)",
    '}',
    "const node = await import('@agent-relay/factory/node')",
    "if (!node.default) throw new Error('node default export missing')",
    'NODE',
  ].join('\n'),
  'hosted-control-plane': [
    'npx vitest run \\',
    '  src/hosted/orchestrator.test.ts \\',
    '  src/hosted/state-store.test.ts \\',
    '  src/hosted/worker-safety.test.ts',
  ].join('\n'),
  'cloud-observability': [
    'npx vitest run \\',
    '  src/observability/events.test.ts \\',
    '  src/observability/instance-identity.test.ts \\',
    '  src/observability/outbox.test.ts \\',
    '  src/observability/cloud-reporter.test.ts \\',
    '  src/cost/pricing.test.ts \\',
    '  src/cost/ledger.test.ts \\',
    '  test/e2e/run-cost-accounting.test.ts \\',
    '  src/cli/fleet.test.ts',
  ].join('\n'),
  'release-verification': [
    'npm run build',
    'npm run featuremap:check',
    'npm test',
    'node bin/factory.mjs --help >/dev/null',
  ].join('\n'),
  'proactive-health': [
    'npx vitest run \\',
    '  .agentworkforce/agents/factory-feature-guardian/manifest-contract.test.ts \\',
    '  .agentworkforce/agents/factory-feature-guardian/agent.test.ts \\',
    '  src/feature-guardian/conversation.test.ts',
  ].join('\n'),
  'maintainability-review': [
    'npx vitest run \\',
    '  .agentworkforce/agents/factory-maintainability/persona.test.ts',
  ].join('\n'),
  'loop-and-recovery': [
    'npx vitest run \\',
    '  src/orchestrator/factory.test.ts \\',
    '  src/orchestrator/process-identity.test.ts \\',
    '  src/orchestrator/reaper.test.ts \\',
    '  src/state/file-state-store.test.ts',
  ].join('\n'),
  'verification-environments': [
    'npm run build',
    'npx vitest run \\',
    '  src/environments/verification-stack-descriptor.test.ts \\',
    '  src/environments/verification-stack-deployer.test.ts \\',
    '  src/environments/kubernetes-provider.test.ts \\',
    '  src/environments/load-harness.test.ts \\',
    '  src/environments/verification-pipeline.test.ts \\',
    '  src/orchestrator/environment-reaper.test.ts',
  ].join('\n'),
};

/** Resolve one named manifest procedure without allowing a heading escape. */
export async function resolveFactoryGuardianProcedure(
  ctx: WorkforceCtx,
  feature: GuardianFeatureSnapshot,
): Promise<GuardianProcedure> {
  if (!/^[a-z0-9-]+$/u.test(feature.procedure)) {
    throw new Error(`Factory feature ${feature.id} has an invalid procedure name`);
  }
  const path = resolveProceduresPath(ctx.sandbox.cwd);
  const raw = await ctx.sandbox.readFile(path);
  const escaped = feature.procedure.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const match = raw.match(
    new RegExp(`^## ${escaped}\\s*$([\\s\\S]*?)(?=^## |(?![\\s\\S]))`, 'mu'),
  );
  if (!match?.[1]) {
    throw new Error(`Missing Factory verification procedure: ${feature.procedure}`);
  }
  const body = match[1].trim();
  const prerequisites = body.match(/\*\*Prerequisites:\*\*\s*([\s\S]*?)(?=\n```|\n\n)/u)?.[1]
    ?.replace(/\s+/gu, ' ')
    .trim();
  if (!prerequisites) {
    throw new Error(`Factory procedure ${feature.procedure} has no explicit prerequisites`);
  }
  const command = body.match(/```bash\s*\n([\s\S]*?)\n```/u)?.[1]?.trim();
  if (!command) {
    throw new Error(`Factory procedure ${feature.procedure} has no executable Bash command`);
  }
  return { name: feature.procedure, path: PROCEDURES_RELPATH, prerequisites, body, command };
}

/** Keep live/provider tiers opt-in and never turn missing prerequisites into confirmation. */
export async function gateFactoryGuardianTier(
  ctx: WorkforceCtx,
  feature: GuardianFeatureSnapshot,
  procedure: GuardianProcedure,
) {
  const command = FACTORY_PROCEDURE_COMMANDS[procedure.name];
  if (!command) {
    return { outcome: 'manual' as const, reason: `MANUAL: ${procedure.name} has no bounded automation command.` };
  }
  const repository = `${ctx.sandbox.cwd}/${FACTORY_REPO_RELPATH}`;
  const packagePrerequisites = await ctx.sandbox.exec(
    'test -f package.json && test -d node_modules && command -v node >/dev/null && command -v npm >/dev/null',
    { cwd: repository, timeoutMs: 10_000 },
  );
  if (packagePrerequisites.exitCode !== 0) {
    return {
      outcome: 'skip' as const,
      reason: 'SKIP: the scoped Factory checkout or its installed Node dependencies are unavailable.',
    };
  }
  const maximum = Number(factoryGuardianInput(ctx, 'GUARDIAN_VERIFY_MAX_TIER') ?? '2');
  if (!Number.isInteger(maximum) || feature.tier > maximum) {
    return {
      outcome: 'manual' as const,
      reason: `MANUAL: tier ${feature.tier} exceeds the configured automated tier ${Number.isInteger(maximum) ? maximum : 2}.`,
    };
  }
  if (feature.tier >= 6) {
    return { outcome: 'manual' as const, reason: 'MANUAL: tier 6 requires a selected disposable live issue or pull request.' };
  }
  const requiresProvider = procedure.name === 'provider-discovery';
  if (feature.tier >= 3 || requiresProvider) {
    const optIn = new Set(
      (factoryGuardianInput(ctx, 'GUARDIAN_LIVE_VERIFY_OPT_IN') ?? '')
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean),
    );
    if (!optIn.has(feature.id) && !optIn.has(feature.procedure)) {
      return {
        outcome: 'manual' as const,
        reason: `MANUAL: tier ${feature.tier} needs explicit opt-in for ${feature.id} or ${feature.procedure}.`,
      };
    }
  }
  if (
    requiresProvider &&
    (!factoryGuardianInput(ctx, 'FACTORY_VERIFY_CANARY_ISSUE') ||
      !factoryGuardianInput(ctx, 'FACTORY_VERIFY_CONFIG'))
  ) {
    return { outcome: 'skip' as const, reason: 'SKIP: the disposable provider issue or config prerequisite is unavailable.' };
  }
  if (requiresProvider) {
    const configuredPath = factoryGuardianInput(ctx, 'FACTORY_VERIFY_CONFIG') as string;
    const configPath = configuredPath.startsWith('/')
      ? configuredPath
      : `${repository}/${configuredPath}`;
    const configAvailable = await ctx.sandbox.exec(`test -f ${shellQuote(configPath)}`, {
      cwd: repository,
      timeoutMs: 5_000,
    });
    if (configAvailable.exitCode !== 0) {
      return { outcome: 'skip' as const, reason: 'SKIP: the selected disposable provider config is not readable.' };
    }
  }
  if (feature.tier === 3 && !requiresProvider) {
    return {
      outcome: 'manual' as const,
      reason: `MANUAL: ${procedure.name} requires provider assertions beyond its bounded unattended command.`,
    };
  }
  if (feature.tier === 4 && factoryGuardianInput(ctx, 'FACTORY_GUARDIAN_FLEET_OPT_IN') !== 'true') {
    return { outcome: 'skip' as const, reason: 'SKIP: no explicitly authorized disposable fleet is available.' };
  }
  if (feature.tier === 4) {
    return {
      outcome: 'manual' as const,
      reason: `MANUAL: ${procedure.name} requires a live fleet lifecycle whose provider cleanup cannot be proven by the unattended runner.`,
    };
  }
  if (feature.tier === 5 &&
    (!ctx.credentials.tryRequire() || factoryGuardianInput(ctx, 'FACTORY_GUARDIAN_CLOUD_OPT_IN') !== 'true')) {
    return { outcome: 'skip' as const, reason: 'SKIP: cloud credentials and writable disposable mount opt-in are unavailable.' };
  }
  if (feature.tier === 5) {
    return {
      outcome: 'manual' as const,
      reason: `MANUAL: ${procedure.name} requires provider readback and cleanup in a selected disposable cloud scope.`,
    };
  }
  return { outcome: 'available' as const, reason: `Tier ${feature.tier} prerequisites are available.` };
}

/** Execute only the exact bounded command documented by the named procedure. */
export async function runFactoryGuardianProcedure(
  ctx: WorkforceCtx,
  feature: GuardianFeatureSnapshot,
  procedure: GuardianProcedure,
  runKey: string,
): Promise<GuardianProcedureResult> {
  const command = FACTORY_PROCEDURE_COMMANDS[procedure.name];
  if (!command || procedure.command.trim() !== command.trim()) {
    return {
      source: 'procedure',
      result: 'negative',
      outcome: 'failed',
      verifier: 'factory-feature-guardian:procedure-runner',
      summary: `Procedure drift: ${procedure.name} no longer contains the allowlisted exact tier-safe command.`,
      commands: [],
      positiveAssertions: [],
      negativeAssertions: ['Documented command did not match the Factory adapter allowlist.'],
      tests: { passed: 0, failed: 0 },
      cleanup: ['No command executed.'],
    };
  }
  const run = runKey.replace(/[^a-zA-Z0-9_.-]/gu, '-').slice(-80);
  const repository = `${ctx.sandbox.cwd}/${FACTORY_REPO_RELPATH}`;
  const tempWorkspace = `/tmp/factory-feature-guardian-${run || randomUUID()}`;
  const configuredPath = factoryGuardianInput(ctx, 'FACTORY_VERIFY_CONFIG');
  const configPath = configuredPath
    ? (configuredPath.startsWith('/') ? configuredPath : `${repository}/${configuredPath}`)
    : `${tempWorkspace}/factory.config.json`;
  const wrapped = [
    'set -Eeuo pipefail',
    `TMP=${shellQuote(tempWorkspace)}`,
    'rm -rf -- "$TMP"',
    'mkdir -m 700 "$TMP"',
    'cleanup() {',
    '  status=$?',
    '  cd /',
    '  rm -rf "$TMP"',
    '  if [[ -e "$TMP" ]]; then',
    '    printf "__FACTORY_GUARDIAN_CLEANUP_FAILED__\\n" >&2',
    '    status=1',
    '  else',
    '    printf "__FACTORY_GUARDIAN_CLEANUP_OK__\\n"',
    '  fi',
    '  exit "$status"',
    '}',
    'trap cleanup EXIT',
    `RUN="${run}"`,
    `CONFIG=${shellQuote(configPath)}`,
    'export TMP RUN CONFIG',
    `SOURCE=${shellQuote(repository)}`,
    'mkdir "$TMP/repo"',
    '(cd "$SOURCE" && tar --exclude=.git --exclude=node_modules --exclude=dist --exclude=artifacts -cf - .) | tar -xf - -C "$TMP/repo"',
    'mkdir "$TMP/repo/node_modules"',
    'find "$SOURCE/node_modules" -mindepth 1 -maxdepth 1 ! -name .vite ! -name .cache -exec ln -s {} "$TMP/repo/node_modules/" \\;',
    'export npm_config_cache="$TMP/npm-cache"',
    'cd "$TMP/repo"',
    command,
  ].join('\n');
  let execution: Awaited<ReturnType<WorkforceCtx['sandbox']['exec']>>;
  try {
    execution = await ctx.sandbox.exec(wrapped, {
      cwd: repository,
      timeoutMs: 240_000,
    });
  } catch (error) {
    const cleanup = await ctx.sandbox.exec(
      `rm -rf -- ${shellQuote(tempWorkspace)} && test ! -e ${shellQuote(tempWorkspace)}`,
      { cwd: repository, timeoutMs: 10_000 },
    );
    return {
      source: 'procedure',
      result: 'negative',
      outcome: 'failed',
      verifier: 'factory-feature-guardian:procedure-runner',
      summary: `${procedure.name} execution failed before a result was returned: ${boundedText(String(error), 1_000)}`,
      commands: [command],
      positiveAssertions: [],
      negativeAssertions: ['The sandbox did not return a completed procedure result.'],
      tests: { passed: 0, failed: 0 },
      cleanup: [cleanup.exitCode === 0
        ? 'Out-of-band cleanup removed the deterministic temporary checkout.'
        : 'Out-of-band cleanup could not confirm temporary-checkout removal.'],
    };
  }
  const outOfBandCleanup = await ctx.sandbox.exec(
    `rm -rf -- ${shellQuote(tempWorkspace)} && test ! -e ${shellQuote(tempWorkspace)}`,
    { cwd: repository, timeoutMs: 10_000 },
  );
  const counts = parseFactoryGuardianTestCounts(execution.output);
  const output = boundedText(execution.output.trim(), 2_000);
  const cleaned = execution.output.includes('__FACTORY_GUARDIAN_CLEANUP_OK__') &&
    outOfBandCleanup.exitCode === 0;
  const expectsTests = /(?:^|\n)(?:npm test|npx vitest)\b/u.test(command);
  const invalidTestEvidence = counts.failed > 0 ||
    (expectsTests && counts.passed === 0 && counts.failed === 0);
  if (execution.exitCode !== 0 || !cleaned || invalidTestEvidence) {
    return {
      source: 'procedure',
      result: 'negative',
      outcome: 'failed',
      verifier: 'factory-feature-guardian:procedure-runner',
      summary: `${procedure.name} failed with exit ${execution.exitCode}: ${output || '(no output)'}`,
      commands: [command],
      positiveAssertions: ['Exact tier-safe documented command and isolated cleanup trap were selected.'],
      negativeAssertions: [
        `Command exited ${execution.exitCode}.`,
        ...(counts.failed > 0 ? [`Observed ${counts.failed} failed tests despite the shell exit status.`] : []),
        ...(expectsTests && counts.passed === 0 && counts.failed === 0
          ? ['The documented test command emitted no parseable test result.']
          : []),
        ...(!cleaned ? ['Isolated workspace cleanup was not positively observed.'] : []),
      ],
      tests: counts,
      cleanup: [cleaned
        ? 'Observed in-process and out-of-band removal of the unique temporary checkout.'
        : 'Temporary-checkout removal was not confirmed by both cleanup paths.'],
    };
  }
  return {
    source: 'procedure',
    result: 'positive',
    outcome: 'passed',
    verifier: 'factory-feature-guardian:procedure-runner',
    summary: `${procedure.name} passed${counts.passed ? ` with ${counts.passed} tests` : ''}.`,
    commands: [command],
    positiveAssertions: [
      'The exact tier-safe checked-in procedure command exited 0.',
      'The named procedure and command allowlist matched.',
    ],
    negativeAssertions: [],
    tests: counts,
    cleanup: ['Observed in-process and out-of-band removal of the unique temporary checkout.'],
  };
}

export const factoryFeatureGuardianAdapters: FeatureGuardianAdapters = {
  loadCatalog: loadFactoryGuardianCatalog,
  resolveProcedure: resolveFactoryGuardianProcedure,
  gateTier: gateFactoryGuardianTier,
  runProcedure: runFactoryGuardianProcedure,
  isAuthorizedConfirmer: (ctx, actorId) => {
    const authorities = [
      factoryGuardianInput(ctx, 'SLACK_USER_WILL'),
      factoryGuardianInput(ctx, 'SLACK_USER_KHALIQ'),
    ].filter((value): value is string => Boolean(value));
    return authorities.includes(actorId);
  },
  clarification: (feature, procedure, turn, turnNumber) => {
    const surface = [feature.cli && `CLI \`${feature.cli}\``, feature.api && `API/config \`${feature.api}\``]
      .filter(Boolean)
      .join(' / ');
    const focus = turnNumber === 1
      ? 'What exact command or user-visible path did you try, and what did you observe?'
      : 'Please paste the positive and negative assertion results plus any cleanup evidence; if it was not run, say which prerequisite is unavailable.';
    const verification = turn.verification
      ? [
        `Guardian check: ${turn.verification.summary}`,
        ...(turn.verification.positiveAssertions?.map((value) => `PASS: ${value}`) ?? []),
        ...(turn.verification.negativeAssertions?.map((value) => `FAIL: ${value}`) ?? []),
        ...(turn.verification.cleanup?.map((value) => `Cleanup: ${value}`) ?? []),
      ].join(' ')
      : '';
    return [
      `I can't confirm *${feature.name}* from that response yet (clarification ${turnNumber}).`,
      `${surface}; source ${feature.locations.map((path) => `\`${path}\``).join(', ')}.`,
      `Use \`${procedure.name}\` at tier ${feature.tier}: ${procedure.prerequisites}`,
      verification,
      focus,
    ].filter(Boolean).join(' ');
  },
  classifyDefect: (_feature, turn, evidence) => classifyFactoryDefect(turn.text, evidence),
  isDefectEstablished: (_feature, turn) => hasConcreteFactoryDefectEvidence(turn.text),
  repositoryForFeature: () => 'AgentWorkforce/factory',
  issuePolicy: ({ feature, conversation, defectKind, slackBacklink }) =>
    factoryGuardianIssuePolicy(feature, conversation, defectKind, slackBacklink),
};

function factoryGuardianIssuePolicy(
  feature: GuardianFeatureSnapshot,
  conversation: Parameters<FeatureGuardianAdapters['issuePolicy']>[0]['conversation'],
  defectKind: GuardianDefectKind,
  slackBacklink: string,
): GuardianIssuePolicy {
  const dedupeKey = `factory:${feature.id}:${feature.procedure}`;
  const commands = conversation.evidence.flatMap((entry) => entry.commands ?? []);
  const observed = conversation.evidence
    .filter((entry) => entry.result === 'negative')
    .map((entry) => `- ${entry.summary}`)
    .join('\n') || '- The scoped guardian response reported a defect.';
  const evidence = conversation.evidence.flatMap((entry) => [
    `- ${entry.source}/${entry.result}: ${entry.summary}`,
    ...(entry.positiveAssertions?.map((assertion) => `  - PASS: ${assertion}`) ?? []),
    ...(entry.negativeAssertions?.map((assertion) => `  - FAIL: ${assertion}`) ?? []),
    ...(entry.tests ? [`  - Tests: ${entry.tests.passed} passed, ${entry.tests.failed} failed`] : []),
    ...(entry.cleanup?.map((cleanup) => `  - Cleanup: ${cleanup}`) ?? []),
  ]).join('\n');
  return {
    repository: 'AgentWorkforce/factory',
    defectKind,
    dedupeKey,
    labels: ['factory-ready'],
    title: `[Feature guardian] ${feature.name}: ${defectKind} defect`,
    body: [
      remediationMarker(dedupeKey),
      '## Guardian finding',
      '',
      `Feature \`${feature.id}\` has an established **${defectKind}** defect.`,
      `Manifest revision: \`${conversation.manifestRevision}\`; generation: \`${conversation.generation}\`.`,
      '',
      '## Expected behavior',
      '',
      feature.description,
      '',
      '## Observed behavior',
      '',
      observed,
      '',
      '## Reproduction and evidence',
      '',
      evidence,
      ...(commands.length > 0 ? ['', '```bash', ...commands, '```'] : []),
      '',
      '## Affected surfaces',
      '',
      `- Feature: \`${feature.id}\` (${feature.category})`,
      `- Procedure: \`${PROCEDURES_RELPATH}#${feature.procedure}\``,
      ...feature.locations.map((path) => `- Source: \`${path}\``),
      `- Verification tier: ${feature.tier}`,
      `- Slack thread: ${slackBacklink}`,
      '',
      '## Definition of done',
      '',
      `- [ ] Correct the ${defectKind} defect at the affected Factory surface.`,
      `- [ ] Add or update a regression test that fails before the fix and passes after it.`,
      `- [ ] Run the exact \`${feature.procedure}\` procedure and record positive/negative assertions and cleanup.`,
      '- [ ] Reconcile the manifest, procedure, and runbook so they describe the implemented behavior.',
      '- [ ] Re-run the feature guardian check and produce one exact-revision confirmation record.',
    ].join('\n'),
  };
}

function classifyFactoryDefect(
  text: string,
  evidence: readonly { source?: string; result?: string; summary: string }[],
): GuardianDefectKind {
  if (evidence.some((entry) =>
    entry.source === 'procedure' &&
    entry.result === 'negative' &&
    /\bprocedure drift\b/iu.test(entry.summary),
  )) return 'procedure';
  const actorEvidence = evidence
    .filter((entry) => entry.source === 'actor')
    .map((entry) => entry.summary)
    .join('\n');
  const haystack = `${text}\n${actorEvidence}`.toLowerCase();
  if (/\b(manifest|catalog|feature map|featuremap)\b/u.test(haystack)) return 'manifest';
  if (/\b(procedure|runbook|verification steps?)\b/u.test(haystack)) return 'procedure';
  if (/\b(documentation|docs?|readme)\b/u.test(haystack)) return 'documentation';
  if (/\b(test|spec|fixture|assertion)\b/u.test(haystack)) return 'test';
  return 'implementation';
}

function hasConcreteFactoryDefectEvidence(text: string): boolean {
  const normalized = text.trim();
  if (normalized.length < 32) return false;
  const hasArtifact = /```[\s\S]+```/u.test(normalized) ||
    /`[^`\n]+`/u.test(normalized) ||
    /(?:^|\s)(?:src|test|tests|docs|\.agentworkforce)\/[\w./-]+/u.test(normalized);
  const hasComparison = /\b(expected|actual|observed|outputs?|returns?|reports?|says?|but|instead)\b/iu.test(normalized);
  return hasArtifact && hasComparison;
}

function factoryGuardianInput(ctx: WorkforceCtx, name: string): string | undefined {
  const spec = ctx.persona?.inputSpecs?.[name];
  const value = process.env[spec?.env ?? name] ?? ctx.persona?.inputs?.[name] ?? spec?.default;
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

export function parseFactoryGuardianTestCounts(output: string): { passed: number; failed: number } {
  const summary = output
    .replace(/\u001b\[[0-9;]*m/gu, '')
    .split(/\r?\n/u)
    .filter((line) => /^\s*Tests(?:\s|:)/u.test(line))
    .at(-1) ?? '';
  const passed = Number(summary.match(/\b(\d+)\s+passed\b/iu)?.[1] ?? 0);
  const failed = Number(summary.match(/\b(\d+)\s+failed\b/iu)?.[1] ?? 0);
  return { passed, failed };
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function boundedText(value: string, max: number): string {
  return value.length <= max ? value : `${value.slice(0, max - 1)}…`;
}

export interface FactoryGuardianHandlerDependencies {
  conversation?: GuardianConversationDependencies;
}

export async function runFactoryFeatureGuardian(
  ctx: WorkforceCtx,
  event: WorkforceEvent,
  dependencies: FactoryGuardianHandlerDependencies = {},
): Promise<void> {
  if (event.type === 'cron.tick') {
    await runGuardian(ctx, event);
    return;
  }
  await runGuardianConversationTurn(
    ctx,
    event,
    factoryFeatureGuardianAdapters,
    dependencies.conversation,
  );
}

export default defineFeatureGuardianAgent({
  adapters: factoryFeatureGuardianAdapters,
  scheduled: runGuardian,
  channelInput: 'SLACK_CHANNEL',
  schedules: [{ name: 'hourly-check', cron: '0 * * * *', tz: 'America/New_York' }],
});
