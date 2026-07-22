import { readFileSync } from 'node:fs';

import { describe, expect, it } from 'vitest';

import agent from './agent.js';
import persona from './persona.js';

type RelayfileMount = {
  requiredReadPaths: string[];
  writeOnlyPaths: string[];
};

type CompiledPersona = {
  id: string;
  capabilities?: {
    pullRequest?: {
      enabled?: boolean;
      writeback?: boolean;
      fetchDepth?: number | 'full';
    };
  };
  integrations: {
    github: {
      scope?: { repo?: string };
      relayfileMount?: RelayfileMount;
    };
  };
  onEvent?: string;
};

const agentworkforceRoot = new URL('../../', import.meta.url);
const expectedAuthority = '/github/repos/AgentWorkforce/factory/issues/**';

function githubMount(value: typeof persona): RelayfileMount {
  const integrations = value.integrations as Record<
    string,
    { relayfileMount?: RelayfileMount }
  >;
  const mount = integrations.github?.relayfileMount;
  expect(mount).toBeDefined();
  return mount as RelayfileMount;
}

function terminalTreeContains(authority: string, path: string): boolean {
  expect(authority).toMatch(/^\/[^*?\[\]{}]+\/\*\*$/u);
  const root = authority.slice(0, -3);
  return path === root || path.startsWith(`${root}/`);
}

describe('factory maintainability reviewer contract', () => {
  it('is repository-qualified, read-only, and backed by full git history', () => {
    expect(persona.id).toBe('factory-maintainability');
    expect(persona.integrations.github.scope).toEqual({
      repo: 'AgentWorkforce/factory',
    });
    expect(persona.capabilities.pullRequest).toEqual({
      enabled: true,
      writeback: false,
      fetchDepth: 'full',
    });
    expect(persona).not.toHaveProperty('memory');
  });

  it('wakes only for Factory PR revisions and carries the issue-comment companion', () => {
    const triggers = agent.triggers as Record<
      string,
      Array<{ on: string; paths?: string[] }>
    >;
    const expectedPaths = [
      '/github/repos/AgentWorkforce/factory/pulls/**',
      expectedAuthority,
    ];

    expect(triggers.github).toEqual([
      { on: 'pull_request.opened', paths: expectedPaths, maxConcurrency: 1 },
      { on: 'pull_request.synchronize', paths: expectedPaths, maxConcurrency: 1 },
    ]);
  });

  it('uses one terminal issue-tree authority for idempotency and review delivery', () => {
    const mount = githubMount(persona);
    expect(mount).toEqual({
      requiredReadPaths: [expectedAuthority],
      writeOnlyPaths: [],
    });

    const reviewPath =
      '/github/repos/AgentWorkforce/factory/issues/180/comments/review-maintainability-deadbeef.json';
    expect(terminalTreeContains(expectedAuthority, reviewPath)).toBe(true);
    expect(new Set([...mount.requiredReadPaths, ...mount.writeOnlyPaths]).size).toBe(1);
  });

  it('ships a Factory-specific charter and compiled deployment manifest', () => {
    const charter = readFileSync(
      new URL('workforce/personas/maintainability.md', agentworkforceRoot),
      'utf8',
    );
    const compiled = JSON.parse(
      readFileSync(
        new URL('agents/factory-maintainability/persona.json', agentworkforceRoot),
        'utf8',
      ),
    ) as CompiledPersona;

    expect(charter).toContain('# The Factory Maintainability Charter');
    expect(charter).toContain('.agentworkforce/features/critical-paths.md');
    expect(charter).not.toContain('HoopSheet');
    expect(compiled.id).toBe('factory-maintainability');
    expect(compiled.integrations.github.scope).toEqual({
      repo: 'AgentWorkforce/factory',
    });
    expect(compiled.integrations.github.relayfileMount).toEqual({
      requiredReadPaths: [expectedAuthority],
      writeOnlyPaths: [],
    });
    expect(compiled.capabilities?.pullRequest).toEqual({
      enabled: true,
      writeback: false,
      fetchDepth: 'full',
    });
    expect(compiled.onEvent).toBe('./agent.ts');
  });
});
