#!/usr/bin/env node

import assert from 'node:assert/strict'
import { execFileSync, spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const artifactDir = resolve(root, process.env.FACTORY_E2E_ARTIFACT_DIR ?? 'artifacts')
const attestationPath = join(artifactDir, 'factory-e2e-attestation.json')
const startedAt = new Date().toISOString()
const testedCheckoutSha = git('rev-parse', 'HEAD')
const headSha = process.env.FACTORY_E2E_HEAD_SHA?.trim() || testedCheckoutSha
const baseSha = process.env.FACTORY_E2E_BASE_SHA?.trim() || undefined
const consumer = mkdtempSync(join(tmpdir(), 'factory-packed-e2e-'))
const packDir = join(consumer, 'pack')
const checks = []

try {
  assert.match(headSha, /^[0-9a-f]{40}$/u, 'head SHA must be a full Git object ID')
  assert.match(testedCheckoutSha, /^[0-9a-f]{40}$/u, 'tested checkout SHA must be a full Git object ID')
  checks.push('head-sha-bound')

  mkdirSync(packDir, { recursive: true })
  const pack = JSON.parse(exec('npm', ['pack', '--json', '--pack-destination', packDir]))
  assert.equal(pack.length, 1, 'npm pack must produce exactly one artifact')
  const tarball = join(packDir, pack[0].filename)
  const tarballSha256 = createHash('sha256').update(readFileSync(tarball)).digest('hex')

  writeFileSync(join(consumer, 'package.json'), `${JSON.stringify({
    name: 'factory-e2e-consumer',
    private: true,
    type: 'module',
  }, null, 2)}\n`)
  exec('npm', [
    'install',
    '--ignore-scripts',
    '--no-package-lock',
    '--no-audit',
    '--no-fund',
    tarball,
  ], consumer)
  checks.push('packed-consumer-install')

  const cli = join(consumer, 'node_modules', '.bin', process.platform === 'win32' ? 'factory.cmd' : 'factory')
  const cliResult = spawnSync(cli, ['--help'], { cwd: consumer, encoding: 'utf8' })
  assert.equal(cliResult.status, 0, `packed CLI failed: ${cliResult.stderr || cliResult.stdout}`)
  assert.match(cliResult.stdout, /factory/u)
  checks.push('packed-cli-smoke')

  const scenarioPath = join(consumer, 'packed-e2e-scenario.mjs')
  copyFileSync(resolve(root, 'scripts/packed-e2e-scenario.mjs'), scenarioPath)
  const scenarioResult = spawnSync(process.execPath, [scenarioPath], {
    cwd: consumer,
    encoding: 'utf8',
    env: { ...process.env, NODE_NO_WARNINGS: '1' },
  })
  assert.equal(
    scenarioResult.status,
    0,
    `packed public-API scenario failed:\n${scenarioResult.stderr || scenarioResult.stdout}`,
  )
  const scenario = JSON.parse(scenarioResult.stdout)
  assert.equal(scenario.result, 'passed')
  assert.ok(Array.isArray(scenario.checks) && scenario.checks.length >= 6)
  checks.push(...scenario.checks)

  mkdirSync(dirname(attestationPath), { recursive: true })
  const attestation = {
    schemaVersion: 1,
    package: {
      name: pack[0].name,
      version: pack[0].version,
      filename: pack[0].filename,
      sha256: tarballSha256,
      size: pack[0].size,
      unpackedSize: pack[0].unpackedSize,
    },
    git: { headSha, testedCheckoutSha, ...(baseSha ? { baseSha } : {}) },
    runtime: { node: process.version, platform: process.platform, arch: process.arch },
    checks,
    result: 'passed',
    startedAt,
    completedAt: new Date().toISOString(),
  }
  writeFileSync(attestationPath, `${JSON.stringify(attestation, null, 2)}\n`)
  console.log(`[factory-e2e] ${checks.length} checks passed for ${headSha}`)
  console.log(`[factory-e2e] attestation: ${attestationPath}`)
} finally {
  rmSync(consumer, { recursive: true, force: true })
}

function git(...args) {
  return execFileSync('git', args, { cwd: root, encoding: 'utf8' }).trim()
}

function exec(command, args, cwd = root) {
  return execFileSync(command, args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
}
