#!/usr/bin/env node

import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { PreviewProcessSupervisor } from '../dist/node/preview-process.js'
import { TailscalePreviewManager } from '../dist/node/tailscale-preview.js'

const scriptPath = fileURLToPath(import.meta.url)
const repoRoot = resolve(dirname(scriptPath), '..')
const tailscaleBinary = process.env.TAILSCALE_BIN ?? 'tailscale'
const httpsPort = integerFromEnv('FACTORY_PREVIEW_E2E_HTTPS_PORT', 10_000)
const targetPort = integerFromEnv('FACTORY_PREVIEW_E2E_TARGET_PORT', 43_129)
const workspace = process.env.FACTORY_PREVIEW_E2E_WORKSPACE ?? `factory-preview-e2e-${randomUUID()}`
const owner = `${workspace}:owner`
const service = 'factory-preview-e2e'
const checkoutPath = repoRoot
const responseMarker = process.env.FACTORY_PREVIEW_E2E_RESPONSE_MARKER ?? `factory-preview-e2e:${randomUUID()}`
const serverSource = `require('node:http').createServer((_request, response) => response.end(${JSON.stringify(responseMarker)})).listen(Number(process.env.PORT), '127.0.0.1')`
const startCommand = `${JSON.stringify(process.execPath)} -e ${JSON.stringify(serverSource)}`
const configuredTemporaryRoot = process.env.FACTORY_PREVIEW_E2E_TEMP_ROOT
const temporaryRoot = configuredTemporaryRoot ?? mkdtempSync(join(tmpdir(), 'factory-preview-e2e-'))
const registryPath = join(temporaryRoot, 'registry.json')
const referencePath = join(temporaryRoot, 'reference.json')

const config = {
  provider: 'tailscale-serve',
  access: 'tailnet',
  services: {
    [service]: {
      port: targetPort,
      portSpan: 1,
      httpsPort,
      startCommand,
    },
  },
  tailscaleBinary,
  registryPath,
  httpsPortRange: [httpsPort, httpsPort],
}

const startInput = {
  namespace: workspace,
  owner,
  issueKey: 'E2E-PREVIEW',
  service,
  repo: 'AgentWorkforce/factory',
  targetPort,
  preferredHttpsPort: httpsPort,
  startCommand,
  checkoutPath,
  node: 'self',
}

if (process.argv.includes('--start-child')) {
  const manager = new TailscalePreviewManager({ config })
  const reference = await manager.start(startInput)
  writeFileSync(referencePath, JSON.stringify(reference), 'utf8')
  process.exit(0)
}

const statusBefore = serveStatus()
const manager = new TailscalePreviewManager({ config })
let reference
let recovered
let orphan
try {
  execFileSync(process.execPath, [scriptPath, '--start-child'], {
    env: {
      ...process.env,
      TAILSCALE_BIN: tailscaleBinary,
      FACTORY_PREVIEW_E2E_HTTPS_PORT: String(httpsPort),
      FACTORY_PREVIEW_E2E_TARGET_PORT: String(targetPort),
      FACTORY_PREVIEW_E2E_WORKSPACE: workspace,
      FACTORY_PREVIEW_E2E_TEMP_ROOT: temporaryRoot,
      FACTORY_PREVIEW_E2E_RESPONSE_MARKER: responseMarker,
    },
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  reference = JSON.parse(readFileSync(referencePath, 'utf8'))
  assert.equal(reference.access, 'tailnet')
  assert.equal(reference.lifetime, 'issue')
  assert.equal(reference.process.cwd, checkoutPath)

  const initialResponse = await fetchText(reference.url)
  assert.equal(initialResponse, responseMarker)

  // A fresh manager represents the next agent/daemon owner. It must recover
  // the exact route and detached process rather than starting a duplicate.
  recovered = await manager.start(startInput)
  assert.equal(recovered.id, reference.id)
  assert.equal(recovered.process.pid, reference.process.pid)
  assert.equal(recovered.process.startTime, reference.process.startTime)
  assert.equal(await fetchText(recovered.url), responseMarker)

  const liveStatus = serveStatus()
  assert.equal(routeTarget(liveStatus, httpsPort), `http://127.0.0.1:${targetPort}`)
  assert.equal(routeAllowsFunnel(liveStatus, httpsPort), false)

  assert.equal(await manager.remove(recovered), true)
  assert.equal(await new PreviewProcessSupervisor().isRunning(recovered.process), false)
  await assert.rejects(fetchText(recovered.url, 3_000))
  await assert.rejects(fetchText(`http://127.0.0.1:${targetPort}/`, 3_000))
  assert.deepEqual(withoutPort(serveStatus(), httpsPort), withoutPort(statusBefore, httpsPort))

  // Start a second preview from a short-lived process, then deliberately leave
  // it behind. A fresh manager's startup-sweep contract must reap both the
  // provider route and detached command from registry identity alone.
  execFileSync(process.execPath, [scriptPath, '--start-child'], {
    env: {
      ...process.env,
      TAILSCALE_BIN: tailscaleBinary,
      FACTORY_PREVIEW_E2E_HTTPS_PORT: String(httpsPort),
      FACTORY_PREVIEW_E2E_TARGET_PORT: String(targetPort),
      FACTORY_PREVIEW_E2E_WORKSPACE: workspace,
      FACTORY_PREVIEW_E2E_TEMP_ROOT: temporaryRoot,
      FACTORY_PREVIEW_E2E_RESPONSE_MARKER: responseMarker,
    },
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  orphan = JSON.parse(readFileSync(referencePath, 'utf8'))
  assert.equal(await fetchText(orphan.url), responseMarker)

  const sweep = await new TailscalePreviewManager({ config }).sweep({
    namespace: workspace,
    activeOwners: [],
    activePreviewIds: [],
  })
  assert.deepEqual(sweep.skipped, [])
  assert.deepEqual(sweep.reaped.map((preview) => preview.id), [orphan.id])
  assert.equal(await new PreviewProcessSupervisor().isRunning(orphan.process), false)
  await assert.rejects(fetchText(orphan.url, 3_000))
  await assert.rejects(fetchText(`http://127.0.0.1:${targetPort}/`, 3_000))
  assert.deepEqual(withoutPort(serveStatus(), httpsPort), withoutPort(statusBefore, httpsPort))

  console.log(JSON.stringify({
    ok: true,
    provider: recovered.provider,
    access: recovered.access,
    url: recovered.url,
    targetPort: recovered.targetPort,
    httpsPort: recovered.httpsPort,
    recoveredSamePreview: recovered.id === reference.id,
    recoveredSameProcess: recovered.process.pid === reference.process.pid,
    responseMarker,
    teardownConfirmed: true,
    orphanSweepConfirmed: true,
    otherPortServeConfigurationPreserved: true,
  }, null, 2))
} finally {
  let cleanupSucceeded = true
  const cleanupErrors = []
  const cleanupReference = orphan ?? recovered ?? reference
  if (cleanupReference) {
    try {
      if (!await manager.remove(cleanupReference)) {
        cleanupSucceeded = false
        cleanupErrors.push('provider manager could not confirm preview removal')
      }
    } catch (error) {
      cleanupSucceeded = false
      cleanupErrors.push(`preview removal threw: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
  try {
    const cleanupSweep = await manager.sweep({ namespace: workspace, activeOwners: [], activePreviewIds: [] })
    if (cleanupSweep.skipped.length > 0) {
      cleanupSucceeded = false
      cleanupErrors.push(`cleanup sweep skipped: ${JSON.stringify(cleanupSweep.skipped)}`)
    }
  } catch (error) {
    cleanupSucceeded = false
    cleanupErrors.push(`cleanup sweep threw: ${error instanceof Error ? error.message : String(error)}`)
  }
  if (cleanupReference?.process && await new PreviewProcessSupervisor().isRunning(cleanupReference.process)) {
    cleanupSucceeded = false
    cleanupErrors.push(`managed wrapper process ${cleanupReference.process.pid} is still running`)
  }
  if (await isReachable(`http://127.0.0.1:${targetPort}/`, 1_000)) {
    cleanupSucceeded = false
    cleanupErrors.push(`managed local target port ${targetPort} is still reachable`)
  }
  try {
    if (routeTarget(serveStatus(), httpsPort) === `http://127.0.0.1:${targetPort}`) {
      cleanupSucceeded = false
      cleanupErrors.push(`managed Tailscale route on HTTPS port ${httpsPort} is still present`)
    }
  } catch (error) {
    cleanupSucceeded = false
    cleanupErrors.push(`final Serve status check threw: ${error instanceof Error ? error.message : String(error)}`)
  }
  if (cleanupSucceeded) {
    // Only remove a directory this invocation created. The child receives the
    // path through the environment, and an operator-supplied recovery path may
    // contain evidence or other files that the harness must not delete.
    if (!configuredTemporaryRoot) rmSync(temporaryRoot, { recursive: true, force: true })
  } else {
    process.exitCode = 1
    console.error(`E2E cleanup was incomplete; recovery artifacts retained at ${temporaryRoot}`)
    for (const error of cleanupErrors) console.error(`- ${error}`)
  }
}

function serveStatus() {
  return JSON.parse(execFileSync(tailscaleBinary, ['serve', 'status', '--json'], {
    encoding: 'utf8',
  }))
}

async function fetchText(url, timeoutMs = 10_000) {
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) })
  assert.equal(response.ok, true, `${url} returned HTTP ${response.status}`)
  return await response.text()
}

function routeTarget(status, port) {
  const suffix = `:${port}`
  for (const [hostPort, server] of Object.entries(status?.Web ?? {})) {
    if (hostPort.endsWith(suffix)) return server?.Handlers?.['/']?.Proxy
  }
  return undefined
}

function routeAllowsFunnel(status, port) {
  const suffix = `:${port}`
  return Object.entries(status?.AllowFunnel ?? {})
    .some(([hostPort, allowed]) => hostPort.endsWith(suffix) && allowed === true)
}

function withoutPort(status, port) {
  const copy = structuredClone(status ?? {})
  delete copy.TCP?.[String(port)]
  for (const key of Object.keys(copy.Web ?? {})) {
    if (key.endsWith(`:${port}`)) delete copy.Web[key]
  }
  for (const key of Object.keys(copy.AllowFunnel ?? {})) {
    if (key.endsWith(`:${port}`)) delete copy.AllowFunnel[key]
  }
  return copy
}

async function isReachable(url, timeoutMs) {
  try {
    await fetchText(url, timeoutMs)
    return true
  } catch {
    return false
  }
}

function integerFromEnv(name, fallback) {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isInteger(value) || value < 1 || value > 65_535) {
    throw new Error(`${name} must be an integer between 1 and 65535`)
  }
  return value
}
