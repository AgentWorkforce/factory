#!/usr/bin/env node
import { appendFile, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  createFactory,
  InternalFleetClient,
  MountLinearWriteback,
  RelayfileCloudMountClient,
  resolveFactoryStates,
} from '../dist/index.js'

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)))
const logPath = process.env.FACTORY_E2E_LOG ?? '/tmp/factory-e2e-programmatic-live.log'
const statusPath = process.env.FACTORY_E2E_STATUS ?? '/tmp/factory-e2e-programmatic-status.json'

async function readJson(path) {
  return JSON.parse(await readFile(path, 'utf8'))
}

function renderArg(arg) {
  if (arg instanceof Error) {
    return JSON.stringify({
      name: arg.name,
      message: arg.message,
      stack: arg.stack,
      cause: arg.cause,
    })
  }
  if (arg && typeof arg === 'object') {
    return JSON.stringify(arg, (_key, value) => {
      if (value instanceof Error) {
        return {
          name: value.name,
          message: value.message,
          stack: value.stack,
          cause: value.cause,
        }
      }
      return value
    })
  }
  return String(arg)
}

async function log(level, message, ...args) {
  const renderedArgs = args.length
    ? ` ${args.map(renderArg).join(' ')}`
    : ''
  await appendFile(logPath, `[${new Date().toISOString()}] ${level} ${message}${renderedArgs}\n`)
}

const logger = {
  debug: (message, ...args) => void log('debug', message, ...args),
  info: (message, ...args) => void log('info', message, ...args),
  warn: (message, ...args) => void log('warn', message, ...args),
  error: (message, ...args) => void log('error', message, ...args),
}

const config = await readJson(join(repoRoot, 'factory.config.json'))
const opencode = await readJson(join(repoRoot, 'opencode.json'))
const workspaceKey = process.env.RELAY_WORKSPACE_KEY ?? opencode.mcp?.['agent-relay']?.environment?.RELAY_API_KEY

if (!workspaceKey) {
  throw new Error('No relay workspace key found in RELAY_WORKSPACE_KEY or opencode.json')
}

await writeFile(logPath, '')
await writeFile(statusPath, JSON.stringify({ pid: process.pid, starting: true, at: new Date().toISOString() }, null, 2))

const mount = await RelayfileCloudMountClient.fromConfig({ workspaceKey, isAllowedDraft: () => true })
const stateResolution = await resolveFactoryStates(mount, config)
const fleet = new InternalFleetClient({
  cwd: repoRoot,
  connectionPath: join(repoRoot, '.agentworkforce/relay/connection.json'),
  workspaceKey,
  logger,
})
const linear = MountLinearWriteback(mount, {
  safety: config.safety,
  logger,
  readbackConfirmAttempts: Number(process.env.FACTORY_E2E_READBACK_ATTEMPTS ?? 180),
  readbackConfirmDelayMs: Number(process.env.FACTORY_E2E_READBACK_DELAY_MS ?? 1000),
})
const factory = createFactory(config, { mount, fleet, stateResolution, linear, logger })

async function writeStatus(extra = {}) {
  await writeFile(
    statusPath,
    JSON.stringify({ pid: process.pid, at: new Date().toISOString(), status: factory.status(), ...extra }, null, 2),
  )
}

let stopping = false
async function stop(signal) {
  if (stopping) return
  stopping = true
  await log('info', `stopping on ${signal}`)
  try {
    await factory.stop()
    await writeStatus({ stopped: true, signal })
  } finally {
    process.exit(0)
  }
}

process.on('SIGINT', () => void stop('SIGINT'))
process.on('SIGTERM', () => void stop('SIGTERM'))

await factory.start({ mode: 'live' })
await log('info', 'factory live runner started')
await writeStatus({ started: true })

setInterval(() => void writeStatus({ started: true }), 2000)
