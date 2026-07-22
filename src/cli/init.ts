import { access, writeFile } from 'node:fs/promises'
import { constants } from 'node:fs'
import { spawn } from 'node:child_process'
import { join, resolve } from 'node:path'

import {
  RelayfileCloudMountClient,
  resolveFactoryWorkspace,
  type MountClient,
  type ResolvedFactoryWorkspace,
} from '../index'

type FactoryInitMount = MountClient & {
  ensureLocalMount?: (startDir: string, options?: { acceptableWorkspaceIds?: readonly string[] }) => Promise<void>
}

export interface FactoryInitOptions {
  cwd?: string
  repo?: string
  workspaceId?: string
  stdout?: Pick<NodeJS.WriteStream, 'write'>
  stderr?: Pick<NodeJS.WriteStream, 'write'>
  commandExists?: (command: string) => Promise<boolean>
  gitRemote?: (cwd: string) => Promise<string | undefined>
  resolveWorkspace?: () => Promise<ResolvedFactoryWorkspace>
  cloudMountFromConfig?: (config: { workspaceId: string }) => Promise<FactoryInitMount>
  ensureLocalMount?: (workspaceId: string, cwd: string, options?: { acceptableWorkspaceIds?: readonly string[] }) => Promise<void>
}

export interface FactoryInitResult {
  configPath: string
  repo: string
  workspaceId: string
}

/**
 * Make the current checkout dispatch GitHub-native issues. The checks happen
 * before writing config so a successful init is immediately runnable.
 */
export async function initializeFactory(options: FactoryInitOptions = {}): Promise<FactoryInitResult> {
  const cwd = resolve(options.cwd ?? process.cwd())
  const out = options.stdout ?? process.stdout
  const err = options.stderr ?? process.stderr
  const repo = validateRepo(options.repo ?? repoFromRemote(await (options.gitRemote ?? originRemote)(cwd)))
  const commandExists = options.commandExists ?? executableOnPath

  const missing = (await Promise.all(['agent-relay', 'relayfile'].map(async (command) => (
    await commandExists(command) ? undefined : command
  )))).filter((command): command is string => command !== undefined)
  if (missing.length > 0) {
    err.write('Factory needs a couple of local tools before it can run:\n')
    for (const command of missing) {
      err.write(`  - ${command} is not installed or is not on PATH\n`)
    }
    err.write('Install and sign in to those tools, then run `factory init` again. No files were changed.\n')
    throw new Error(`Missing required dependency${missing.length === 1 ? '' : 'ies'}: ${missing.join(', ')}`)
  }

  const workspace = options.workspaceId
    ? { workspaceId: options.workspaceId }
    : await (options.resolveWorkspace ?? resolveFactoryWorkspace)()
  const workspaceId = workspace.workspaceId
  if (!workspaceId) throw new Error('Could not find an active Relay workspace. Sign in to relayfile, or run `factory init --workspace <workspace-id>`.')

  const configPath = join(cwd, 'factory.config.json')
  await assertAbsent(configPath)

  out.write(`Checking ${repo} in Relay workspace ${workspaceId}…\n`)
  const mount = await (options.cloudMountFromConfig ?? RelayfileCloudMountClient.fromConfig)({ workspaceId })
  const mountOptions = {
    acceptableWorkspaceIds: 'cloudWorkspaceId' in workspace && workspace.cloudWorkspaceId ? [workspace.cloudWorkspaceId] : undefined,
  }
  if (options.ensureLocalMount) {
    await options.ensureLocalMount(workspaceId, cwd, mountOptions)
  } else if (mount.ensureLocalMount) {
    await mount.ensureLocalMount(cwd, mountOptions)
  } else {
    throw new Error('The connected Relay workspace cannot start a local mount. Update @agent-relay/factory and run `factory init` again.')
  }
  const githubStatus = await mount.ensureSubRoot(`/github/repos/${repo}`, { timeoutMs: 90_000 })
  if (githubStatus !== 'ready') {
    err.write(`GitHub repository ${repo} is not connected to this Relay workspace.\n`)
    err.write('Connect GitHub with push access for this repository in Agent Relay, wait for the repository to sync, then run `factory init` again. No config was written.\n')
    throw new Error(`GitHub connection for ${repo} is not ready (${githubStatus})`)
  }

  const [owner, name] = repo.split('/') as [string, string]
  const config = {
    workspaceId,
    issueSource: 'github',
    repos: {
      org: owner,
      names: [name],
      default: repo,
      clonePaths: { [repo]: cwd },
    },
  }
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' })
  out.write(`Factory is ready for ${repo}. Created ${configPath}.\n`)
  out.write('Add the `factory` label to an open GitHub issue, then run `factory run-once --dry-run`.\n')
  return { configPath, repo, workspaceId }
}

function validateRepo(value: string | undefined): string {
  if (!value || !/^[^/\s]+\/[^/\s]+$/u.test(value)) {
    throw new Error('Could not determine the GitHub repository. Run `factory init owner/repo` from a Git checkout, or pass an explicit owner/repo.')
  }
  return value
}

function repoFromRemote(remote: string | undefined): string | undefined {
  if (!remote) return undefined
  const ssh = /^(?:ssh:\/\/)?git@github\.com[:/]([^/\s]+\/[^/\s]+?)(?:\.git)?\/?$/u.exec(remote)
  const https = /^https?:\/\/github\.com\/([^/\s]+\/[^/\s]+?)(?:\.git)?\/?$/u.exec(remote)
  return ssh?.[1] ?? https?.[1]
}

async function assertAbsent(path: string): Promise<void> {
  try {
    await access(path, constants.F_OK)
    throw new Error(`Refusing to overwrite existing ${path}. Edit it manually or move it aside before running factory init.`)
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Refusing')) throw error
  }
}

function originRemote(cwd: string): Promise<string | undefined> {
  return new Promise((resolveRemote) => {
    const child = spawn('git', ['config', '--get', 'remote.origin.url'], { cwd, stdio: ['ignore', 'pipe', 'ignore'] })
    const chunks: Buffer[] = []
    child.stdout?.on('data', (chunk: Buffer) => chunks.push(chunk))
    child.on('close', (code) => resolveRemote(code === 0 ? Buffer.concat(chunks).toString('utf8').trim() || undefined : undefined))
    child.on('error', () => resolveRemote(undefined))
  })
}

function executableOnPath(command: string): Promise<boolean> {
  return new Promise((resolveExists) => {
    const child = spawn(command, ['--version'], { stdio: 'ignore' })
    child.on('close', (code) => resolveExists(code === 0))
    child.on('error', () => resolveExists(false))
  })
}
