import { spawn } from 'node:child_process'

const MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024

export interface CommandResult {
  stdout: string
  stderr: string
}

export interface RunCommandOptions {
  cwd?: string
  input?: string
  timeoutMs?: number
  signal?: AbortSignal
}

export interface CommandRunner {
  run(command: string, args: string[], options?: RunCommandOptions): Promise<CommandResult>
}

export interface KubernetesConnection {
  kubeconfig?: string
  context?: string
}

export class CommandExecutionError extends Error {
  constructor(
    message: string,
    public readonly command: string,
    public readonly args: readonly string[],
    public readonly stdout: string,
    public readonly stderr: string,
    options?: ErrorOptions,
  ) {
    super(message, options)
    this.name = 'CommandExecutionError'
  }
}

export class ProcessCommandRunner implements CommandRunner {
  async run(command: string, args: string[], options: RunCommandOptions = {}): Promise<CommandResult> {
    return await new Promise<CommandResult>((resolve, reject) => {
      const child = spawn(command, args, {
        cwd: options.cwd,
        env: process.env,
        stdio: ['pipe', 'pipe', 'pipe'],
      })
      let stdout = ''
      let stderr = ''
      let timedOut = false
      let forceTimer: ReturnType<typeof setTimeout> | undefined
      let settled = false

      child.stdout.setEncoding('utf8')
      child.stderr.setEncoding('utf8')
      child.stdout.on('data', (chunk: string) => {
        stdout = `${stdout}${chunk}`.slice(-MAX_COMMAND_OUTPUT_BYTES)
      })
      child.stderr.on('data', (chunk: string) => {
        stderr = `${stderr}${chunk}`.slice(-MAX_COMMAND_OUTPUT_BYTES)
      })

      const stop = (): void => {
        if (child.exitCode !== null) return
        child.kill('SIGTERM')
        forceTimer = setTimeout(() => child.kill('SIGKILL'), 2_000)
        forceTimer.unref()
      }
      const abort = (): void => stop()
      options.signal?.addEventListener('abort', abort, { once: true })

      const timer = options.timeoutMs === undefined
        ? undefined
          : setTimeout(() => {
            timedOut = true
            stop()
          }, options.timeoutMs)

      child.on('error', (cause) => {
        if (settled) return
        settled = true
        if (timer) clearTimeout(timer)
        if (forceTimer) clearTimeout(forceTimer)
        options.signal?.removeEventListener('abort', abort)
        reject(new CommandExecutionError(
          `Failed to start ${command}: ${cause.message}`,
          command,
          args,
          stdout,
          stderr,
          { cause },
        ))
      })
      child.on('close', (code, signal) => {
        if (settled) return
        settled = true
        if (timer) clearTimeout(timer)
        if (forceTimer) clearTimeout(forceTimer)
        options.signal?.removeEventListener('abort', abort)
        if (code === 0 && !timedOut) {
          resolve({ stdout, stderr })
          return
        }
        const detail = options.signal?.aborted
          ? 'was aborted'
          : timedOut
          ? `timed out after ${options.timeoutMs}ms`
          : `exited with ${code ?? signal ?? 'unknown status'}`
        reject(new CommandExecutionError(
          `${command} ${detail}${stderr.trim() ? `: ${stderr.trim()}` : ''}`,
          command,
          args,
          stdout,
          stderr,
        ))
      })

      if (options.signal?.aborted) {
        stop()
      } else if (options.input === undefined) {
        child.stdin.end()
      } else {
        child.stdin.end(options.input)
      }
    })
  }
}

export function kubectlConnectionArgs(connection: KubernetesConnection): string[] {
  return [
    ...(connection.kubeconfig ? ['--kubeconfig', connection.kubeconfig] : []),
    ...(connection.context ? ['--context', connection.context] : []),
  ]
}

export function helmConnectionArgs(connection: KubernetesConnection): string[] {
  return [
    ...(connection.kubeconfig ? ['--kubeconfig', connection.kubeconfig] : []),
    ...(connection.context ? ['--kube-context', connection.context] : []),
  ]
}
