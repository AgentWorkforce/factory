import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

export interface GhRunResult {
  stdout: string
  stderr?: string
}

export type GhRunner = (args: string[]) => Promise<GhRunResult>

/**
 * Compatibility runner for the two deliberately local-user flows that have
 * not yet moved to a Relayfile read surface: explicit `github.identity=user`
 * PR publication and legacy probe-PR guards. Factory lifecycle and babysitter
 * writes must never use this runner.
 */
export const defaultGhRunner: GhRunner = async (args) => {
  const { stdout, stderr } = await execFileAsync('gh', args, { maxBuffer: 1024 * 1024 })
  return { stdout, stderr }
}
