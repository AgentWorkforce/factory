import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

import type { FactoryInFlightRegistryProcess } from '../types'

const execFileAsync = promisify(execFile)

export type ProcessIdentity = Pick<FactoryInFlightRegistryProcess, 'pid' | 'cmdline' | 'startTime'>

export async function readProcessIdentity(pid: number): Promise<ProcessIdentity | undefined> {
  try {
    const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'lstart=', '-o', 'command='])
    const line = stdout.trim()
    if (!line) return undefined
    const match = line.match(/^(\S+\s+\S+\s+\d+\s+\d+:\d+:\d+\s+\d+)\s+([\s\S]+)$/u)
    if (!match) return undefined
    return { pid, startTime: match[1]!, cmdline: match[2]! }
  } catch {
    return undefined
  }
}
