import type { Capability, FleetClient, RosterEntry, SendInput, SpawnInput, SpawnResult } from '../ports'

const notImplemented = () => new Error('RelayFleetClient not implemented — see relay#1056')

// TODO(relay#1056): implement this over the ../relay SDK once the fleet protocol lands there.
export class RelayFleetClient implements FleetClient {
  async spawn(_input: SpawnInput): Promise<SpawnResult> {
    throw notImplemented()
  }

  async resume(_input: { name?: string; sessionRef: string; node?: 'self' | string; capability?: Capability }): Promise<SpawnResult> {
    throw notImplemented()
  }

  async release(_name: string, _reason?: string): Promise<void> {
    throw notImplemented()
  }

  async roster(): Promise<RosterEntry> {
    throw notImplemented()
  }

  async sendMessage(_input: SendInput): Promise<void> {
    throw notImplemented()
  }

  async waitForInjected(
    _input: SendInput,
    _opts?: { timeoutMs?: number },
  ): Promise<{ eventId: string; targets: string[] }> {
    throw notImplemented()
  }

  onDeliveryFailed(_listener: (info: { to: string; msgId?: string; reason?: string }) => void): () => void {
    throw notImplemented()
  }

  onAgentExit(_listener: (name: string, reason?: string) => void): () => void {
    throw notImplemented()
  }
}
