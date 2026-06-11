import type {
  ChangeEvent as RelayFileChangeEvent,
  SubscribeOptions as RelayFileSubscribeOptions,
  Subscription as RelayFileSubscription,
} from '@relayfile/sdk'

export type ChangeEvent = RelayFileChangeEvent
export type SubscribeOptions = RelayFileSubscribeOptions
export type Subscription = RelayFileSubscription

export interface EventPage {
  events: ChangeEvent[]
  nextCursor?: string | null
}

export interface MountClient {
  readFile(path: string): Promise<{ content: unknown; revision?: string }>
  writeFile(path: string, content: unknown): Promise<void>
  listTree(prefix: string): Promise<string[]>
  subscribe(globs: string[], onChange: (event: ChangeEvent) => void, opts?: SubscribeOptions): Subscription
  getEvents(opts: { cursor?: string; limit?: number }): Promise<EventPage>
  confirmWrite(path: string, opts?: { timeoutMs?: number }): Promise<'acked' | 'pending' | 'failed' | 'timeout'>
  ensureSubRoot(prefix: string, opts?: { timeoutMs?: number }): Promise<'ready' | 'absent'>
}
