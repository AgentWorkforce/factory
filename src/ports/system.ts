export interface Logger {
  debug?(message: string, ...args: unknown[]): void
  info?(message: string, ...args: unknown[]): void
  warn?(message: string, ...args: unknown[]): void
  error?(message: string, ...args: unknown[]): void
}

export interface TelemetrySink {
  increment?(name: string, value?: number, metadata?: Record<string, unknown>): void
  gauge?(name: string, value: number, metadata?: Record<string, unknown>): void
}

export interface Clock {
  now(): number
  sleep(ms: number): Promise<void>
}
