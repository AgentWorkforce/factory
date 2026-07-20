import type { LocalMountHealthEvent } from '../mount/relayfile-cloud-mount-client'
import type { MountClient } from '../ports'

export const FACTORY_MOUNT_HEALTH_PATH = '/factory/observability/mount-health/current.json'

export interface FactoryMountHealthRecord {
  schemaVersion: 'factory.mount-health.v1'
  type: 'factory.mount-health'
  workspaceId: string
  state: LocalMountHealthEvent['state']
  reason: LocalMountHealthEvent['reason']
  degradedMounts: number
  occurredAt: string
}

/** Publish the bounded, non-secret mount health contract through Relayfile. */
export async function publishFactoryMountHealth(
  mount: MountClient,
  workspaceId: string,
  event: LocalMountHealthEvent,
  occurredAt = new Date().toISOString(),
): Promise<void> {
  const record: FactoryMountHealthRecord = {
    schemaVersion: 'factory.mount-health.v1',
    type: 'factory.mount-health',
    workspaceId,
    state: event.state,
    reason: event.reason,
    degradedMounts: event.degradedMounts,
    occurredAt,
  }
  await mount.writeFile(FACTORY_MOUNT_HEALTH_PATH, record)
}
