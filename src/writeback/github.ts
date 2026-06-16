import type { MountClient } from '../ports'
import type { PrSummary } from '../types'
import { asRecord, wrappedPayload } from './shared'

const repoDir = (repo: string): string => {
  if (repo.includes('__')) {
    return repo
  }

  const [owner, name] = repo.split('/')
  if (!owner || !name) {
    throw new Error(`GitHub repo must be owner/repo or owner__repo: ${repo}`)
  }

  return `${owner}__${name}`
}

const prPath = (repo: string, number: number): string =>
  `/github/repos/${repoDir(repo)}/pulls/by-id/${number}.json`

export const MountGithubRead = (mount: MountClient) => ({
  async getPr(repo: string, number: number): Promise<PrSummary> {
    const { content } = await mount.readFile(prPath(repo, number))
    const payload = wrappedPayload(content)

    return {
      repo,
      number: numberValue(payload.number) ?? number,
      title: typeof payload.title === 'string' ? payload.title : undefined,
      url: typeof payload.url === 'string' ? payload.url : undefined,
      state: typeof payload.state === 'string' ? payload.state : undefined,
      headRef: refName(payload.headRef) ?? refName(payload.head) ?? stringValue(payload.head_ref),
      baseRef: refName(payload.baseRef) ?? refName(payload.base) ?? stringValue(payload.base_ref),
      author: refName(payload.author) ?? stringValue(payload.user),
      filesChanged: filesChanged(payload.files_changed ?? payload.filesChanged ?? payload.files),
    }
  },
})

const stringValue = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined

const numberValue = (value: unknown): number | undefined =>
  typeof value === 'number' ? value : undefined

const refName = (value: unknown): string | undefined => {
  if (typeof value === 'string') {
    return value
  }
  const record = asRecord(value)
  return stringValue(record?.name) ?? stringValue(record?.ref) ?? stringValue(record?.login)
}

const filesChanged = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined
  }

  const files = value
    .map((entry) => typeof entry === 'string' ? entry : stringValue(asRecord(entry)?.path) ?? stringValue(asRecord(entry)?.filename))
    .filter((entry): entry is string => Boolean(entry))
  return files.length > 0 ? files : undefined
}
