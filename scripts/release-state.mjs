#!/usr/bin/env node

import { pathToFileURL } from 'node:url'

function parseBoolean(value, name) {
  if (value === 'true') return true
  if (value === 'false') return false
  throw new Error(`${name} must be true or false`)
}

export function planReleaseState({
  published,
  tagTarget,
  head,
  tagPayloadMatches,
  registryPayloadMatches,
}) {
  if (!head) throw new Error('head is required')

  if (published && !registryPayloadMatches) {
    throw new Error('published package payload or provenance does not match this checkout')
  }

  if (!tagTarget) {
    return {
      state: published ? 'recover-missing-tag' : 'new-release',
      createTag: true,
      publish: !published,
    }
  }

  if (tagTarget === head) {
    return {
      state: published ? 'complete' : 'resume-after-tag',
      createTag: false,
      publish: !published,
    }
  }

  if (published && tagPayloadMatches) {
    return {
      state: 'complete-equivalent-legacy-tag',
      createTag: false,
      publish: false,
    }
  }

  throw new Error(`release tag points to ${tagTarget}, not ${head}`)
}

function readArgs(argv) {
  const values = new Map()
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index]
    const value = argv[index + 1]
    if (!key?.startsWith('--') || value === undefined) {
      throw new Error(`invalid argument near ${key ?? '<end>'}`)
    }
    values.set(key.slice(2), value)
  }
  return {
    published: parseBoolean(values.get('published'), 'published'),
    tagTarget: values.get('tag-target') ?? '',
    head: values.get('head') ?? '',
    tagPayloadMatches: parseBoolean(values.get('tag-payload-matches'), 'tag-payload-matches'),
    registryPayloadMatches: parseBoolean(
      values.get('registry-payload-matches'),
      'registry-payload-matches',
    ),
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    console.log(JSON.stringify(planReleaseState(readArgs(process.argv.slice(2)))))
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
  }
}
