#!/usr/bin/env node

import { lstat, readdir, readFile, readlink } from 'node:fs/promises'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

export async function comparePackageTrees(leftRoot, rightRoot) {
  const differences = []
  await compareEntry(leftRoot, rightRoot, '.', differences)
  return differences
}

async function compareEntry(leftPath, rightPath, relativePath, differences) {
  const [left, right] = await Promise.all([
    lstat(leftPath).catch(() => undefined),
    lstat(rightPath).catch(() => undefined),
  ])
  if (!left || !right) {
    differences.push(`${relativePath}: missing from ${left ? 'right' : 'left'} tree`)
    return
  }

  const leftType = fileType(left)
  const rightType = fileType(right)
  if (leftType !== rightType) {
    differences.push(`${relativePath}: type ${leftType} != ${rightType}`)
    return
  }

  const leftMode = left.mode & 0o777
  const rightMode = right.mode & 0o777
  if (leftMode !== rightMode) {
    differences.push(
      `${relativePath}: mode ${leftMode.toString(8)} != ${rightMode.toString(8)}`,
    )
  }

  if (left.isDirectory()) {
    const [leftNames, rightNames] = await Promise.all([readdir(leftPath), readdir(rightPath)])
    const names = [...new Set([...leftNames, ...rightNames])].sort()
    for (const name of names) {
      await compareEntry(
        join(leftPath, name),
        join(rightPath, name),
        relativePath === '.' ? name : `${relativePath}/${name}`,
        differences,
      )
    }
  } else if (left.isSymbolicLink()) {
    const [leftTarget, rightTarget] = await Promise.all([readlink(leftPath), readlink(rightPath)])
    if (leftTarget !== rightTarget) {
      differences.push(`${relativePath}: symlink ${leftTarget} != ${rightTarget}`)
    }
  } else if (left.isFile()) {
    const [leftBytes, rightBytes] = await Promise.all([readFile(leftPath), readFile(rightPath)])
    if (!leftBytes.equals(rightBytes)) differences.push(`${relativePath}: content differs`)
  }
}

function fileType(stat) {
  if (stat.isDirectory()) return 'directory'
  if (stat.isFile()) return 'file'
  if (stat.isSymbolicLink()) return 'symlink'
  return 'other'
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [, , leftRoot, rightRoot] = process.argv
  if (!leftRoot || !rightRoot) {
    console.error('usage: compare-package-trees.mjs LEFT_TREE RIGHT_TREE')
    process.exitCode = 2
  } else {
    const differences = await comparePackageTrees(leftRoot, rightRoot)
    if (differences.length > 0) {
      for (const difference of differences) console.error(difference)
      process.exitCode = 1
    }
  }
}
