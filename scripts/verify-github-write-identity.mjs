#!/usr/bin/env node
// Proves, against live GitHub, that a babysitter-shaped comment written through
// the Relayfile GitHub connection is authored by Factory's GitHub App and not
// by the user logged into the local `gh` CLI.
//
// This is deliberately not a unit test. A fake mount can only prove Factory
// asks for app authorship; only a real write can prove GitHub recorded it.
//
//   node scripts/verify-github-write-identity.mjs --repo OWNER/NAME --issue N
//
// Exit codes (the result is the exit code, never the log text):
//   0  the comment exists on GitHub and its author is an App/Bot
//   1  the comment was authored by a human, or by the local `gh` user
//   2  the write never completed, or the author could not be read back
//   3  bad usage / no GitHub connection on the workspace
//   4  the connection reached GitHub as an app, and GitHub refused the write
//      because that installation lacks permission on this repository. The
//      identity is right and the grant is missing — an operator fix, not a
//      code fix, and deliberately NOT reported as a pass.
import { RelayfileCloudMountClient } from '../dist/mount/relayfile-cloud-mount-client.js'
import { readGithubCommentAuthor } from '../dist/mount/relayfile-github-connection-write.js'

const args = new Map()
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index], process.argv[index + 1])
}
const repo = args.get('--repo')
const issueNumber = Number(args.get('--issue'))
const workspaceId = args.get('--workspace') ?? process.env.FACTORY_WORKSPACE_ID

if (!repo?.includes('/') || !Number.isSafeInteger(issueNumber) || issueNumber <= 0) {
  console.error('usage: verify-github-write-identity.mjs --repo OWNER/NAME --issue N [--workspace ID]')
  process.exit(3)
}

const fail = (code, message) => {
  console.error(`FAIL(${code}): ${message}`)
  process.exit(code)
}

// The guarded-draft predicate mirrors the CLI's GitHub writeback allowlist.
const isAllowedDraft = (path, _content, opts) =>
  opts?.guarded === true &&
  /^\/github\/repos\/[^/]+\/[^/]+\/issues\/[1-9]\d*\/comments\/factory-[^/]+\.json$/u.test(path)

let mount
try {
  mount = await RelayfileCloudMountClient.fromConfig({ workspaceId, isAllowedDraft })
} catch (error) {
  fail(3, `could not open the Relayfile workspace mount: ${error.message}`)
}

if (!mount.githubWrite?.postIssueComment) {
  fail(3, 'this workspace has no GitHub connection that can author issue comments')
}

const marker = `factory-identity-probe-${process.pid}-${Math.random().toString(36).slice(2, 10)}`
const body = [
  `<!-- ${marker} -->`,
  'Automated identity check for issue #221: this comment was written through the',
  'Relayfile GitHub connection. If GitHub attributes it to the Factory app rather',
  'than to a person, the babysitter write path carries the intended identity.',
].join('\n')

let receipt
try {
  receipt = await mount.githubWrite.postIssueComment({ repo, number: issueNumber, body })
} catch (error) {
  await mount.dispose?.()
  // GitHub returns this only to a GitHub App installation token, so it is
  // itself evidence about identity: the write was NOT attempted as the local
  // `gh` user. It is a missing grant, reported as its own exit code rather
  // than blurred into a generic failure.
  if (/not accessible by integration/iu.test(error.message)) {
    fail(4, `GitHub refused the app-authored write on ${repo}: ${error.message}. `
      + 'The workspace GitHub App installation needs issues:write (and pull_requests:write) on this repository.')
  }
  fail(2, `the connection write did not complete: ${error.message}`)
}

if (!receipt?.commentId) {
  await mount.dispose?.()
  fail(2, 'the write acknowledgement carried no provider comment id, so no author can be read back')
}

// Independently read the provider-reconciled author through the SDK mount. A
// write acknowledgement only proves the operation completed; this projection
// proves which actor GitHub recorded without introducing a `gh` dependency.
let recordedAuthor
for (let attempt = 0; attempt < 30 && !recordedAuthor; attempt += 1) {
  recordedAuthor = await readGithubCommentAuthor(mount, {
    repo,
    issueNumber,
    commentId: receipt.commentId,
  })
  if (!recordedAuthor) await new Promise((resolve) => setTimeout(resolve, 1_000))
}

await mount.dispose?.()

if (!recordedAuthor) {
  fail(2, 'the provider comment was acknowledged but its author did not reconcile into the SDK mount')
}

const login = recordedAuthor.login ?? '(none)'
const type = recordedAuthor.type ?? '(none)'
const url = `https://github.com/${repo}/issues/${issueNumber}#issuecomment-${receipt.commentId}`
console.log(JSON.stringify({
  repo,
  issue: issueNumber,
  commentId: receipt.commentId,
  url,
  requestedAuthor: receipt.author,
  recordedAuthor: { login, type },
}, null, 2))

if (type !== 'Bot' && !login.endsWith('[bot]')) {
  fail(1, `GitHub recorded the comment author as ${login} (type ${type}), which is not the Factory GitHub App`)
}

console.log(`OK: ${url} is authored by ${login} (${type}), not by the process operator`)
