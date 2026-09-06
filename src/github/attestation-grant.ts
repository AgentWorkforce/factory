/**
 * Late attestation grants for a published pull request.
 *
 * This used to live on the local-`gh` publish path, which is the only place a
 * pull request was ever opened from. Publishing agent work is now App-only
 * (see `mount/relayfile-github-connection-write`), so the grant moved with it —
 * otherwise retiring the `gh` publisher would have silently stopped feeding the
 * attestation ledger, which is a worse outcome than the split audit trail it
 * was retired to fix.
 */

/**
 * Post a late attestation grant to the relay auth API so the session reference
 * rides through to the attestation ledger after the commit is pushed. The call
 * is a best-effort fire-and-forget: it requires RELAYAUTH_URL,
 * RELAY_ATTEST_API_KEY, and RELAY_ATTEST_AGENT_ID to be set in the agent
 * environment; when any of those are absent the function resolves immediately.
 * RELAY_ATTEST_SESSION_ID is optional — when set it threads the session
 * reference into the ledger entry so attestation records are linkable to the
 * Claude Code / Codex session that produced the commit.
 */
export async function postAttestationGrant(repo: string, sessionRef?: string): Promise<void> {
  const baseUrl = process.env.RELAYAUTH_URL
  const apiKey = process.env.RELAY_ATTEST_API_KEY
  const agentId = process.env.RELAY_ATTEST_AGENT_ID
  if (!baseUrl || !apiKey || !agentId) return

  const url = new URL('v1/attestations/grants', baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`).toString()
  await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
    },
    body: JSON.stringify({
      agentId,
      repo,
      late: true,
      ...(sessionRef ? { sessionRef } : {}),
    }),
    signal: AbortSignal.timeout(5000),
  })
}
