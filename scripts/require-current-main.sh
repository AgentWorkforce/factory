#!/usr/bin/env bash
set -euo pipefail

if [ "${GITHUB_REF:-}" != "refs/heads/main" ]; then
  echo "::error::Live releases must be dispatched from main, not ${GITHUB_REF:-<unset>}" >&2
  exit 1
fi

if [ "${1:-}" = "--ref-only" ]; then
  exit 0
fi

if [ "$#" -ne 0 ]; then
  echo "usage: $0 [--ref-only]" >&2
  exit 2
fi

EXPECTED_SHA=${GITHUB_SHA:?GITHUB_SHA is required}
REMOTE_MAIN=$(git ls-remote origin refs/heads/main | awk '{print $1}')
if [ -z "$REMOTE_MAIN" ] || [ "$REMOTE_MAIN" != "$EXPECTED_SHA" ]; then
  echo "::error::Checked-out SHA $EXPECTED_SHA is not current origin/main ($REMOTE_MAIN)" >&2
  exit 1
fi
