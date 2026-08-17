#!/usr/bin/env bash
# TEMPLATE verify script. Run from the repo root, on the checked-out PR branch.
set -euo pipefail

npm test -- export-flow.e2e.test.ts
