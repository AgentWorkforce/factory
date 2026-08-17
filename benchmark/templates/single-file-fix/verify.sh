#!/usr/bin/env bash
# TEMPLATE verify script. Run from the repo root, on the checked-out PR branch.
# Exit 0 = task passed; any other exit code = failed. Keep this narrow and
# deterministic — it is the ground truth the runner scores against, not a
# smoke test the implementer agent can talk its way around.
set -euo pipefail

npm test -- format.test.ts
