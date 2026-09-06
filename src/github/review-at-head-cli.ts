/**
 * Runnable entry for the review-at-head CI check (factory#432 part c).
 * Kept separate from `review-at-head-check.ts` so that module stays free of
 * process side effects and can be exercised directly from tests.
 */

import { main } from './review-at-head-check'

process.exitCode = await main(process.argv.slice(2))
