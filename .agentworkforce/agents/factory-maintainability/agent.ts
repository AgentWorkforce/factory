import { defineReviewAgent, gitHistory, prDiff } from '@agentworkforce/review-kit';

/**
 * Factory's maintainability historian, cloud surface.
 *
 * Review-kit owns the shared PR plumbing: event parsing, repository scoping,
 * draft and skip-label gates, checkout execution, output normalization, and
 * once-per-revision delivery. This file declares only the repository-specific
 * review doctrine and evidence.
 *
 * The doctrine lives in the checked-out repository rather than this handler.
 * That keeps the cloud reviewer aligned with Factory's versioned critical-path
 * catalog and lets a charter change take effect without duplicating it here.
 */
export default defineReviewAgent({
  repo: 'AgentWorkforce/factory',
  charter: '.agentworkforce/workforce/personas/maintainability.md',
  lens: 'maintainability',

  // A historian needs both the proposed change and the decisions that produced
  // the current design. The persona therefore requests the complete history.
  evidence: [prDiff(), gitHistory()],
});
