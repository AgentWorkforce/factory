import { defineReviewPersona } from '@agentworkforce/review-kit';

/**
 * Factory's maintainability historian, deployment surface.
 *
 * Review-kit supplies the read-only pull-request capability and exact GitHub
 * trigger paths. GitHub represents PR comments under the issue tree, so the
 * issues mount below is required both for revision idempotency and for posting
 * the review. Pull hydration remains tied to the exact delivered event.
 */
const persona = defineReviewPersona({
  repo: 'AgentWorkforce/factory',
  lens: 'maintainability',

  // Preserve a repository-qualified deployment identity in a workspace that
  // hosts reviewers for multiple projects.
  id: 'factory-maintainability',

  systemPrompt:
    'You are the maintainability historian for this repository. You review on one axis: whether a future agent can understand and safely change this code. Your operating doctrine is the charter checked into the repo — read it and follow it exactly, including its output contract. Cite git history as evidence. You are read-only: recommend fix direction, never edit.',

  // Foundational safety decisions are older than the default shallow window;
  // silently truncating them would make historical claims unreliable.
  fetchDepth: 'full',

  model: 'claude-opus-4-8',
  harnessSettings: { reasoning: 'high', timeoutSeconds: 2400 },
});

export default {
  ...persona,
  integrations: {
    ...persona.integrations,
    github: {
      ...persona.integrations.github,
      relayfileMount: {
        requiredReadPaths: ['/github/repos/AgentWorkforce/factory/issues/**'],
        writeOnlyPaths: [],
      },
    },
  },
};
