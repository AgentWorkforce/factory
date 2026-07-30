import { describe, expect, it } from 'vitest'

import {
  containsCoderabbitReviewRequest,
  FACTORY_CODERABBIT_REVIEW_BODY,
  isAllowedFactoryGithubWritebackDraft,
  isFactoryGithubWritebackPath,
} from './review-request'

describe('isFactoryGithubWritebackPath', () => {
  it('allows only the deterministic factory review-request comment path', () => {
    expect(isFactoryGithubWritebackPath(
      '/github/repos/AgentWorkforce/factory/pulls/42/comments/factory-coderabbit-review.json',
    )).toBe(true)
    expect(isFactoryGithubWritebackPath(
      '/github/repos/AgentWorkforce/factory/pulls/42/comments/arbitrary.json',
    )).toBe(false)
  })

  it('allows only the fixed review-request body at the public comment path', () => {
    const path = '/github/repos/AgentWorkforce/factory/pulls/42/comments/factory-coderabbit-review.json'
    expect(isAllowedFactoryGithubWritebackDraft(path, {
      body: FACTORY_CODERABBIT_REVIEW_BODY,
    })).toBe(true)
    expect(isAllowedFactoryGithubWritebackDraft(path, {
      body: 'arbitrary public comment',
    })).toBe(false)
    expect(isAllowedFactoryGithubWritebackDraft(path, {
      body: FACTORY_CODERABBIT_REVIEW_BODY,
      title: 'unexpected extra field',
    })).toBe(false)
  })

  it('does not mistake a public marker-only comment for a review command', () => {
    expect(containsCoderabbitReviewRequest(
      '<!-- factory-coderabbit-review-request -->',
    )).toBe(false)
    expect(containsCoderabbitReviewRequest(
      FACTORY_CODERABBIT_REVIEW_BODY,
    )).toBe(true)
  })
})
