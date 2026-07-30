export const CODERABBIT_REVIEW_REQUEST = '@coderabbitai review'
export const FACTORY_CODERABBIT_REVIEW_MARKER = '<!-- factory-coderabbit-review-request -->'
export const FACTORY_CODERABBIT_REVIEW_BODY =
  `${CODERABBIT_REVIEW_REQUEST}\n${FACTORY_CODERABBIT_REVIEW_MARKER}`

export const factoryCoderabbitReviewCorrelationId = (repo: string, number: number): string =>
  `factory:coderabbit-review:${repo.toLowerCase()}#${number}`

export const containsCoderabbitReviewRequest = (value: string): boolean =>
  value.includes(CODERABBIT_REVIEW_REQUEST) && value.includes(FACTORY_CODERABBIT_REVIEW_MARKER)

export const isFactoryGithubWritebackPath = (path: string): boolean =>
  /^\/github\/repos\/[^/]+\/[^/]+\/(?:pull-requests\/factory-[^/]+\.json|refs\/(?:factory\.json|refs%2Fheads%2Ffactory%2F[^/]+\.json)|pulls\/[1-9]\d*\/(?:close\.json|comments\/factory-coderabbit-review\.json))$/iu.test(path)

export const isAllowedFactoryGithubWritebackDraft = (path: string, content: unknown): boolean => {
  if (!isFactoryGithubWritebackPath(path)) return false
  if (!/\/pulls\/[1-9]\d*\/comments\/factory-coderabbit-review\.json$/iu.test(path)) return true
  if (content === null || typeof content !== 'object' || Array.isArray(content)) return false
  const record = content as Record<string, unknown>
  return Object.keys(record).length === 1 && record.body === FACTORY_CODERABBIT_REVIEW_BODY
}
