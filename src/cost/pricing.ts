export interface ModelPricing {
  costPer1MInput: number
  costPer1MOutput: number
}

/**
 * Factory's metered model catalog. Keep the shape aligned with NightCTO's
 * openrouter-client model ladder so model-routing work can share prices later.
 */
export const MODEL_PRICING: Readonly<Record<string, ModelPricing>> = Object.freeze({
  'deepseek/deepseek-chat-v3-0324': {
    costPer1MInput: 0.27,
    costPer1MOutput: 1.1,
  },
  'openai/gpt-4o-mini': {
    costPer1MInput: 0.15,
    costPer1MOutput: 0.6,
  },
  'anthropic/claude-haiku-4.5': {
    costPer1MInput: 1,
    costPer1MOutput: 5,
  },
  'anthropic/claude-sonnet-4.5': {
    costPer1MInput: 3,
    costPer1MOutput: 15,
  },
  'openai/gpt-5.4': {
    costPer1MInput: 5,
    costPer1MOutput: 15,
  },
  'openai/gpt-5.4-pro': {
    costPer1MInput: 15,
    costPer1MOutput: 60,
  },
})

export interface EstimateCostUsdOptions {
  /** A bounded accounting notice; callback failures are deliberately ignored. */
  onUnpricedModel?: (model: string) => void
}

/** Estimate a metered model cost without ever making accounting control flow fatal. */
export function estimateCostUsd(
  model: string,
  inputTokens: number,
  outputTokens: number,
  options: EstimateCostUsdOptions = {},
): number | null {
  const pricing = MODEL_PRICING[model]
  if (!pricing) {
    try {
      options.onUnpricedModel?.(model)
    } catch {
      // Observability and accounting callbacks must never affect a run.
    }
    return null
  }
  if (!validTokenCount(inputTokens) || !validTokenCount(outputTokens)) return null

  const inputCost = (inputTokens / 1_000_000) * pricing.costPer1MInput
  const outputCost = (outputTokens / 1_000_000) * pricing.costPer1MOutput
  return roundUsd(inputCost + outputCost)
}

export const roundUsd = (value: number): number => Number(value.toFixed(6))

const validTokenCount = (value: number): boolean =>
  Number.isSafeInteger(value) && value >= 0
