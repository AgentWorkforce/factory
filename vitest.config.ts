import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: [
      'src/**/*.test.ts',
      '.agentworkforce/agents/**/*.test.ts',
      'test/e2e/dispatch-identity-real-broker.test.ts',
      'test/e2e/run-cost-accounting.test.ts',
    ],
    exclude: ['node_modules/**', 'dist/**', 'out/**'],
  },
})
