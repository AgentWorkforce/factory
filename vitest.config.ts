import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/**/*.test.ts', '.agentworkforce/agents/**/*.test.ts'],
    exclude: ['node_modules/**', 'dist/**', 'out/**'],
  },
})
