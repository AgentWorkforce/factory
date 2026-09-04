import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: [
      'src/**/*.test.ts',
      'benchmark/**/*.test.ts',
      '.agentworkforce/agents/**/*.test.ts',
      'test/e2e/ask-a-teammate.test.ts',
      'test/e2e/dispatch-identity-real-broker.test.ts',
      'test/e2e/run-cost-accounting.test.ts',
    ],
    exclude: ['node_modules/**', 'dist/**', 'out/**'],
    // MITIGATION, not a fix (#442).
    //
    // The registration race that made `fleet.test.ts`'s relay-dispatch-ownership
    // test fail is fixed at its source, in the fake. This is for what is left:
    // these CLI tests each drive a whole dispatch through real fs I/O and take
    // 2-10s of genuine in-process work, so at Vitest's 5s default a loaded
    // runner fails a rotating cast of them. Measured here on 8 cores at load
    // average ~155, one run of `src/cli/fleet.test.ts` failed 10 different
    // tests, every one between 4.3s and 10.4s -- which is exactly the
    // "each rerun fails a different test" signature reported on #442.
    //
    // 20s is ~2x the slowest run observed under that load and 4x the default,
    // while staying under REMOTE_AGENT_REGISTRATION_TIMEOUT_MS (30s) so a
    // regression of the race above still fails fast instead of being absorbed.
    // It buys headroom; it does not make any test cheaper. The real cure is to
    // stop these tests doing seconds of real I/O apiece.
    testTimeout: 20_000,
  },
})
