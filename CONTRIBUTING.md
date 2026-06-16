# Contributing

Work in small, reviewable changes and keep the published package shape stable.

Before opening a PR, run:

```bash
npm ci
npm run build
npm test
npm pack --dry-run
```

The package is published publicly as `@agent-relay/factory`. Release publishing
is handled by `.github/workflows/publish.yml`; do not publish from automation
outside that workflow.
