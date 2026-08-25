import { describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

/**
 * A whole-file cross-check for one class of defect: a `Map`/`Set` field on
 * `FactoryLoop` written under one key function and read under another.
 *
 * This is the scan that found #367. `#329` rekeyed the *readers* of
 * `#abandonedDispatchReasons` to the provider-neutral `dispatchLifecycleKey()`
 * and left the only writer on `issueKey()`. Neither side is wrong on its own,
 * both compile, and the two key spaces can never collide by construction —
 * `issueKey` is `<key>:<uuid>:<path>`, `dispatchLifecycleKey` is
 * `github:<owner>/<repo>#<n>` | `linear:<uuid>` | `issue:<uuid>` — so the write
 * was simply invisible to every read. Nothing in the type system, the linter or
 * a per-call-site review catches that; only comparing the whole field's
 * accessors does.
 *
 * COVERAGE, stated plainly so a green run is not read as more than it is. Only
 * arguments this scan can resolve are classified: a direct `keyFn(...)` call, a
 * `const` bound to one inside the same function, or a template literal whose
 * first interpolation is one (the composite `<key>:<dry-run>:<phase>` shape
 * `dispatch()` uses, which is deliberately a *different* key space from the
 * bare key). A key arriving as a function parameter is not resolved and is
 * reported as unresolved rather than assumed compatible — those sites still
 * need review by hand. `analysedFieldFloor` below is the must-not-fire: a scan
 * that stopped resolving anything would otherwise report zero mismatches and
 * pass silently.
 */

const KEY_FNS = new Set([
  'issueKey',
  'dispatchLifecycleKey',
  'dispatchIssueIdentity',
  'issueStateKey',
  'trackerKey',
])
const ACCESSORS = new Set(['get', 'set', 'has', 'delete'])

// Fields resolved on the current file. A refactor may legitimately move this
// number; a collapse towards zero means the scan stopped seeing its subject.
const ANALYSED_FIELD_FLOOR = 25

type KeyUse = { keyFn: string; line: number }

const analyse = (filePath: string): {
  fields: Map<string, Map<string, number[]>>
  unresolved: string[]
} => {
  const source = ts.createSourceFile(
    filePath,
    readFileSync(filePath, 'utf8'),
    ts.ScriptTarget.ESNext,
    true,
  )
  const lineOf = (node: ts.Node): number =>
    source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1

  const fields = new Map<string, Map<string, number[]>>()
  const unresolved: string[] = []

  const classify = (expr: ts.Expression | undefined, scopes: Array<Map<string, string>>): string | undefined => {
    if (!expr) return undefined
    if (ts.isCallExpression(expr) && ts.isIdentifier(expr.expression) && KEY_FNS.has(expr.expression.text)) {
      return expr.expression.text
    }
    if (ts.isIdentifier(expr)) {
      for (let i = scopes.length - 1; i >= 0; i--) {
        const bound = scopes[i]!.get(expr.text)
        if (bound) return bound
      }
      return undefined
    }
    // `${keyFn(issue)}:...` is its own key space, not the bare key. Reporting
    // it as distinct is exactly what surfaced #367's second field.
    if (ts.isTemplateExpression(expr) && expr.head.text === '' && expr.templateSpans.length > 0) {
      const first = classify(expr.templateSpans[0]!.expression, scopes)
      if (first) return `${first}+composite`
    }
    return undefined
  }

  const record = (field: string, use: KeyUse): void => {
    const byKeyFn = fields.get(field) ?? new Map<string, number[]>()
    fields.set(field, byKeyFn)
    const lines = byKeyFn.get(use.keyFn) ?? []
    byKeyFn.set(use.keyFn, lines)
    lines.push(use.line)
  }

  const walk = (node: ts.Node, scopes: Array<Map<string, string>>): void => {
    let scoped = scopes
    if (ts.isFunctionLike(node)) scoped = [...scopes, new Map<string, string>()]

    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name)) {
      const keyFn = classify(node.initializer, scoped)
      if (keyFn) scoped[scoped.length - 1]!.set(node.name.text, keyFn)
    }

    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      ACCESSORS.has(node.expression.name.text) &&
      ts.isPropertyAccessExpression(node.expression.expression) &&
      node.expression.expression.expression.kind === ts.SyntaxKind.ThisKeyword &&
      ts.isPrivateIdentifier(node.expression.expression.name)
    ) {
      const field = node.expression.expression.name.text
      const arg = node.arguments[0]
      const keyFn = classify(arg, scoped)
      if (keyFn) record(field, { keyFn, line: lineOf(node) })
      else if (arg) unresolved.push(`${field} @${lineOf(node)}`)
    }

    ts.forEachChild(node, (child) => walk(child, scoped))
  }

  walk(source, [new Map<string, string>()])
  return { fields, unresolved }
}

describe('FactoryLoop keyed-field cross-check (#367)', () => {
  const factoryPath = fileURLToPath(new URL('./factory.ts', import.meta.url))
  const { fields, unresolved } = analyse(factoryPath)

  it('keys every resolvable field of FactoryLoop under exactly one key function', () => {
    const mismatched = [...fields]
      .filter(([, byKeyFn]) => byKeyFn.size > 1)
      .map(([field, byKeyFn]) => ({
        field,
        keys: Object.fromEntries([...byKeyFn].map(([keyFn, lines]) => [keyFn, lines.sort((a, b) => a - b)])),
      }))
      .sort((a, b) => a.field.localeCompare(b.field))

    expect(mismatched).toEqual([])
  })

  // MUST-NOT-FIRE for the check above. Without this, deleting `KEY_FNS` — or
  // any change that makes `classify` stop resolving — turns the assertion into
  // a vacuous pass while the invariant it guards goes unenforced.
  it('still resolves the fields it is meant to be checking', () => {
    expect(fields.size).toBeGreaterThanOrEqual(ANALYSED_FIELD_FLOOR)
    expect([...fields.keys()]).toContain('#abandonedDispatchReasons')
    expect([...fields.keys()]).toContain('#dispatchInFlight')
    // The unresolved sites are a known blind spot, not a silent one: keys that
    // arrive as parameters are reviewed by hand. Asserting the bucket exists
    // keeps that limitation visible in the test output rather than implied.
    expect(unresolved.length).toBeGreaterThan(0)
  })
})
