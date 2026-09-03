/**
 * Module Syntax Guard.
 *
 * Dynamically imports every .js module under docs/js/ and fails if any
 * module throws a SyntaxError. This catches parse-time regressions (e.g. an
 * edit that deletes a `catch` clause and leaves a dangling `try`) that no
 * other test would notice, since most tests only import a handful of
 * specific modules.
 *
 * Only SyntaxError is treated as a failure here. Some docs/js modules touch
 * browser globals (window, document, fetch, etc.) at import time and will
 * throw a ReferenceError or TypeError when imported under plain Node — that
 * is a runtime environment difference, not a parse failure, and is not what
 * this guard is checking for. We narrow the assertion to SyntaxError so the
 * test stays meaningful without requiring a full browser/DOM shim for every
 * module in the tree.
 *
 * Runs via built-in `node --test` with zero dependencies.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const DOCS_JS_ROOT = join(__dirname, '..', 'docs', 'js');

function findJsFiles(dir) {
  const results = [];
  for (const entry of readdirSync(dir)) {
    const fullPath = join(dir, entry);
    const stats = statSync(fullPath);
    if (stats.isDirectory()) {
      results.push(...findJsFiles(fullPath));
    } else if (entry.endsWith('.js')) {
      results.push(fullPath);
    }
  }
  return results;
}

test('Module syntax guard: every docs/js module parses without SyntaxError', async () => {
  const jsFiles = findJsFiles(DOCS_JS_ROOT);
  assert.ok(jsFiles.length > 0, 'Expected to find .js files under docs/js/');

  const syntaxFailures = [];

  for (const file of jsFiles) {
    try {
      await import(pathToFileURL(file).href);
    } catch (err) {
      if (err instanceof SyntaxError || err.name === 'SyntaxError') {
        syntaxFailures.push(`${file}: ${err.message}`);
      }
      // Non-SyntaxError failures (missing browser globals, etc.) are
      // acceptable — this guard only checks that modules parse.
    }
  }

  assert.deepStrictEqual(
    syntaxFailures,
    [],
    `SyntaxError(s) found while importing docs/js modules:\n${syntaxFailures.join('\n')}`
  );
});
