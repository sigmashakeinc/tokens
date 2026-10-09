// The package ships as its own repository. No file may import a path that
// leaves the package directory, and the old shared/ contract path must stay gone.

import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";

const ROOT = resolve(import.meta.dir, "..", "..");
const SKIP_DIRS = new Set(["node_modules", ".git", "dist", "fixtures"]);

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    if (SKIP_DIRS.has(entry)) continue;
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...sourceFiles(full));
    else if (/\.(ts|tsx|js|mjs)$/.test(entry) && !full.includes(join("src", "public"))) {
      out.push(full);
    }
  }
  return out;
}

const SPECIFIER = /(?:from\s+|import\s*\(\s*|import\s+|require\s*\(\s*)["'](\.{1,2}\/[^"']*)["']/g;

describe("package is self-contained", () => {
  const files = sourceFiles(ROOT);

  test("it finds source files to check", () => {
    expect(files.length).toBeGreaterThan(20);
  });

  test("no relative import leaves the package directory", () => {
    const escapes: string[] = [];
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      for (const match of text.matchAll(SPECIFIER)) {
        const target = resolve(dirname(file), match[1]!);
        const rel = relative(ROOT, target);
        if (rel.startsWith("..")) escapes.push(`${relative(ROOT, file)} -> ${match[1]}`);
      }
    }
    expect(escapes).toEqual([]);
  });

  test("nothing refers to the monorepo shared/ directory", () => {
    const hits: string[] = [];
    for (const file of files) {
      if (file.endsWith("self-contained.test.ts")) continue;
      const text = readFileSync(file, "utf8");
      if (/["'`](\.\.\/)+shared\//.test(text)) hits.push(relative(ROOT, file));
    }
    expect(hits).toEqual([]);
  });

  test("the token usage contract lives in src/", () => {
    expect(statSync(join(ROOT, "src", "token-usage-contract.ts")).isFile()).toBe(true);
  });
});
