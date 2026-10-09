import { describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "..", "..");
const SRC = join(ROOT, "src");

// Patterns that catch hardcoded credentials but allow code that *handles* them
// (e.g., a regex literal that parses PEM-format keys is not a hardcoded key).
const SECRET_PATTERNS = [
  /api[_-]?key\s*=\s*["'][a-zA-Z0-9_-]{16,}["']/i,
  /password\s*=\s*["'][^"']{8,}["']/i,
  // Match a full PEM key block (header + body + footer), not just a header
  // mention. Code that *parses* PEM has only the header string; code that
  // *embeds* a key has the full multi-line block.
  /-----BEGIN (RSA |EC )?PRIVATE KEY-----\s*\n[A-Za-z0-9+/=\s]{64,}\n-----END/,
];

function collectTs(dir: string): string[] {
  const out: string[] = [];
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return out;
  }
  for (const entry of entries) {
    if (entry === "node_modules" || entry.startsWith(".")) continue;
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...collectTs(full));
    else if ((full.endsWith(".ts") || full.endsWith(".tsx")) && !full.endsWith(".test.ts"))
      out.push(full);
  }
  return out;
}

describe("sigmashake-tokens sast", () => {
  const files = existsSync(SRC) ? collectTs(SRC) : [];
  test("no hardcoded credentials in src/", () => {
    const violations: string[] = [];
    for (const f of files) {
      const c = readFileSync(f, "utf8");
      for (const p of SECRET_PATTERNS) if (p.test(c)) violations.push(`${f}: ${p}`);
    }
    expect(violations).toEqual([]);
  });
  test("no AWS access key strings", () => {
    const v: string[] = [];
    for (const f of files) {
      const c = readFileSync(f, "utf8");
      if (/\bAKIA[0-9A-Z]{16}\b/.test(c)) v.push(f);
    }
    expect(v).toEqual([]);
  });
});
