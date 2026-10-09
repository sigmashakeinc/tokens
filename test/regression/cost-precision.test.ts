// Regression: cost math precision for 7-figure token counts.
// Past bug: integer overflow when input_tokens > 2^31 forced through Number.

import { expect, test } from "bun:test";
import { costFor } from "../../src/pricing.ts";

test("cost math handles 10M-token sessions without precision loss", () => {
  const c = costFor("claude-sonnet-4-6", {
    inputTokens: 10_000_000,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  });
  // Sonnet input is $3/MTok → $30 for 10M tokens.
  expect(c).toBeCloseTo(30);
});

test("cost math handles fractional output without floating-point drift", () => {
  const c = costFor("claude-haiku-4-5-20251001", {
    inputTokens: 0,
    outputTokens: 1_000_001,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  });
  // Haiku output is $5/MTok → ~$5.000005 for 1M+1 tokens.
  expect(c).toBeGreaterThan(5);
  expect(c).toBeLessThan(5.001);
});
