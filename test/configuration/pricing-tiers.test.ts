// Configuration: every published Anthropic model family resolves to a tier.

import { expect, test } from "bun:test";
import { costFor, costForRequest, priceFor, priceForWithSource } from "../../src/pricing.ts";

const emptyUsage = {
  inputTokens: 0,
  outputTokens: 0,
  cacheReadTokens: 0,
  cacheWrite5mTokens: 0,
  cacheWrite1hTokens: 0,
};

test("GPT-5.6 families resolve to exact standard-tier prices", () => {
  const expected = {
    "gpt-5.6-sol": [5, 0.5, 6.25, 30],
    "gpt-5.6-terra": [2.5, 0.25, 3.125, 15],
    "gpt-5.6-luna": [1, 0.1, 1.25, 6],
  } as const;
  for (const [model, [input, cacheRead, cacheWrite, output]] of Object.entries(expected)) {
    const priced = priceForWithSource(model);
    expect(priced.source).toBe("table");
    expect(priced.price.input).toBe(input);
    expect(priced.price.cacheRead).toBe(cacheRead);
    expect(priced.price.cacheWrite5m).toBe(cacheWrite);
    expect(priced.price.cacheWrite1h).toBe(cacheWrite);
    expect(priced.price.output).toBe(output);
  }
});

test("GPT-5.6 dated snapshots and preview suffixes resolve to their family", () => {
  expect(priceForWithSource("gpt-5.6-sol-2026-07-01").price.input).toBe(5);
  expect(priceForWithSource("gpt-5.6-luna-preview").price.output).toBe(6);
});

test("GPT-5.6 family matching rejects unrestricted substring collisions", () => {
  for (const model of ["gpt-5.6-solar", "gpt-5.6-nova", "gpt-5.7"]) {
    expect(priceForWithSource(model).source).toBe("fallback");
  }
  expect(priceForWithSource("gpt-5.6-solar").price.input).not.toBe(5);
});

test("GPT-5.6 request pricing is explicit about the long-context threshold", () => {
  const mixedUsage = {
    inputTokens: 1_000_000,
    cacheReadTokens: 1_000_000,
    cacheWrite5mTokens: 1_000_000,
    cacheWrite1hTokens: 0,
    outputTokens: 1_000_000,
  };
  expect(costForRequest("gpt-5.6-terra", mixedUsage, 272_000)).toEqual({
    cost: 20.875,
    longContext: "base-price",
  });
  expect(costForRequest("gpt-5.6-terra", mixedUsage, 272_001)).toEqual({
    cost: 34.25,
    longContext: "multiplied",
  });
  expect(costForRequest("gpt-5.6-terra", mixedUsage)).toEqual({
    cost: 20.875,
    longContext: "adjustment-required",
  });
  expect(costForRequest("gpt-5.6-sol", mixedUsage, 272_001)).toEqual({
    cost: 68.5,
    longContext: "multiplied",
  });
  expect(
    costForRequest("gpt-5.6-sol", { ...emptyUsage, inputTokens: 1_000_000 }, 272_001).cost,
  ).toBe(10);
  expect(
    costForRequest("gpt-5.6-sol", { ...emptyUsage, outputTokens: 1_000_000 }, 272_001).cost,
  ).toBe(45);
});

test("Grok models are unpriced: cost 0 and source unavailable", () => {
  const usage = {
    inputTokens: 800,
    outputTokens: 50,
    cacheReadTokens: 200,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  };
  expect(priceForWithSource("grok-4.6").source).toBe("unavailable");
  expect(priceForWithSource("grok-4.5").price).toEqual({
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite5m: 0,
    cacheWrite1h: 0,
  });
  expect(costFor("grok-4.6", usage)).toBe(0);
  expect(costFor("grok-beta", usage)).toBe(0);
});

test("Codex cached input and reasoning subsets are not double-counted", () => {
  // Scanner stores input exclusive of cached input; reasoning is already in
  // output and is not passed as an extra billable class.
  const actual = costFor("gpt-5.6-sol", {
    inputTokens: 600,
    cacheReadTokens: 400,
    outputTokens: 20,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  });
  expect(actual).toBeCloseTo((600 * 5 + 400 * 0.5 + 20 * 30) / 1_000_000);
});

test("every Claude 4.x model resolves to a tier", () => {
  const models = [
    "claude-opus-4-7",
    "claude-opus-4-6",
    "claude-sonnet-4-6",
    "claude-sonnet-4-5",
    "claude-haiku-4-5-20251001",
  ];
  for (const m of models) {
    const p = priceFor(m);
    expect(p.input).toBeGreaterThan(0);
    expect(p.output).toBeGreaterThan(0);
  }
});

test("every Claude 5 family model (Fable 5, Sonnet 5) resolves from the table", () => {
  const models = [
    "claude-fable-5",
    "claude-fable-5-20260101",
    "claude-sonnet-5",
    "claude-sonnet-5-20260615",
  ];
  for (const m of models) {
    const priced = priceForWithSource(m);
    expect(priced.source).toBe("table");
    expect(priced.price.input).toBeGreaterThan(0);
    expect(priced.price.output).toBeGreaterThan(0);
  }
});

test("unknown model falls back to a positive tier (never zero)", () => {
  const p = priceFor("totally-fake-future-model");
  expect(p.input).toBeGreaterThan(0);
  expect(p.output).toBeGreaterThan(0);
});

test("unknown model's fallback is visibly flagged, not silently priced as a table hit", () => {
  const priced = priceForWithSource("totally-fake-future-model");
  expect(priced.source).toBe("fallback");
});
