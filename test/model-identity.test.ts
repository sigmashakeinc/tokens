// A2: canonical normalizeModelId(raw) unit coverage. Structural
// vendor/prefix matching only — no includes()/substring shortcuts.

import { describe, expect, test } from "bun:test";
import { normalizeModelId } from "../src/model-identity.ts";

describe("normalizeModelId — recognized Claude ids", () => {
  test("bracketed context-window suffix", () => {
    const id = normalizeModelId("claude-fable-5[1m]");
    expect(id).toEqual({
      family: "fable-5",
      displayName: "Fable 5",
      harness: "claude",
      recognized: true,
    });
  });

  test("bare opus-5", () => {
    expect(normalizeModelId("claude-opus-5")).toEqual({
      family: "opus-5",
      displayName: "Opus 5",
      harness: "claude",
      recognized: true,
    });
  });

  test("bare sonnet-5", () => {
    expect(normalizeModelId("claude-sonnet-5")).toEqual({
      family: "sonnet-5",
      displayName: "Sonnet 5",
      harness: "claude",
      recognized: true,
    });
  });

  test("dated haiku-4-5 suffix resolves to the 4.5 family, not 4", () => {
    const id = normalizeModelId("claude-haiku-4-5-20251001");
    expect(id.family).toBe("haiku-4-5");
    expect(id.displayName).toBe("Haiku 4.5");
    expect(id.harness).toBe("claude");
    expect(id.recognized).toBe(true);
  });

  test("bare haiku-4 (no minor version) is a distinct, shorter family", () => {
    const id = normalizeModelId("claude-haiku-4-20250801");
    expect(id.family).toBe("haiku-4");
    expect(id.recognized).toBe(true);
  });

  test("bedrock region-prefixed opus-5 form", () => {
    const id = normalizeModelId("us.anthropic.claude-opus-5-20250917-v1:0");
    expect(id.family).toBe("opus-5");
    expect(id.harness).toBe("claude");
    expect(id.recognized).toBe(true);
  });

  test("other bedrock/region prefixes (eu, apac)", () => {
    expect(normalizeModelId("eu.anthropic.claude-sonnet-5-20250917-v1:0").family).toBe("sonnet-5");
    expect(normalizeModelId("apac.anthropic.claude-haiku-4-5-20251001-v2:1").family).toBe(
      "haiku-4-5",
    );
  });
});

describe("normalizeModelId — recognized Codex/GPT ids", () => {
  test("gpt-5.6-sol", () => {
    expect(normalizeModelId("gpt-5.6-sol")).toEqual({
      family: "gpt-5.6-sol",
      displayName: "GPT-5.6 Sol",
      harness: "codex",
      recognized: true,
    });
  });

  test("gpt-5.5", () => {
    expect(normalizeModelId("gpt-5.5")).toEqual({
      family: "gpt-5.5",
      displayName: "GPT-5.5",
      harness: "codex",
      recognized: true,
    });
  });

  test("gpt-5.3-codex-spark is distinct from gpt-5.3-codex", () => {
    const spark = normalizeModelId("gpt-5.3-codex-spark");
    expect(spark.family).toBe("gpt-5.3-codex-spark");
    expect(spark.recognized).toBe(true);
    const plain = normalizeModelId("gpt-5.3-codex");
    expect(plain.family).toBe("gpt-5.3-codex");
    expect(plain.recognized).toBe(true);
  });
});

describe("normalizeModelId — prefix-matching correctness (does not misroute siblings)", () => {
  test("gpt-5-opus-review is NOT classified as Claude", () => {
    const id = normalizeModelId("gpt-5-opus-review");
    expect(id.harness).not.toBe("claude");
    expect(id.harness).toBe("codex");
    // Not a real gpt-5 snapshot suffix grammar either — must not silently
    // collapse onto the bare "gpt-5" family/pricing tier.
    expect(id.recognized).toBe(false);
  });

  test("claude-opus-50 is not silently absorbed by the claude-opus-5 key", () => {
    const id = normalizeModelId("claude-opus-50-20250101");
    expect(id.family).not.toBe("opus-5");
  });

  test("gpt-5.6-solar is not absorbed by the gpt-5.6-sol key", () => {
    const id = normalizeModelId("gpt-5.6-solar");
    expect(id.family).not.toBe("gpt-5.6-sol");
  });
});

describe("normalizeModelId — recognized Grok ids", () => {
  test("grok-4.6", () => {
    expect(normalizeModelId("grok-4.6")).toEqual({
      family: "grok-4.6",
      displayName: "Grok 4.6",
      harness: "grok",
      recognized: true,
    });
  });

  test("grok-4.5 is distinct from grok-4.6", () => {
    expect(normalizeModelId("grok-4.5")).toEqual({
      family: "grok-4.5",
      displayName: "Grok 4.5",
      harness: "grok",
      recognized: true,
    });
    expect(normalizeModelId("grok-4.6").family).not.toBe("grok-4.5");
  });

  test("grok-* vendor ids stay on the grok harness and remain recognized", () => {
    const id = normalizeModelId("grok-4.6-preview");
    expect(id.harness).toBe("grok");
    expect(id.recognized).toBe(true);
    const other = normalizeModelId("grok-beta");
    expect(other.harness).toBe("grok");
    expect(other.recognized).toBe(true);
    expect(other.family).toBe("grok-beta");
  });
});

describe("normalizeModelId — unrecognized ids are retained, never dropped", () => {
  test("unknown vendor id", () => {
    const id = normalizeModelId("totally-unmapped-vendor-model-x1");
    expect(id.recognized).toBe(false);
    expect(id.harness).toBe("unknown");
    expect(id.displayName).toBe("totally-unmapped-vendor-model-x1");
  });

  test("empty/undefined input never throws", () => {
    expect(normalizeModelId("").recognized).toBe(false);
    expect(normalizeModelId(undefined).recognized).toBe(false);
    expect(normalizeModelId(null).recognized).toBe(false);
  });
});

describe("haiku-5 family", () => {
  test("claude-haiku-5-5 is recognized as haiku-5, not haiku-4", () => {
    const id = normalizeModelId("claude-haiku-5-5");
    expect(id.family).toBe("haiku-5");
    expect(id.harness).toBe("claude");
    expect(id.recognized).toBe(true);
    expect(normalizeModelId("claude-haiku-5-5-20261001").family).toBe("haiku-5");
    expect(normalizeModelId("claude-haiku-4-5-20251001").family).toBe("haiku-4-5");
  });
});
