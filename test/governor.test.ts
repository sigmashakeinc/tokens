import { describe, expect, test } from "bun:test";
import {
  computeGrokFamilyMix,
  defaultGovernorPolicy,
  evaluateHookCall,
  evaluateTokenomics,
  type GrokMixModelBucket,
  hasReducerWrapper,
  isVerboseCmd,
  parseHookInput,
} from "../src/governor.ts";
import { evaluateGovernorCli, parseGovernorArgs } from "../src/governor-cli.ts";
import type { TokenomicsResult } from "../src/tokenomics.ts";

function mixBucket(
  model: string,
  inputTokens: number,
  outputTokens: number,
  totalTokens?: number,
): GrokMixModelBucket {
  return {
    model,
    inputTokens,
    outputTokens,
    totalTokens: totalTokens ?? inputTokens + outputTokens,
  };
}

function zeroTotals() {
  return {
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    totalTokens: 0,
    cost: 0,
    messages: 0,
    observedSpanSec: 0,
    tokensPerSecond: null,
  };
}

function zeroSavings() {
  return {
    count: 0,
    origBytes: 0,
    compBytes: 0,
    savedBytes: 0,
    savedPct: 0,
    savedTokensEst: 0,
  };
}

function tokenomics(
  overrides: Partial<{
    inputTokens: number;
    cacheReadTokens: number;
    totalTokens: number;
    messages: number;
    filesScanned: number;
    capped: boolean;
    warnFindings: number;
  }> = {},
): TokenomicsResult {
  const totals = {
    ...zeroTotals(),
    inputTokens: overrides.inputTokens ?? 100,
    cacheReadTokens: overrides.cacheReadTokens ?? 0,
    totalTokens: overrides.totalTokens ?? 100,
    messages: overrides.messages ?? 1,
  };
  const findings = Array.from({ length: overrides.warnFindings ?? 0 }, (_, i) => ({
    kind: "oversized-output" as const,
    severity: "warn" as const,
    sessionId: `session-${i}`,
    detail: "large tool result",
    count: 1,
  }));
  const stk = zeroSavings();
  const headroom = zeroSavings();

  return {
    consumption: {
      allTime: totals,
      windowHours: 72,
      window: { ...totals },
    },
    savings: {
      byTool: { stk, headroom },
      combined: zeroSavings(),
      ledgerPath: "",
      present: false,
    },
    efficiency: {
      observedByTool: {},
      findings,
      ponytail: { newFiles: 0, depInstalls: 0 },
      windowHours: 72,
      filesScanned: overrides.filesScanned ?? 1,
      capped: overrides.capped ?? false,
    },
  };
}

describe("tokenomics governor policy", () => {
  test("advisory mode reports block decisions without denying the hook", () => {
    const result = evaluateTokenomics(
      tokenomics({ totalTokens: 50 }),
      defaultGovernorPolicy({ enforce: false, maxWindowTokens: 10 }),
    );

    expect(result.decision).toBe("block");
    expect(result.exitCode).toBe(0);
    expect(result.reasons.map((r) => r.code)).toContain("window-token-budget");
  });

  test("enforce mode turns block decisions into hook denial exit code", () => {
    const result = evaluateTokenomics(
      tokenomics({ totalTokens: 50 }),
      defaultGovernorPolicy({ enforce: true, maxWindowTokens: 10 }),
    );

    expect(result.decision).toBe("block");
    expect(result.exitCode).toBe(2);
  });

  test("cache-read ratio catches sessions that reread too much cached context", () => {
    const result = evaluateTokenomics(
      tokenomics({ inputTokens: 100, cacheReadTokens: 901 }),
      defaultGovernorPolicy({ maxCacheReadRatio: 8 }),
    );

    expect(result.decision).toBe("block");
    expect(result.reasons.map((r) => r.code)).toContain("cache-read-ratio");
  });

  test("unknown telemetry fails open by default", () => {
    const result = evaluateTokenomics(
      tokenomics({ filesScanned: 0, messages: 0, totalTokens: 0, inputTokens: 0 }),
      defaultGovernorPolicy({ enforce: true, failOnUnknown: false }),
    );

    expect(result.dataState).toBe("unknown");
    expect(result.decision).toBe("warn");
    expect(result.exitCode).toBe(0);
  });

  test("capped telemetry fails closed in enforce mode", () => {
    const result = evaluateTokenomics(
      tokenomics({ capped: true }),
      defaultGovernorPolicy({ enforce: true, failOnCapped: true }),
    );

    expect(result.dataState).toBe("capped");
    expect(result.decision).toBe("block");
    expect(result.exitCode).toBe(2);
  });

  test("repeated efficiency warnings stay advisory until a hard budget is crossed", () => {
    const result = evaluateTokenomics(
      tokenomics({ warnFindings: 3 }),
      defaultGovernorPolicy({ enforce: true, maxWarnFindings: 2 }),
    );

    expect(result.decision).toBe("warn");
    expect(result.exitCode).toBe(0);
    expect(result.reasons.map((r) => r.code)).toContain("efficiency-findings");
  });
});

describe("grok family active-share mix", () => {
  test("computeGrokFamilyMix uses active tokens and excludes bare grok-4 from the ratio", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 90, 0, 900),
      mixBucket("grok-4.5", 10, 0, 50),
      mixBucket("grok-4", 500, 0, 5000),
    ]);

    expect(mix.grok45Active).toBe(10);
    expect(mix.grok46Active).toBe(90);
    expect(mix.grokFamilyActive).toBe(100);
    expect(mix.grok46ActiveShare).toBeCloseTo(0.9);
    expect(mix.grok46TotalShare).toBeCloseTo(900 / 950);
  });

  test("91% grok-4.6 active share warns but never blocks, even with enforce", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 91, 0),
      mixBucket("grok-4.5", 9, 0),
    ]);
    const result = evaluateTokenomics(
      tokenomics(),
      defaultGovernorPolicy({ enforce: true }),
      undefined,
      mix,
    );

    expect(mix.grok46ActiveShare).toBeCloseTo(0.91);
    expect(result.decision).toBe("warn");
    expect(result.exitCode).toBe(0);
    expect(result.reasons.map((r) => r.code)).toContain("grok46-active-share");
    expect(result.reasons.find((r) => r.code === "grok46-active-share")?.severity).toBe("warn");
    expect(result.observed.grok46ActiveShare).toBeCloseTo(0.91);
    expect(result.observed.grokFamilyActive).toBe(100);
  });

  test("50% share under default 0.70 threshold does not emit grok46-active-share", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 50, 0),
      mixBucket("grok-4.5", 50, 0),
    ]);
    const result = evaluateTokenomics(
      tokenomics(),
      defaultGovernorPolicy(),
      undefined,
      mix,
    );

    expect(result.decision).toBe("allow");
    expect(result.reasons.map((r) => r.code)).not.toContain("grok46-active-share");
    expect(result.observed.grok46ActiveShare).toBeCloseTo(0.5);
  });

  test("maxGrok46ActiveShare 0 disables the check", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 99, 0),
      mixBucket("grok-4.5", 1, 0),
    ]);
    const result = evaluateTokenomics(
      tokenomics(),
      defaultGovernorPolicy({ maxGrok46ActiveShare: 0 }),
      undefined,
      mix,
    );

    expect(result.decision).toBe("allow");
    expect(result.reasons.map((r) => r.code)).not.toContain("grok46-active-share");
  });

  test("omitted mix or zero grok-family active skips without fabricating a 0 share", () => {
    const omitted = evaluateTokenomics(tokenomics(), defaultGovernorPolicy());
    expect(omitted.reasons.map((r) => r.code)).not.toContain("grok46-active-share");
    expect(omitted.observed.grok46ActiveShare).toBeUndefined();
    expect(omitted.observed.grokFamilyActive).toBeUndefined();

    const emptyMix = computeGrokFamilyMix([mixBucket("grok-4", 100, 0)]);
    expect(emptyMix.grokFamilyActive).toBe(0);
    expect(emptyMix.grok46ActiveShare).toBeNull();

    const skipped = evaluateTokenomics(
      tokenomics(),
      defaultGovernorPolicy({ maxGrok46ActiveShare: 0.1 }),
      undefined,
      emptyMix,
    );
    expect(skipped.reasons.map((r) => r.code)).not.toContain("grok46-active-share");
    expect(skipped.observed.grok46ActiveShare).toBeNull();
    expect(skipped.observed.grokFamilyActive).toBe(0);
  });

  test("mix warn alone does not turn a clean result into block", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 80, 0),
      mixBucket("grok-4.5", 20, 0),
    ]);
    const result = evaluateTokenomics(
      tokenomics({ totalTokens: 100, inputTokens: 100, cacheReadTokens: 0 }),
      defaultGovernorPolicy({
        enforce: true,
        maxWindowTokens: 1_000_000,
        maxCacheReadRatio: 8,
        maxGrok46ActiveShare: 0.7,
      }),
      undefined,
      mix,
    );

    expect(result.decision).toBe("warn");
    expect(result.exitCode).toBe(0);
    expect(result.reasons.every((r) => r.severity !== "block")).toBe(true);
    expect(result.reasons.map((r) => r.code)).toEqual(["grok46-active-share"]);
  });
});

describe("tokenomics governor hooks", () => {
  test("parses Claude Code hook envelopes", () => {
    const parsed = parseHookInput(
      JSON.stringify({ tool_name: "Bash", tool_input: { command: "rg TODO src" } }),
    );

    expect(parsed.ok).toBe(true);
    expect(parsed.call?.source).toBe("claude");
    expect(parsed.call?.toolName).toBe("Bash");
  });

  test("parses SSG hook envelopes", () => {
    const parsed = parseHookInput(JSON.stringify({ tool: "Bash", input: { command: "cat log" } }));

    expect(parsed.ok).toBe(true);
    expect(parsed.call?.source).toBe("ssg");
    expect(parsed.call?.toolName).toBe("Bash");
  });

  test("malformed hook input remains advisory even in enforce mode", () => {
    const report = evaluateGovernorCli(
      tokenomics(),
      parseGovernorArgs(["--hook", "--enforce"]),
      "{not-json",
    );

    expect(report.hook?.ok).toBe(false);
    expect(report.governor.decision).toBe("warn");
    expect(report.governor.exitCode).toBe(0);
    expect(report.governor.reasons.map((r) => r.code)).toContain("hook-input-invalid");
  });

  test("evaluateGovernorCli threads grok mix into observed and warn reasons", () => {
    const mix = computeGrokFamilyMix([
      mixBucket("grok-4.6", 91, 0),
      mixBucket("grok-4.5", 9, 0),
    ]);
    const report = evaluateGovernorCli(
      tokenomics(),
      parseGovernorArgs(["--enforce"]),
      undefined,
      mix,
    );

    expect(report.governor.decision).toBe("warn");
    expect(report.governor.exitCode).toBe(0);
    expect(report.governor.reasons.map((r) => r.code)).toContain("grok46-active-share");
    expect(report.governor.observed.grok46ActiveShare).toBeCloseTo(0.91);
  });

  test("detects verbose commands without classifying focused package commands as verbose", () => {
    expect(isVerboseCmd("git diff -- src")).toBe(true);
    expect(isVerboseCmd("curl -fsS http://127.0.0.1:7913/api/tokenomics")).toBe(true);
    expect(isVerboseCmd(["bun", "test", "test/governor.test.ts"].join(" "))).toBe(false);
  });

  test("recognizes Sigma-owned reducers as shaping wrappers", () => {
    expect(hasReducerWrapper("stk git diff -- src")).toBe(true);
    expect(hasReducerWrapper("git diff -- src | headroom compress --type log -")).toBe(true);
    expect(hasReducerWrapper("git diff -- src")).toBe(false);
  });

  test("blocks unshaped verbose Bash calls before tool execution", () => {
    const reasons = evaluateHookCall(
      { toolName: "Bash", input: { command: "git diff -- src" }, source: "claude" },
      defaultGovernorPolicy({ shapeVerboseBash: true }),
    );

    expect(reasons.map((r) => r.code)).toEqual(["verbose-bash-unreduced"]);
  });

  test("allows verbose Bash calls that are already routed through a reducer", () => {
    const reasons = evaluateHookCall(
      { toolName: "Bash", input: { command: "stk git diff -- src" }, source: "claude" },
      defaultGovernorPolicy({ shapeVerboseBash: true }),
    );

    expect(reasons).toEqual([]);
  });
});
