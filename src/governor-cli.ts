import {
  defaultGovernorPolicy,
  evaluateTokenomics,
  type GovernorPolicy,
  type GovernorResult,
  type GrokFamilyMix,
  type HookParseResult,
  parseHookInput,
} from "./governor.ts";
import type { TokenomicsResult } from "./tokenomics.ts";

export interface GovernorCliOptions {
  help: boolean;
  json: boolean;
  hook: boolean;
  policy: GovernorPolicy;
}

export interface GovernorCliReport {
  ok: boolean;
  hook?: HookParseResult;
  governor: GovernorResult;
}

export const GOVERNOR_HELP = `Tokenomics governor

Usage:
  govern [--hook] [--enforce] [--json]
         [--max-window-tokens N] [--max-cache-ratio N] [--max-warn-findings N]
         [--fail-unknown|--allow-unknown] [--fail-capped|--allow-capped]
         [--no-shape]

Hook mode:
  Reads Claude/SSG hook JSON from stdin. Supports both {tool_name, tool_input}
  and {tool, input}. In --enforce mode, a block exits 2 for hook denial.

Defaults:
  Advisory mode exits 0 even when it reports a block decision. --enforce turns
  block decisions into exit 2. Telemetry unknown is warn by default; capped
  telemetry blocks in enforce mode unless --allow-capped is passed.`;

function readNumberFlag(argv: string[], name: string): number | undefined {
  const idx = argv.indexOf(name);
  if (idx < 0) return undefined;
  const n = Number(argv[idx + 1]);
  return Number.isFinite(n) && n >= 0 ? n : undefined;
}

export function parseGovernorArgs(argv: string[]): GovernorCliOptions {
  const enforce = argv.includes("--enforce")
    ? true
    : argv.includes("--advisory")
      ? false
      : undefined;
  const policy = defaultGovernorPolicy({
    ...(enforce !== undefined ? { enforce } : {}),
    ...(argv.includes("--no-shape") ? { shapeVerboseBash: false } : {}),
    ...(argv.includes("--fail-unknown") ? { failOnUnknown: true } : {}),
    ...(argv.includes("--allow-unknown") ? { failOnUnknown: false } : {}),
    ...(argv.includes("--fail-capped") ? { failOnCapped: true } : {}),
    ...(argv.includes("--allow-capped") ? { failOnCapped: false } : {}),
  });

  const maxWindowTokens = readNumberFlag(argv, "--max-window-tokens");
  if (maxWindowTokens !== undefined) policy.maxWindowTokens = maxWindowTokens;
  const maxCacheReadRatio = readNumberFlag(argv, "--max-cache-ratio");
  if (maxCacheReadRatio !== undefined) policy.maxCacheReadRatio = maxCacheReadRatio;
  const maxWarnFindings = readNumberFlag(argv, "--max-warn-findings");
  if (maxWarnFindings !== undefined) policy.maxWarnFindings = maxWarnFindings;

  return {
    help: argv.includes("--help") || argv.includes("-h"),
    json: argv.includes("--json"),
    hook: argv.includes("--hook"),
    policy,
  };
}

export async function readStdinText(): Promise<string> {
  return await new Response(Bun.stdin.stream()).text();
}

function withHookDiagnostic(
  result: GovernorResult,
  hook: HookParseResult | undefined,
): GovernorResult {
  if (!hook || hook.ok) return result;
  const reasons = [
    ...result.reasons,
    {
      code: "hook-input-invalid",
      severity: "warn" as const,
      message: hook.error ?? "Hook input could not be parsed.",
    },
  ];
  return {
    ...result,
    decision: result.decision === "allow" ? "warn" : result.decision,
    reasons,
  };
}

export function evaluateGovernorCli(
  tokenomics: TokenomicsResult,
  options: GovernorCliOptions,
  hookRaw?: string,
  grokMix?: GrokFamilyMix | null,
): GovernorCliReport {
  const hook = options.hook ? parseHookInput(hookRaw ?? "") : undefined;
  const base = evaluateTokenomics(
    tokenomics,
    options.policy,
    hook?.ok ? hook.call : undefined,
    grokMix,
  );
  const governor = withHookDiagnostic(base, hook);
  return {
    ok: governor.decision !== "block",
    ...(hook ? { hook } : {}),
    governor,
  };
}

function fmt(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(Math.round(n));
}

export function formatGovernorText(report: GovernorCliReport): string {
  const g = report.governor;
  const lines = [
    `Tokenomics governor: ${g.decision.toUpperCase()} (${g.enforce ? "enforce" : "advisory"})`,
    `  window: ${fmt(g.observed.windowTokens)} tokens / ${g.observed.windowHours}h`,
    `  cache/input: ${g.observed.cacheReadRatio.toFixed(2)}x`,
    `  data: ${g.dataState}${g.observed.capped ? " (capped)" : ""}`,
  ];
  if (report.hook && !report.hook.ok) lines.push(`  hook: ${report.hook.error}`);
  for (const reason of g.reasons) {
    const observed =
      reason.observed === undefined
        ? ""
        : ` observed=${Number.isInteger(reason.observed) ? fmt(reason.observed) : reason.observed.toFixed(2)}`;
    const threshold =
      reason.threshold === undefined
        ? ""
        : ` threshold=${Number.isInteger(reason.threshold) ? fmt(reason.threshold) : reason.threshold}`;
    lines.push(`  - ${reason.severity}: ${reason.code}: ${reason.message}${observed}${threshold}`);
  }
  if (g.decision === "block") {
    lines.push(
      "  action: checkpoint current state, compact/resume, or rerun verbose commands via stk/headroom.",
    );
  }
  return lines.join("\n");
}
