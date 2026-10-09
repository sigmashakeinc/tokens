import { normalizeModelId } from "./model-identity.ts";
import type { TokenomicsResult } from "./tokenomics.ts";

export type GovernorDecision = "allow" | "warn" | "block";
export type GovernorDataState = "ok" | "unknown" | "capped";

export interface GovernorPolicy {
  enforce: boolean;
  shapeVerboseBash: boolean;
  maxWindowTokens: number;
  maxCacheReadRatio: number;
  maxWarnFindings: number;
  /** Max grok-4.6 share of grok-4.5+4.6 *active* tokens (input+output). 0 disables. */
  maxGrok46ActiveShare: number;
  failOnCapped: boolean;
  failOnUnknown: boolean;
}

export interface GovernorReason {
  code: string;
  severity: "info" | "warn" | "block";
  message: string;
  observed?: number;
  threshold?: number;
}

export interface GovernorObserved {
  windowHours: number;
  windowTokens: number;
  inputTokens: number;
  cacheReadTokens: number;
  cacheReadRatio: number;
  warnFindings: number;
  filesScanned: number;
  capped: boolean;
  grok45Active?: number;
  grok46Active?: number;
  grokFamilyActive?: number;
  grok46ActiveShare?: number | null;
}

/** Minimal byModel row for Grok-family mix (ModelBucket / store snapshot compatible). */
export interface GrokMixModelBucket {
  model: string;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

/** Grok-4.5 vs 4.6 mix. Active = input+output (excludes cache). Shares are null when denom is 0. */
export interface GrokFamilyMix {
  grok45Active: number;
  grok46Active: number;
  grokFamilyActive: number;
  grok46ActiveShare: number | null;
  /** Diagnostic only — not used for the warn threshold. */
  grok45Total: number;
  grok46Total: number;
  grokFamilyTotal: number;
  grok46TotalShare: number | null;
}

export interface GovernorResult {
  decision: GovernorDecision;
  enforce: boolean;
  exitCode: number;
  dataState: GovernorDataState;
  reasons: GovernorReason[];
  thresholds: GovernorPolicy;
  observed: GovernorObserved;
}

export interface HookCall {
  toolName: string;
  input: Record<string, unknown>;
  source: "claude" | "ssg";
}

export interface HookParseResult {
  ok: boolean;
  call?: HookCall;
  error?: string;
}

const DEFAULT_MAX_WINDOW_TOKENS = 250_000_000;
const DEFAULT_MAX_CACHE_READ_RATIO = 8;
const DEFAULT_MAX_WARN_FINDINGS = 5;
const DEFAULT_MAX_GROK46_ACTIVE_SHARE = 0.7;

const GROK46_ACTIVE_SHARE_MESSAGE =
  "Grok-4.6 active-token share of Grok-family is high; coding slices should be grok-4.5. grok-4.6 is reserved for the orchestrator main loop, named *-lead.md coordinators, and the hardest architectural/diagnostic slice.";

function envFlag(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return /^(1|true|yes|on)$/i.test(raw);
}

function envNumber(name: string, fallback: number): number {
  const n = Number(process.env[name]);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

export function defaultGovernorPolicy(overrides?: Partial<GovernorPolicy>): GovernorPolicy {
  return {
    enforce: envFlag("TOKENOMICS_GOVERNOR_ENFORCE", false),
    shapeVerboseBash: envFlag("TOKENOMICS_GOVERNOR_SHAPE_BASH", true),
    maxWindowTokens: envNumber("TOKENOMICS_GOVERNOR_MAX_WINDOW_TOKENS", DEFAULT_MAX_WINDOW_TOKENS),
    maxCacheReadRatio: envNumber(
      "TOKENOMICS_GOVERNOR_MAX_CACHE_READ_RATIO",
      DEFAULT_MAX_CACHE_READ_RATIO,
    ),
    maxWarnFindings: envNumber("TOKENOMICS_GOVERNOR_MAX_WARN_FINDINGS", DEFAULT_MAX_WARN_FINDINGS),
    maxGrok46ActiveShare: envNumber(
      "TOKENOMICS_GOVERNOR_MAX_GROK46_ACTIVE_SHARE",
      DEFAULT_MAX_GROK46_ACTIVE_SHARE,
    ),
    failOnCapped: envFlag("TOKENOMICS_GOVERNOR_FAIL_ON_CAPPED", true),
    failOnUnknown: envFlag("TOKENOMICS_GOVERNOR_FAIL_ON_UNKNOWN", false),
    ...overrides,
  };
}

/**
 * Active-token mix of grok-4.5 vs grok-4.6. Family ids come from
 * normalizeModelId — bare "grok-4" / other grok ids are excluded from the
 * 4.5/4.6 denominator. Active = inputTokens + outputTokens (no cache).
 */
export function computeGrokFamilyMix(byModel: readonly GrokMixModelBucket[]): GrokFamilyMix {
  let grok45Active = 0;
  let grok46Active = 0;
  let grok45Total = 0;
  let grok46Total = 0;

  for (const bucket of byModel) {
    const family = normalizeModelId(bucket.model).family;
    if (family !== "grok-4.5" && family !== "grok-4.6") continue;
    const active = bucket.inputTokens + bucket.outputTokens;
    if (family === "grok-4.5") {
      grok45Active += active;
      grok45Total += bucket.totalTokens;
    } else {
      grok46Active += active;
      grok46Total += bucket.totalTokens;
    }
  }

  const grokFamilyActive = grok45Active + grok46Active;
  const grokFamilyTotal = grok45Total + grok46Total;
  return {
    grok45Active,
    grok46Active,
    grokFamilyActive,
    grok46ActiveShare: grokFamilyActive > 0 ? grok46Active / grokFamilyActive : null,
    grok45Total,
    grok46Total,
    grokFamilyTotal,
    grok46TotalShare: grokFamilyTotal > 0 ? grok46Total / grokFamilyTotal : null,
  };
}

export function isVerboseCmd(cmd: string): boolean {
  return /\b(cat|grep|jq|curl|kubectl|find|rg)\b/.test(cmd) || /\bgit\s+(log|diff)\b/.test(cmd);
}

export function hasReducerWrapper(cmd: string): boolean {
  return (
    /\|\s*stk\b/.test(cmd) ||
    /\|\s*headroom\b/.test(cmd) ||
    /^stk\s/.test(cmd) ||
    /^headroom\s/.test(cmd)
  );
}

export function parseHookInput(raw: string): HookParseResult {
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, error: "empty hook input" };

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { ok: false, error: "malformed hook JSON" };
  }

  if (typeof parsed !== "object" || parsed === null) {
    return { ok: false, error: "hook JSON is not an object" };
  }

  const obj = parsed as Record<string, unknown>;
  const claudeTool = typeof obj.tool_name === "string" ? obj.tool_name : "";
  const ssgTool = typeof obj.tool === "string" ? obj.tool : "";
  const toolName = claudeTool || ssgTool;
  const inputValue = claudeTool ? obj.tool_input : obj.input;

  if (!toolName) return { ok: false, error: "hook input missing tool name" };
  if (typeof inputValue !== "object" || inputValue === null) {
    return { ok: false, error: "hook input missing tool input object" };
  }

  return {
    ok: true,
    call: {
      toolName,
      input: inputValue as Record<string, unknown>,
      source: claudeTool ? "claude" : "ssg",
    },
  };
}

function observedFrom(result: TokenomicsResult, grokMix?: GrokFamilyMix | null): GovernorObserved {
  const window = result.consumption.window;
  const observed: GovernorObserved = {
    windowHours: result.consumption.windowHours,
    windowTokens: window.totalTokens,
    inputTokens: window.inputTokens,
    cacheReadTokens: window.cacheReadTokens,
    cacheReadRatio: window.inputTokens > 0 ? window.cacheReadTokens / window.inputTokens : 0,
    warnFindings: result.efficiency.findings.filter((f) => f.severity === "warn").length,
    filesScanned: result.efficiency.filesScanned,
    capped: result.efficiency.capped,
  };
  if (grokMix) {
    observed.grok45Active = grokMix.grok45Active;
    observed.grok46Active = grokMix.grok46Active;
    observed.grokFamilyActive = grokMix.grokFamilyActive;
    observed.grok46ActiveShare = grokMix.grok46ActiveShare;
  }
  return observed;
}

function finalize(
  policy: GovernorPolicy,
  dataState: GovernorDataState,
  observed: GovernorObserved,
  reasons: GovernorReason[],
): GovernorResult {
  const hasBlock = reasons.some((r) => r.severity === "block");
  const hasWarn = reasons.some((r) => r.severity === "warn");
  const decision: GovernorDecision = hasBlock ? "block" : hasWarn ? "warn" : "allow";
  return {
    decision,
    enforce: policy.enforce,
    exitCode: policy.enforce && decision === "block" ? 2 : 0,
    dataState,
    reasons,
    thresholds: policy,
    observed,
  };
}

export function evaluateHookCall(
  call: HookCall | undefined,
  policy = defaultGovernorPolicy(),
): GovernorReason[] {
  if (!call || !policy.shapeVerboseBash) return [];
  if (call.toolName !== "Bash") return [];

  const cmd = typeof call.input.command === "string" ? call.input.command.trim() : "";
  if (!cmd || !isVerboseCmd(cmd) || hasReducerWrapper(cmd)) return [];

  return [
    {
      code: "verbose-bash-unreduced",
      severity: "block",
      message:
        "Verbose Bash command is not token-shaped. Re-run through `stk <cmd>` for logs/diffs or `headroom wrap <cmd>` when the original output may be needed.",
    },
  ];
}

export function evaluateTokenomics(
  result: TokenomicsResult,
  policy = defaultGovernorPolicy(),
  hookCall?: HookCall,
  grokMix?: GrokFamilyMix | null,
): GovernorResult {
  const observed = observedFrom(result, grokMix);
  const reasons: GovernorReason[] = [];
  let dataState: GovernorDataState = "ok";

  if (observed.filesScanned === 0 && result.consumption.window.messages === 0) {
    dataState = "unknown";
    reasons.push({
      code: "telemetry-unknown",
      severity: policy.failOnUnknown ? "block" : "warn",
      message:
        "Tokenomics telemetry is empty or unavailable; governor cannot prove the budget state.",
    });
  }

  if (observed.capped) {
    dataState = "capped";
    reasons.push({
      code: "telemetry-capped",
      severity: policy.failOnCapped ? "block" : "warn",
      message: "Tokenomics scan hit its cap; treat the budget state as incomplete.",
    });
  }

  if (policy.maxWindowTokens > 0 && observed.windowTokens > policy.maxWindowTokens) {
    reasons.push({
      code: "window-token-budget",
      severity: "block",
      message: "Rolling token budget exceeded; checkpoint, compact, and resume from a handoff.",
      observed: observed.windowTokens,
      threshold: policy.maxWindowTokens,
    });
  }

  if (
    policy.maxCacheReadRatio > 0 &&
    observed.inputTokens > 0 &&
    observed.cacheReadRatio > policy.maxCacheReadRatio
  ) {
    reasons.push({
      code: "cache-read-ratio",
      severity: "block",
      message:
        "Cache-read/input ratio is too high; shorten the session or compact before more work.",
      observed: observed.cacheReadRatio,
      threshold: policy.maxCacheReadRatio,
    });
  }

  if (policy.maxWarnFindings > 0 && observed.warnFindings > policy.maxWarnFindings) {
    reasons.push({
      code: "efficiency-findings",
      severity: "warn",
      message: "Recent tokenomics findings show repeated reducer bypass or oversized output.",
      observed: observed.warnFindings,
      threshold: policy.maxWarnFindings,
    });
  }

  // Grok-4.6 active-share is warn-only — never block, even when enforce=true.
  if (
    grokMix &&
    grokMix.grokFamilyActive > 0 &&
    grokMix.grok46ActiveShare !== null &&
    policy.maxGrok46ActiveShare > 0 &&
    grokMix.grok46ActiveShare > policy.maxGrok46ActiveShare
  ) {
    reasons.push({
      code: "grok46-active-share",
      severity: "warn",
      message: GROK46_ACTIVE_SHARE_MESSAGE,
      observed: grokMix.grok46ActiveShare,
      threshold: policy.maxGrok46ActiveShare,
    });
  }

  reasons.push(...evaluateHookCall(hookCall, policy));

  return finalize(policy, dataState, observed, reasons);
}
