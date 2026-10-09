// Scans Claude Code session logs at ~/.claude/projects/<slug>/<sessionId>.jsonl,
// parses each line incrementally (only the bytes since last scan), and writes
// new usage rows into the SQLite store. Aggregations query the store.
//
// Incremental ingestion: per-file state (mtime, size, last byte offset, last
// line number) is persisted in `file_state` so a restart picks up where it
// left off and we never reparse a file in full.

import { createHash, randomUUID } from "node:crypto";
import { type Dirent, statSync } from "node:fs";
import { open, readdir, readFile, stat, statfs } from "node:fs/promises";
import { homedir } from "node:os";
import { join, relative, sep } from "node:path";
import {
  classifyCodexCheckpointMigration,
  isDeployedDepthTwoMigration,
} from "./codex-checkpoint-migration";
import { type Harness, normalizeModelId } from "./model-identity.ts";
import { claudeProjectsDir, codexSessionsDir, grokLogsDir, grokSessionsDir } from "./paths.ts";
import {
  costFor,
  type ModelPrice,
  type PriceSource,
  priceForWithSource,
  type UsageRow,
} from "./pricing.ts";
import {
  type CapturedEvent,
  type CapturedFileCommit,
  CLAUDE_USAGE_PARSER_VERSION,
  CODEX_USAGE_PARSER_VERSION,
  type DiscoveredAlias,
  type DiscoveryProviderState,
  GROK_USAGE_PARSER_VERSION,
  getStore,
  type LegacyReconciliationObservation,
  type LegacyReconciliationPendingRow,
  type NormalizedTokenClasses,
  type SourceFileState,
  type SourceReconciliationSnapshot,
  type StoredEvent,
  type TokenSource,
  type TokenStore,
  type UsageAggregateSnapshot,
} from "./store.ts";
import { claudeProviderRecordIdentity } from "./token-usage-contract.ts";
import {
  aggregateUsageOffThread,
  shutdownUsageAggregationWorker,
} from "./usage-aggregation-worker.ts";

function projectsRoot(): string {
  return claudeProjectsDir();
}

function codexSessionsRoot(): string {
  return codexSessionsDir();
}

function grokLogsRoot(): string {
  if (process.env.GROK_LOGS_DIR) return process.env.GROK_LOGS_DIR;
  // Isolated ingest tests override Claude/Codex roots. Do not walk the
  // operator ~/.grok tree into those tmp stores unless GROK_* is set.
  if (process.env.CLAUDE_PROJECTS_DIR || process.env.CODEX_SESSIONS_DIR) {
    return join(homedir(), ".grok-tokens-test-absent", "logs");
  }
  return grokLogsDir();
}

function grokSessionsRoot(): string {
  if (process.env.GROK_SESSIONS_DIR) return process.env.GROK_SESSIONS_DIR;
  if (process.env.CLAUDE_PROJECTS_DIR || process.env.CODEX_SESSIONS_DIR) {
    return join(homedir(), ".grok-tokens-test-absent", "sessions");
  }
  return grokSessionsDir();
}

interface GrokSessionMeta {
  model: string;
  cwd: string;
  project: string;
  projectSlug: string;
}

const grokSessionIndex = new Map<string, GrokSessionMeta>();

interface RawUsage {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_input_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_creation?: {
    ephemeral_5m_input_tokens?: number;
    ephemeral_1h_input_tokens?: number;
  };
}

interface RawMessage {
  role?: string;
  model?: string;
  usage?: RawUsage;
  id?: string;
}

interface RawLine {
  type?: string;
  uuid?: string;
  message?: RawMessage;
  requestId?: string;
  timestamp?: string;
  cwd?: string;
  sessionId?: string;
}

interface CodexUsageEnvelope {
  last: unknown;
  total: unknown;
  hasLast: boolean;
  hasTotal: boolean;
}

interface CodexTotals {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  reasoningOutputTokens: number | null;
}

interface CodexLine {
  type?: string;
  timestamp?: string;
  payload?: unknown;
}

const DEFECT_LINE_SAMPLE_LIMIT = 8;

type CodexForkPhase = "none" | "inherited" | "live" | "ambiguous";

type CodexUsageDisposition =
  | "billedUsageRows"
  | "inheritedSuppressedRows"
  | "ambiguousQuarantinedRows"
  | "zeroNoopRows"
  | "unrecoverableResetRows"
  | "invalidUsageRows";

interface CodexMetadata {
  sessionId: string;
  model: string;
  cwd: string;
  project: string;
  projectSlug: string;
  totals: CodexTotals | null;
  /** Creation instant decoded only from an observed payload.session_id UUIDv7. */
  sessionTimestampMs: number | null;
  /** Parser contract that produced this checkpoint. Absent means legacy. */
  usageParserVersion?: number;
  /** Presence is an exact-once guard for the v3 thread_source role repair. */
  threadSourceRoleRepaired?: boolean | null;
  /** Exact-once guard and replay progress for proven Codex subagent lineage. */
  codexLineageRepaired?: boolean | null;
  /** Exact-once guard for the failed depth-1 await-root rollout. */
  depthOneDirectLineageRepaired?: boolean | null;
  /** Exact-once guard for observed absent/present Codex lineage shapes. */
  codexLineageShapeRepaired?: boolean | null;
  /** Exact-once guard for the four deployed v3 checkpoint backfill shapes. */
  codexDeployedCheckpointRepaired?: boolean | null;
  lineageDepth?: 1 | 2 | null;
  rootSessionId?: string | null;
  lineageReplayStage?: "await-parent" | "await-root" | "ready" | null;
  /** Sticky rollout identity and fork classification for this generation. */
  childSessionId: string | null;
  forkParentId: string | null;
  phase: CodexForkPhase;
  boundaryByte: number | null;
  boundaryLine: number | null;
  firstLiveRowPending: boolean;
  /** Rolling identity over every syntactic token_count row and disposition. */
  rollingDigest: string;
  tokenCountRows: number;
  billedUsageRows: number;
  inheritedSuppressedRows: number;
  ambiguousQuarantinedRows: number;
  zeroNoopRows: number;
  unrecoverableResetRows: number;
  invalidUsageRows: number;
  /** Sticky semantic completeness, independent of a retryable terminal suffix. */
  semanticUsageComplete: boolean;
  /** Permanent syntactic defects observed before a later valid JSONL line. */
  defectLineCount: number;
  /** Bounded, zero-based sample of the first permanent defect line numbers. */
  defectLineNumbers: number[];
  /** Sticky for this source generation. A complete malformed usage record
   * makes discovery incomplete until the file is rewritten or a newer parser
   * explicitly replays it; later no-op scans must not silently clear it. */
  usageComplete: boolean;
}

const codexMetadataCache = new Map<string, CodexMetadata>();

export function _resetScannerStateForTest(): void {
  codexMetadataCache.clear();
  grokSessionIndex.clear();
  invalidateAggregationCache();
  shutdownUsageAggregationWorker();
}

export function _scannerCodexCacheSizeForTest(): number {
  return codexMetadataCache.size;
}

export function _scannerCodexCacheKeysForTest(): string[] {
  return [...codexMetadataCache.keys()].sort();
}

function dateKey(iso: string): string {
  return iso.slice(0, 10);
}

function slugToProject(slug: string): string {
  const trimmed = slug.replace(/^-/, "");
  return `/${trimmed.replace(/-/g, "/")}`;
}

function projectShortName(slug: string): string {
  const parts = slug.replace(/^-/, "").split("-");
  for (let i = parts.length - 2; i >= 0; i--) {
    if (parts[i] === "sigmashake" && parts[i + 1]) return `sigmashake-${parts[i + 1]}`;
  }
  return parts.slice(-2).join("-") || slug;
}

function cwdToProjectSlug(cwd: string): string {
  const clean = cwd.trim();
  if (!clean) return "codex";
  return `-${clean.replace(/^\/+/, "").replace(/[\\/]+/g, "-")}`;
}

function codexMetadataFor(cwd: string, sessionId: string, model: string): CodexMetadata {
  const projectSlug = cwdToProjectSlug(cwd);
  return {
    sessionId,
    model,
    cwd,
    project: projectShortName(projectSlug),
    projectSlug,
    totals: null,
    sessionTimestampMs: null,
    usageParserVersion: CODEX_USAGE_PARSER_VERSION,
    threadSourceRoleRepaired: true,
    codexLineageRepaired: true,
    depthOneDirectLineageRepaired: true,
    codexLineageShapeRepaired: true,
    codexDeployedCheckpointRepaired: true,
    lineageDepth: null,
    rootSessionId: null,
    lineageReplayStage: null,
    childSessionId: null,
    forkParentId: null,
    phase: "none",
    boundaryByte: null,
    boundaryLine: null,
    firstLiveRowPending: false,
    rollingDigest: createHash("sha256").update("").digest("hex"),
    tokenCountRows: 0,
    billedUsageRows: 0,
    inheritedSuppressedRows: 0,
    ambiguousQuarantinedRows: 0,
    zeroNoopRows: 0,
    unrecoverableResetRows: 0,
    invalidUsageRows: 0,
    semanticUsageComplete: true,
    defectLineCount: 0,
    defectLineNumbers: [],
    usageComplete: true,
  };
}

interface UsageDefectCheckpoint {
  semanticUsageComplete: boolean;
  defectLineCount: number;
  defectLineNumbers: number[];
}

function validUsageDefectCheckpoint(value: Record<string, unknown>): boolean {
  const count = value.defectLineCount;
  const lines = value.defectLineNumbers;
  return (
    typeof value.semanticUsageComplete === "boolean" &&
    isNonNegativeSafeInteger(count) &&
    Array.isArray(lines) &&
    lines.length === Math.min(count, DEFECT_LINE_SAMPLE_LIMIT) &&
    lines.every(
      (line, index) =>
        isNonNegativeSafeInteger(line) && (index === 0 || line > (lines[index - 1] as number)),
    )
  );
}

function usageDefectCheckpointFromJson(
  value: string | null,
  fallbackUsageComplete: boolean,
): UsageDefectCheckpoint {
  if (value) {
    try {
      const parsed = asRecord(JSON.parse(value));
      if (parsed && validUsageDefectCheckpoint(parsed)) {
        return {
          semanticUsageComplete: parsed.semanticUsageComplete as boolean,
          defectLineCount: parsed.defectLineCount as number,
          defectLineNumbers: [...(parsed.defectLineNumbers as number[])],
        };
      }
    } catch {
      // Versioned replay rebuilds invalid legacy checkpoint metadata below.
    }
  }
  return {
    semanticUsageComplete: fallbackUsageComplete,
    defectLineCount: 0,
    defectLineNumbers: [],
  };
}

function recordPermanentDefect(checkpoint: UsageDefectCheckpoint, lineNumber: number): void {
  checkpoint.defectLineCount += 1;
  if (checkpoint.defectLineNumbers.length < DEFECT_LINE_SAMPLE_LIMIT) {
    checkpoint.defectLineNumbers.push(lineNumber);
  }
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === "object" && v !== null && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : null;
}

function asString(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() ? v : undefined;
}

export function _codexSessionTimestampFromUuidForTest(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const match = /^([0-9a-f]{8})-([0-9a-f]{4})-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.exec(
    value,
  );
  if (!match) return null;
  const timestampMs = Number.parseInt(`${match[1]}${match[2]}`, 16);
  if (!Number.isSafeInteger(timestampMs) || timestampMs < 0) return null;
  const instant = new Date(timestampMs);
  return Number.isNaN(instant.getTime()) ? null : timestampMs;
}

export function _parseCodexExplicitTimestampForTest(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|([+-])(\d{2}):(\d{2}))$/.exec(
      value,
    );
  if (!match) return null;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, fraction = ""] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const millisecond = Number(`${fraction}000`.slice(0, 3));
  const offsetHour = match[10] ? Number(match[10]) : 0;
  const offsetMinute = match[11] ? Number(match[11]) : 0;
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return null;
  }
  const local = new Date(0);
  local.setUTCFullYear(year, month - 1, day);
  local.setUTCHours(hour, minute, second, millisecond);
  if (
    local.getUTCFullYear() !== year ||
    local.getUTCMonth() !== month - 1 ||
    local.getUTCDate() !== day ||
    local.getUTCHours() !== hour ||
    local.getUTCMinutes() !== minute ||
    local.getUTCSeconds() !== second
  ) {
    return null;
  }
  const offsetSign = match[9] === "-" ? -1 : 1;
  const offsetMs = offsetSign * (offsetHour * 60 + offsetMinute) * 60_000;
  const timestampMs = local.getTime() - offsetMs;
  return Number.isNaN(new Date(timestampMs).getTime()) ? null : timestampMs;
}

function tokenCount(v: unknown): number | null {
  return typeof v === "number" && Number.isSafeInteger(v) && v >= 0 ? v : null;
}

function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

interface ParsedLine {
  event: StoredEvent | null;
  usageComplete: boolean;
}

function optionalTokenCount(record: Record<string, unknown>, key: string): number | null {
  if (!(key in record)) return 0;
  return tokenCount(record[key]);
}

function parseClaudeLine(
  line: string,
  ctx: {
    filePath: string;
    sessionId: string;
    project: string;
    projectSlug: string;
    lineNo: number;
  },
): ParsedLine {
  if (!line) return { event: null, usageComplete: true };
  let parsed: unknown;
  try {
    parsed = JSON.parse(line) as unknown;
  } catch {
    return { event: null, usageComplete: false };
  }
  const o = asRecord(parsed) as RawLine | null;
  if (!o) return { event: null, usageComplete: true };
  // Older exported Claude transcripts omitted the top-level type while
  // retaining the same message/usage envelope. Accept that legacy shape,
  // but never interpret an explicitly different record type as usage.
  if (o.type !== undefined && o.type !== "assistant") {
    return { event: null, usageComplete: true };
  }
  const m = o.message;
  if (!m || typeof m !== "object") return { event: null, usageComplete: true };
  const usage = m.usage;
  if (usage === undefined) return { event: null, usageComplete: true };
  if (!usage || typeof usage !== "object") return { event: null, usageComplete: false };
  const model: string = m.model || "unknown";
  if (model === "<synthetic>") return { event: null, usageComplete: true };
  const ts = Date.parse(o.timestamp || "");
  const usageRecord = usage as unknown as Record<string, unknown>;
  const input = tokenCount(usageRecord.input_tokens);
  const output = tokenCount(usageRecord.output_tokens);
  const cacheRead = optionalTokenCount(usageRecord, "cache_read_input_tokens");
  const hasCacheCreateTotal = "cache_creation_input_tokens" in usageRecord;
  const declaredCacheCreateTotal = hasCacheCreateTotal
    ? tokenCount(usageRecord.cache_creation_input_tokens)
    : null;
  const creationValue = usageRecord.cache_creation;
  const creation = creationValue === undefined ? {} : asRecord(creationValue);
  const create5m = creation ? optionalTokenCount(creation, "ephemeral_5m_input_tokens") : null;
  const create1h = creation ? optionalTokenCount(creation, "ephemeral_1h_input_tokens") : null;
  const cacheCreateBreakdown = create5m !== null && create1h !== null ? create5m + create1h : null;
  const effectiveDeclaredCacheCreateTotal =
    declaredCacheCreateTotal === 0 && cacheCreateBreakdown !== null && cacheCreateBreakdown > 0
      ? null
      : declaredCacheCreateTotal;
  if (
    !Number.isFinite(ts) ||
    input === null ||
    output === null ||
    cacheRead === null ||
    (hasCacheCreateTotal && declaredCacheCreateTotal === null) ||
    create5m === null ||
    create1h === null ||
    (effectiveDeclaredCacheCreateTotal !== null &&
      effectiveDeclaredCacheCreateTotal < create5m + create1h)
  ) {
    return { event: null, usageComplete: false };
  }
  const cacheCreateTotal = effectiveDeclaredCacheCreateTotal ?? create5m + create1h;
  const remainder = cacheCreateTotal - create5m - create1h;
  const cacheWrite5m = create5m + remainder;
  const cacheWrite1h = create1h;

  const row: UsageRow = {
    inputTokens: input,
    outputTokens: output,
    cacheReadTokens: cacheRead,
    cacheWrite5mTokens: cacheWrite5m,
    cacheWrite1hTokens: cacheWrite1h,
  };
  const cost = costFor(model, row);
  const projectSlug = asString(o.cwd) ? cwdToProjectSlug(o.cwd!) : ctx.projectSlug;

  return {
    event: {
      ts,
      date: dateKey(o.timestamp || ""),
      model,
      project: projectShortName(projectSlug),
      projectSlug,
      sessionId: ctx.sessionId,
      ...row,
      cost,
      dedupeKey: claudeProviderRecordIdentity(o, { sessionId: ctx.sessionId }).logicalMessageKey,
      filePath: ctx.filePath,
      lineOffset: ctx.lineNo,
      source: "claude",
      tokenClasses: {
        input,
        cachedRead: cacheRead,
        cacheWrite: cacheWrite5m + cacheWrite1h,
        // Anthropic's local record exposes provider output as one class; it does
        // not expose a separable reasoning count.
        visibleOutput: output,
        reasoningOutput: null,
        complete: false,
      },
    },
    usageComplete: true,
  };
}

function grokSessionFallback(sid: string): GrokSessionMeta {
  return {
    model: "unknown",
    cwd: "",
    project: "grok",
    projectSlug: cwdToProjectSlug(sid ? `grok/${sid}` : "grok"),
  };
}

function parseGrokLine(
  line: string,
  ctx: {
    filePath: string;
    lineNo: number;
  },
): ParsedLine {
  if (!line) return { event: null, usageComplete: true };
  let parsed: unknown;
  try {
    parsed = JSON.parse(line) as unknown;
  } catch {
    return { event: null, usageComplete: false };
  }
  const o = asRecord(parsed);
  if (!o) return { event: null, usageComplete: true };
  if (o.msg !== "shell.turn.inference_done") return { event: null, usageComplete: true };
  const sid = asString(o.sid);
  const tsRaw = asString(o.ts);
  const ctxRecord = asRecord(o.ctx);
  if (!sid || !tsRaw || !ctxRecord) return { event: null, usageComplete: false };
  const ts = Date.parse(tsRaw);
  const prompt = tokenCount(ctxRecord.prompt_tokens);
  const cached = optionalTokenCount(ctxRecord, "cached_prompt_tokens");
  const completion = tokenCount(ctxRecord.completion_tokens);
  const hasReasoning = "reasoning_tokens" in ctxRecord;
  const reasoning = hasReasoning ? tokenCount(ctxRecord.reasoning_tokens) : null;
  if (
    !Number.isFinite(ts) ||
    prompt === null ||
    cached === null ||
    completion === null ||
    cached > prompt ||
    (hasReasoning && (reasoning === null || reasoning > completion))
  ) {
    return { event: null, usageComplete: false };
  }
  const inputTokens = prompt - cached;
  const session = grokSessionIndex.get(sid) ?? grokSessionFallback(sid);
  const row: UsageRow = {
    inputTokens,
    outputTokens: completion,
    cacheReadTokens: cached,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  };
  return {
    event: {
      ts,
      date: dateKey(tsRaw),
      model: session.model,
      project: session.project,
      projectSlug: session.projectSlug,
      sessionId: sid,
      ...row,
      cost: 0,
      dedupeKey: `grok:${sid}:${tsRaw}:${ctx.lineNo}`,
      filePath: ctx.filePath,
      lineOffset: ctx.lineNo,
      source: "grok",
      tokenClasses: {
        input: inputTokens,
        cachedRead: cached,
        cacheWrite: null,
        visibleOutput: hasReasoning && reasoning !== null ? completion - reasoning : null,
        reasoningOutput: reasoning,
        complete: false,
      },
    },
    usageComplete: true,
  };
}

interface CodexForkEvidence {
  sourceParentId: string | undefined;
  topLevelParentId: string | undefined;
  legacyThreadSourceParentId: string | undefined;
  threadSource: "subagent" | "user" | undefined;
  depth: number | undefined;
  hasThreadSpawn: boolean;
  hasEvidence: boolean;
  invalid: boolean;
}

function codexForkEvidence(payload: Record<string, unknown>): CodexForkEvidence {
  const source = asRecord(payload.source);
  const subagent = asRecord(source?.subagent);
  const rawThreadSpawn = subagent?.thread_spawn;
  const hasThreadSpawn = rawThreadSpawn !== null && rawThreadSpawn !== undefined;
  const threadSpawn = asRecord(rawThreadSpawn);
  const sourceParentId = asString(threadSpawn?.parent_thread_id);
  const depth = threadSpawn?.depth;
  const hasTopLevelParentField = Object.hasOwn(payload, "parent_thread_id");
  const rawTopLevelParent = payload.parent_thread_id;
  const topLevelParentId = asString(rawTopLevelParent);
  const hasThreadSourceField = Object.hasOwn(payload, "thread_source");
  const rawThreadSource = payload.thread_source;
  const legacyThreadSource = asRecord(rawThreadSource);
  const legacyThreadSourceParentId = asString(legacyThreadSource?.parent_thread_id);
  const threadSource =
    rawThreadSource === "user" || rawThreadSource === "subagent" ? rawThreadSource : undefined;
  const invalidThreadSpawn =
    hasThreadSpawn &&
    (!threadSpawn ||
      !sourceParentId ||
      typeof depth !== "number" ||
      !Number.isSafeInteger(depth) ||
      (depth !== 1 && depth !== 2));
  const invalidTopLevelParent =
    hasTopLevelParentField &&
    rawTopLevelParent !== null &&
    rawTopLevelParent !== undefined &&
    topLevelParentId === undefined;
  const invalidThreadSource =
    hasThreadSourceField &&
    (rawThreadSource === null ||
      rawThreadSource === undefined ||
      (threadSource === undefined && legacyThreadSourceParentId === undefined));
  return {
    sourceParentId,
    topLevelParentId,
    legacyThreadSourceParentId,
    threadSource,
    depth: typeof depth === "number" && Number.isSafeInteger(depth) ? depth : undefined,
    hasThreadSpawn,
    hasEvidence:
      hasThreadSpawn ||
      topLevelParentId !== undefined ||
      legacyThreadSourceParentId !== undefined ||
      threadSource === "subagent",
    invalid:
      invalidThreadSpawn ||
      invalidTopLevelParent ||
      invalidThreadSource ||
      (sourceParentId !== undefined &&
        topLevelParentId !== undefined &&
        sourceParentId !== topLevelParentId) ||
      (legacyThreadSourceParentId !== undefined &&
        ((sourceParentId !== undefined && legacyThreadSourceParentId !== sourceParentId) ||
          (topLevelParentId !== undefined && legacyThreadSourceParentId !== topLevelParentId))),
  };
}

function quarantineCodexFork(state: CodexMetadata): void {
  state.phase = "ambiguous";
  state.firstLiveRowPending = false;
  state.usageComplete = false;
}

function classifyCodexSession(line: CodexLine, state: CodexMetadata): void {
  if (line.type !== "session_meta") return;
  // Classification is monotonic. In particular, weak or malformed metadata
  // must never become billable merely because a later row looks stronger.
  if (state.phase === "ambiguous") return;
  const payload = asRecord(line.payload);
  if (!payload) return;
  const childId = asString(payload.id);
  const sessionId = asString(payload.session_id);
  const forkParentId = asString(payload.forked_from_id);
  const evidence = codexForkEvidence(payload);
  const hasForkField = Object.hasOwn(payload, "forked_from_id");
  const hasInvalidForkValue =
    hasForkField &&
    payload.forked_from_id !== null &&
    payload.forked_from_id !== undefined &&
    forkParentId === undefined;
  const hasTopLevelParentField = Object.hasOwn(payload, "parent_thread_id");
  const corroborates = (parentId: string): boolean =>
    (!evidence.legacyThreadSourceParentId || evidence.legacyThreadSourceParentId === parentId) &&
    (!evidence.topLevelParentId || evidence.topLevelParentId === parentId);
  const depthOneFork = Boolean(
    childId &&
      sessionId &&
      childId !== sessionId &&
      state.tokenCountRows === 0 &&
      !evidence.invalid &&
      (evidence.threadSource === "subagent" || evidence.legacyThreadSourceParentId === sessionId) &&
      evidence.hasThreadSpawn &&
      evidence.depth === 1 &&
      evidence.sourceParentId === sessionId &&
      corroborates(sessionId) &&
      hasTopLevelParentField &&
      evidence.topLevelParentId === sessionId &&
      (!hasForkField || (hasForkField && forkParentId === sessionId)),
  );
  const depthTwoFork = Boolean(
    childId &&
      sessionId &&
      forkParentId &&
      childId !== forkParentId &&
      childId !== sessionId &&
      forkParentId !== sessionId &&
      state.tokenCountRows === 0 &&
      !evidence.invalid &&
      evidence.threadSource === "subagent" &&
      evidence.hasThreadSpawn &&
      evidence.depth === 2 &&
      hasForkField &&
      evidence.sourceParentId === forkParentId &&
      hasTopLevelParentField &&
      evidence.topLevelParentId === forkParentId &&
      corroborates(forkParentId),
  );
  const strongFork = depthOneFork || depthTwoFork;
  const observedChildId = childId ?? (!forkParentId ? sessionId : undefined);

  if (state.phase === "inherited" && state.tokenCountRows === 0) {
    const rootSessionId = state.rootSessionId ?? state.forkParentId;
    const copiedDepthOneParent = Boolean(
      state.lineageDepth === 2 &&
        state.lineageReplayStage === "await-parent" &&
        state.forkParentId &&
        rootSessionId &&
        childId === state.forkParentId &&
        sessionId === rootSessionId &&
        !hasInvalidForkValue &&
        hasForkField &&
        forkParentId === rootSessionId &&
        hasTopLevelParentField &&
        evidence.topLevelParentId === rootSessionId &&
        !evidence.invalid &&
        evidence.threadSource === "subagent" &&
        evidence.depth === 1 &&
        evidence.sourceParentId === rootSessionId &&
        corroborates(rootSessionId),
    );
    if (copiedDepthOneParent) {
      state.lineageReplayStage = "await-root";
      return;
    }
    const copiedRoot = Boolean(
      state.lineageReplayStage !== "await-parent" &&
        rootSessionId &&
        childId === rootSessionId &&
        sessionId === rootSessionId &&
        !hasInvalidForkValue &&
        !hasForkField &&
        !hasTopLevelParentField &&
        evidence.threadSource === "user" &&
        !evidence.hasEvidence &&
        !evidence.invalid,
    );
    if (copiedRoot) {
      state.lineageReplayStage = "ready";
      return;
    }
  }

  if (state.childSessionId !== null) {
    if (
      !observedChildId ||
      observedChildId !== state.childSessionId ||
      (state.forkParentId !== null
        ? !strongFork || (forkParentId ?? evidence.sourceParentId) !== state.forkParentId
        : strongFork || forkParentId !== undefined || evidence.hasEvidence || evidence.invalid)
    ) {
      quarantineCodexFork(state);
    }
    return;
  }

  if (strongFork && childId && sessionId && evidence.sourceParentId) {
    state.childSessionId = childId;
    state.forkParentId = evidence.sourceParentId;
    state.rootSessionId = sessionId;
    state.lineageDepth = depthTwoFork ? 2 : 1;
    state.lineageReplayStage = depthTwoFork ? "await-parent" : "ready";
    state.sessionId = childId;
    state.phase = "inherited";
    state.firstLiveRowPending = false;
    state.sessionTimestampMs =
      state.sessionTimestampMs ?? _codexSessionTimestampFromUuidForTest(sessionId);
    return;
  }

  if (
    hasInvalidForkValue ||
    forkParentId !== undefined ||
    evidence.hasEvidence ||
    evidence.invalid ||
    (childId !== undefined && sessionId !== undefined && childId !== sessionId) ||
    !observedChildId
  ) {
    quarantineCodexFork(state);
    return;
  }

  state.childSessionId = observedChildId;
  state.sessionId = observedChildId;
  state.sessionTimestampMs =
    state.sessionTimestampMs ?? _codexSessionTimestampFromUuidForTest(sessionId);
}

function updateCodexMetadata(line: CodexLine, state: CodexMetadata): void {
  const payload = asRecord(line.payload);
  if (!payload) return;
  classifyCodexSession(line, state);
  const model = asString(payload.model) || state.model;
  const cwd = asString(payload.cwd) || state.cwd;
  const projectSlug = cwdToProjectSlug(cwd);
  state.model = model;
  state.cwd = cwd;
  state.projectSlug = projectSlug;
  state.project = projectShortName(projectSlug);
}

function codexTimestampFrom(line: CodexLine, state: CodexMetadata): number | null {
  return _parseCodexExplicitTimestampForTest(line.timestamp) ?? state.sessionTimestampMs;
}

function codexUsageFrom(line: CodexLine): CodexUsageEnvelope {
  const payload = asRecord(line.payload);
  const info = asRecord(payload?.info);
  return {
    last: info?.last_token_usage,
    total: info?.total_token_usage,
    hasLast: Boolean(info && "last_token_usage" in info),
    hasTotal: Boolean(info && "total_token_usage" in info),
  };
}

function isCodexRateLimitOnly(payload: Record<string, unknown> | null): boolean {
  return payload?.info === null && asRecord(payload.rate_limits) !== null;
}

function codexTotalsFromUsage(value: unknown): CodexTotals | null {
  const usage = asRecord(value);
  if (!usage) return null;
  const inputTokens = tokenCount(usage.input_tokens);
  const cachedInputTokens = tokenCount(usage.cached_input_tokens);
  const outputTokens = tokenCount(usage.output_tokens);
  const hasReasoning = "reasoning_output_tokens" in usage;
  const reasoningOutputTokens = hasReasoning ? tokenCount(usage.reasoning_output_tokens) : null;
  const hasDeclaredTotal = "total_tokens" in usage;
  const declaredTotal = hasDeclaredTotal ? tokenCount(usage.total_tokens) : null;
  const computedTotal =
    inputTokens === null || outputTokens === null ? null : inputTokens + outputTokens;
  if (
    inputTokens === null ||
    cachedInputTokens === null ||
    outputTokens === null ||
    computedTotal === null ||
    !Number.isSafeInteger(computedTotal) ||
    (hasReasoning && reasoningOutputTokens === null) ||
    (hasDeclaredTotal && (declaredTotal === null || declaredTotal !== computedTotal)) ||
    cachedInputTokens > inputTokens ||
    (reasoningOutputTokens !== null && reasoningOutputTokens > outputTokens)
  ) {
    return null;
  }
  return {
    inputTokens,
    cachedInputTokens,
    outputTokens,
    reasoningOutputTokens,
  };
}

function addCodexTotals(left: CodexTotals, right: CodexTotals): CodexTotals | null {
  const inputTokens = left.inputTokens + right.inputTokens;
  const cachedInputTokens = left.cachedInputTokens + right.cachedInputTokens;
  const outputTokens = left.outputTokens + right.outputTokens;
  const reasoningOutputTokens =
    left.reasoningOutputTokens === null || right.reasoningOutputTokens === null
      ? null
      : left.reasoningOutputTokens + right.reasoningOutputTokens;
  if (
    !Number.isSafeInteger(inputTokens) ||
    !Number.isSafeInteger(cachedInputTokens) ||
    !Number.isSafeInteger(outputTokens) ||
    (reasoningOutputTokens !== null && !Number.isSafeInteger(reasoningOutputTokens))
  ) {
    return null;
  }
  return { inputTokens, cachedInputTokens, outputTokens, reasoningOutputTokens };
}

interface CodexNormalizedRow {
  row: UsageRow;
  classes: NormalizedTokenClasses;
}

interface CodexRowResult {
  normalized: CodexNormalizedRow | null;
  usageComplete: boolean;
  disposition: Extract<
    CodexUsageDisposition,
    "billedUsageRows" | "zeroNoopRows" | "unrecoverableResetRows" | "invalidUsageRows"
  >;
}

function codexDispositionTotal(state: CodexMetadata): number {
  return (
    state.billedUsageRows +
    state.inheritedSuppressedRows +
    state.ambiguousQuarantinedRows +
    state.zeroNoopRows +
    state.unrecoverableResetRows +
    state.invalidUsageRows
  );
}

function recordCodexDisposition(
  state: CodexMetadata,
  disposition: CodexUsageDisposition,
  line: string,
): void {
  state.tokenCountRows += 1;
  state[disposition] += 1;
  state.rollingDigest = createHash("sha256")
    .update(state.rollingDigest)
    .update("\0")
    .update(disposition)
    .update("\0")
    .update(line)
    .digest("hex");
  if (state.tokenCountRows !== codexDispositionTotal(state)) {
    throw new Error("Codex token_count disposition conservation failed");
  }
}

function advanceCodexInheritedBaseline(
  envelope: CodexUsageEnvelope,
  state: CodexMetadata,
): boolean {
  if (envelope.hasTotal) {
    const current = codexTotalsFromUsage(envelope.total);
    if (!current) return false;
    state.totals = current;
    return true;
  }
  if (!envelope.hasLast) return false;
  const last = codexTotalsFromUsage(envelope.last);
  if (!last) return false;
  const accumulated = state.totals ? addCodexTotals(state.totals, last) : last;
  if (!accumulated) return false;
  state.totals = accumulated;
  return true;
}

function codexCumulativeDecreased(current: CodexTotals, previous: CodexTotals): boolean {
  if (
    current.inputTokens < previous.inputTokens ||
    current.cachedInputTokens < previous.cachedInputTokens ||
    current.outputTokens < previous.outputTokens ||
    current.inputTokens - current.cachedInputTokens <
      previous.inputTokens - previous.cachedInputTokens
  ) {
    return true;
  }
  if (
    current.reasoningOutputTokens !== null &&
    previous.reasoningOutputTokens !== null &&
    (current.reasoningOutputTokens < previous.reasoningOutputTokens ||
      current.outputTokens - current.reasoningOutputTokens <
        previous.outputTokens - previous.reasoningOutputTokens)
  ) {
    return true;
  }
  return false;
}

function codexLastCoherentWithCurrent(last: CodexTotals, current: CodexTotals): boolean {
  if (
    last.inputTokens > current.inputTokens ||
    last.cachedInputTokens > current.cachedInputTokens ||
    last.outputTokens > current.outputTokens ||
    last.inputTokens - last.cachedInputTokens > current.inputTokens - current.cachedInputTokens ||
    (last.reasoningOutputTokens === null) !== (current.reasoningOutputTokens === null)
  ) {
    return false;
  }
  if (last.reasoningOutputTokens === null || current.reasoningOutputTokens === null) return true;
  return (
    last.reasoningOutputTokens <= current.reasoningOutputTokens &&
    last.outputTokens - last.reasoningOutputTokens <=
      current.outputTokens - current.reasoningOutputTokens
  );
}

function codexRowFromUsage(envelope: CodexUsageEnvelope, state: CodexMetadata): CodexRowResult {
  if (envelope.hasTotal) {
    const current = codexTotalsFromUsage(envelope.total);
    // A present invalid total is authoritative evidence of a malformed row;
    // it must not fall through to a superficially valid `last` object.
    if (!current) {
      return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
    }
    const previous = state.totals || {
      inputTokens: 0,
      cachedInputTokens: 0,
      outputTokens: 0,
      reasoningOutputTokens: current.reasoningOutputTokens === null ? null : 0,
    };
    state.totals = current;
    // A provider counter reset is a baseline observation, not a usage delta.
    // Reset every component together so asymmetric rollback cannot fabricate
    // cached/active/visible usage. The next monotonic total grows from here.
    if (codexCumulativeDecreased(current, previous)) {
      if (state.phase === "live" && state.firstLiveRowPending) {
        state.firstLiveRowPending = false;
        if (!envelope.hasLast) {
          return {
            normalized: null,
            usageComplete: false,
            disposition: "unrecoverableResetRows",
          };
        }
        const last = codexTotalsFromUsage(envelope.last);
        if (!last) {
          return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
        }
        if (!codexLastCoherentWithCurrent(last, current)) {
          // `current` remains the authoritative reseeded baseline. An
          // internally valid but impossible last-token decomposition must not
          // be billed or allowed to poison the next cumulative delta.
          return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
        }
        const resetRow: UsageRow = {
          inputTokens: last.inputTokens - last.cachedInputTokens,
          outputTokens: last.outputTokens,
          cacheReadTokens: last.cachedInputTokens,
          cacheWrite5mTokens: 0,
          cacheWrite1hTokens: 0,
        };
        if (!resetRow.inputTokens && !resetRow.outputTokens && !resetRow.cacheReadTokens) {
          return { normalized: null, usageComplete: true, disposition: "zeroNoopRows" };
        }
        return {
          usageComplete: true,
          disposition: "billedUsageRows",
          normalized: {
            row: resetRow,
            classes: {
              input: resetRow.inputTokens,
              cachedRead: resetRow.cacheReadTokens,
              cacheWrite: null,
              visibleOutput:
                last.reasoningOutputTokens === null
                  ? null
                  : resetRow.outputTokens - last.reasoningOutputTokens,
              reasoningOutput: last.reasoningOutputTokens,
              complete: false,
            },
          },
        };
      }
      state.firstLiveRowPending = false;
      return { normalized: null, usageComplete: true, disposition: "zeroNoopRows" };
    }
    state.firstLiveRowPending = false;
    const inputTokens =
      current.inputTokens -
      current.cachedInputTokens -
      (previous.inputTokens - previous.cachedInputTokens);
    const outputTokens = current.outputTokens - previous.outputTokens;
    const reasoningOutput =
      current.reasoningOutputTokens !== null && previous.reasoningOutputTokens !== null
        ? current.reasoningOutputTokens - previous.reasoningOutputTokens
        : null;
    const row: UsageRow = {
      inputTokens,
      outputTokens,
      cacheReadTokens: current.cachedInputTokens - previous.cachedInputTokens,
      cacheWrite5mTokens: 0,
      cacheWrite1hTokens: 0,
    };
    return {
      usageComplete: true,
      disposition:
        row.inputTokens || row.outputTokens || row.cacheReadTokens
          ? "billedUsageRows"
          : "zeroNoopRows",
      normalized:
        row.inputTokens || row.outputTokens || row.cacheReadTokens
          ? {
              row,
              classes: {
                input: row.inputTokens,
                cachedRead: row.cacheReadTokens,
                cacheWrite: null,
                visibleOutput: reasoningOutput === null ? null : outputTokens - reasoningOutput,
                reasoningOutput,
                complete: false,
              },
            }
          : null,
    };
  }
  state.firstLiveRowPending = false;
  if (!envelope.hasLast) {
    return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
  }
  const last = codexTotalsFromUsage(envelope.last);
  if (!last) {
    return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
  }
  const accumulated = state.totals ? addCodexTotals(state.totals, last) : last;
  if (!accumulated) {
    return { normalized: null, usageComplete: false, disposition: "invalidUsageRows" };
  }
  // Last-only rows are billable immediately, but also form the cumulative
  // baseline if a later row starts exposing total_token_usage.
  state.totals = accumulated;
  const row: UsageRow = {
    inputTokens: last.inputTokens - last.cachedInputTokens,
    outputTokens: last.outputTokens,
    cacheReadTokens: last.cachedInputTokens,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
  };
  if (!row.inputTokens && !row.outputTokens && !row.cacheReadTokens) {
    return { normalized: null, usageComplete: true, disposition: "zeroNoopRows" };
  }
  return {
    usageComplete: true,
    disposition: "billedUsageRows",
    normalized: {
      row,
      classes: {
        input: row.inputTokens,
        cachedRead: row.cacheReadTokens,
        cacheWrite: null,
        visibleOutput:
          last.reasoningOutputTokens === null
            ? null
            : row.outputTokens - last.reasoningOutputTokens,
        reasoningOutput: last.reasoningOutputTokens,
        complete: false,
      },
    },
  };
}

function parseCodexLine(
  line: string,
  ctx: {
    filePath: string;
    lineNo: number;
    byteStart: number;
    state: CodexMetadata;
    allowMissingDepthTwoAncestors?: boolean;
  },
): ParsedLine {
  if (!line) return { event: null, usageComplete: true };
  let parsed: unknown;
  try {
    parsed = JSON.parse(line) as unknown;
  } catch {
    ctx.state.usageComplete = false;
    return { event: null, usageComplete: false };
  }
  const o = asRecord(parsed) as CodexLine | null;
  if (!o) return { event: null, usageComplete: true };

  updateCodexMetadata(o, ctx.state);
  const payload = asRecord(o.payload);
  if (
    o.type === "inter_agent_communication_metadata" &&
    payload?.trigger_turn === true &&
    ctx.state.phase === "inherited" &&
    (ctx.state.lineageReplayStage === undefined || ctx.state.lineageReplayStage === "ready")
  ) {
    ctx.state.phase = "live";
    ctx.state.boundaryByte = ctx.byteStart;
    ctx.state.boundaryLine = ctx.lineNo;
    ctx.state.firstLiveRowPending = true;
  }
  if (payload?.type !== "token_count") return { event: null, usageComplete: true };
  if (isCodexRateLimitOnly(payload)) {
    recordCodexDisposition(ctx.state, "zeroNoopRows", line);
    return { event: null, usageComplete: true };
  }
  if (
    ctx.allowMissingDepthTwoAncestors === true &&
    ctx.state.phase === "inherited" &&
    ctx.state.lineageDepth === 2 &&
    ctx.state.lineageReplayStage === "await-parent"
  ) {
    // Only an exact deployed-checkpoint replay may rely on the already
    // corroborated depth-2 child envelope when copied ancestor rows are absent.
    ctx.state.lineageReplayStage = "ready";
  }
  if (ctx.state.phase === "ambiguous") {
    recordCodexDisposition(ctx.state, "ambiguousQuarantinedRows", line);
    ctx.state.usageComplete = false;
    return { event: null, usageComplete: false };
  }
  if (
    ctx.state.phase === "inherited" &&
    ctx.state.lineageReplayStage !== undefined &&
    ctx.state.lineageReplayStage !== "ready"
  ) {
    quarantineCodexFork(ctx.state);
    recordCodexDisposition(ctx.state, "ambiguousQuarantinedRows", line);
    return { event: null, usageComplete: false };
  }
  const envelope = codexUsageFrom(o);
  if (ctx.state.phase === "inherited") {
    const complete = advanceCodexInheritedBaseline(envelope, ctx.state);
    const disposition: CodexUsageDisposition = complete
      ? "inheritedSuppressedRows"
      : "invalidUsageRows";
    recordCodexDisposition(ctx.state, disposition, line);
    ctx.state.usageComplete &&= complete;
    return { event: null, usageComplete: complete };
  }
  const ts = codexTimestampFrom(o, ctx.state);
  if (ts === null) {
    recordCodexDisposition(ctx.state, "invalidUsageRows", line);
    ctx.state.usageComplete = false;
    return { event: null, usageComplete: false };
  }
  const result = codexRowFromUsage(envelope, ctx.state);
  recordCodexDisposition(ctx.state, result.disposition, line);
  ctx.state.usageComplete &&= result.usageComplete;
  const normalized = result.normalized;
  if (!normalized) return { event: null, usageComplete: result.usageComplete };
  const { row } = normalized;

  const cost = costFor(ctx.state.model, row);

  return {
    event: {
      ts,
      date: dateKey(new Date(ts).toISOString()),
      model: ctx.state.model,
      project: ctx.state.project,
      projectSlug: ctx.state.projectSlug,
      sessionId: ctx.state.sessionId,
      ...row,
      cost,
      dedupeKey: null,
      filePath: ctx.filePath,
      lineOffset: ctx.lineNo,
      source: "codex",
      tokenClasses: normalized.classes,
    },
    usageComplete: result.usageComplete,
  };
}

interface FileEntry {
  paths: string[];
  representativePath: string;
  slug: string;
  source: TokenSource;
  device: string;
  inode: string;
}

interface DiscoveryResult {
  files: FileEntry[];
  source: TokenSource;
  available: boolean;
  complete: boolean;
}

const DISCOVERY_MAX_DEPTH = 12;
const DISCOVERY_MAX_PATHS = 20_000;
const CAPTURE_MAX_BYTES = 128 * 1024 * 1024;

async function discoverJsonl(root: string, source: TokenSource): Promise<DiscoveryResult> {
  const byIdentity = new Map<string, FileEntry>();
  const visitedDirectories = new Set<string>();
  let complete = true;
  let discoveredPaths = 0;
  try {
    const rootStat = await stat(root);
    if (!rootStat.isDirectory()) return { files: [], source, available: false, complete: false };
  } catch {
    return { files: [], source, available: false, complete: false };
  }

  async function walk(dir: string, depth: number): Promise<void> {
    if (depth > DISCOVERY_MAX_DEPTH) {
      complete = false;
      return;
    }
    let dirStat: Awaited<ReturnType<typeof stat>>;
    let entries: Dirent[];
    try {
      dirStat = await stat(dir);
      const directoryIdentity = `${dirStat.dev}:${dirStat.ino}`;
      if (visitedDirectories.has(directoryIdentity)) return;
      visitedDirectories.add(directoryIdentity);
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      complete = false;
      return;
    }
    entries.sort((a, b) => a.name.localeCompare(b.name));
    for (const e of entries) {
      const path = join(dir, e.name);
      let entryStat: Awaited<ReturnType<typeof stat>>;
      try {
        entryStat = await stat(path);
      } catch {
        complete = false;
        continue;
      }
      if (entryStat.isDirectory()) {
        await walk(path, depth + 1);
      } else if (entryStat.isFile() && e.name.endsWith(".jsonl")) {
        discoveredPaths += 1;
        if (discoveredPaths > DISCOVERY_MAX_PATHS) {
          complete = false;
          return;
        }
        const identity = `${entryStat.dev}:${entryStat.ino}`;
        const rel = relative(root, path);
        const slug = source === "claude" ? rel.split(sep)[0] || "claude" : "codex";
        const existing = byIdentity.get(identity);
        if (existing) {
          existing.paths.push(path);
          existing.paths.sort();
          existing.representativePath = existing.paths[0]!;
        } else {
          byIdentity.set(identity, {
            paths: [path],
            representativePath: path,
            slug,
            source,
            device: String(entryStat.dev),
            inode: String(entryStat.ino),
          });
        }
      }
    }
  }
  await walk(root, 0);
  return {
    files: [...byIdentity.values()].sort((a, b) =>
      a.representativePath.localeCompare(b.representativePath),
    ),
    source,
    available: true,
    complete,
  };
}

async function discoverGrokUsage(): Promise<DiscoveryResult> {
  const logsRoot = grokLogsRoot();
  try {
    const rootStat = await stat(logsRoot);
    if (!rootStat.isDirectory()) {
      return { files: [], source: "grok", available: false, complete: false };
    }
  } catch {
    return { files: [], source: "grok", available: false, complete: false };
  }
  const unified = join(logsRoot, "unified.jsonl");
  try {
    const fileStat = await stat(unified);
    if (!fileStat.isFile()) return { files: [], source: "grok", available: true, complete: true };
    return {
      files: [
        {
          paths: [unified],
          representativePath: unified,
          slug: "grok",
          source: "grok",
          device: String(fileStat.dev),
          inode: String(fileStat.ino),
        },
      ],
      source: "grok",
      available: true,
      complete: true,
    };
  } catch {
    return { files: [], source: "grok", available: true, complete: true };
  }
}

async function refreshGrokSessionIndex(): Promise<void> {
  grokSessionIndex.clear();
  const root = grokSessionsRoot();
  const visitedDirectories = new Set<string>();
  let discoveredPaths = 0;

  async function walk(dir: string, depth: number): Promise<void> {
    if (depth > DISCOVERY_MAX_DEPTH) return;
    let dirStat: Awaited<ReturnType<typeof stat>>;
    let entries: Dirent[];
    try {
      dirStat = await stat(dir);
      const directoryIdentity = `${dirStat.dev}:${dirStat.ino}`;
      if (visitedDirectories.has(directoryIdentity)) return;
      visitedDirectories.add(directoryIdentity);
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    entries.sort((a, b) => a.name.localeCompare(b.name));
    for (const entry of entries) {
      const path = join(dir, entry.name);
      let entryStat: Awaited<ReturnType<typeof stat>>;
      try {
        entryStat = await stat(path);
      } catch {
        continue;
      }
      if (entryStat.isDirectory()) {
        await walk(path, depth + 1);
        continue;
      }
      if (!entryStat.isFile() || entry.name !== "summary.json") continue;
      discoveredPaths += 1;
      if (discoveredPaths > DISCOVERY_MAX_PATHS) return;
      const sid = path.split(sep).slice(-2, -1)[0];
      if (!sid) continue;
      try {
        const raw = asRecord(JSON.parse(await readFile(path, "utf8")));
        const model = asString(raw?.current_model_id) || "unknown";
        const gitRoot = asString(raw?.git_root_dir);
        const encodedWorkspace = relative(root, join(path, "..", ".."));
        let cwd = gitRoot || "";
        if (!cwd && encodedWorkspace && encodedWorkspace !== ".") {
          try {
            cwd = decodeURIComponent(encodedWorkspace.split(sep)[0] || "");
          } catch {
            cwd = "";
          }
        }
        const projectSlug = cwdToProjectSlug(cwd || "grok");
        grokSessionIndex.set(sid, {
          model,
          cwd,
          project: projectShortName(projectSlug),
          projectSlug,
        });
      } catch {
        grokSessionIndex.set(sid, grokSessionFallback(sid));
      }
    }
  }

  try {
    const rootStat = await stat(root);
    if (!rootStat.isDirectory()) return;
  } catch {
    return;
  }
  await walk(root, 0);
}

async function readCapturedBytes(
  fd: Awaited<ReturnType<typeof open>>,
  capturedSize: number,
): Promise<Buffer> {
  if (capturedSize > CAPTURE_MAX_BYTES) {
    throw new Error(`token source exceeds ${CAPTURE_MAX_BYTES} byte capture bound`);
  }
  const buffer = Buffer.alloc(capturedSize);
  let position = 0;
  while (position < capturedSize) {
    const result = await fd.read(buffer, position, capturedSize - position, position);
    if (result.bytesRead === 0) throw new Error("token source short read");
    position += result.bytesRead;
  }
  return buffer;
}

function cloneCodexState(state: CodexMetadata): CodexMetadata {
  return {
    ...state,
    totals: state.totals ? { ...state.totals } : null,
    defectLineNumbers: [...(state.defectLineNumbers ?? [])],
  };
}

function validPersistedCodexTotals(value: unknown): value is CodexTotals | null {
  if (value === null) return true;
  const totals = asRecord(value);
  if (!totals) return false;
  const input = tokenCount(totals.inputTokens);
  const cached = tokenCount(totals.cachedInputTokens);
  const output = tokenCount(totals.outputTokens);
  const reasoning =
    totals.reasoningOutputTokens === null ? null : tokenCount(totals.reasoningOutputTokens);
  return (
    input !== null &&
    cached !== null &&
    output !== null &&
    (totals.reasoningOutputTokens === null || reasoning !== null) &&
    cached <= input &&
    (reasoning === null || reasoning <= output)
  );
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function validCurrentCodexState(state: CodexMetadata): boolean {
  const counters = [
    state.tokenCountRows,
    state.billedUsageRows,
    state.inheritedSuppressedRows,
    state.ambiguousQuarantinedRows,
    state.zeroNoopRows,
    state.unrecoverableResetRows,
    state.invalidUsageRows,
  ];
  if (
    !counters.every(isNonNegativeSafeInteger) ||
    state.tokenCountRows !== codexDispositionTotal(state) ||
    (state.childSessionId !== null && typeof state.childSessionId !== "string") ||
    (state.forkParentId !== null && typeof state.forkParentId !== "string") ||
    !["none", "inherited", "live", "ambiguous"].includes(state.phase) ||
    !/^[0-9a-f]{64}$/.test(state.rollingDigest) ||
    typeof state.firstLiveRowPending !== "boolean" ||
    (state.threadSourceRoleRepaired !== undefined &&
      state.threadSourceRoleRepaired !== null &&
      typeof state.threadSourceRoleRepaired !== "boolean") ||
    (state.codexLineageRepaired !== undefined &&
      state.codexLineageRepaired !== null &&
      typeof state.codexLineageRepaired !== "boolean") ||
    (state.depthOneDirectLineageRepaired !== undefined &&
      state.depthOneDirectLineageRepaired !== null &&
      typeof state.depthOneDirectLineageRepaired !== "boolean") ||
    (state.codexLineageShapeRepaired !== undefined &&
      state.codexLineageShapeRepaired !== null &&
      typeof state.codexLineageShapeRepaired !== "boolean") ||
    (state.codexDeployedCheckpointRepaired !== undefined &&
      state.codexDeployedCheckpointRepaired !== null &&
      typeof state.codexDeployedCheckpointRepaired !== "boolean") ||
    (state.lineageDepth !== undefined &&
      state.lineageDepth !== null &&
      state.lineageDepth !== 1 &&
      state.lineageDepth !== 2) ||
    (state.rootSessionId !== undefined &&
      state.rootSessionId !== null &&
      typeof state.rootSessionId !== "string") ||
    (state.lineageReplayStage !== undefined &&
      state.lineageReplayStage !== null &&
      !["await-parent", "await-root", "ready"].includes(state.lineageReplayStage)) ||
    (state.boundaryByte !== null && !isNonNegativeSafeInteger(state.boundaryByte)) ||
    (state.boundaryLine !== null && !isNonNegativeSafeInteger(state.boundaryLine)) ||
    !validUsageDefectCheckpoint(state as unknown as Record<string, unknown>)
  ) {
    return false;
  }
  if (state.childSessionId !== null && state.sessionId !== state.childSessionId) return false;
  if (state.phase === "inherited" || state.phase === "live") {
    if (
      state.childSessionId === null ||
      state.forkParentId === null ||
      state.childSessionId === state.forkParentId
    ) {
      return false;
    }
  } else if (state.phase === "none" && state.forkParentId !== null) {
    return false;
  }
  if (state.phase === "live") {
    if (state.boundaryByte === null || state.boundaryLine === null) return false;
  } else if (state.boundaryByte !== null || state.boundaryLine !== null) {
    return false;
  }
  return !state.firstLiveRowPending || state.phase === "live";
}

function codexStateFromJson(value: string | null): CodexMetadata | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(value) as CodexMetadata;
    const baseValid =
      typeof parsed.sessionId === "string" &&
      typeof parsed.model === "string" &&
      typeof parsed.cwd === "string" &&
      typeof parsed.project === "string" &&
      typeof parsed.projectSlug === "string" &&
      typeof parsed.usageComplete === "boolean" &&
      (parsed.sessionTimestampMs === undefined ||
        parsed.sessionTimestampMs === null ||
        (Number.isSafeInteger(parsed.sessionTimestampMs) &&
          parsed.sessionTimestampMs >= 0 &&
          !Number.isNaN(new Date(parsed.sessionTimestampMs).getTime()))) &&
      (parsed.usageParserVersion === undefined ||
        (Number.isSafeInteger(parsed.usageParserVersion) && parsed.usageParserVersion >= 1)) &&
      validPersistedCodexTotals(parsed.totals);
    if (
      baseValid &&
      (parsed.usageParserVersion === undefined ||
        parsed.usageParserVersion < CODEX_USAGE_PARSER_VERSION ||
        validCurrentCodexState(parsed))
    ) {
      const defectCheckpoint = usageDefectCheckpointFromJson(value, parsed.usageComplete);
      return cloneCodexState({
        ...parsed,
        ...defectCheckpoint,
        sessionTimestampMs: parsed.sessionTimestampMs ?? null,
      });
    }
  } catch {
    // Invalid legacy state is rebuilt from the committed prefix below.
  }
  return null;
}

function rawCodexUsageParserVersion(value: string | null): number | null {
  if (!value) return null;
  try {
    const version = asRecord(JSON.parse(value))?.usageParserVersion;
    return typeof version === "number" && Number.isSafeInteger(version) && version >= 1
      ? version
      : null;
  } catch {
    return null;
  }
}

interface StagedFileResult {
  batch: CapturedFileCommit;
  generationKey: string;
  priorGenerationKeys: string[];
  unchangedAtSnapshot: boolean;
  stagedCodexState?: CodexMetadata;
  usageComplete: boolean;
  newDefectCount?: number;
  defectLineNumbers?: number[];
}

export interface MetadataTrustMountInfo {
  device: bigint;
  filesystemName: string;
}

export interface MetadataTrustAnchor {
  fd: number;
  mountId: string | null;
  device: bigint;
  close(): Promise<void>;
}

export interface MetadataTrustOperations {
  mountIdForFd(fd: number): Promise<string | null>;
  readMountInfo(): Promise<Map<string, MetadataTrustMountInfo>>;
  openAnchorFromFd(fd: number, device: bigint): Promise<MetadataTrustAnchor>;
  statfsTypeForFd(fd: number): Promise<bigint | null>;
}

export interface MetadataTrustContext {
  supports(fd: number, device: bigint): Promise<boolean>;
  close(): Promise<void>;
}

export interface IngestHooks {
  /** Deterministic test seam for mutating a captured file before post-fstat. */
  afterCapturedRead?: (path: string) => void | Promise<void>;
  /** Test-only override for the filesystem support predicate. Production
   * callers omit this and use the fail-closed statfs whitelist below. */
  metadataFastPathSupported?: (path: string) => boolean | Promise<boolean>;
  /** Test-only operation seam for the descriptor-bound trust cache. */
  metadataTrustOperations?: MetadataTrustOperations;
  /** Test-only seam for classifying pending legacy paths before commit. */
  legacyReconciliationStat?: (path: string) => unknown;
  /** Test-only failure seam after one source line has been staged in memory. */
  afterStagedLine?: (path: string, lineNumber: number) => void | Promise<void>;
}

export function classifyLegacyReconciliationPending(
  pending: readonly LegacyReconciliationPendingRow[],
  statFn: (path: string) => unknown = statSync,
): LegacyReconciliationObservation[] {
  return pending.map((row): LegacyReconciliationObservation => {
    try {
      statFn(row.path);
      return { ...row, observed: "readable" };
    } catch (error) {
      const code =
        typeof error === "object" && error !== null && "code" in error
          ? (error as { code?: unknown }).code
          : undefined;
      return { ...row, observed: code === "ENOENT" ? "ENOENT" : "other-error" };
    }
  });
}

interface FileSnapshotMetadata {
  dev: bigint;
  ino: bigint;
  size: bigint;
  mtimeNs: bigint;
  ctimeNs: bigint;
}

function sameFileSnapshot(left: FileSnapshotMetadata, right: FileSnapshotMetadata): boolean {
  return (
    left.dev === right.dev &&
    left.ino === right.ino &&
    left.size === right.size &&
    left.mtimeNs === right.mtimeNs &&
    left.ctimeNs === right.ctimeNs
  );
}

function fileSnapshotMetadata(value: unknown): FileSnapshotMetadata | null {
  if (value === null || typeof value !== "object") return null;
  const candidate = value as Partial<FileSnapshotMetadata>;
  return typeof candidate.dev === "bigint" &&
    typeof candidate.ino === "bigint" &&
    typeof candidate.size === "bigint" &&
    typeof candidate.mtimeNs === "bigint" &&
    typeof candidate.ctimeNs === "bigint"
    ? {
        dev: candidate.dev,
        ino: candidate.ino,
        size: candidate.size,
        mtimeNs: candidate.mtimeNs,
        ctimeNs: candidate.ctimeNs,
      }
    : null;
}

// Only local Linux filesystems with a documented nanosecond inode timestamp
// contract may bypass content verification. Network, FUSE, overlay, unknown
// filesystems, non-Linux platforms, and statfs errors all verify the prefix.
const TRUSTED_LINUX_METADATA_FILESYSTEMS = new Map([
  ["ext4", 0xef53n],
  ["xfs", 0x58465342n],
  ["btrfs", 0x9123683en],
  ["tmpfs", 0x01021994n],
]);

export function _metadataFilesystemTrustedForTest(
  filesystemType: bigint,
  filesystemName: string,
  platform = process.platform,
): boolean {
  return (
    platform === "linux" &&
    TRUSTED_LINUX_METADATA_FILESYSTEMS.get(filesystemName) === BigInt.asUintN(64, filesystemType)
  );
}

function linuxDeviceNumber(majorValue: string, minorValue: string): bigint | null {
  if (!/^\d+$/.test(majorValue) || !/^\d+$/.test(minorValue)) return null;
  try {
    const major = BigInt(majorValue);
    const minor = BigInt(minorValue);
    return (
      ((major & 0xfffff000n) << 32n) |
      ((major & 0xfffn) << 8n) |
      ((minor & 0xffffff00n) << 12n) |
      (minor & 0xffn)
    );
  } catch {
    return null;
  }
}

function parseLinuxMountInfo(value: string): Map<string, MetadataTrustMountInfo> {
  const mounts = new Map<string, MetadataTrustMountInfo>();
  for (const line of value.split("\n")) {
    const separator = line.indexOf(" - ");
    if (separator < 0) continue;
    const fields = line.slice(0, separator).split(" ");
    const mountId = fields[0];
    const deviceParts = fields[2]?.split(":");
    const filesystemName = line.slice(separator + 3).split(" ", 1)[0];
    if (!mountId || !/^\d+$/.test(mountId) || deviceParts?.length !== 2 || !filesystemName) {
      continue;
    }
    const device = linuxDeviceNumber(deviceParts[0]!, deviceParts[1]!);
    if (device !== null) mounts.set(mountId, { device, filesystemName });
  }
  return mounts;
}

async function linuxMountIdForFd(fd: number): Promise<string | null> {
  const fdInfo = await readFile(`/proc/self/fdinfo/${fd}`, "utf8");
  return /^mnt_id:\s*(\d+)$/m.exec(fdInfo)?.[1] ?? null;
}

const PRODUCTION_METADATA_TRUST_OPERATIONS: MetadataTrustOperations = {
  mountIdForFd: linuxMountIdForFd,
  async readMountInfo() {
    return parseLinuxMountInfo(await readFile("/proc/self/mountinfo", "utf8"));
  },
  async openAnchorFromFd(fd) {
    const handle = await open(`/proc/self/fd/${fd}`, "r");
    try {
      const [mountId, anchorStat] = await Promise.all([
        linuxMountIdForFd(handle.fd),
        handle.stat({ bigint: true }),
      ]);
      if (typeof anchorStat.dev !== "bigint") throw new Error("anchor lacks bigint device");
      return {
        fd: handle.fd,
        mountId,
        device: anchorStat.dev,
        close: async () => handle.close(),
      };
    } catch (error) {
      await handle.close().catch(() => {});
      throw error;
    }
  },
  async statfsTypeForFd(fd) {
    const filesystem = await statfs(`/proc/self/fd/${fd}`, { bigint: true });
    return typeof filesystem.type === "bigint" ? filesystem.type : null;
  },
};

function createMetadataTrustContext(
  operations: MetadataTrustOperations,
  platform: NodeJS.Platform,
): MetadataTrustContext {
  const cached = new Map<
    string,
    { device: bigint; supported: boolean; anchor?: MetadataTrustAnchor }
  >();
  let closed = false;
  const closeAnchor = async (anchor: MetadataTrustAnchor): Promise<void> => {
    await anchor.close().catch(() => {});
  };
  return {
    async supports(fd, device) {
      if (closed || platform !== "linux") return false;
      let mountId: string | null;
      try {
        mountId = await operations.mountIdForFd(fd);
      } catch {
        return false;
      }
      if (!mountId) return false;
      const known = cached.get(mountId);
      if (known) return known.device === device && known.supported;

      let anchor: MetadataTrustAnchor | null = null;
      try {
        // An unseen mount is classified from a fresh snapshot while the
        // source fd pins it. Trusted mounts retain a second anchor fd for the
        // rest of this scan, preventing mount-ID reuse after the source closes.
        const mount = (await operations.readMountInfo()).get(mountId);
        if (!mount || mount.device !== device) {
          cached.set(mountId, { device, supported: false });
          return false;
        }
        if (!TRUSTED_LINUX_METADATA_FILESYSTEMS.has(mount.filesystemName)) {
          cached.set(mountId, { device, supported: false });
          return false;
        }
        anchor = await operations.openAnchorFromFd(fd, device);
        if (anchor.mountId !== mountId || anchor.device !== device) {
          await closeAnchor(anchor);
          cached.set(mountId, { device, supported: false });
          return false;
        }
        const filesystemType = await operations.statfsTypeForFd(anchor.fd);
        const supported =
          filesystemType !== null &&
          _metadataFilesystemTrustedForTest(filesystemType, mount.filesystemName, platform);
        if (!supported) {
          await closeAnchor(anchor);
          cached.set(mountId, { device, supported: false });
          return false;
        }
        cached.set(mountId, { device, supported: true, anchor });
        return true;
      } catch {
        if (anchor) await closeAnchor(anchor);
        cached.set(mountId, { device, supported: false });
        return false;
      }
    },
    async close() {
      if (closed) return;
      closed = true;
      await Promise.all(
        [...cached.values()].flatMap((entry) => (entry.anchor ? [closeAnchor(entry.anchor)] : [])),
      );
      cached.clear();
    },
  };
}

export function _createMetadataTrustContextForTest(
  operations: MetadataTrustOperations,
  platform: NodeJS.Platform = process.platform,
): MetadataTrustContext {
  return createMetadataTrustContext(operations, platform);
}

async function metadataFastPathSupported(
  fd: Awaited<ReturnType<typeof open>>,
  path: string,
  hooks: IngestHooks,
  trustContext: MetadataTrustContext,
  device: bigint,
): Promise<boolean> {
  if (hooks.metadataFastPathSupported) return hooks.metadataFastPathSupported(path);
  return trustContext.supports(fd.fd, device);
}

function usageCompleteFromStateJson(source: TokenSource, value: string | null): boolean | null {
  if (source === "codex") return codexStateFromJson(value)?.usageComplete ?? null;
  if (!value) return null;
  try {
    const parsed = asRecord(JSON.parse(value));
    return typeof parsed?.usageComplete === "boolean" ? parsed.usageComplete : null;
  } catch {
    return null;
  }
}

function rawClaudeUsageParserVersion(value: string | null): number | null {
  if (!value) return null;
  try {
    const version = asRecord(JSON.parse(value))?.usageParserVersion;
    return typeof version === "number" && Number.isSafeInteger(version) && version >= 1
      ? version
      : null;
  } catch {
    return null;
  }
}

function validCurrentClaudeCheckpoint(value: string | null): boolean {
  if (!value) return false;
  try {
    const checkpoint = asRecord(JSON.parse(value));
    return Boolean(
      checkpoint &&
        checkpoint.usageParserVersion === CLAUDE_USAGE_PARSER_VERSION &&
        typeof checkpoint.usageComplete === "boolean" &&
        validUsageDefectCheckpoint(checkpoint),
    );
  } catch {
    return false;
  }
}

function claudeCheckpointRequiresReplay(value: string | null): boolean {
  let checkpoint: Record<string, unknown> | null = null;
  try {
    checkpoint = value ? asRecord(JSON.parse(value)) : null;
  } catch {
    checkpoint = null;
  }
  const version = rawClaudeUsageParserVersion(value);
  return (
    (version === null || version < CLAUDE_USAGE_PARSER_VERSION) &&
    checkpoint?.usageComplete !== true
  );
}

async function stageFile(
  file: FileEntry,
  sourceSnapshot: SourceReconciliationSnapshot,
  metadataTrustContext: MetadataTrustContext,
  hooks: IngestHooks,
): Promise<StagedFileResult> {
  const fd = await open(file.representativePath, "r");
  try {
    const preStat = await fd.stat({ bigint: true });
    if (!preStat.isFile()) throw new Error("token source is not a regular file");
    const pre = fileSnapshotMetadata(preStat);
    if (!pre) throw new Error("token source lacks bigint nanosecond metadata");
    if (pre.size > BigInt(CAPTURE_MAX_BYTES)) {
      throw new Error(`token source exceeds ${CAPTURE_MAX_BYTES} byte capture bound`);
    }
    const capturedSize = Number(pre.size);
    const device = pre.dev.toString();
    const inode = pre.ino.toString();
    const priorGenerationKeys = sourceSnapshot.getSourceGenerationKeys(
      file.source,
      device,
      inode,
      file.paths,
    );
    let previous = sourceSnapshot.hasPendingLegacyPath(file.paths)
      ? null
      : sourceSnapshot.getLatestSourceFile(file.source, device, inode);
    const rawCodexVersion =
      file.source === "codex" && previous
        ? rawCodexUsageParserVersion(previous.codexStateJson)
        : null;
    const rawClaudeVersion =
      file.source === "claude" && previous
        ? rawClaudeUsageParserVersion(previous.codexStateJson)
        : null;
    const futureParserCheckpoint = Boolean(
      previous &&
        ((file.source === "codex" &&
          rawCodexVersion !== null &&
          rawCodexVersion > CODEX_USAGE_PARSER_VERSION) ||
          (file.source === "claude" &&
            rawClaudeVersion !== null &&
            rawClaudeVersion > CLAUDE_USAGE_PARSER_VERSION)),
    );
    if (previous && futureParserCheckpoint) {
      // A newer binary owns this checkpoint schema. Do not inspect appended
      // bytes or rewrite any checkpoint field with semantics we cannot know.
      const batch: CapturedFileCommit = {
        state: previous,
        aliases: file.paths,
        events: [],
      };
      return {
        batch,
        generationKey: previous.generationKey,
        priorGenerationKeys,
        unchangedAtSnapshot: sourceSnapshot.isCapturedBatchUnchanged(batch),
        usageComplete: false,
      };
    }
    const invalidCurrentClaudeCheckpoint = Boolean(
      previous &&
        file.source === "claude" &&
        rawClaudeVersion === CLAUDE_USAGE_PARSER_VERSION &&
        !validCurrentClaudeCheckpoint(previous.codexStateJson),
    );
    if (previous && invalidCurrentClaudeCheckpoint) {
      const batch: CapturedFileCommit = {
        state: previous,
        aliases: file.paths,
        events: [],
      };
      return {
        batch,
        generationKey: previous.generationKey,
        priorGenerationKeys,
        unchangedAtSnapshot: sourceSnapshot.isCapturedBatchUnchanged(batch),
        usageComplete: false,
      };
    }
    const fastPathCodexState =
      file.source === "codex" && previous ? codexStateFromJson(previous.codexStateJson) : null;
    const generationHasOccurrences = Boolean(
      previous && sourceSnapshot.hasGenerationOccurrences(previous.generationKey),
    );
    let codexMigration =
      file.source === "codex" && previous
        ? classifyCodexCheckpointMigration(previous.codexStateJson, generationHasOccurrences)
        : null;
    const replayClaudeGeneration = Boolean(
      file.source === "claude" &&
        previous &&
        claudeCheckpointRequiresReplay(previous.codexStateJson),
    );
    const canReplaceCodexGeneration = Boolean(
      codexMigration?.replacementAuthorized &&
        previous &&
        sourceSnapshot.canStageReplacement("codex", previous.generationKey, file.paths),
    );
    const canReplaceClaudeGeneration = Boolean(
      replayClaudeGeneration &&
        previous &&
        generationHasOccurrences &&
        sourceSnapshot.canStageReplacement("claude", previous.generationKey, file.paths),
    );
    const canReplaceGeneration = canReplaceClaudeGeneration || canReplaceCodexGeneration;
    const checkpointWouldRequireFullReplay = Boolean(
      previous &&
        (replayClaudeGeneration ||
          (file.source === "codex" &&
            codexMigration &&
            (codexMigration.readRequired || codexMigration.reason === "fail-closed"))),
    );
    if (
      previous &&
      generationHasOccurrences &&
      checkpointWouldRequireFullReplay &&
      !canReplaceGeneration
    ) {
      // Historical replacement is fail-closed unless this is the sole active
      // generation with exact active aliases. Deeper occurrence/source checks
      // are repeated inside the eventual write transaction.
      const batch: CapturedFileCommit = {
        state: previous,
        aliases: file.paths,
        events: [],
      };
      return {
        batch,
        generationKey: previous.generationKey,
        priorGenerationKeys,
        unchangedAtSnapshot: sourceSnapshot.isCapturedBatchUnchanged(batch),
        usageComplete: false,
      };
    }
    const fastPathUsageComplete = previous
      ? file.source === "codex"
        ? (fastPathCodexState?.usageComplete ?? null)
        : usageCompleteFromStateJson(file.source, previous.codexStateJson)
      : null;
    if (
      previous &&
      previous.mtimeNs !== null &&
      previous.ctimeNs !== null &&
      BigInt(previous.capturedSize) === pre.size &&
      previous.mtimeNs === pre.mtimeNs.toString() &&
      previous.ctimeNs === pre.ctimeNs.toString() &&
      previous.lastByteOffset === capturedSize &&
      fastPathUsageComplete !== null &&
      !replayClaudeGeneration &&
      !codexMigration?.readRequired &&
      codexMigration?.reason !== "fail-closed" &&
      (await metadataFastPathSupported(
        fd,
        file.representativePath,
        hooks,
        metadataTrustContext,
        pre.dev,
      ))
    ) {
      // Seal the metadata-only snapshot with a second fstat. A trailing
      // partial line never enters this branch because its committed offset is
      // smaller than capturedSize, so it remains eligible for later newline
      // completion.
      const post = fileSnapshotMetadata(await fd.stat({ bigint: true }));
      if (post && sameFileSnapshot(pre, post)) {
        const batch: CapturedFileCommit = {
          state: {
            ...previous,
            representativePath: file.representativePath,
          },
          aliases: file.paths,
          events: [],
        };
        return {
          batch,
          generationKey: previous.generationKey,
          priorGenerationKeys,
          unchangedAtSnapshot: sourceSnapshot.isCapturedBatchUnchanged(batch),
          usageComplete: fastPathUsageComplete,
        };
      }
    }

    const bytes = await readCapturedBytes(fd, capturedSize);
    await hooks.afterCapturedRead?.(file.representativePath);
    const post = fileSnapshotMetadata(await fd.stat({ bigint: true }));
    if (!post || !sameFileSnapshot(pre, post)) {
      throw new Error("token source changed during bounded capture");
    }

    let startOffset = previous?.lastByteOffset ?? 0;
    let lineNo = previous?.lastLineNumber ?? 0;
    if (previous) {
      const prefixMatches =
        startOffset <= capturedSize &&
        sha256(bytes.subarray(0, startOffset)) === previous.committedPrefixSha256;
      if (!prefixMatches) {
        previous = null;
        codexMigration = null;
        startOffset = 0;
        lineNo = 0;
      }
    }
    const generationKey =
      previous?.generationKey ?? `${file.source}:${device}:${inode}:${randomUUID()}`;
    const sessionId = file.representativePath
      .split(sep)
      .pop()!
      .replace(/\.jsonl$/, "");
    const project = projectShortName(file.slug);

    let stagedCodexState: CodexMetadata | undefined;
    const persistedUsageComplete = previous
      ? usageCompleteFromStateJson(file.source, previous.codexStateJson)
      : true;
    const priorDefectCheckpoint = usageDefectCheckpointFromJson(
      previous?.codexStateJson ?? null,
      persistedUsageComplete ?? true,
    );
    const priorDefectLineCount = priorDefectCheckpoint.defectLineCount;
    let defectCheckpoint = priorDefectCheckpoint;
    let replacementExpectedState: SourceFileState | null = null;
    if (file.source === "codex") {
      const persisted = previous ? codexStateFromJson(previous.codexStateJson) : null;
      const replaceHistorical = Boolean(
        previous && codexMigration?.replacementAuthorized && canReplaceCodexGeneration,
      );
      const replayCheckpoint = Boolean(
        previous &&
          (replaceHistorical || (!generationHasOccurrences && codexMigration?.readRequired)),
      );
      if (replayCheckpoint) {
        // Parser migrations always rebuild the bounded source from byte zero.
        // Existing events are replaced only by the atomic generation branch.
        startOffset = 0;
        lineNo = 0;
        if (replaceHistorical && previous) replacementExpectedState = previous;
      }
      stagedCodexState = cloneCodexState(
        !replayCheckpoint && persisted
          ? persisted
          : codexMetadataFor("", sessionId, "gpt-5-codex-unknown"),
      );
      stagedCodexState.usageParserVersion = CODEX_USAGE_PARSER_VERSION;
      defectCheckpoint = stagedCodexState;
    } else if (previous && replayClaudeGeneration) {
      startOffset = 0;
      lineNo = 0;
      defectCheckpoint = {
        semanticUsageComplete: true,
        defectLineCount: 0,
        defectLineNumbers: [],
      };
      if (generationHasOccurrences && canReplaceClaudeGeneration) {
        replacementExpectedState = previous;
      }
    }

    let semanticUsageComplete = defectCheckpoint.semanticUsageComplete;
    if (stagedCodexState) stagedCodexState.usageComplete = semanticUsageComplete;
    let consumed = startOffset;
    const capturedEvents: CapturedEvent[] = [];
    const completeLines: Array<{
      from: number;
      nl: number;
      lineBytes: Buffer;
      line: string;
      syntacticallyValid: boolean;
    }> = [];
    let lineCursor = startOffset;
    while (lineCursor < capturedSize) {
      const nl = bytes.indexOf(0x0a, lineCursor);
      if (nl < 0) break;
      const lineBytes = bytes.subarray(lineCursor, nl);
      const line = lineBytes.toString("utf8");
      let syntacticallyValid = true;
      try {
        JSON.parse(line);
      } catch {
        syntacticallyValid = false;
      }
      completeLines.push({ from: lineCursor, nl, lineBytes, line, syntacticallyValid });
      lineCursor = nl + 1;
    }
    let lastSyntacticallyValidIndex = -1;
    for (const [index, capturedLine] of completeLines.entries()) {
      if (capturedLine.syntacticallyValid) lastSyntacticallyValidIndex = index;
    }
    const terminalMalformedIndex =
      lastSyntacticallyValidIndex + 1 < completeLines.length
        ? lastSyntacticallyValidIndex + 1
        : null;
    const processLineCount = terminalMalformedIndex ?? completeLines.length;
    for (let index = 0; index < processLineCount; index += 1) {
      const capturedLine = completeLines[index]!;
      const { from, nl, lineBytes, line } = capturedLine;
      if (!capturedLine.syntacticallyValid) {
        recordPermanentDefect(defectCheckpoint, lineNo);
        await hooks.afterStagedLine?.(file.representativePath, lineNo);
        consumed = nl + 1;
        lineNo += 1;
        continue;
      }
      const parsedLine =
        file.source === "codex" && stagedCodexState
          ? parseCodexLine(line, {
              filePath: file.representativePath,
              lineNo,
              byteStart: from,
              state: stagedCodexState,
              allowMissingDepthTwoAncestors: isDeployedDepthTwoMigration(
                codexMigration?.reason ?? "none",
              ),
            })
          : file.source === "grok"
            ? parseGrokLine(line, {
                filePath: file.representativePath,
                lineNo,
              })
            : parseClaudeLine(line, {
                filePath: file.representativePath,
                sessionId,
                project,
                projectSlug: file.slug,
                lineNo,
              });
      semanticUsageComplete &&=
        parsedLine.usageComplete && (stagedCodexState?.usageComplete ?? true);
      const event = parsedLine.event;
      if (event) {
        let parsed: Record<string, unknown> | null = null;
        try {
          parsed = asRecord(JSON.parse(line));
        } catch {
          parsed = null;
        }
        const payload = asRecord(parsed?.payload);
        const requestId = asString(parsed?.requestId) ?? asString(payload?.request_id);
        const claudeIdentity =
          file.source === "claude"
            ? claudeProviderRecordIdentity(parsed, { sessionId: event.sessionId })
            : null;
        const canonical =
          claudeIdentity?.canonicalIdentity ??
          `${file.source}\0${event.sessionId}\0${canonicalJson(parsed)}`;
        capturedEvents.push({
          event,
          canonicalKey: sha256(canonical),
          rawRecordSha256: sha256(lineBytes),
          immutableIdentity: claudeIdentity?.immutableIdentity ?? null,
          immutablePayloadKey: claudeIdentity?.immutablePayload
            ? sha256(claudeIdentity.immutablePayload)
            : null,
          logicalMessageKey:
            claudeIdentity?.logicalMessageKey ?? (requestId ? `request:${requestId}` : null),
          collapseRevisions: claudeIdentity?.collapseRevisions ?? false,
          byteStart: from,
          byteEnd: nl + 1,
          lineNumber: lineNo,
        });
      }
      await hooks.afterStagedLine?.(file.representativePath, lineNo);
      consumed = nl + 1;
      lineNo += 1;
    }
    defectCheckpoint.semanticUsageComplete = semanticUsageComplete;
    const terminalIncomplete = terminalMalformedIndex !== null || lineCursor < capturedSize;
    const usageComplete = semanticUsageComplete && !terminalIncomplete;
    if (stagedCodexState) {
      stagedCodexState.semanticUsageComplete = semanticUsageComplete;
      stagedCodexState.defectLineCount = defectCheckpoint.defectLineCount;
      stagedCodexState.defectLineNumbers = [...defectCheckpoint.defectLineNumbers];
      stagedCodexState.usageComplete = usageComplete;
    }
    const replacementReady = Boolean(
      replacementExpectedState &&
        usageComplete &&
        (file.source === "claude" ||
          (stagedCodexState &&
            stagedCodexState.phase !== "ambiguous" &&
            stagedCodexState.tokenCountRows === codexDispositionTotal(stagedCodexState) &&
            stagedCodexState.ambiguousQuarantinedRows === 0 &&
            stagedCodexState.unrecoverableResetRows === 0 &&
            stagedCodexState.invalidUsageRows === 0)),
    );
    const historicalQuarantine = replacementExpectedState !== null && !replacementReady;
    const batch: CapturedFileCommit = {
      state: {
        generationKey,
        source: file.source,
        device,
        inode,
        representativePath: file.representativePath,
        capturedSize,
        // Kept only because v4 databases require the legacy column. The seal
        // and fast path exclusively use the exact decimal nanoseconds below.
        mtimeMs: Number(pre.mtimeNs / 1_000_000n),
        mtimeNs: pre.mtimeNs.toString(),
        ctimeNs: pre.ctimeNs.toString(),
        lastByteOffset: consumed,
        lastLineNumber: lineNo,
        committedPrefixSha256: sha256(bytes.subarray(0, consumed)),
        codexStateJson: stagedCodexState
          ? JSON.stringify(stagedCodexState)
          : JSON.stringify({
              usageParserVersion:
                file.source === "grok" ? GROK_USAGE_PARSER_VERSION : CLAUDE_USAGE_PARSER_VERSION,
              semanticUsageComplete,
              defectLineCount: defectCheckpoint.defectLineCount,
              defectLineNumbers: defectCheckpoint.defectLineNumbers,
              usageComplete,
            }),
      },
      aliases: file.paths,
      events: historicalQuarantine || stagedCodexState?.phase === "ambiguous" ? [] : capturedEvents,
      ...(replacementExpectedState && replacementReady
        ? {
            replacement: {
              expectedState: replacementExpectedState,
              expectedAliases: [...file.paths],
            },
          }
        : {}),
    };
    return {
      batch,
      generationKey,
      priorGenerationKeys,
      unchangedAtSnapshot: sourceSnapshot.isCapturedBatchUnchanged(batch),
      ...(stagedCodexState ? { stagedCodexState } : {}),
      usageComplete,
      ...(defectCheckpoint.defectLineCount > priorDefectLineCount
        ? {
            newDefectCount: defectCheckpoint.defectLineCount - priorDefectLineCount,
            defectLineNumbers: [...defectCheckpoint.defectLineNumbers],
          }
        : {}),
    };
  } finally {
    await fd.close();
  }
}

export interface IngestSummary {
  files: number;
  newEvents: number;
  events: StoredEvent[];
  scannedAt: number;
  mutated: boolean;
  revision: number;
}

// A scan stages every discovered file in memory before committing so a
// single scan-wide transaction stays deterministic. On a large corpus (many
// GB of JSONL across 1k+ files) staging + a single terminal commit can run
// past the bounded ingest worker's kill timeout, in which case the worker is
// SIGKILLed before its one commit ever runs and every byte of progress is
// discarded. To make forward progress durable across a kill, flush staged
// files as a committed batch periodically during the scan instead of only at
// the very end: each flush is its own `commitScan` transaction, so file_state
// checkpoints for already-flushed files persist even if a later file (or the
// process itself) never finishes.
const SCAN_COMMIT_MAX_BATCH_FILES = 40;
const SCAN_COMMIT_MAX_BATCH_INTERVAL_MS = 20_000;

export async function ingestOnce(
  store: TokenStore = getStore(),
  hooks: IngestHooks = {},
): Promise<IngestSummary> {
  await refreshGrokSessionIndex();
  const discoveries = await Promise.all([
    discoverJsonl(projectsRoot(), "claude"),
    discoverJsonl(codexSessionsRoot(), "codex"),
    discoverGrokUsage(),
  ]);
  const allNew: StoredEvent[] = [];
  const metadataTrustContext = createMetadataTrustContext(
    hooks.metadataTrustOperations ?? PRODUCTION_METADATA_TRUST_OPERATIONS,
    process.platform,
  );

  let mutatedAny = false;
  let latestRevision = store.getRevision();
  let pendingStaged: StagedFileResult[] = [];
  let pendingAliases: DiscoveredAlias[] = [];
  // `reconcileDiscovery` only deactivates a stale alias when its provider is
  // complete, and it deactivates every active alias NOT present in the
  // `aliases` array passed alongside that completeness flag. The final flush
  // of a scan is the only one that ever marks a provider complete, so it is
  // the only one that needs the full-scan alias set -- every file discovered
  // this run, not just the tail staged since the previous mid-scan flush.
  const allAliases: DiscoveredAlias[] = [];
  let chunkSnapshot: SourceReconciliationSnapshot = store.captureSourceReconciliationSnapshot();
  let chunkStartedAt = performance.now();
  // Only files that actually changed since `chunkSnapshot` was captured make
  // a mid-scan flush worthwhile. A steady-state rescan of an unchanged
  // corpus must stay a single `commitScan`/snapshot round trip (verified by
  // test/store-generation-lookup.test.ts) -- forcing a mid-scan flush there
  // would needlessly toggle provider `complete` false-then-true and report a
  // spurious mutation on every rescan.
  let hasPendingWork = false;

  const logDefects = (staged: StagedFileResult[]): void => {
    for (const item of staged) {
      if (!item.newDefectCount) continue;
      const sampledLines = (item.defectLineNumbers ?? []).map((line) => line + 1).join(",");
      console.error(
        `token source permanent JSONL defects source=${item.batch.state.source} path=${item.batch.state.representativePath} added=${item.newDefectCount} total=${usageDefectCheckpointFromJson(item.batch.state.codexStateJson, false).defectLineCount}${sampledLines ? ` first-lines=${sampledLines}` : ""}`,
      );
    }
  };

  // Parser state is process-local acceleration only. Publish it after each
  // chunk's own outer database transaction succeeds, then evict superseded
  // generations without deleting one committed by another staged file in the
  // same chunk.
  const publishCodexAcceleration = (staged: StagedFileResult[]): void => {
    const committedGenerationKeys = new Set(staged.map((item) => item.generationKey));
    for (const item of staged) {
      if (item.stagedCodexState) {
        codexMetadataCache.set(item.generationKey, cloneCodexState(item.stagedCodexState));
      }
    }
    for (const item of staged) {
      for (const priorGenerationKey of item.priorGenerationKeys) {
        if (!committedGenerationKeys.has(priorGenerationKey)) {
          codexMetadataCache.delete(priorGenerationKey);
        }
      }
    }
  };

  /** Commits everything staged since the last flush as one bounded, durable
   * transaction. `providers`/`legacyReconciliationObservations` only carry
   * their discovery-wide meaning on the final flush of a scan: every
   * mid-scan flush marks both sources incomplete so `reconcileDiscovery`
   * never deactivates an alias for a file this run has not staged yet. No
   * file is ever checkpointed as complete except by the commit that actually
   * captured its full staged batch -- a partially staged file simply is not
   * in `pendingStaged` yet, so a kill before its own flush leaves its prior
   * checkpoint (or absence of one) untouched. `aliases` is supplied by the
   * caller rather than derived from `pendingStaged`: a mid-scan flush's
   * `providers` are always incomplete so `reconcileDiscovery` ignores its
   * aliases entirely, while the final, discovery-complete flush must see
   * every alias discovered this run (`allAliases`), not just the tail staged
   * since the previous flush -- otherwise it deactivates every alias an
   * earlier chunk already committed. */
  const flush = (
    providers: DiscoveryProviderState[],
    legacyReconciliationObservations: LegacyReconciliationObservation[],
    aliases: DiscoveredAlias[],
  ): void => {
    const staged = pendingStaged;
    const snapshot = chunkSnapshot;
    pendingStaged = [];
    pendingAliases = [];
    const committed = store.commitScan({
      batches: staged.map((item) => item.batch),
      aliases,
      providers,
      legacyReconciliationObservations,
      sourceSnapshotRevision: snapshot.revision,
      unchangedBatchIndexes: staged.flatMap((item, index) =>
        item.unchangedAtSnapshot ? [index] : [],
      ),
    });
    mutatedAny ||= committed.mutated;
    latestRevision = committed.revision;
    logDefects(staged);
    allNew.push(...committed.insertedEvents);
    publishCodexAcceleration(staged);
  };

  try {
    for (const discovery of discoveries) {
      for (const file of discovery.files) {
        try {
          const result = await stageFile(file, chunkSnapshot, metadataTrustContext, hooks);
          pendingStaged.push(result);
          if (!result.unchangedAtSnapshot) hasPendingWork = true;
          for (const path of file.paths) {
            const alias = { path, generationKey: result.generationKey, source: file.source };
            pendingAliases.push(alias);
            allAliases.push(alias);
          }
        } catch (error) {
          discovery.complete = false;
          console.error(
            `token source reconciliation failed for ${file.representativePath}:`,
            error,
          );
        }
        if (!hasPendingWork) continue;
        const elapsedMs = performance.now() - chunkStartedAt;
        if (
          pendingStaged.length < SCAN_COMMIT_MAX_BATCH_FILES &&
          elapsedMs < SCAN_COMMIT_MAX_BATCH_INTERVAL_MS
        ) {
          continue;
        }
        const midScanProviders = discoveries.map((discoveryEntry) => ({
          source: discoveryEntry.source,
          available: discoveryEntry.available,
          complete: false,
        }));
        try {
          flush(midScanProviders, [], []);
        } catch (error) {
          if (String(error).includes("token record conflict")) throw error;
          console.error("token scan mid-scan commit failed:", error);
          const failedClosed = store.markProvidersIncomplete(midScanProviders);
          mutatedAny ||= failedClosed.mutated;
          latestRevision = failedClosed.revision;
        }
        hasPendingWork = false;
        chunkSnapshot = store.captureSourceReconciliationSnapshot();
        chunkStartedAt = performance.now();
      }
    }
  } finally {
    await metadataTrustContext.close();
  }

  const providers = discoveries.map((discovery) => ({
    source: discovery.source,
    available: discovery.available,
    complete: discovery.complete,
  }));
  const legacyReconciliationObservations = classifyLegacyReconciliationPending(
    chunkSnapshot.pendingLegacyReconciliations,
    hooks.legacyReconciliationStat,
  );
  try {
    flush(providers, legacyReconciliationObservations, allAliases);
  } catch (error) {
    if (String(error).includes("token record conflict")) throw error;
    console.error("token scan commit failed:", error);
    const failedClosed = store.markProvidersIncomplete(providers);
    return {
      files: discoveries.reduce((total, discovery) => total + discovery.files.length, 0),
      newEvents: allNew.length,
      events: allNew,
      scannedAt: Date.now(),
      mutated: mutatedAny || failedClosed.mutated,
      revision: failedClosed.revision,
    };
  }

  return {
    files: discoveries.reduce((total, discovery) => total + discovery.files.length, 0),
    newEvents: allNew.length,
    events: allNew,
    scannedAt: Date.now(),
    mutated: mutatedAny,
    revision: latestRevision,
  };
}

// === Aggregation ===

export interface DailyBucket {
  date: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
}

export interface ModelBucket {
  model: string;
  /** Human-friendly label from normalizeModelId(model), e.g. "Opus 5". */
  displayName: string;
  /** Canonical family key from normalizeModelId(model), e.g. "opus-5". */
  family: string;
  /** Vendor/harness from normalizeModelId(model): "claude" | "codex". Only
   *  ever "unknown" transiently — unrecognized raw ids are routed to
   *  Aggregations.unrecognized instead of appearing here (see the
   *  conservation invariant on Aggregations.unrecognized). */
  harness: Harness;
  /** True for every entry the /api/usage byModel array actually returns
   *  (unrecognized raw ids are routed to Aggregations.unrecognized
   *  instead). The in-memory day/session aggregator below reuses this
   *  same bucket shape before that split, so the field stays a plain
   *  boolean rather than a literal `true`. */
  recognized: boolean;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
  /** Wall-clock seconds between this model's earliest and latest event ts. */
  observedSpanSec: number;
  /** Period-average BURN RATE (totalTokens ÷ observedSpanSec), tok/s — not a
   *  model "generation speed" claim (each event has one timestamp, no
   *  per-message latency). Null when observedSpanSec <= 0 (single event or no
   *  span) — never fabricated as 0. */
  tokensPerSecond: number | null;
}

/** A raw model id that normalizeModelId() could not resolve to a known
 *  family. Tokens are NEVER dropped: everything that isn't a recognized
 *  ModelBucket lands here instead, keyed by its exact raw id, so
 *  sum(byModel[].totalTokens) + sum(unrecognized[].totalTokens) ===
 *  totals.totalTokens always holds. */
export interface UnrecognizedModelBucket {
  rawModel: string;
  totalTokens: number;
  messages: number;
}

/** Per-harness (Claude Code vs Codex vs Grok) usage + throughput bucket.
 *  Keyed and labeled by the same harness identity that populates
 *  ModelBucket.harness (TokenSource is a subset of Harness) — never a
 *  second, divergent derivation. */
export interface HarnessBucket {
  harness: Harness;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
  observedSpanSec: number;
  tokensPerSecond: number | null;
}

export interface ProjectBucket {
  project: string;
  projectSlug: string;
  cwd: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
  sessions: number;
  lastTs: number;
}

export interface SessionBucket {
  sessionId: string;
  project: string;
  projectSlug: string;
  models: string[];
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
  firstTs: number;
  lastTs: number;
}

export interface Totals {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
  /** Wall-clock seconds between the earliest and latest event ts in scope. */
  observedSpanSec: number;
  /** Period-average BURN RATE (totalTokens ÷ observedSpanSec), tok/s — not a
   *  model "generation speed" claim. Null when observedSpanSec <= 0. */
  tokensPerSecond: number | null;
}

export interface Aggregations {
  totals: Totals;
  daily: DailyBucket[];
  byModel: ModelBucket[];
  /** Raw model ids normalizeModelId() could not resolve. See the
   *  conservation invariant documented on UnrecognizedModelBucket. */
  unrecognized: UnrecognizedModelBucket[];
  byProject: ProjectBucket[];
  topSessions: SessionBucket[];
  byHarness: HarnessBucket[];
  scannedAt: number;
  files: number;
  range: { from: string | null; to: string | null };
  /** Price actually applied per model, plus provenance: "table" = matched an
   *  explicit pricing.ts entry, "fallback" = unknown model silently priced
   *  at the Sonnet-tier rate — dashboard/CLI/MCP surfaces should flag
   *  "fallback" entries rather than presenting them as exact. */
  pricing: Record<string, ModelPrice & { source: PriceSource }>;
}

/** Common usage-tally fields shared by Totals / DailyBucket / ModelBucket /
 *  ProjectBucket / SessionBucket / HarnessBucket — factored out so the
 *  per-event accumulation isn't copy-pasted six times over. */
interface UsageAccumulable {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
}

function accumulate(target: UsageAccumulable, ev: StoredEvent, total: number): void {
  target.inputTokens += ev.inputTokens;
  target.outputTokens += ev.outputTokens;
  target.cacheReadTokens += ev.cacheReadTokens;
  target.cacheWrite5mTokens += ev.cacheWrite5mTokens;
  target.cacheWrite1hTokens += ev.cacheWrite1hTokens;
  target.totalTokens += total;
  target.cost += ev.cost;
  target.messages += 1;
}

/** Observed wall-clock span in seconds, from independently-tracked min/max ts
 *  (never assumes sorted input — always Math.min/Math.max, never array
 *  first/last). Never negative; 0 for a single event or an empty set. */
export function spanSecFor(minTs: number, maxTs: number): number {
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return 0;
  return Math.max(0, (maxTs - minTs) / 1000);
}

/** Period-average throughput: totalTokens ÷ observed span (a BURN RATE /
 *  THROUGHPUT figure, tokens per wall-clock second — never a model
 *  "generation speed" claim, since each event carries only one timestamp).
 *  Null — never 0 — when there's no observed span to divide by. */
export function tokensPerSecFor(totalTokens: number, spanSec: number): number | null {
  return spanSec > 0 ? totalTokens / spanSec : null;
}

export function aggregateEvents(
  events: StoredEvent[],
  meta: { files: number; scannedAt: number },
): Aggregations {
  const daily = new Map<string, DailyBucket>();
  const byModel = new Map<string, ModelBucket>();
  const byProject = new Map<string, ProjectBucket>();
  const sessions = new Map<string, SessionBucket>();
  const byHarness = new Map<string, HarnessBucket>();
  const modelSpan = new Map<string, { min: number; max: number }>();
  const harnessSpan = new Map<string, { min: number; max: number }>();
  let totalsMin = Number.POSITIVE_INFINITY;
  let totalsMax = Number.NEGATIVE_INFINITY;

  const totals: Totals = {
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

  for (const ev of events) {
    const total =
      ev.inputTokens +
      ev.outputTokens +
      ev.cacheReadTokens +
      ev.cacheWrite5mTokens +
      ev.cacheWrite1hTokens;

    accumulate(totals, ev, total);
    if (ev.ts < totalsMin) totalsMin = ev.ts;
    if (ev.ts > totalsMax) totalsMax = ev.ts;

    let d = daily.get(ev.date);
    if (!d) {
      d = {
        date: ev.date,
        inputTokens: 0,
        outputTokens: 0,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        totalTokens: 0,
        cost: 0,
        messages: 0,
      };
      daily.set(ev.date, d);
    }
    accumulate(d, ev, total);

    let mb = byModel.get(ev.model);
    if (!mb) {
      const identity = normalizeModelId(ev.model);
      mb = {
        model: ev.model,
        displayName: identity.recognized ? identity.displayName : ev.model,
        family: identity.family,
        harness: identity.harness,
        recognized: identity.recognized,
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
      byModel.set(ev.model, mb);
    }
    accumulate(mb, ev, total);
    let mSpan = modelSpan.get(ev.model);
    if (!mSpan) {
      mSpan = { min: ev.ts, max: ev.ts };
      modelSpan.set(ev.model, mSpan);
    } else {
      if (ev.ts < mSpan.min) mSpan.min = ev.ts;
      if (ev.ts > mSpan.max) mSpan.max = ev.ts;
    }

    let hb = byHarness.get(ev.source);
    if (!hb) {
      hb = {
        harness: ev.source,
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
      byHarness.set(ev.source, hb);
    }
    accumulate(hb, ev, total);
    let hSpan = harnessSpan.get(ev.source);
    if (!hSpan) {
      hSpan = { min: ev.ts, max: ev.ts };
      harnessSpan.set(ev.source, hSpan);
    } else {
      if (ev.ts < hSpan.min) hSpan.min = ev.ts;
      if (ev.ts > hSpan.max) hSpan.max = ev.ts;
    }

    let p = byProject.get(ev.project);
    if (!p) {
      p = {
        project: ev.project,
        projectSlug: ev.projectSlug,
        cwd: slugToProject(ev.projectSlug),
        inputTokens: 0,
        outputTokens: 0,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        totalTokens: 0,
        cost: 0,
        messages: 0,
        sessions: 0,
        lastTs: 0,
      };
      byProject.set(ev.project, p);
    }
    accumulate(p, ev, total);
    if (ev.ts > p.lastTs) p.lastTs = ev.ts;

    let s = sessions.get(ev.sessionId);
    if (!s) {
      s = {
        sessionId: ev.sessionId,
        project: ev.project,
        projectSlug: ev.projectSlug,
        models: [],
        inputTokens: 0,
        outputTokens: 0,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        totalTokens: 0,
        cost: 0,
        messages: 0,
        firstTs: Number.POSITIVE_INFINITY,
        lastTs: 0,
      };
      sessions.set(ev.sessionId, s);
    }
    accumulate(s, ev, total);
    if (!s.models.includes(ev.model)) s.models.push(ev.model);
    if (ev.ts < s.firstTs) s.firstTs = ev.ts;
    if (ev.ts > s.lastTs) s.lastTs = ev.ts;
  }

  for (const s of sessions.values()) {
    const p = byProject.get(s.project);
    if (p) p.sessions += 1;
  }

  totals.observedSpanSec = spanSecFor(totalsMin, totalsMax);
  totals.tokensPerSecond = tokensPerSecFor(totals.totalTokens, totals.observedSpanSec);

  for (const [model, mb] of byModel) {
    const span = modelSpan.get(model);
    const spanSec = span ? spanSecFor(span.min, span.max) : 0;
    mb.observedSpanSec = spanSec;
    mb.tokensPerSecond = tokensPerSecFor(mb.totalTokens, spanSec);
  }

  for (const [source, hb] of byHarness) {
    const span = harnessSpan.get(source);
    const spanSec = span ? spanSecFor(span.min, span.max) : 0;
    hb.observedSpanSec = spanSec;
    hb.tokensPerSecond = tokensPerSecFor(hb.totalTokens, spanSec);
  }

  const dailySorted = [...daily.values()].sort((a, b) => a.date.localeCompare(b.date));
  const modelSorted = [...byModel.values()].sort((a, b) => b.totalTokens - a.totalTokens);
  const projectSorted = [...byProject.values()].sort((a, b) => b.totalTokens - a.totalTokens);
  const sessionSorted = [...sessions.values()].sort((a, b) => b.cost - a.cost).slice(0, 25);
  const harnessSorted = [...byHarness.values()].sort((a, b) => b.totalTokens - a.totalTokens);

  const pricing: Record<string, ModelPrice & { source: PriceSource }> = {};
  for (const m of modelSorted) {
    const priced = priceForWithSource(m.model);
    pricing[m.model] = { ...priced.price, source: priced.source };
  }

  const recognizedModels = modelSorted.filter((m) => m.recognized);
  const unrecognized: UnrecognizedModelBucket[] = modelSorted
    .filter((m) => !m.recognized)
    .map((m) => ({ rawModel: m.model, totalTokens: m.totalTokens, messages: m.messages }));

  return {
    totals,
    daily: dailySorted,
    byModel: recognizedModels,
    unrecognized,
    byProject: projectSorted,
    topSessions: sessionSorted,
    byHarness: harnessSorted,
    scannedAt: meta.scannedAt,
    files: meta.files,
    range: {
      from: dailySorted[0]?.date ?? null,
      to: dailySorted[dailySorted.length - 1]?.date ?? null,
    },
    pricing,
  };
}

export async function loadAggregations(opts?: {
  from?: string;
  to?: string;
  rescan?: boolean;
  models?: string[];
  projects?: string[];
  cache?: boolean;
}): Promise<Aggregations> {
  if (opts?.rescan !== false) {
    invalidateAggregationCache();
    return computeAggregations(opts);
  }
  if (opts.cache === false) {
    invalidateAggregationCache();
    return computeAggregations(opts);
  }

  const ttlMs = aggregationCacheTtlMs();
  const key = aggregationCacheKey(opts);
  const now = Date.now();
  const revision = getStore().getRevision();
  const cached = aggregationCache.get(key);
  const cacheAgeMs = cached ? now - cached.computedAt : -1;
  if (cached && cached.revision === revision && cacheAgeMs >= 0 && cacheAgeMs < ttlMs) {
    return cached.result;
  }

  const generation = aggregationCacheGeneration;
  const inFlightKey = `${generation}:${revision}:${key}`;
  const existing = aggregationInFlight.get(inFlightKey);
  if (existing) return existing;

  // Never "bound" work by forgetting a promise that is still running. Doing
  // so permits a repeated key to launch duplicate scans and lets the forgotten
  // scan race a newer cache generation. The request path is deliberately
  // fail-closed once every admission slot is occupied; callers can retry after
  // an active aggregation completes.
  if (aggregationInFlight.size >= AGGREGATION_IN_FLIGHT_MAX) {
    throw new Error(
      `aggregation admission limit reached (${AGGREGATION_IN_FLIGHT_MAX} active queries)`,
    );
  }

  let pending!: Promise<Aggregations>;
  pending = Promise.resolve()
    .then(() => computeAggregations(opts))
    .then((result) => {
      if (
        generation === aggregationCacheGeneration &&
        revision === getStore().getRevision() &&
        ttlMs > 0
      ) {
        rememberAggregation(key, { result, computedAt: Date.now(), revision });
      }
      return result;
    })
    .finally(() => {
      // Invalidation changes the generation while old work drains. Delete only
      // our own registration so an old finalizer can never erase newer work.
      if (aggregationInFlight.get(inFlightKey) === pending) {
        aggregationInFlight.delete(inFlightKey);
      }
    });
  aggregationInFlight.set(inFlightKey, pending);
  return pending;
}

const AGGREGATION_CACHE_MAX_ENTRIES = 16;
const AGGREGATION_IN_FLIGHT_MAX = 16;
const aggregationCache = new Map<
  string,
  { result: Aggregations; computedAt: number; revision: number }
>();
const aggregationInFlight = new Map<string, Promise<Aggregations>>();
let aggregationCacheGeneration = 0;

function aggregationCacheTtlMs(): number {
  const raw = process.env.TOKENS_USAGE_CACHE_TTL_MS;
  if (raw === undefined || raw.trim() === "") return 60_000;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : 60_000;
}

function aggregationCacheKey(opts: {
  from?: string;
  to?: string;
  models?: string[];
  projects?: string[];
}): string {
  const normalize = (values: string[] | undefined): string[] => [...new Set(values ?? [])].sort();
  return JSON.stringify([
    opts.from ?? null,
    opts.to ?? null,
    normalize(opts.models),
    normalize(opts.projects),
  ]);
}

function rememberAggregation(
  key: string,
  entry: { result: Aggregations; computedAt: number; revision: number },
): void {
  aggregationCache.delete(key);
  aggregationCache.set(key, entry);
  while (aggregationCache.size > AGGREGATION_CACHE_MAX_ENTRIES) {
    const oldest = aggregationCache.keys().next().value;
    if (oldest === undefined) break;
    aggregationCache.delete(oldest);
  }
}

/** Invalidates request-path snapshots without allowing an older in-flight
 * computation to repopulate the new cache generation. */
export function invalidateAggregationCache(): void {
  aggregationCacheGeneration++;
  aggregationCache.clear();
}

export function _aggregationCacheSizeForTest(): number {
  return aggregationCache.size;
}

export function _aggregationInFlightSizeForTest(): number {
  return aggregationInFlight.size;
}

function usageSnapshotToAggregations(
  snapshot: UsageAggregateSnapshot,
  scannedAt: number,
): Aggregations {
  const { minTs: totalsMinTs, maxTs: totalsMaxTs, ...totalUsage } = snapshot.totals;
  const totalsSpan = spanSecFor(
    totalsMinTs ?? Number.POSITIVE_INFINITY,
    totalsMaxTs ?? Number.NEGATIVE_INFINITY,
  );
  const totals: Totals = {
    ...totalUsage,
    observedSpanSec: totalsSpan,
    tokensPerSecond: tokensPerSecFor(totalUsage.totalTokens, totalsSpan),
  };
  const byModel: ModelBucket[] = [];
  const unrecognized: UnrecognizedModelBucket[] = [];
  for (const { minTs, maxTs, ...bucket } of snapshot.byModel) {
    const identity = normalizeModelId(bucket.model);
    if (!identity.recognized) {
      unrecognized.push({
        rawModel: bucket.model,
        totalTokens: bucket.totalTokens,
        messages: bucket.messages,
      });
      continue;
    }
    const observedSpanSec = spanSecFor(minTs, maxTs);
    byModel.push({
      ...bucket,
      displayName: identity.displayName,
      family: identity.family,
      harness: identity.harness,
      recognized: true,
      observedSpanSec,
      tokensPerSecond: tokensPerSecFor(bucket.totalTokens, observedSpanSec),
    });
  }
  const byHarness: HarnessBucket[] = snapshot.byHarness.map(({ minTs, maxTs, ...bucket }) => {
    const observedSpanSec = spanSecFor(minTs, maxTs);
    return {
      ...bucket,
      observedSpanSec,
      tokensPerSecond: tokensPerSecFor(bucket.totalTokens, observedSpanSec),
    };
  });
  const byProject: ProjectBucket[] = snapshot.byProject.map((bucket) => ({
    ...bucket,
    cwd: slugToProject(bucket.projectSlug),
  }));
  const pricing: Record<string, ModelPrice & { source: PriceSource }> = {};
  for (const bucket of byModel) {
    const priced = priceForWithSource(bucket.model);
    pricing[bucket.model] = { ...priced.price, source: priced.source };
  }
  for (const bucket of unrecognized) {
    const priced = priceForWithSource(bucket.rawModel);
    pricing[bucket.rawModel] = { ...priced.price, source: priced.source };
  }
  return {
    totals,
    daily: snapshot.daily,
    byModel,
    unrecognized,
    byProject,
    topSessions: snapshot.topSessions,
    byHarness,
    scannedAt,
    files: snapshot.files,
    range: {
      from: snapshot.daily[0]?.date ?? null,
      to: snapshot.daily[snapshot.daily.length - 1]?.date ?? null,
    },
    pricing,
  };
}

async function computeAggregations(opts?: {
  from?: string;
  to?: string;
  rescan?: boolean;
  models?: string[];
  projects?: string[];
}): Promise<Aggregations> {
  const store = getStore();
  let scannedAt = Date.now();
  if (opts?.rescan !== false) {
    const summary = await ingestOnce(store);
    scannedAt = summary.scannedAt;
  }
  const fromTs = opts?.from ? Date.parse(`${opts.from}T00:00:00Z`) : 0;
  const toTs = opts?.to ? Date.parse(`${opts.to}T23:59:59Z`) : Number.MAX_SAFE_INTEGER;
  const aggregateOptions = {
    fromTs,
    toTs,
    models: opts?.models,
    projects: opts?.projects,
  };
  // Request-path reads use a dedicated JS event loop and a read-only SQLite
  // connection. Rescanning remains local because it owns writer-side ingest
  // and is used by the CLI/background lifecycle rather than Bun.serve.
  let snapshot: UsageAggregateSnapshot;
  if (opts?.rescan === false) {
    const workerResult = await aggregateUsageOffThread(store.databasePath, aggregateOptions);
    snapshot = workerResult.snapshot;
    scannedAt = workerResult.scannedAt;
  } else {
    snapshot = store.aggregateUsage(aggregateOptions);
  }
  return usageSnapshotToAggregations(snapshot, scannedAt);
}

// === Day drill-down ===

export interface HourlyBucket {
  hour: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
}

export interface MessageRow {
  ts: number;
  model: string;
  project: string;
  sessionId: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  totalTokens: number;
  cost: number;
}

export interface DayDetail {
  date: string;
  totals: Totals;
  hourly: HourlyBucket[];
  byModel: ModelBucket[];
  byProject: ProjectBucket[];
  sessions: SessionBucket[];
  topMessages: MessageRow[];
  scannedAt: number;
}

export async function loadDayDetail(date: string): Promise<DayDetail> {
  const store = getStore();
  const fromTs = Date.parse(`${date}T00:00:00Z`);
  const toTs = Date.parse(`${date}T23:59:59.999Z`);
  const events = store.selectEvents({ fromTs, toTs });

  const hourly: HourlyBucket[] = Array.from({ length: 24 }, (_, h) => ({
    hour: h,
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    totalTokens: 0,
    cost: 0,
    messages: 0,
  }));
  for (const e of events) {
    const h = new Date(e.ts).getUTCHours();
    const b = hourly[h]!;
    b.inputTokens += e.inputTokens;
    b.outputTokens += e.outputTokens;
    b.cacheReadTokens += e.cacheReadTokens;
    b.cacheWrite5mTokens += e.cacheWrite5mTokens;
    b.cacheWrite1hTokens += e.cacheWrite1hTokens;
    const total =
      e.inputTokens +
      e.outputTokens +
      e.cacheReadTokens +
      e.cacheWrite5mTokens +
      e.cacheWrite1hTokens;
    b.totalTokens += total;
    b.cost += e.cost;
    b.messages += 1;
  }

  const agg = aggregateEvents(events, { files: 0, scannedAt: Date.now() });

  const topMessages: MessageRow[] = events
    .slice()
    .sort((a, b) => {
      const ta =
        a.inputTokens +
        a.outputTokens +
        a.cacheReadTokens +
        a.cacheWrite5mTokens +
        a.cacheWrite1hTokens;
      const tb =
        b.inputTokens +
        b.outputTokens +
        b.cacheReadTokens +
        b.cacheWrite5mTokens +
        b.cacheWrite1hTokens;
      return tb - ta;
    })
    .slice(0, 25)
    .map((e) => ({
      ts: e.ts,
      model: e.model,
      project: e.project,
      sessionId: e.sessionId,
      inputTokens: e.inputTokens,
      outputTokens: e.outputTokens,
      cacheReadTokens: e.cacheReadTokens,
      cacheWriteTokens: e.cacheWrite5mTokens + e.cacheWrite1hTokens,
      totalTokens:
        e.inputTokens +
        e.outputTokens +
        e.cacheReadTokens +
        e.cacheWrite5mTokens +
        e.cacheWrite1hTokens,
      cost: e.cost,
    }));

  return {
    date,
    totals: agg.totals,
    hourly,
    byModel: agg.byModel,
    byProject: agg.byProject,
    sessions: [...agg.topSessions].sort((a, b) => b.cost - a.cost),
    topMessages,
    scannedAt: Date.now(),
  };
}

// === Session drill-down ===

export interface SessionDetail {
  sessionId: string;
  project: string;
  projectSlug: string;
  cwd: string;
  models: string[];
  firstTs: number;
  lastTs: number;
  durationMs: number;
  totals: Totals;
  byModel: ModelBucket[];
  messages: MessageRow[];
  cumulative: Array<{ ts: number; cumulativeCost: number; cumulativeTokens: number }>;
}

export async function loadSessionDetail(sessionId: string): Promise<SessionDetail> {
  const store = getStore();
  const events = store.selectEventsForSession(sessionId);
  if (events.length === 0) {
    return {
      sessionId,
      project: "",
      projectSlug: "",
      cwd: "",
      models: [],
      firstTs: 0,
      lastTs: 0,
      durationMs: 0,
      totals: {
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
      },
      byModel: [],
      messages: [],
      cumulative: [],
    };
  }

  const agg = aggregateEvents(events, { files: 0, scannedAt: Date.now() });
  const first = events[0]!;
  const last = events[events.length - 1]!;
  const models: string[] = [];
  for (const e of events) {
    if (!models.includes(e.model)) models.push(e.model);
  }

  const messages: MessageRow[] = events.map((e) => ({
    ts: e.ts,
    model: e.model,
    project: e.project,
    sessionId: e.sessionId,
    inputTokens: e.inputTokens,
    outputTokens: e.outputTokens,
    cacheReadTokens: e.cacheReadTokens,
    cacheWriteTokens: e.cacheWrite5mTokens + e.cacheWrite1hTokens,
    totalTokens:
      e.inputTokens +
      e.outputTokens +
      e.cacheReadTokens +
      e.cacheWrite5mTokens +
      e.cacheWrite1hTokens,
    cost: e.cost,
  }));

  const cumulative: Array<{ ts: number; cumulativeCost: number; cumulativeTokens: number }> = [];
  let runningCost = 0;
  let runningTokens = 0;
  for (const m of messages) {
    runningCost += m.cost;
    runningTokens += m.totalTokens;
    cumulative.push({ ts: m.ts, cumulativeCost: runningCost, cumulativeTokens: runningTokens });
  }

  return {
    sessionId,
    project: first.project,
    projectSlug: first.projectSlug,
    cwd: slugToProject(first.projectSlug),
    models,
    firstTs: first.ts,
    lastTs: last.ts,
    durationMs: last.ts - first.ts,
    totals: agg.totals,
    byModel: agg.byModel,
    messages,
    cumulative,
  };
}
