// Persistent SQLite store for token usage events and exact-once source
// reconciliation. Legacy billing columns remain the immutable pricing inputs;
// normalized token classes are additive observability metadata.

import { Database } from "bun:sqlite";
import { mkdirSync, statSync } from "node:fs";
import { dirname } from "node:path";
import {
  CODEX_USAGE_PARSER_VERSION,
  classifyCodexCheckpointMigration,
  isCompleteCurrentCodexCheckpoint,
} from "./codex-checkpoint-migration";
import { databasePath, toPosixPath } from "./paths.ts";
import {
  isRateSnapshot,
  type RateEvent,
  type RateSessionActivity,
  type RateSnapshot,
  selectRateSnapshot,
} from "./rate.ts";
import type {
  AuthoritativeTokenSnapshot,
  NormalizedTokenClasses,
  SnapshotModel,
  SnapshotProvider,
  SnapshotRawTotals,
  SnapshotTokenClasses,
  TokenProviderState,
  TokenSource,
} from "./token-usage-contract.ts";
import {
  compareClaudeAuthorityCandidates,
  compareTokenModelNames,
  composeSnapshotTokenClasses,
  dailyTokenWindow,
  isAuthoritativeTokenSnapshot,
  normalizeTokenModel,
  snapshotClassProviders,
  type TokenWindow,
} from "./token-usage-contract.ts";

export type { NormalizedTokenClasses, TokenSource } from "./token-usage-contract.ts";

export function tokenSnapshotDiscoveryComplete(snapshot: AuthoritativeTokenSnapshot): boolean {
  return Object.values(snapshot.providers).every(
    (provider) => !provider.available || provider.discoveryComplete,
  );
}

export function tokenSnapshotHasCompleteProvider(snapshot: AuthoritativeTokenSnapshot): boolean {
  return Object.values(snapshot.providers).some(
    (provider) => provider.available && provider.discoveryComplete,
  );
}

export interface StoredEvent {
  ts: number;
  date: string;
  model: string;
  project: string;
  projectSlug: string;
  sessionId: string;
  /** Immutable legacy pricing input: uncached/active input. */
  inputTokens: number;
  /** Immutable legacy pricing input: provider output, including Codex reasoning once. */
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  filePath: string;
  lineOffset: number;
  source: TokenSource;
  /** Legacy field retained for API/schema compatibility; no longer unique. */
  dedupeKey?: string | null;
  tokenClasses?: NormalizedTokenClasses;
}

export interface FileState {
  path: string;
  mtimeMs: number;
  size: number;
  lastByteOffset: number;
  lastLineNumber: number;
}

export interface SourceFileState {
  generationKey: string;
  source: TokenSource;
  device: string;
  inode: string;
  representativePath: string;
  capturedSize: number;
  /** Legacy coarse timestamp retained for v4 schema compatibility only. */
  mtimeMs: number;
  /** Exact decimal nanoseconds. Null until a legacy row is content-verified. */
  mtimeNs: string | null;
  /** Exact decimal nanoseconds. Null until a legacy row is content-verified. */
  ctimeNs: string | null;
  lastByteOffset: number;
  lastLineNumber: number;
  committedPrefixSha256: string;
  codexStateJson: string | null;
}

export interface CapturedEvent {
  event: StoredEvent;
  canonicalKey: string;
  rawRecordSha256: string;
  immutableIdentity: string | null;
  immutablePayloadKey?: string | null;
  logicalMessageKey: string | null;
  collapseRevisions?: boolean;
  byteStart: number;
  byteEnd: number;
  lineNumber: number;
}

export interface CapturedFileCommit {
  state: SourceFileState;
  aliases: string[];
  events: CapturedEvent[];
  /** Byte-zero parser migration for one already-persisted source generation. */
  replacement?: {
    expectedState: SourceFileState;
    expectedAliases: string[];
  };
}

export interface CapturedFileCommitResult {
  inserted: number;
  insertedEvents: StoredEvent[];
  mutated: boolean;
  revision: number;
}

export interface ScanCommit {
  batches: CapturedFileCommit[];
  aliases: DiscoveredAlias[];
  providers: DiscoveryProviderState[];
  legacyReconciliationObservations?: LegacyReconciliationObservation[];
  /** Revision captured with the source/alias/pending read snapshot. */
  sourceSnapshotRevision?: number;
  /** Batch indexes proven empty and unchanged in that read snapshot. */
  unchangedBatchIndexes?: number[];
}

export interface ScanCommitResult {
  insertedEvents: StoredEvent[];
  mutated: boolean;
  revision: number;
}

export type DiscoveryProviderState = TokenProviderState;

export interface ProviderState extends DiscoveryProviderState {
  defectLines: number;
  updatedAt: number;
}

export interface DiscoveredAlias {
  path: string;
  generationKey: string;
  source: TokenSource;
}

export interface LegacyReconciliationPendingRow {
  path: string;
  source: TokenSource;
  reason: string;
}

export interface LegacyReconciliationObservation extends LegacyReconciliationPendingRow {
  observed: "ENOENT" | "other-error" | "readable";
}

export interface FilePathCountRow {
  filePath: string;
  rowCount: number;
}

export interface InsertEventsResult {
  inserted: number;
  insertedEvents: StoredEvent[];
}

/** Exact persisted row counts as of the completion time of one usage scan. */
export interface CachedTokenStats {
  revision: number;
  events: number;
  files: number;
  scannedAt: number;
}

/** SQLite write contention is retryable; every other storage failure remains
 * fail-closed. Extended result codes retain the primary code in the low byte. */
export function isTransientSqliteContentionError(error: unknown): boolean {
  if (typeof error !== "object" || error === null || Array.isArray(error)) return false;
  const candidate = error as { name?: unknown; code?: unknown; errno?: unknown };
  if (candidate.name !== "SQLiteError") return false;
  const code = typeof candidate.code === "string" ? candidate.code : "";
  const errno = typeof candidate.errno === "number" ? candidate.errno : Number.NaN;
  const primaryCode = Number.isInteger(errno) && errno >= 0 ? errno & 0xff : Number.NaN;
  return (
    code === "SQLITE_BUSY" ||
    code.startsWith("SQLITE_BUSY_") ||
    code === "SQLITE_LOCKED" ||
    code.startsWith("SQLITE_LOCKED_") ||
    primaryCode === 5 ||
    primaryCode === 6
  );
}

/** Revision-pinned rate output published by one bounded ingest worker. */
export interface CachedRateSnapshot {
  revision: number;
  /** Completion heartbeat for the cache publication itself. The rate window's
   * `snapshot.now` remains the exact reducer anchor and may legitimately
   * precede an expensive isolated roll-up. */
  publishedAt: number;
  snapshot: RateSnapshot;
}

export interface SourceReconciliationSnapshot {
  readonly revision: number;
  readonly pendingLegacyReconciliations: readonly LegacyReconciliationPendingRow[];
  getLatestSourceFile(source: TokenSource, device: string, inode: string): SourceFileState | null;
  getSourceGenerationKeys(
    source: TokenSource,
    device: string,
    inode: string,
    paths?: string[],
  ): string[];
  hasPendingLegacyPath(paths: string[]): boolean;
  hasGenerationOccurrences(generationKey: string): boolean;
  canStageReplacement(source: TokenSource, generationKey: string, aliases: string[]): boolean;
  isCapturedBatchUnchanged(batch: CapturedFileCommit): boolean;
}

/** Constant-memory SQL roll-up used by all-time/window summary surfaces. */
export interface EventTotalsAggregate {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  messages: number;
  minTs: number | null;
  maxTs: number | null;
}

export interface UsageAggregateTally {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  totalTokens: number;
  cost: number;
  messages: number;
}

export interface UsageAggregateSnapshot {
  totals: UsageAggregateTally & { minTs: number | null; maxTs: number | null };
  daily: Array<UsageAggregateTally & { date: string }>;
  byModel: Array<UsageAggregateTally & { model: string; minTs: number; maxTs: number }>;
  byProject: Array<
    UsageAggregateTally & {
      project: string;
      projectSlug: string;
      sessions: number;
      lastTs: number;
    }
  >;
  topSessions: Array<
    UsageAggregateTally & {
      sessionId: string;
      project: string;
      projectSlug: string;
      models: string[];
      firstTs: number;
      lastTs: number;
    }
  >;
  /** Keyed and labeled by the same harness identity that populates
   *  ModelBucket.harness in scanner.ts (TokenSource is a subset of Harness)
   *  — never a second, divergent derivation. */
  byHarness: Array<UsageAggregateTally & { harness: TokenSource; minTs: number; maxTs: number }>;
  files: number;
}

export interface UsageAggregateOptions {
  fromTs?: number;
  toTs?: number;
  models?: string[];
  projects?: string[];
}

export interface TokenStoreOptions {
  /** Opens an existing schema without creating directories, migrating, or
   * issuing any database write. Intended for isolated aggregation workers. */
  readonly?: boolean;
}

const SCHEMA_VERSION = 5;
export const CLAUDE_USAGE_PARSER_VERSION = 3;
export const GROK_USAGE_PARSER_VERSION = 1;
export { CODEX_USAGE_PARSER_VERSION } from "./codex-checkpoint-migration";

const DEFECT_LINE_SAMPLE_LIMIT = 8;

function validDefectCoordinates(checkpoint: Record<string, unknown>): boolean {
  const count = checkpoint.defectLineCount;
  const lines = checkpoint.defectLineNumbers;
  return (
    typeof count === "number" &&
    Number.isSafeInteger(count) &&
    count >= 0 &&
    Array.isArray(lines) &&
    lines.length === Math.min(count, DEFECT_LINE_SAMPLE_LIMIT) &&
    lines.every(
      (line, index) =>
        typeof line === "number" &&
        Number.isSafeInteger(line) &&
        line >= 0 &&
        (index === 0 || line > (lines[index - 1] as number)),
    )
  );
}

function validUsageDefectCheckpoint(checkpoint: Record<string, unknown>): boolean {
  return checkpoint.semanticUsageComplete === true && validDefectCoordinates(checkpoint);
}

function checkpointDefectLineCount(value: string | null): number {
  if (!value) return 0;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return 0;
    const checkpoint = parsed as Record<string, unknown>;
    return validDefectCoordinates(checkpoint) ? (checkpoint.defectLineCount as number) : 0;
  } catch {
    return 0;
  }
}

function checkpointUsageComplete(source: TokenSource, value: string | null): boolean {
  if (!value) return false;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return false;
    const checkpoint = parsed as Record<string, unknown>;
    if (source === "codex") return isCompleteCurrentCodexCheckpoint(checkpoint);
    const parserVersion =
      source === "grok" ? GROK_USAGE_PARSER_VERSION : CLAUDE_USAGE_PARSER_VERSION;
    return (
      checkpoint.usageParserVersion === parserVersion &&
      checkpoint.usageComplete === true &&
      validUsageDefectCheckpoint(checkpoint)
    );
  } catch {
    return false;
  }
}

interface EventRow {
  ts: number;
  date: string;
  model: string;
  project: string;
  project_slug: string;
  session_id: string;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_5m_tokens: number;
  cache_write_1h_tokens: number;
  cost: number;
  file_path: string;
  line_offset: number;
  source: TokenSource;
  class_input_tokens: number | null;
  class_cached_read_tokens: number | null;
  class_cache_write_tokens: number | null;
  class_visible_output_tokens: number | null;
  class_reasoning_output_tokens: number | null;
  classes_complete: number;
}

interface TokenSnapshotAggregateRow {
  source: string;
  model: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  messages: number;
  classInputTokens: number | null;
  classCachedReadTokens: number | null;
  classCacheWriteTokens: number | null;
  classVisibleOutputTokens: number | null;
  classReasoningOutputTokens: number | null;
}

interface UsageCursorRow {
  id: number;
  ts: number;
  date: string;
  model: string;
  project: string;
  projectSlug: string;
  sessionId: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWrite5mTokens: number;
  cacheWrite1hTokens: number;
  cost: number;
  source: TokenSource;
}

interface EventOccurrenceRow {
  eventId: number;
  canonicalKey: string;
  byteStart: number;
  byteEnd: number;
  rawRecordSha256: string;
  logicalMessageKey: string | null;
}

interface ReplacementOccurrenceRow {
  event_id: number;
  canonical_key: string;
  observed_path: string;
  byte_start: number;
  byte_end: number;
  line_number: number;
  raw_record_sha256: string;
  logical_message_key: string | null;
  file_path: string;
}

interface ReplacementValidationResult {
  eventIds: number[];
  occurrenceCount: number;
}

interface SourceFileSnapshotRow {
  generation_rowid: number;
  generation_key: string;
  source: TokenSource;
  device: string;
  inode: string;
  representative_path: string;
  captured_size: number;
  mtime_ms: number;
  mtime_ns: string | null;
  ctime_ns: string | null;
  last_byte_offset: number;
  last_line_number: number;
  committed_prefix_sha256: string;
  codex_state_json: string | null;
  updated_at: number;
}

const EVENT_COLUMNS =
  "ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, source, class_input_tokens, class_cached_read_tokens, class_cache_write_tokens, class_visible_output_tokens, class_reasoning_output_tokens, classes_complete";

const RATE_EVENT_COLUMNS =
  "ts, model, input_tokens AS inputTokens, output_tokens AS outputTokens, cache_read_tokens AS cacheReadTokens, cache_write_5m_tokens AS cacheWrite5mTokens, cache_write_1h_tokens AS cacheWrite1hTokens, cost, source";

function rateEventCursorQuery(): string {
  return `SELECT ${RATE_EVENT_COLUMNS} FROM events INDEXED BY idx_events_ts
          WHERE ts >= ? AND ts <= ?
          ORDER BY ts ASC, id ASC`;
}

/** Exact compact cursor SQL exposed for query-plan and allocation guards. */
export const _rateEventCursorQueryForTest = rateEventCursorQuery;

function rateSessionActivityCursorQuery(): string {
  // This removes the unbounded JS session-id Set from the reducer. SQLite may
  // still allocate or spill a native temporary GROUP BY structure; the
  // transient worker cgroup, not this query, supplies the total-process cap.
  return `SELECT MAX(ts) AS latestTs
          FROM events INDEXED BY idx_events_ts
          WHERE ts >= ? AND ts <= ?
          GROUP BY session_id
          ORDER BY session_id ASC`;
}

/** Exact grouped session cursor SQL exposed for query-plan/allocation guards. */
export const _rateSessionActivityCursorQueryForTest = rateSessionActivityCursorQuery;

const USAGE_CURSOR_COLUMNS = `
  id, ts, date, model, project,
  project_slug AS projectSlug, session_id AS sessionId,
  input_tokens AS inputTokens, output_tokens AS outputTokens,
  cache_read_tokens AS cacheReadTokens,
  cache_write_5m_tokens AS cacheWrite5mTokens,
  cache_write_1h_tokens AS cacheWrite1hTokens,
  cost, source`;

function usageAggregateFilter(
  opts: UsageAggregateOptions,
  qualifier = "",
): {
  sql: string;
  params: Array<number | string>;
} {
  const column = (name: string): string => `${qualifier}${name}`;
  const clauses = [`${column("ts")} >= ?`, `${column("ts")} <= ?`];
  const params: Array<number | string> = [opts.fromTs ?? 0, opts.toTs ?? Number.MAX_SAFE_INTEGER];
  if (opts.models && opts.models.length > 0) {
    clauses.push(`${column("model")} IN (${opts.models.map(() => "?").join(",")})`);
    params.push(...opts.models);
  }
  if (opts.projects && opts.projects.length > 0) {
    clauses.push(`${column("project")} IN (${opts.projects.map(() => "?").join(",")})`);
    params.push(...opts.projects);
  }
  return { sql: clauses.join(" AND "), params };
}

function usageSessionCursorQuery(filterSql: string): string {
  return `SELECT ${USAGE_CURSOR_COLUMNS} FROM events INDEXED BY idx_events_session_ts_id
          WHERE ${filterSql}
          ORDER BY session_id ASC, ts ASC, id ASC`;
}

/** Exact production SQL exposed for deterministic query-plan regression tests. */
export const _usageSessionCursorQueryForTest = usageSessionCursorQuery;

function emptyUsageAggregateTally(): UsageAggregateTally {
  return {
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    totalTokens: 0,
    cost: 0,
    messages: 0,
  };
}

function addUsageCursorRow(target: UsageAggregateTally, row: UsageCursorRow): void {
  target.inputTokens += row.inputTokens;
  target.outputTokens += row.outputTokens;
  target.cacheReadTokens += row.cacheReadTokens;
  target.cacheWrite5mTokens += row.cacheWrite5mTokens;
  target.cacheWrite1hTokens += row.cacheWrite1hTokens;
  target.totalTokens +=
    row.inputTokens +
    row.outputTokens +
    row.cacheReadTokens +
    row.cacheWrite5mTokens +
    row.cacheWrite1hTokens;
  target.cost += row.cost;
  target.messages += 1;
}

function dbPath(): string {
  return databasePath();
}

function sourceForLegacyPath(rawPath: string): TokenSource {
  const path = toPosixPath(rawPath);
  const grokLogs = process.env.GROK_LOGS_DIR;
  if (
    path.includes("/.grok/") ||
    (grokLogs !== undefined && path.startsWith(toPosixPath(grokLogs)))
  ) {
    return "grok";
  }
  const configured = process.env.CODEX_SESSIONS_DIR;
  return path.includes("/.codex/") ||
    (configured !== undefined && path.startsWith(toPosixPath(configured)))
    ? "codex"
    : "claude";
}

function eventTableSql(): string {
  return "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, dedupe_key TEXT, source TEXT NOT NULL DEFAULT 'claude', canonical_key TEXT, canonical_ordinal INTEGER, raw_record_sha256 TEXT, immutable_identity TEXT, logical_message_key TEXT, class_input_tokens INTEGER, class_cached_read_tokens INTEGER, class_cache_write_tokens INTEGER, class_visible_output_tokens INTEGER, class_reasoning_output_tokens INTEGER, classes_complete INTEGER NOT NULL DEFAULT 0)";
}

function sourceGenerationPathQuery(pathCount: number): string {
  const placeholders = Array.from({ length: pathCount }, () => "?").join(",");
  return `SELECT generation_key FROM (
    SELECT generation_key, rowid AS generation_rowid
    FROM source_files
    WHERE source = ? AND representative_path IN (${placeholders})
    UNION
    SELECT sf.generation_key, sf.rowid AS generation_rowid
    FROM source_aliases sa
    JOIN source_files sf ON sf.generation_key = sa.generation_key
    WHERE sf.source = ? AND sa.path IN (${placeholders})
  ) ORDER BY generation_rowid`;
}

/** Exact production SQL exposed for deterministic query-plan regression tests. */
export const _sourceGenerationPathQueryForTest = sourceGenerationPathQuery;

function sourceIdentityKey(source: TokenSource, device: string, inode: string): string {
  return `${source}\0${device}\0${inode}`;
}

function sourcePathKey(source: TokenSource, path: string): string {
  return `${source}\0${path}`;
}

function sourceFileStateFromSnapshotRow(row: SourceFileSnapshotRow): SourceFileState {
  return {
    generationKey: row.generation_key,
    source: row.source,
    device: row.device,
    inode: row.inode,
    representativePath: row.representative_path,
    capturedSize: row.captured_size,
    mtimeMs: row.mtime_ms,
    mtimeNs: row.mtime_ns,
    ctimeNs: row.ctime_ns,
    lastByteOffset: row.last_byte_offset,
    lastLineNumber: row.last_line_number,
    committedPrefixSha256: row.committed_prefix_sha256,
    codexStateJson: row.codex_state_json,
  };
}

function sameSourceFileState(left: SourceFileState, right: SourceFileState): boolean {
  return (
    left.generationKey === right.generationKey &&
    left.source === right.source &&
    left.device === right.device &&
    left.inode === right.inode &&
    left.representativePath === right.representativePath &&
    left.capturedSize === right.capturedSize &&
    left.mtimeMs === right.mtimeMs &&
    left.mtimeNs === right.mtimeNs &&
    left.ctimeNs === right.ctimeNs &&
    left.lastByteOffset === right.lastByteOffset &&
    left.lastLineNumber === right.lastLineNumber &&
    left.committedPrefixSha256 === right.committedPrefixSha256 &&
    (left.codexStateJson ?? null) === (right.codexStateJson ?? null)
  );
}

function classesFor(event: StoredEvent): NormalizedTokenClasses {
  const classes = event.tokenClasses ?? {
    input: event.inputTokens,
    cachedRead: event.cacheReadTokens,
    cacheWrite: event.cacheWrite5mTokens + event.cacheWrite1hTokens,
    visibleOutput: event.source === "claude" ? event.outputTokens : null,
    reasoningOutput: null,
    complete: false,
  };
  if (typeof classes.complete !== "boolean") {
    throw new Error("stored event token class completeness must be a boolean");
  }
  for (const [key, value] of Object.entries(classes)) {
    if (key === "complete") continue;
    if (value !== null && (!Number.isSafeInteger(value) || value < 0)) {
      throw new Error(
        `stored event token class ${key} must be a non-negative safe integer or null`,
      );
    }
  }
  return classes;
}

const STATS_CACHE_META_KEY = "exact-stats-v1";
const TOKEN_SNAPSHOT_CACHE_META_KEY = "authoritative-token-snapshot-v1";
const TOKEN_SNAPSHOT_CACHE_MAX_BYTES = 1024 * 1024;
const RATE_SNAPSHOT_CACHE_META_KEY = "rate-snapshot-v1";
const RATE_SNAPSHOT_CACHE_MAX_BYTES = 128 * 1024;

const SNAPSHOT_CLASS_KEYS = [
  "input",
  "cachedRead",
  "cacheWrite",
  "visibleOutput",
  "reasoningOutput",
] as const;

type SnapshotClassKey = (typeof SNAPSHOT_CLASS_KEYS)[number];

function unavailableSnapshotClasses(): SnapshotTokenClasses {
  return {
    input: { tokens: null, complete: false },
    cachedRead: { tokens: null, complete: false },
    cacheWrite: { tokens: null, complete: false },
    visibleOutput: { tokens: null, complete: false },
    reasoningOutput: { tokens: null, complete: false },
    complete: false,
  };
}

function snapshotClassSupported(source: TokenSource, key: SnapshotClassKey): boolean {
  return source === "claude" ? key !== "reasoningOutput" : key !== "cacheWrite";
}

function modelSnapshotClasses(
  source: TokenSource,
  row: TokenSnapshotAggregateRow,
  providerComplete: boolean,
): SnapshotTokenClasses {
  if (!providerComplete) return unavailableSnapshotClasses();
  const values: Record<SnapshotClassKey, number | null> = {
    input: row.classInputTokens,
    cachedRead: row.classCachedReadTokens,
    cacheWrite: row.classCacheWriteTokens,
    visibleOutput: row.classVisibleOutputTokens,
    reasoningOutput: row.classReasoningOutputTokens,
  };
  const classes = unavailableSnapshotClasses();
  for (const key of SNAPSHOT_CLASS_KEYS) {
    const value = values[key];
    const complete = snapshotClassSupported(source, key) && value !== null;
    classes[key] = { tokens: complete ? value : null, complete };
  }
  classes.complete = SNAPSHOT_CLASS_KEYS.every((key) => classes[key].complete);
  return classes;
}

function providerSnapshotClasses(
  source: TokenSource,
  models: SnapshotModel[],
  providerComplete: boolean,
): SnapshotTokenClasses {
  if (!providerComplete) return unavailableSnapshotClasses();
  const classes = unavailableSnapshotClasses();
  for (const key of SNAPSHOT_CLASS_KEYS) {
    const complete =
      snapshotClassSupported(source, key) && models.every((model) => model.classes[key].complete);
    classes[key] = {
      tokens: complete ? models.reduce((sum, model) => sum + model.classes[key].tokens!, 0) : null,
      complete,
    };
  }
  classes.complete = SNAPSHOT_CLASS_KEYS.every((key) => classes[key].complete);
  return classes;
}

function emptySnapshotRawTotals(): SnapshotRawTotals {
  return {
    inputTokens: 0,
    rawInputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    messages: 0,
  };
}

function aggregateRowRawTotals(row: TokenSnapshotAggregateRow): SnapshotRawTotals {
  return {
    inputTokens: row.inputTokens,
    rawInputTokens:
      row.inputTokens + row.cacheReadTokens + row.cacheWrite5mTokens + row.cacheWrite1hTokens,
    outputTokens: row.outputTokens,
    cacheReadTokens: row.cacheReadTokens,
    cacheWrite5mTokens: row.cacheWrite5mTokens,
    cacheWrite1hTokens: row.cacheWrite1hTokens,
    cost: row.cost,
    messages: row.messages,
  };
}

function addSnapshotRawTotals(
  left: SnapshotRawTotals,
  right: SnapshotRawTotals,
): SnapshotRawTotals {
  return {
    inputTokens: left.inputTokens + right.inputTokens,
    rawInputTokens: left.rawInputTokens + right.rawInputTokens,
    outputTokens: left.outputTokens + right.outputTokens,
    cacheReadTokens: left.cacheReadTokens + right.cacheReadTokens,
    cacheWrite5mTokens: left.cacheWrite5mTokens + right.cacheWrite5mTokens,
    cacheWrite1hTokens: left.cacheWrite1hTokens + right.cacheWrite1hTokens,
    cost: left.cost + right.cost,
    messages: left.messages + right.messages,
  };
}

const TOKEN_SNAPSHOT_MAX_MODELS_PER_PROVIDER = 512;
const TOKEN_SNAPSHOT_MAX_MODEL_BYTES = 256;
const TOKEN_SNAPSHOT_OVERFLOW_MODEL = "other";

function mergeNullableSnapshotCount(left: number | null, right: number | null): number | null {
  return left === null || right === null ? null : left + right;
}

function mergeTokenSnapshotAggregateRows(
  left: TokenSnapshotAggregateRow,
  right: TokenSnapshotAggregateRow,
): TokenSnapshotAggregateRow {
  return {
    source: left.source,
    model: left.model,
    inputTokens: left.inputTokens + right.inputTokens,
    outputTokens: left.outputTokens + right.outputTokens,
    cacheReadTokens: left.cacheReadTokens + right.cacheReadTokens,
    cacheWrite5mTokens: left.cacheWrite5mTokens + right.cacheWrite5mTokens,
    cacheWrite1hTokens: left.cacheWrite1hTokens + right.cacheWrite1hTokens,
    cost: left.cost + right.cost,
    messages: left.messages + right.messages,
    classInputTokens: mergeNullableSnapshotCount(left.classInputTokens, right.classInputTokens),
    classCachedReadTokens: mergeNullableSnapshotCount(
      left.classCachedReadTokens,
      right.classCachedReadTokens,
    ),
    classCacheWriteTokens: mergeNullableSnapshotCount(
      left.classCacheWriteTokens,
      right.classCacheWriteTokens,
    ),
    classVisibleOutputTokens: mergeNullableSnapshotCount(
      left.classVisibleOutputTokens,
      right.classVisibleOutputTokens,
    ),
    classReasoningOutputTokens: mergeNullableSnapshotCount(
      left.classReasoningOutputTokens,
      right.classReasoningOutputTokens,
    ),
  };
}

function parseCachedTokenStats(value: string): CachedTokenStats | null {
  try {
    const parsed: unknown = JSON.parse(value);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    const candidate = parsed as Record<string, unknown>;
    for (const field of ["revision", "events", "files", "scannedAt"] as const) {
      const value = candidate[field];
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
        return null;
      }
    }
    return candidate as unknown as CachedTokenStats;
  } catch {
    return null;
  }
}

function parseCachedRateSnapshot(value: string): CachedRateSnapshot | null {
  if (Buffer.byteLength(value, "utf8") > RATE_SNAPSHOT_CACHE_MAX_BYTES) return null;
  try {
    const parsed: unknown = JSON.parse(value);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    const candidate = parsed as Record<string, unknown>;
    if (
      typeof candidate.revision !== "number" ||
      !Number.isSafeInteger(candidate.revision) ||
      candidate.revision < 0 ||
      typeof candidate.publishedAt !== "number" ||
      !Number.isSafeInteger(candidate.publishedAt) ||
      candidate.publishedAt < 0 ||
      !isRateSnapshot(candidate.snapshot)
    ) {
      return null;
    }
    return candidate as unknown as CachedRateSnapshot;
  } catch {
    return null;
  }
}

export class TokenStore {
  private db: Database;
  private deferRevisionBump = false;

  constructor(
    public readonly databasePath: string = dbPath(),
    options: TokenStoreOptions = {},
  ) {
    if (options.readonly) {
      this.db = new Database(databasePath, { readonly: true });
      try {
        // These PRAGMAs are connection-local. query_only is defense in depth
        // on top of SQLITE_OPEN_READONLY; cache_size bounds the worker's page
        // cache without changing the database file.
        this.db.run("PRAGMA query_only = ON");
        this.db.run("PRAGMA busy_timeout = 5000");
        this.db.run("PRAGMA cache_size = -8192");
        const version =
          (this.db.query("PRAGMA user_version").get() as { user_version?: number } | undefined)
            ?.user_version ?? 0;
        if (version !== SCHEMA_VERSION) {
          throw new Error(
            `tokens read-only database schema ${version} does not match supported schema ${SCHEMA_VERSION}`,
          );
        }
      } catch (error) {
        this.db.close();
        throw error;
      }
      return;
    }

    mkdirSync(dirname(databasePath), { recursive: true });
    this.db = new Database(databasePath);
    this.db.run("PRAGMA busy_timeout = 5000");
    this.db.run("PRAGMA journal_mode = WAL");
    this.db.run("PRAGMA synchronous = NORMAL");
    this.db.run("PRAGMA foreign_keys = ON");
    this.migrate();
  }

  private migrate(): void {
    const version =
      (this.db.query("PRAGMA user_version").get() as { user_version?: number } | undefined)
        ?.user_version ?? 0;
    if (version > SCHEMA_VERSION) {
      this.db.close();
      throw new Error(
        `tokens database schema ${version} is newer than supported schema ${SCHEMA_VERSION}`,
      );
    }
    this.ensureLegacyTables();
    if (version < 2) this.migrateV2SourceColumn();
    if (version < 3) this.migrateV3DedupeKeyColumn();
    if (version < 4) this.migrateV4ExactOnce();
    this.ensureV4TablesAndIndexes();
    if (version < 4) this.prepareLegacyFilesForRebuild();
    this.ensureV5SourceFileNanosecondColumns();
  }

  private ensureLegacyTables(): void {
    this.db.run(
      "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, date TEXT NOT NULL, model TEXT NOT NULL, project TEXT NOT NULL, project_slug TEXT NOT NULL, session_id TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, cache_read_tokens INTEGER NOT NULL, cache_write_5m_tokens INTEGER NOT NULL, cache_write_1h_tokens INTEGER NOT NULL, cost REAL NOT NULL, file_path TEXT NOT NULL, line_offset INTEGER NOT NULL, dedupe_key TEXT, source TEXT NOT NULL DEFAULT 'claude', UNIQUE(file_path, line_offset))",
    );
    this.db.run(
      "CREATE TABLE IF NOT EXISTS file_state (path TEXT PRIMARY KEY, mtime_ms INTEGER NOT NULL, size INTEGER NOT NULL, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
    );
  }

  private migrateV2SourceColumn(): void {
    const cols = this.db.query("PRAGMA table_info(events)").all() as Array<{ name: string }>;
    if (!cols.some((c) => c.name === "source")) {
      this.db.run("ALTER TABLE events ADD COLUMN source TEXT NOT NULL DEFAULT 'claude'");
    }
    this.db.run(
      "UPDATE events SET source = 'codex' WHERE file_path LIKE '%/.codex/%' OR file_path LIKE '%\\.codex\\%'",
    );
    const codexRoot = process.env.CODEX_SESSIONS_DIR;
    if (codexRoot) {
      this.db
        .query("UPDATE events SET source = 'codex' WHERE file_path LIKE ?")
        .run(codexRoot.concat("%"));
    }
  }

  private migrateV3DedupeKeyColumn(): void {
    const cols = this.db.query("PRAGMA table_info(events)").all() as Array<{ name: string }>;
    if (!cols.some((c) => c.name === "dedupe_key")) {
      this.db.run("ALTER TABLE events ADD COLUMN dedupe_key TEXT");
    }
  }

  private migrateV4ExactOnce(): void {
    const table = this.db
      .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'")
      .get() as { sql?: string } | undefined;
    const cols = this.db.query("PRAGMA table_info(events)").all() as Array<{ name: string }>;
    const hasV4 = cols.some((c) => c.name === "canonical_key");
    const hasLegacyPathUnique = /UNIQUE\s*\(\s*file_path\s*,\s*line_offset\s*\)/i.test(
      table?.sql ?? "",
    );
    if (hasV4 && !hasLegacyPathUnique) return;

    const hasDedupe = cols.some((c) => c.name === "dedupe_key");
    const hasSource = cols.some((c) => c.name === "source");
    const tx = this.db.transaction(() => {
      this.db.run("DROP INDEX IF EXISTS idx_events_dedupe_key");
      this.db.run("ALTER TABLE events RENAME TO events_v3");
      this.db.run(eventTableSql());
      this.db.run(
        `INSERT INTO events (id, ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, dedupe_key, source, class_input_tokens, class_cached_read_tokens, class_cache_write_tokens, class_visible_output_tokens, class_reasoning_output_tokens, classes_complete)
         SELECT id, ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, ${hasDedupe ? "dedupe_key" : "NULL"}, ${hasSource ? "source" : "'claude'"}, input_tokens, cache_read_tokens, cache_write_5m_tokens + cache_write_1h_tokens, CASE WHEN ${hasSource ? "source" : "'claude'"} = 'claude' THEN output_tokens ELSE NULL END, NULL, 0 FROM events_v3`,
      );
      this.db.run("DROP TABLE events_v3");
    });
    tx();
  }

  private ensureV4TablesAndIndexes(): void {
    const statements = [
      "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)",
      "CREATE INDEX IF NOT EXISTS idx_events_date ON events(date)",
      "CREATE INDEX IF NOT EXISTS idx_events_model ON events(model)",
      "CREATE INDEX IF NOT EXISTS idx_events_project ON events(project)",
      "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id)",
      "CREATE INDEX IF NOT EXISTS idx_events_session_ts_id ON events(session_id, ts, id)",
      "CREATE INDEX IF NOT EXISTS idx_events_dedupe_key ON events(dedupe_key)",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_canonical ON events(canonical_key, canonical_ordinal) WHERE canonical_key IS NOT NULL",
      "CREATE TABLE IF NOT EXISTS source_files (generation_key TEXT PRIMARY KEY, source TEXT NOT NULL, device TEXT NOT NULL, inode TEXT NOT NULL, representative_path TEXT NOT NULL, captured_size INTEGER NOT NULL, mtime_ms REAL NOT NULL, mtime_ns TEXT, ctime_ns TEXT, last_byte_offset INTEGER NOT NULL, last_line_number INTEGER NOT NULL, committed_prefix_sha256 TEXT NOT NULL, codex_state_json TEXT, updated_at INTEGER NOT NULL)",
      "CREATE INDEX IF NOT EXISTS idx_source_files_identity ON source_files(source, device, inode, updated_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_source_files_representative ON source_files(source, representative_path)",
      "CREATE TABLE IF NOT EXISTS source_aliases (path TEXT PRIMARY KEY, generation_key TEXT NOT NULL REFERENCES source_files(generation_key), source TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, updated_at INTEGER NOT NULL)",
      "CREATE INDEX IF NOT EXISTS idx_source_aliases_generation ON source_aliases(generation_key)",
      "CREATE TABLE IF NOT EXISTS event_occurrences (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES events(id), canonical_key TEXT NOT NULL, generation_key TEXT NOT NULL REFERENCES source_files(generation_key), observed_path TEXT NOT NULL, byte_start INTEGER NOT NULL, byte_end INTEGER NOT NULL, line_number INTEGER NOT NULL, raw_record_sha256 TEXT NOT NULL, logical_message_key TEXT, observed_at INTEGER NOT NULL, UNIQUE(generation_key, observed_path, line_number))",
      "CREATE INDEX IF NOT EXISTS idx_event_occurrences_event ON event_occurrences(event_id)",
      "CREATE TABLE IF NOT EXISTS immutable_records (identity TEXT PRIMARY KEY, canonical_key TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS provider_state (source TEXT PRIMARY KEY, available INTEGER NOT NULL, complete INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
      "CREATE TABLE IF NOT EXISTS legacy_reconciliation_pending (path TEXT PRIMARY KEY, source TEXT NOT NULL, reason TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
      "INSERT OR IGNORE INTO store_meta (key, value) VALUES ('revision', '0')",
    ];
    for (const statement of statements) this.db.run(statement);
  }

  /** Adds the lossless v5 metadata contract under one write reservation.
   *
   * The schema is re-checked after BEGIN IMMEDIATE, so two processes opening a
   * v4 (or pre-release v5) database cannot both decide to add the same column.
   * Existing rows intentionally receive NULL and must be content-verified
   * once before the scanner may take a metadata-only path. A pre-release
   * ctime_ms column is harmless and intentionally left unused rather than
   * depending on newer SQLite DROP COLUMN support during startup. */
  private ensureV5SourceFileNanosecondColumns(): void {
    this.db
      .transaction(() => {
        let columns = this.db.query("PRAGMA table_info(source_files)").all() as Array<{
          name: string;
        }>;
        if (!columns.some((column) => column.name === "mtime_ns")) {
          this.db.run("ALTER TABLE source_files ADD COLUMN mtime_ns TEXT");
          columns = this.db.query("PRAGMA table_info(source_files)").all() as Array<{
            name: string;
          }>;
        }
        if (!columns.some((column) => column.name === "ctime_ns")) {
          this.db.run("ALTER TABLE source_files ADD COLUMN ctime_ns TEXT");
        }
        this.db.run(`PRAGMA user_version = ${SCHEMA_VERSION}`);
      })
      .immediate();
  }

  /** Legacy cursors carry no content hash. Readable physical files are purged
   * transactionally and rebuilt from byte zero. ENOENT rows become
   * completeness-excluded expired-missing tombstones that remain tracked for
   * late reconciliation; other unreadable rows keep their provider incomplete. */
  private prepareLegacyFilesForRebuild(): void {
    const cursorPaths = this.db.query("SELECT path FROM file_state ORDER BY path").all() as Array<{
      path: string;
    }>;
    const eventPaths = this.db
      .query(
        "SELECT file_path AS path, source FROM events WHERE canonical_key IS NULL GROUP BY file_path, source ORDER BY file_path",
      )
      .all() as Array<{ path: string; source: TokenSource }>;
    if (cursorPaths.length === 0 && eventPaths.length === 0) return;
    const candidates = new Map<string, TokenSource>();
    for (const row of cursorPaths) candidates.set(row.path, sourceForLegacyPath(row.path));
    for (const row of eventPaths) candidates.set(row.path, row.source);
    const readable = new Map<string, { identity: string; source: TokenSource }>();
    const identitiesToRebuild = new Set<string>();
    const pending: Array<{ path: string; source: TokenSource }> = [];
    const availability = new Map<TokenSource, boolean>();

    for (const [path, source] of candidates) {
      try {
        const file = statSync(path);
        if (!file.isFile()) throw new Error("not a regular file");
        const identity = `${file.dev}:${file.ino}`;
        readable.set(path, { identity, source });
        identitiesToRebuild.add(identity);
        availability.set(source, true);
      } catch {
        pending.push({ path, source });
        if (!availability.has(source)) availability.set(source, false);
      }
    }

    const purgePaths = [...readable.entries()]
      .filter(([, value]) => identitiesToRebuild.has(value.identity))
      .map(([path]) => path);
    for (const path of purgePaths) {
      const info = readable.get(path);
      if (info) availability.set(info.source, true);
    }
    const tx = this.db.transaction(() => {
      if (purgePaths.length > 0) {
        const placeholders = purgePaths.map(() => "?").join(",");
        this.db
          .query(
            `DELETE FROM event_occurrences WHERE event_id IN (SELECT id FROM events WHERE file_path IN (${placeholders}))`,
          )
          .run(...purgePaths);
        this.db.query(`DELETE FROM events WHERE file_path IN (${placeholders})`).run(...purgePaths);
        this.db.query(`DELETE FROM file_state WHERE path IN (${placeholders})`).run(...purgePaths);
        this.db
          .query(`DELETE FROM source_aliases WHERE path IN (${placeholders})`)
          .run(...purgePaths);
        this.db.run(
          "DELETE FROM source_files WHERE generation_key NOT IN (SELECT generation_key FROM source_aliases) AND generation_key NOT IN (SELECT generation_key FROM event_occurrences)",
        );
      }
      for (const item of pending) {
        this.db
          .prepare(
            "INSERT OR REPLACE INTO legacy_reconciliation_pending (path, source, reason) VALUES (?, ?, 'unreadable')",
          )
          .run(item.path, item.source);
      }
      const now = Date.now();
      for (const [source, available] of availability) {
        this.db
          .prepare(
            "INSERT INTO provider_state (source, available, complete, updated_at) VALUES (?, ?, 0, ?) ON CONFLICT(source) DO UPDATE SET available = excluded.available, complete = 0, updated_at = excluded.updated_at",
          )
          .run(source, available ? 1 : 0, now);
      }
      this.bumpRevision();
    });
    tx();
  }

  /** Captures all source reconciliation authority under one SQLite read
   * snapshot. Scanner file staging then performs only in-memory lookups. */
  captureSourceReconciliationSnapshot(): SourceReconciliationSnapshot {
    const captured = this.db
      .transaction(() => ({
        revision: this.getRevision(),
        sourceFiles: this.db
          .query(
            "SELECT rowid AS generation_rowid, generation_key, source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json, updated_at FROM source_files ORDER BY rowid",
          )
          .all() as SourceFileSnapshotRow[],
        aliases: this.db
          .query("SELECT path, generation_key, source, active FROM source_aliases")
          .all() as Array<{
          path: string;
          generation_key: string;
          source: TokenSource;
          active: number;
        }>,
        pendingPaths: this.db
          .query("SELECT path, source, reason FROM legacy_reconciliation_pending")
          .all() as LegacyReconciliationPendingRow[],
        occurrenceGenerations: this.db
          .query("SELECT DISTINCT generation_key FROM event_occurrences")
          .all() as Array<{ generation_key: string }>,
      }))
      .deferred();

    const byGeneration = new Map<
      string,
      { state: SourceFileState; rowid: number; updatedAt: number }
    >();
    const latestByIdentity = new Map<
      string,
      { state: SourceFileState; rowid: number; updatedAt: number }
    >();
    const generationsByIdentity = new Map<string, string[]>();
    const generationsByRepresentativePath = new Map<string, string[]>();
    for (const row of captured.sourceFiles) {
      const state = sourceFileStateFromSnapshotRow(row);
      const entry = { state, rowid: row.generation_rowid, updatedAt: row.updated_at };
      byGeneration.set(state.generationKey, entry);
      const identity = sourceIdentityKey(state.source, state.device, state.inode);
      const physical = generationsByIdentity.get(identity);
      if (physical) physical.push(state.generationKey);
      else generationsByIdentity.set(identity, [state.generationKey]);
      const latest = latestByIdentity.get(identity);
      if (
        !latest ||
        entry.updatedAt > latest.updatedAt ||
        (entry.updatedAt === latest.updatedAt && entry.rowid > latest.rowid)
      ) {
        latestByIdentity.set(identity, entry);
      }
      const representative = sourcePathKey(state.source, state.representativePath);
      const pathGenerations = generationsByRepresentativePath.get(representative);
      if (pathGenerations) pathGenerations.push(state.generationKey);
      else generationsByRepresentativePath.set(representative, [state.generationKey]);
    }
    const aliasesByPath = new Map(captured.aliases.map((row) => [row.path, row]));
    const activeAliasesByGeneration = new Map<string, string[]>();
    for (const alias of captured.aliases) {
      if (alias.active !== 1) continue;
      const paths = activeAliasesByGeneration.get(alias.generation_key);
      if (paths) paths.push(alias.path);
      else activeAliasesByGeneration.set(alias.generation_key, [alias.path]);
    }
    const pendingPaths = new Set(captured.pendingPaths.map((row) => row.path));
    const occurrenceGenerations = new Set(
      captured.occurrenceGenerations.map((row) => row.generation_key),
    );

    return {
      revision: captured.revision,
      pendingLegacyReconciliations: captured.pendingPaths.map((row) => ({ ...row })),
      getLatestSourceFile(source, device, inode) {
        const state = latestByIdentity.get(sourceIdentityKey(source, device, inode))?.state;
        return state ? { ...state } : null;
      },
      getSourceGenerationKeys(source, device, inode, paths = []) {
        const physical = generationsByIdentity.get(sourceIdentityKey(source, device, inode)) ?? [];
        if (paths.length === 0) return [...physical];
        const pathCandidates = new Set<string>();
        for (const path of paths) {
          for (const key of generationsByRepresentativePath.get(sourcePathKey(source, path)) ??
            []) {
            pathCandidates.add(key);
          }
          const alias = aliasesByPath.get(path);
          if (alias && byGeneration.get(alias.generation_key)?.state.source === source) {
            pathCandidates.add(alias.generation_key);
          }
        }
        const orderedPaths = [...pathCandidates].sort((left, right) => {
          const leftRowid = byGeneration.get(left)?.rowid ?? 0;
          const rightRowid = byGeneration.get(right)?.rowid ?? 0;
          return leftRowid < rightRowid ? -1 : leftRowid > rightRowid ? 1 : 0;
        });
        const seen = new Set<string>();
        const result: string[] = [];
        for (const key of [...physical, ...orderedPaths]) {
          if (seen.has(key)) continue;
          seen.add(key);
          result.push(key);
        }
        return result;
      },
      hasPendingLegacyPath(paths) {
        return paths.some((path) => pendingPaths.has(path));
      },
      hasGenerationOccurrences(generationKey) {
        return occurrenceGenerations.has(generationKey);
      },
      canStageReplacement(source, generationKey, aliases) {
        const target = byGeneration.get(generationKey)?.state;
        if (!target || target.source !== source || !occurrenceGenerations.has(generationKey)) {
          return false;
        }
        const physical =
          generationsByIdentity.get(sourceIdentityKey(source, target.device, target.inode)) ?? [];
        if (physical.length !== 1 || physical[0] !== generationKey) return false;
        const expected = [...new Set(aliases)].sort();
        const active = [...(activeAliasesByGeneration.get(generationKey) ?? [])].sort();
        if (
          expected.length === 0 ||
          expected.length !== aliases.length ||
          active.length !== expected.length ||
          active.some((path, index) => path !== expected[index])
        ) {
          return false;
        }
        return expected.every((path) => {
          const alias = aliasesByPath.get(path);
          return (
            !pendingPaths.has(path) &&
            alias?.generation_key === generationKey &&
            alias.source === source &&
            alias.active === 1
          );
        });
      },
      isCapturedBatchUnchanged(batch) {
        if (batch.events.length > 0 || batch.aliases.some((path) => pendingPaths.has(path))) {
          return false;
        }
        const persisted = byGeneration.get(batch.state.generationKey)?.state;
        if (!persisted || !sameSourceFileState(persisted, batch.state)) return false;
        return batch.aliases.every((path) => {
          const alias = aliasesByPath.get(path);
          return (
            alias?.generation_key === batch.state.generationKey &&
            alias.source === batch.state.source &&
            alias.active === 1
          );
        });
      },
    };
  }

  getRevision(): number {
    const row = this.db.query("SELECT value FROM store_meta WHERE key = 'revision'").get() as
      | { value: string }
      | undefined;
    const revision = Number(row?.value ?? 0);
    return Number.isSafeInteger(revision) && revision >= 0 ? revision : 0;
  }

  /** Runs `fn` under one SQLite read snapshot so multiple aggregates cannot
   * straddle a concurrent writer commit (WAL readers share the transaction
   * snapshot for the duration of the callback). */
  withReadSnapshot<T>(fn: () => T): T {
    return this.db.transaction(fn)();
  }

  private bumpRevision(): number {
    this.db.run(
      "UPDATE store_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'revision'",
    );
    return this.getRevision();
  }

  getLatestSourceFile(source: TokenSource, device: string, inode: string): SourceFileState | null {
    const row = this.db
      .query(
        "SELECT generation_key, source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json FROM source_files WHERE source = ? AND device = ? AND inode = ? ORDER BY updated_at DESC, rowid DESC LIMIT 1",
      )
      .get(source, device, inode) as
      | {
          generation_key: string;
          source: TokenSource;
          device: string;
          inode: string;
          representative_path: string;
          captured_size: number;
          mtime_ms: number;
          mtime_ns: string | null;
          ctime_ns: string | null;
          last_byte_offset: number;
          last_line_number: number;
          committed_prefix_sha256: string;
          codex_state_json: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      generationKey: row.generation_key,
      source: row.source,
      device: row.device,
      inode: row.inode,
      representativePath: row.representative_path,
      capturedSize: row.captured_size,
      mtimeMs: row.mtime_ms,
      mtimeNs: row.mtime_ns,
      ctimeNs: row.ctime_ns,
      lastByteOffset: row.last_byte_offset,
      lastLineNumber: row.last_line_number,
      committedPrefixSha256: row.committed_prefix_sha256,
      codexStateJson: row.codex_state_json,
    };
  }

  getSourceGenerationKeys(
    source: TokenSource,
    device: string,
    inode: string,
    paths: string[] = [],
  ): string[] {
    const physicalRows = this.db
      .query(
        "SELECT generation_key FROM source_files WHERE source = ? AND device = ? AND inode = ? ORDER BY rowid",
      )
      .values(source, device, inode);
    if (paths.length === 0) {
      return physicalRows.flatMap((row) => (typeof row[0] === "string" ? [row[0]] : []));
    }
    const pathRows = this.db
      .query(sourceGenerationPathQuery(paths.length))
      .values(source, ...paths, source, ...paths);
    return [...physicalRows, ...pathRows].flatMap((row, index, rows) => {
      const key = row[0];
      if (typeof key !== "string") return [];
      return rows.findIndex((candidate) => candidate[0] === key) === index ? [key] : [];
    });
  }

  hasPendingLegacyPath(paths: string[]): boolean {
    if (paths.length === 0) return false;
    const placeholders = paths.map(() => "?").join(",");
    const row = this.db
      .query(
        `SELECT 1 AS found FROM legacy_reconciliation_pending WHERE path IN (${placeholders}) LIMIT 1`,
      )
      .get(...paths) as { found: number } | null | undefined;
    return row !== null && row !== undefined;
  }

  private getOccurrence(
    generationKey: string,
    lineNumber: number,
    observedPath?: string,
  ): EventOccurrenceRow | null {
    const row =
      observedPath === undefined
        ? this.db
            .query(
              "SELECT event_id, canonical_key, byte_start, byte_end, raw_record_sha256, logical_message_key FROM event_occurrences WHERE generation_key = ? AND line_number = ? LIMIT 1",
            )
            .values(generationKey, lineNumber)[0]
        : this.db
            .query(
              "SELECT event_id, canonical_key, byte_start, byte_end, raw_record_sha256, logical_message_key FROM event_occurrences WHERE generation_key = ? AND observed_path = ? AND line_number = ? LIMIT 1",
            )
            .values(generationKey, observedPath, lineNumber)[0];
    if (!row) return null;
    const [eventId, canonicalKey, byteStart, byteEnd, rawRecordSha256, logicalMessageKey] = row;
    if (
      typeof eventId !== "number" ||
      typeof canonicalKey !== "string" ||
      typeof byteStart !== "number" ||
      typeof byteEnd !== "number" ||
      typeof rawRecordSha256 !== "string" ||
      (logicalMessageKey !== null && typeof logicalMessageKey !== "string")
    ) {
      throw new Error("invalid persisted event occurrence");
    }
    return {
      eventId,
      canonicalKey,
      byteStart,
      byteEnd,
      rawRecordSha256,
      logicalMessageKey,
    };
  }

  private assertOccurrenceMatch(
    occurrence: EventOccurrenceRow,
    captured: CapturedEvent,
    generationKey: string,
    eventId?: number,
  ): void {
    if (
      (eventId !== undefined && occurrence.eventId !== eventId) ||
      occurrence.canonicalKey !== captured.canonicalKey ||
      occurrence.byteStart !== captured.byteStart ||
      occurrence.byteEnd !== captured.byteEnd ||
      occurrence.rawRecordSha256 !== captured.rawRecordSha256 ||
      occurrence.logicalMessageKey !== captured.logicalMessageKey
    ) {
      throw new Error(`captured occurrence conflict: ${generationKey}:${captured.lineNumber}`);
    }
  }

  private assertReplacementSafe(batch: CapturedFileCommit): ReplacementValidationResult {
    const replacement = batch.replacement;
    if (!replacement) return { eventIds: [], occurrenceCount: 0 };
    const generationKey = batch.state.generationKey;
    const expected = replacement.expectedState;
    const source = batch.state.source;
    const replacementName = source === "codex" ? "Codex" : source === "grok" ? "Grok" : "Claude";
    if (expected.source !== source || expected.generationKey !== generationKey) {
      throw new Error(`${replacementName} replacement target is invalid`);
    }
    const current = this.db
      .query(
        "SELECT source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json FROM source_files WHERE generation_key = ?",
      )
      .get(generationKey) as
      | Omit<SourceFileSnapshotRow, "generation_rowid" | "generation_key" | "updated_at">
      | undefined;
    if (!current) throw new Error(`${replacementName} replacement generation is missing`);
    const currentState: SourceFileState = {
      generationKey,
      source: current.source,
      device: current.device,
      inode: current.inode,
      representativePath: current.representative_path,
      capturedSize: current.captured_size,
      mtimeMs: current.mtime_ms,
      mtimeNs: current.mtime_ns,
      ctimeNs: current.ctime_ns,
      lastByteOffset: current.last_byte_offset,
      lastLineNumber: current.last_line_number,
      committedPrefixSha256: current.committed_prefix_sha256,
      codexStateJson: current.codex_state_json,
    };
    if (!sameSourceFileState(currentState, expected)) {
      throw new Error(`${replacementName} replacement source state changed`);
    }
    let priorCheckpoint: Record<string, unknown> | null;
    try {
      const parsed = JSON.parse(current.codex_state_json ?? "null") as unknown;
      priorCheckpoint =
        typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
          ? (parsed as Record<string, unknown>)
          : null;
    } catch {
      // Malformed legacy checkpoints may be deliberately selected for a
      // versioned replay. Their contents are untrusted; replacement safety is
      // established below from exact source/alias/occurrence coverage and the
      // fully rebuilt current-version checkpoint.
      priorCheckpoint = null;
    }
    if (source === "claude") {
      const priorVersion = priorCheckpoint?.usageParserVersion;
      const priorVersionIsMissingOrOlder =
        typeof priorVersion !== "number" ||
        !Number.isSafeInteger(priorVersion) ||
        priorVersion < CLAUDE_USAGE_PARSER_VERSION;
      if (!priorVersionIsMissingOrOlder || priorCheckpoint?.usageComplete === true) {
        throw new Error(
          "Claude replacement refuses a complete, current, or future parser generation",
        );
      }
      let rebuilt: Record<string, unknown>;
      try {
        const parsed = JSON.parse(batch.state.codexStateJson ?? "null") as unknown;
        const record =
          typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
            ? (parsed as Record<string, unknown>)
            : null;
        if (!record) throw new Error("invalid");
        rebuilt = record;
      } catch {
        throw new Error("Claude replacement result checkpoint is invalid");
      }
      if (
        rebuilt.usageParserVersion !== CLAUDE_USAGE_PARSER_VERSION ||
        rebuilt.usageComplete !== true ||
        !validUsageDefectCheckpoint(rebuilt)
      ) {
        throw new Error("Claude replacement result is incomplete");
      }
      return this.assertReplacementOccurrenceCoverage(batch, current);
    }
    const migration = classifyCodexCheckpointMigration(current.codex_state_json, true);
    if (!migration.replacementAuthorized) {
      throw new Error("Codex replacement refuses an unknown, current, or future parser generation");
    }
    let rebuilt: Record<string, unknown>;
    try {
      const parsed = JSON.parse(batch.state.codexStateJson ?? "null") as unknown;
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error("invalid");
      }
      rebuilt = parsed as Record<string, unknown>;
    } catch {
      throw new Error("Codex replacement result checkpoint is invalid");
    }
    const counter = (name: string): number | null => {
      const value = rebuilt[name];
      return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
    };
    const dispositionFields = [
      "billedUsageRows",
      "inheritedSuppressedRows",
      "ambiguousQuarantinedRows",
      "zeroNoopRows",
      "unrecoverableResetRows",
      "invalidUsageRows",
    ] as const;
    const tokenCountRows = counter("tokenCountRows");
    const dispositionCounts = dispositionFields.map(counter);
    if (
      rebuilt.usageParserVersion !== CODEX_USAGE_PARSER_VERSION ||
      rebuilt.threadSourceRoleRepaired !== true ||
      rebuilt.codexLineageRepaired !== true ||
      rebuilt.depthOneDirectLineageRepaired !== true ||
      rebuilt.codexLineageShapeRepaired !== true ||
      rebuilt.codexDeployedCheckpointRepaired !== true ||
      !validUsageDefectCheckpoint(rebuilt) ||
      (rebuilt.phase !== "none" && rebuilt.phase !== "inherited" && rebuilt.phase !== "live") ||
      rebuilt.usageComplete !== true ||
      tokenCountRows === null ||
      dispositionCounts.some((value) => value === null) ||
      tokenCountRows !== dispositionCounts.reduce<number>((sum, value) => sum + (value ?? 0), 0) ||
      counter("billedUsageRows") !== batch.events.length ||
      counter("ambiguousQuarantinedRows") !== 0 ||
      counter("unrecoverableResetRows") !== 0 ||
      counter("invalidUsageRows") !== 0
    ) {
      throw new Error("Codex replacement result is incomplete or non-conserving");
    }

    return this.assertReplacementOccurrenceCoverage(batch, current);
  }

  private assertReplacementOccurrenceCoverage(
    batch: CapturedFileCommit,
    current: Omit<SourceFileSnapshotRow, "generation_rowid" | "generation_key" | "updated_at">,
  ): ReplacementValidationResult {
    const replacement = batch.replacement;
    if (!replacement) return { eventIds: [], occurrenceCount: 0 };
    const source = batch.state.source;
    const replacementName = source === "codex" ? "Codex" : source === "grok" ? "Grok" : "Claude";
    const generationKey = batch.state.generationKey;
    const aliases = [...new Set(batch.aliases)].sort();
    const expectedAliases = [...new Set(replacement.expectedAliases)].sort();
    if (
      aliases.length === 0 ||
      aliases.length !== batch.aliases.length ||
      expectedAliases.length !== replacement.expectedAliases.length ||
      aliases.length !== expectedAliases.length ||
      aliases.some((path, index) => path !== expectedAliases[index])
    ) {
      throw new Error(`${replacementName} replacement aliases changed`);
    }
    const activeAliases = this.db
      .query(
        "SELECT path FROM source_aliases WHERE generation_key = ? AND source = ? AND active = 1 ORDER BY path",
      )
      .all(generationKey, source) as Array<{ path: string }>;
    if (
      activeAliases.length !== aliases.length ||
      activeAliases.some((row, index) => row.path !== aliases[index])
    ) {
      throw new Error(`${replacementName} replacement active aliases changed`);
    }
    const generations = this.db
      .query(
        "SELECT generation_key FROM source_files WHERE source = ? AND device = ? AND inode = ? ORDER BY rowid",
      )
      .all(source, current.device, current.inode) as Array<{ generation_key: string }>;
    if (generations.length !== 1 || generations[0]?.generation_key !== generationKey) {
      throw new Error(`${replacementName} replacement has multiple source generations`);
    }
    const placeholders = aliases.map(() => "?").join(",");
    const orphan = this.db
      .query(
        `SELECT 1 AS found FROM events e WHERE e.source = ? AND e.file_path IN (${placeholders}) AND NOT EXISTS (SELECT 1 FROM event_occurrences eo WHERE eo.event_id = e.id) LIMIT 1`,
      )
      .get(source, ...aliases) as { found: number } | null | undefined;
    if (orphan) throw new Error(`${replacementName} replacement found an orphan event`);

    const occurrences = this.db
      .query(
        "SELECT eo.event_id, eo.canonical_key, eo.observed_path, eo.byte_start, eo.byte_end, eo.line_number, eo.raw_record_sha256, eo.logical_message_key, e.file_path FROM event_occurrences eo JOIN events e ON e.id = eo.event_id WHERE eo.generation_key = ? ORDER BY eo.line_number, eo.observed_path",
      )
      .all(generationKey) as ReplacementOccurrenceRow[];
    if (occurrences.length === 0) {
      throw new Error(`${replacementName} replacement occurrence coverage is missing`);
    }
    const aliasSet = new Set(aliases);
    const bySourceOccurrence = new Map<number, ReplacementOccurrenceRow[]>();
    for (const occurrence of occurrences) {
      if (
        !aliasSet.has(occurrence.observed_path) ||
        occurrence.line_number < 0 ||
        occurrence.line_number >= current.last_line_number ||
        occurrence.byte_start < 0 ||
        occurrence.byte_end <= occurrence.byte_start ||
        occurrence.byte_end > current.last_byte_offset
      ) {
        throw new Error(`${replacementName} replacement occurrence coverage is invalid`);
      }
      const rows = bySourceOccurrence.get(occurrence.line_number);
      if (rows) rows.push(occurrence);
      else bySourceOccurrence.set(occurrence.line_number, [occurrence]);
    }
    const eventIds = new Set<number>();
    for (const rows of bySourceOccurrence.values()) {
      if (
        rows.length !== aliases.length ||
        rows.some((row, index) => row.observed_path !== aliases[index])
      ) {
        throw new Error(`${replacementName} replacement occurrence aliases are incomplete`);
      }
      const first = rows[0];
      if (
        !first ||
        rows.some(
          (row) =>
            row.event_id !== first.event_id ||
            row.canonical_key !== first.canonical_key ||
            row.byte_start !== first.byte_start ||
            row.byte_end !== first.byte_end ||
            row.line_number !== first.line_number ||
            row.raw_record_sha256 !== first.raw_record_sha256 ||
            row.logical_message_key !== first.logical_message_key,
        )
      ) {
        throw new Error(`${replacementName} replacement occurrence identities conflict`);
      }
      eventIds.add(first.event_id);
    }
    if (source === "codex" && eventIds.size !== bySourceOccurrence.size) {
      throw new Error("Codex replacement occurrence identities conflict");
    }
    return { eventIds: [...eventIds], occurrenceCount: bySourceOccurrence.size };
  }

  commitCapturedFile(batch: CapturedFileCommit): CapturedFileCommitResult {
    const insertedEvents: StoredEvent[] = [];
    let mutated = false;
    const now = Date.now();
    const insertEvent = this.db.prepare(
      "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, dedupe_key, source, canonical_key, canonical_ordinal, raw_record_sha256, immutable_identity, logical_message_key, class_input_tokens, class_cached_read_tokens, class_cache_write_tokens, class_visible_output_tokens, class_reasoning_output_tokens, classes_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    const insertOccurrence = this.db.prepare(
      "INSERT OR IGNORE INTO event_occurrences (event_id, canonical_key, generation_key, observed_path, byte_start, byte_end, line_number, raw_record_sha256, logical_message_key, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    const tx = this.db.transaction(() => {
      const replacementAuthority = this.assertReplacementSafe(batch);
      if (batch.replacement) {
        const deleted = this.db
          .prepare("DELETE FROM event_occurrences WHERE generation_key = ?")
          .run(batch.state.generationKey);
        if (deleted.changes !== batch.aliases.length * replacementAuthority.occurrenceCount) {
          const replacementName =
            batch.state.source === "codex"
              ? "Codex"
              : batch.state.source === "grok"
                ? "Grok"
                : "Claude";
          throw new Error(`${replacementName} replacement occurrence deletion was incomplete`);
        }
        mutated = true;
      }
      const pendingAliases = batch.aliases.filter((path) => {
        const row = this.db
          .query("SELECT 1 AS found FROM legacy_reconciliation_pending WHERE path = ?")
          .get(path) as { found: number } | null | undefined;
        return row !== null && row !== undefined;
      });
      for (const path of pendingAliases) {
        this.db.prepare("DELETE FROM event_occurrences WHERE observed_path = ?").run(path);
        this.db
          .prepare("DELETE FROM events WHERE file_path = ? AND canonical_key IS NULL")
          .run(path);
        this.db.prepare("DELETE FROM file_state WHERE path = ?").run(path);
        this.db.prepare("DELETE FROM source_aliases WHERE path = ?").run(path);
      }
      if (pendingAliases.length > 0) {
        this.db.run(
          "DELETE FROM source_files WHERE generation_key NOT IN (SELECT generation_key FROM source_aliases) AND generation_key NOT IN (SELECT generation_key FROM event_occurrences)",
        );
        mutated = true;
      }
      const previousState = this.db
        .query(
          "SELECT source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json FROM source_files WHERE generation_key = ?",
        )
        .get(batch.state.generationKey) as
        | {
            source: TokenSource;
            device: string;
            inode: string;
            representative_path: string;
            captured_size: number;
            mtime_ms: number;
            mtime_ns: string | null;
            ctime_ns: string | null;
            last_byte_offset: number;
            last_line_number: number;
            committed_prefix_sha256: string;
            codex_state_json: string | null;
          }
        | undefined;
      if (
        !previousState ||
        previousState.source !== batch.state.source ||
        previousState.device !== batch.state.device ||
        previousState.inode !== batch.state.inode ||
        previousState.representative_path !== batch.state.representativePath ||
        previousState.captured_size !== batch.state.capturedSize ||
        previousState.mtime_ms !== batch.state.mtimeMs ||
        previousState.mtime_ns !== batch.state.mtimeNs ||
        previousState.ctime_ns !== batch.state.ctimeNs ||
        previousState.last_byte_offset !== batch.state.lastByteOffset ||
        previousState.last_line_number !== batch.state.lastLineNumber ||
        previousState.committed_prefix_sha256 !== batch.state.committedPrefixSha256 ||
        (previousState.codex_state_json ?? null) !== (batch.state.codexStateJson ?? null)
      ) {
        const result = this.db
          .prepare(
            "INSERT INTO source_files (generation_key, source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(generation_key) DO UPDATE SET source = excluded.source, device = excluded.device, inode = excluded.inode, representative_path = excluded.representative_path, captured_size = excluded.captured_size, mtime_ms = excluded.mtime_ms, mtime_ns = excluded.mtime_ns, ctime_ns = excluded.ctime_ns, last_byte_offset = excluded.last_byte_offset, last_line_number = excluded.last_line_number, committed_prefix_sha256 = excluded.committed_prefix_sha256, codex_state_json = excluded.codex_state_json, updated_at = excluded.updated_at WHERE source_files.source IS NOT excluded.source OR source_files.device IS NOT excluded.device OR source_files.inode IS NOT excluded.inode OR source_files.representative_path IS NOT excluded.representative_path OR source_files.captured_size IS NOT excluded.captured_size OR source_files.mtime_ms IS NOT excluded.mtime_ms OR source_files.mtime_ns IS NOT excluded.mtime_ns OR source_files.ctime_ns IS NOT excluded.ctime_ns OR source_files.last_byte_offset IS NOT excluded.last_byte_offset OR source_files.last_line_number IS NOT excluded.last_line_number OR source_files.committed_prefix_sha256 IS NOT excluded.committed_prefix_sha256 OR source_files.codex_state_json IS NOT excluded.codex_state_json",
          )
          .run(
            batch.state.generationKey,
            batch.state.source,
            batch.state.device,
            batch.state.inode,
            batch.state.representativePath,
            batch.state.capturedSize,
            batch.state.mtimeMs,
            batch.state.mtimeNs,
            batch.state.ctimeNs,
            batch.state.lastByteOffset,
            batch.state.lastLineNumber,
            batch.state.committedPrefixSha256,
            batch.state.codexStateJson,
            now,
          );
        if (result.changes > 0) mutated = true;
      }

      for (const alias of batch.aliases) {
        const previous = this.db
          .query("SELECT generation_key, source, active FROM source_aliases WHERE path = ?")
          .get(alias) as { generation_key: string; source: string; active: number } | undefined;
        if (
          !previous ||
          previous.generation_key !== batch.state.generationKey ||
          previous.source !== batch.state.source ||
          previous.active !== 1
        ) {
          const result = this.db
            .prepare(
              "INSERT INTO source_aliases (path, generation_key, source, active, updated_at) VALUES (?, ?, ?, 1, ?) ON CONFLICT(path) DO UPDATE SET generation_key = excluded.generation_key, source = excluded.source, active = 1, updated_at = excluded.updated_at WHERE source_aliases.generation_key IS NOT excluded.generation_key OR source_aliases.source IS NOT excluded.source OR source_aliases.active IS NOT 1",
            )
            .run(alias, batch.state.generationKey, batch.state.source, now);
          if (result.changes > 0) mutated = true;
        }
      }

      const rankInBatch = new Map<string, number>();
      const priorRanks = new Map<string, number>();
      for (const captured of batch.events) {
        const occurrence = this.getOccurrence(batch.state.generationKey, captured.lineNumber);
        if (occurrence) {
          this.assertOccurrenceMatch(occurrence, captured, batch.state.generationKey);
          const eventRow = this.db
            .query(`SELECT ${EVENT_COLUMNS} FROM events WHERE id = ?`)
            .get(occurrence.eventId) as EventRow | undefined;
          if (!eventRow)
            throw new Error(`captured occurrence missing event: ${occurrence.eventId}`);
          if (
            this.skipCanonicalConflict(
              eventRow,
              captured.event,
              classesFor(captured.event),
              captured.canonicalKey,
            )
          ) {
            continue;
          }
          for (const alias of batch.aliases) {
            const aliasOccurrence = this.getOccurrence(
              batch.state.generationKey,
              captured.lineNumber,
              alias,
            );
            if (aliasOccurrence) {
              this.assertOccurrenceMatch(
                aliasOccurrence,
                captured,
                batch.state.generationKey,
                occurrence.eventId,
              );
              continue;
            }
            const inserted = insertOccurrence.run(
              occurrence.eventId,
              captured.canonicalKey,
              batch.state.generationKey,
              alias,
              captured.byteStart,
              captured.byteEnd,
              captured.lineNumber,
              captured.rawRecordSha256,
              captured.logicalMessageKey,
              now,
            );
            if (inserted.changes > 0) mutated = true;
          }
          continue;
        }
        let ordinal = 0;
        if (!captured.collapseRevisions) {
          let prior = priorRanks.get(captured.canonicalKey);
          if (prior === undefined) {
            const row = this.db
              .query(
                "SELECT COUNT(DISTINCT line_number) AS c FROM event_occurrences WHERE generation_key = ? AND canonical_key = ?",
              )
              .get(batch.state.generationKey, captured.canonicalKey) as { c: number } | undefined;
            prior = row?.c ?? 0;
            priorRanks.set(captured.canonicalKey, prior);
          }
          const batchRank = rankInBatch.get(captured.canonicalKey) ?? 0;
          rankInBatch.set(captured.canonicalKey, batchRank + 1);
          ordinal = prior + batchRank;
        }

        if (captured.immutableIdentity) {
          const immutablePayloadKey = captured.immutablePayloadKey ?? captured.canonicalKey;
          const immutable = this.db
            .query("SELECT canonical_key FROM immutable_records WHERE identity = ?")
            .get(captured.immutableIdentity) as { canonical_key: string } | undefined;
          if (immutable && immutable.canonical_key !== immutablePayloadKey) {
            throw new Error(`immutable token record conflict: ${captured.immutableIdentity}`);
          }
          if (!immutable) {
            this.db
              .prepare("INSERT INTO immutable_records (identity, canonical_key) VALUES (?, ?)")
              .run(captured.immutableIdentity, immutablePayloadKey);
            mutated = true;
          }
        }

        const existing = this.db
          .query(
            `SELECT id, raw_record_sha256, ${EVENT_COLUMNS} FROM events WHERE canonical_key = ? AND canonical_ordinal = ?`,
          )
          .get(captured.canonicalKey, ordinal) as
          | ({ id: number; raw_record_sha256: string | null } & EventRow)
          | undefined;
        let eventId = existing?.id;
        const classes = classesFor(captured.event);
        if (existing) {
          if (!captured.collapseRevisions) {
            if (
              this.skipCanonicalConflict(existing, captured.event, classes, captured.canonicalKey)
            ) {
              continue;
            }
          } else {
            const existingDigest = existing.raw_record_sha256 ?? "";
            const candidateWins =
              compareClaudeAuthorityCandidates(
                { timestampMs: captured.event.ts, rawRecordSha256: captured.rawRecordSha256 },
                { timestampMs: existing.ts, rawRecordSha256: existingDigest },
              ) < 0;
            if (candidateWins) {
              const e = captured.event;
              const updated = this.db
                .prepare(
                  "UPDATE events SET ts = ?, date = ?, model = ?, project = ?, project_slug = ?, session_id = ?, input_tokens = ?, output_tokens = ?, cache_read_tokens = ?, cache_write_5m_tokens = ?, cache_write_1h_tokens = ?, cost = ?, file_path = ?, line_offset = ?, dedupe_key = ?, source = ?, raw_record_sha256 = ?, immutable_identity = ?, logical_message_key = ?, class_input_tokens = ?, class_cached_read_tokens = ?, class_cache_write_tokens = ?, class_visible_output_tokens = ?, class_reasoning_output_tokens = ?, classes_complete = ? WHERE id = ?",
                )
                .run(
                  e.ts,
                  e.date,
                  e.model,
                  e.project,
                  e.projectSlug,
                  e.sessionId,
                  e.inputTokens,
                  e.outputTokens,
                  e.cacheReadTokens,
                  e.cacheWrite5mTokens,
                  e.cacheWrite1hTokens,
                  e.cost,
                  e.filePath,
                  e.lineOffset,
                  e.dedupeKey ?? null,
                  e.source,
                  captured.rawRecordSha256,
                  captured.immutableIdentity,
                  captured.logicalMessageKey,
                  classes.input,
                  classes.cachedRead,
                  classes.cacheWrite,
                  classes.visibleOutput,
                  classes.reasoningOutput,
                  classes.complete ? 1 : 0,
                  existing.id,
                );
              if (updated.changes > 0) mutated = true;
            }
          }
        } else {
          const e = captured.event;
          const result = insertEvent.run(
            e.ts,
            e.date,
            e.model,
            e.project,
            e.projectSlug,
            e.sessionId,
            e.inputTokens,
            e.outputTokens,
            e.cacheReadTokens,
            e.cacheWrite5mTokens,
            e.cacheWrite1hTokens,
            e.cost,
            e.filePath,
            e.lineOffset,
            e.dedupeKey ?? null,
            e.source,
            captured.canonicalKey,
            ordinal,
            captured.rawRecordSha256,
            captured.immutableIdentity,
            captured.logicalMessageKey,
            classes.input,
            classes.cachedRead,
            classes.cacheWrite,
            classes.visibleOutput,
            classes.reasoningOutput,
            classes.complete ? 1 : 0,
          );
          eventId = Number(result.lastInsertRowid);
          insertedEvents.push({ ...e, tokenClasses: classes });
          mutated = true;
        }

        if (eventId === undefined) throw new Error("token event id unavailable after commit");

        for (const alias of batch.aliases) {
          const aliasOccurrence = this.getOccurrence(
            batch.state.generationKey,
            captured.lineNumber,
            alias,
          );
          if (aliasOccurrence) {
            this.assertOccurrenceMatch(
              aliasOccurrence,
              captured,
              batch.state.generationKey,
              eventId,
            );
            continue;
          }
          const inserted = insertOccurrence.run(
            eventId,
            captured.canonicalKey,
            batch.state.generationKey,
            alias,
            captured.byteStart,
            captured.byteEnd,
            captured.lineNumber,
            captured.rawRecordSha256,
            captured.logicalMessageKey,
            now,
          );
          if (inserted.changes > 0) mutated = true;
        }
      }
      if (batch.replacement) {
        const deleteUnreferenced = this.db.prepare(
          "DELETE FROM events WHERE id = ? AND NOT EXISTS (SELECT 1 FROM event_occurrences WHERE event_id = ?)",
        );
        for (const eventId of replacementAuthority.eventIds) {
          deleteUnreferenced.run(eventId, eventId);
        }
      }
      for (const path of pendingAliases) {
        this.db.prepare("DELETE FROM legacy_reconciliation_pending WHERE path = ?").run(path);
      }
      if (mutated && !this.deferRevisionBump) this.bumpRevision();
    });
    tx();
    return {
      inserted: insertedEvents.length,
      insertedEvents,
      mutated,
      revision: this.getRevision(),
    };
  }

  private skipCanonicalConflict(
    row: EventRow,
    event: StoredEvent,
    classes: NormalizedTokenClasses,
    canonicalKey: string,
  ): boolean {
    try {
      this.assertCanonicalMatch(row, event, classes, canonicalKey);
      return false;
    } catch (error) {
      if (!String(error).includes("canonical token record conflict")) throw error;
      // unified.jsonl offsets/class metadata can drift; skip this occurrence
      // instead of aborting the ingest tick (which stale-locks the HUD).
      console.error(`canonical token record conflict skipped: ${canonicalKey}`);
      return true;
    }
  }

  private assertCanonicalMatch(
    row: EventRow,
    event: StoredEvent,
    _classes: NormalizedTokenClasses,
    canonicalKey: string,
  ): void {
    if (
      row.ts !== event.ts ||
      row.date !== event.date ||
      row.model !== event.model ||
      row.project !== event.project ||
      row.project_slug !== event.projectSlug ||
      row.session_id !== event.sessionId ||
      row.input_tokens !== event.inputTokens ||
      row.output_tokens !== event.outputTokens ||
      row.cache_read_tokens !== event.cacheReadTokens ||
      row.cache_write_5m_tokens !== event.cacheWrite5mTokens ||
      row.cache_write_1h_tokens !== event.cacheWrite1hTokens ||
      row.cost !== event.cost ||
      row.source !== event.source
    ) {
      throw new Error(`canonical token record conflict: ${canonicalKey}`);
    }
  }

  reconcileDiscovery(
    aliases: DiscoveredAlias[],
    providers: DiscoveryProviderState[],
    legacyReconciliationObservations: LegacyReconciliationObservation[] = [],
  ): {
    mutated: boolean;
    revision: number;
    expiredMissingBySource: Partial<Record<TokenSource, number>>;
  } {
    let mutated = false;
    const expiredMissingBySource: Partial<Record<TokenSource, number>> = {};
    const now = Date.now();
    const tx = this.db.transaction(() => {
      for (const provider of providers) {
        if (provider.available && provider.complete) {
          for (const observation of legacyReconciliationObservations) {
            if (observation.source !== provider.source || observation.observed !== "ENOENT") {
              continue;
            }
            const result = this.db
              .prepare(
                "UPDATE legacy_reconciliation_pending SET reason = 'expired-missing' WHERE path = ? AND source = ? AND reason = ? AND reason IS NOT 'expired-missing'",
              )
              .run(observation.path, observation.source, observation.reason);
            if (result.changes > 0) {
              expiredMissingBySource[provider.source] =
                (expiredMissingBySource[provider.source] ?? 0) + result.changes;
              mutated = true;
            }
          }
        }
        const complete = provider.complete;
        const previous = this.db
          .query("SELECT available, complete FROM provider_state WHERE source = ?")
          .get(provider.source) as { available: number; complete: number } | undefined;
        if (
          !previous ||
          previous.available !== (provider.available ? 1 : 0) ||
          previous.complete !== (complete ? 1 : 0)
        ) {
          const result = this.db
            .prepare(
              "INSERT INTO provider_state (source, available, complete, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(source) DO UPDATE SET available = excluded.available, complete = excluded.complete, updated_at = excluded.updated_at WHERE provider_state.available IS NOT excluded.available OR provider_state.complete IS NOT excluded.complete",
            )
            .run(provider.source, provider.available ? 1 : 0, complete ? 1 : 0, now);
          if (result.changes > 0) mutated = true;
        }
        if (complete) {
          const activePaths = new Set(
            aliases.filter((a) => a.source === provider.source).map((a) => a.path),
          );
          const existing = this.db
            .query("SELECT path FROM source_aliases WHERE source = ? AND active = 1")
            .all(provider.source) as Array<{ path: string }>;
          for (const row of existing) {
            if (!activePaths.has(row.path)) {
              const result = this.db
                .prepare(
                  "UPDATE source_aliases SET active = 0, updated_at = ? WHERE path = ? AND active IS NOT 0",
                )
                .run(now, row.path);
              if (result.changes > 0) mutated = true;
            }
          }
        }
      }
      if (mutated && !this.deferRevisionBump) this.bumpRevision();
    });
    tx();
    return { mutated, revision: this.getRevision(), expiredMissingBySource };
  }

  /** Commits a fully staged scan as one transaction. Bun's SQLite driver
   * implements nested `db.transaction` calls as SAVEPOINTs, so the virtual
   * per-file method remains the deterministic failure-injection seam while
   * this outer transaction provides scan-wide atomicity. */
  commitScan(scan: ScanCommit): ScanCommitResult {
    const outer = this.db.transaction(() => {
      // This scan-level SAVEPOINT is deliberately inside the outer
      // transaction: a caught immutable/canonical conflict rolls every prior
      // file batch back before fail-closed provider state is committed.
      const applyAll = this.db.transaction(() => {
        const authorityRevision = this.getRevision();
        if (
          scan.batches.some((batch) => batch.replacement !== undefined) &&
          scan.sourceSnapshotRevision !== authorityRevision
        ) {
          throw new Error("Codex replacement source snapshot revision changed");
        }
        const skippableBatchIndexes =
          scan.sourceSnapshotRevision === authorityRevision
            ? new Set(scan.unchangedBatchIndexes ?? [])
            : null;
        const before = this.db.query("SELECT COALESCE(MAX(id), 0) AS id FROM events").get() as {
          id: number;
        };
        let mutated = false;
        let expiredMissingBySource: Partial<Record<TokenSource, number>> = {};
        const alreadyDeferred = this.deferRevisionBump;
        this.deferRevisionBump = true;
        try {
          for (const [index, batch] of scan.batches.entries()) {
            if (batch.events.length === 0 && skippableBatchIndexes?.has(index)) continue;
            const result = this.commitCapturedFile(batch);
            mutated ||= result.mutated;
          }
          const reconciled = this.reconcileDiscovery(
            scan.aliases,
            scan.providers,
            scan.legacyReconciliationObservations,
          );
          mutated ||= reconciled.mutated;
          expiredMissingBySource = reconciled.expiredMissingBySource;
        } finally {
          this.deferRevisionBump = alreadyDeferred;
        }
        if (mutated && !alreadyDeferred) this.bumpRevision();
        // Re-read rows created by this scan after every collapsed-winner
        // replacement has settled. This prevents SSE deltas from publishing a
        // transient path-order loser that no longer exists in authority.
        const insertedEvents = this.mapRows(
          this.db
            .query(`SELECT ${EVENT_COLUMNS} FROM events WHERE id > ? ORDER BY id`)
            .all(before.id) as EventRow[],
        );
        return {
          insertedEvents,
          mutated,
          revision: this.getRevision(),
          expiredMissingBySource,
        };
      });
      try {
        return { committed: applyAll(), conflict: null };
      } catch (error) {
        if (!String(error).includes("token record conflict")) throw error;
        this.markProvidersIncomplete(scan.providers);
        return {
          committed: null,
          conflict: error instanceof Error ? error : new Error(String(error)),
        };
      }
    });
    // Acquire the write reservation before checking the snapshot revision, so
    // another opener cannot change authority between the guard and commit.
    const outcome = outer.immediate();
    if (outcome.conflict) throw outcome.conflict;
    if (!outcome.committed) throw new Error("token scan commit produced no result");
    const expirySummary = Object.entries(outcome.committed.expiredMissingBySource)
      .flatMap(([source, count]) => ((count ?? 0) > 0 ? [`${source}=${count}`] : []))
      .join(" ");
    if (expirySummary)
      console.error(`token legacy reconciliation expired-missing ${expirySummary}`);
    return {
      insertedEvents: outcome.committed.insertedEvents,
      mutated: outcome.committed.mutated,
      revision: outcome.committed.revision,
    };
  }

  markProvidersIncomplete(providers: DiscoveryProviderState[]): {
    mutated: boolean;
    revision: number;
  } {
    return this.reconcileDiscovery(
      [],
      providers.map((provider) => ({ ...provider, complete: false })),
    );
  }

  getProviderStates(): ProviderState[] {
    const activeCheckpoints = this.db
      .query(
        `SELECT sf.source, sf.codex_state_json
         FROM source_files sf
         JOIN (
           SELECT DISTINCT generation_key
           FROM source_aliases
           WHERE active = 1
         ) active ON active.generation_key = sf.generation_key`,
      )
      .all() as Array<{ source: TokenSource; codex_state_json: string | null }>;
    const defectsBySource: Record<TokenSource, number> = { claude: 0, codex: 0, grok: 0 };
    for (const checkpoint of activeCheckpoints) {
      defectsBySource[checkpoint.source] += checkpointDefectLineCount(checkpoint.codex_state_json);
    }
    const rows = this.db
      .query("SELECT source, available, complete, updated_at FROM provider_state ORDER BY source")
      .all() as Array<{
      source: TokenSource;
      available: number;
      complete: number;
      updated_at: number;
    }>;
    return rows.map((row) => ({
      source: row.source,
      available: row.available === 1,
      complete: row.complete === 1,
      defectLines: defectsBySource[row.source],
      updatedAt: row.updated_at,
    }));
  }

  insertEvents(events: StoredEvent[]): InsertEventsResult {
    if (events.length === 0) return { inserted: 0, insertedEvents: [] };
    const insertedEvents: StoredEvent[] = [];
    const stmt = this.db.prepare(
      "INSERT INTO events (ts, date, model, project, project_slug, session_id, input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost, file_path, line_offset, dedupe_key, source, class_input_tokens, class_cached_read_tokens, class_cache_write_tokens, class_visible_output_tokens, class_reasoning_output_tokens, classes_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    const tx = this.db.transaction(() => {
      for (const e of events) {
        const c = classesFor(e);
        stmt.run(
          e.ts,
          e.date,
          e.model,
          e.project,
          e.projectSlug,
          e.sessionId,
          e.inputTokens,
          e.outputTokens,
          e.cacheReadTokens,
          e.cacheWrite5mTokens,
          e.cacheWrite1hTokens,
          e.cost,
          e.filePath,
          e.lineOffset,
          e.dedupeKey ?? null,
          e.source,
          c.input,
          c.cachedRead,
          c.cacheWrite,
          c.visibleOutput,
          c.reasoningOutput,
          c.complete ? 1 : 0,
        );
        insertedEvents.push({ ...e, tokenClasses: c });
      }
      this.bumpRevision();
    });
    tx();
    return { inserted: insertedEvents.length, insertedEvents };
  }

  getFileState(path: string): FileState | null {
    const row = this.db
      .query(
        "SELECT path, mtime_ms, size, last_byte_offset, last_line_number FROM file_state WHERE path = ?",
      )
      .get(path) as
      | {
          path: string;
          mtime_ms: number;
          size: number;
          last_byte_offset: number;
          last_line_number: number;
        }
      | undefined;
    return row
      ? {
          path: row.path,
          mtimeMs: row.mtime_ms,
          size: row.size,
          lastByteOffset: row.last_byte_offset,
          lastLineNumber: row.last_line_number,
        }
      : null;
  }

  upsertFileState(state: FileState): void {
    this.db
      .prepare(
        "INSERT INTO file_state (path, mtime_ms, size, last_byte_offset, last_line_number, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(path) DO UPDATE SET mtime_ms = excluded.mtime_ms, size = excluded.size, last_byte_offset = excluded.last_byte_offset, last_line_number = excluded.last_line_number, updated_at = excluded.updated_at",
      )
      .run(
        state.path,
        state.mtimeMs,
        state.size,
        state.lastByteOffset,
        state.lastLineNumber,
        Date.now(),
      );
  }

  selectEventsForSession(sessionId: string): StoredEvent[] {
    return this.mapRows(
      this.db
        .query(`SELECT ${EVENT_COLUMNS} FROM events WHERE session_id = ? ORDER BY ts ASC`)
        .all(sessionId) as EventRow[],
    );
  }

  /** Latest single request for a session, by (ts DESC, id DESC) — the `id`
   * tiebreak deterministically picks the most-recently-inserted row when two
   * events share an exact millisecond timestamp. This backs a context-size
   * ESTIMATE for the auto-compact nudge; it is never a substitute for
   * selectEventsForSession's full-history ordering. */
  selectLatestEventForSession(sessionId: string): StoredEvent | null {
    const sql =
      "SELECT " +
      EVENT_COLUMNS +
      " FROM events WHERE session_id = ? ORDER BY ts DESC, id DESC LIMIT 1";
    const row = this.db.query(sql).get(sessionId) as EventRow | undefined;
    return row ? this.mapRows([row])[0]! : null;
  }

  aggregateEventTotals(opts?: { fromTs?: number; toTs?: number }): EventTotalsAggregate {
    const fromTs = opts?.fromTs ?? 0;
    const toTs = opts?.toTs ?? Number.MAX_SAFE_INTEGER;
    const row = this.db
      .query(
        `SELECT
          COALESCE(SUM(input_tokens), 0) AS inputTokens,
          COALESCE(SUM(output_tokens), 0) AS outputTokens,
          COALESCE(SUM(cache_read_tokens), 0) AS cacheReadTokens,
          COALESCE(SUM(cache_write_5m_tokens), 0) AS cacheWrite5mTokens,
          COALESCE(SUM(cache_write_1h_tokens), 0) AS cacheWrite1hTokens,
          COALESCE(SUM(cost), 0) AS cost,
          COUNT(*) AS messages,
          MIN(ts) AS minTs,
          MAX(ts) AS maxTs
        FROM events
        WHERE ts >= ? AND ts <= ?`,
      )
      .get(fromTs, toTs) as EventTotalsAggregate | undefined;
    return (
      row ?? {
        inputTokens: 0,
        outputTokens: 0,
        cacheReadTokens: 0,
        cacheWrite5mTokens: 0,
        cacheWrite1hTokens: 0,
        cost: 0,
        messages: 0,
        minTs: null,
        maxTs: null,
      }
    );
  }

  /** Builds the complete `/api/usage` snapshot with ordered SQLite cursors.
   *
   * No matching event array is ever materialized. The first cursor preserves
   * the historical `(ts, id)` reducer order for exact floating addition and
   * first-seen bucket identity. The second cursor groups one session at a time
   * and retains only the best 25, so session memory is fixed even when every
   * event belongs to a different session. Both cursors share one read
   * transaction and the exact same filter predicate. */
  aggregateUsage(opts: UsageAggregateOptions = {}): UsageAggregateSnapshot {
    const filter = usageAggregateFilter(opts);
    return this.db
      .transaction(() => {
        const totalUsage = emptyUsageAggregateTally();
        let totalsMinTs: number | null = null;
        let totalsMaxTs: number | null = null;
        const dailyBuckets = new Map<string, UsageAggregateSnapshot["daily"][number]>();
        const modelBuckets = new Map<string, UsageAggregateSnapshot["byModel"][number]>();
        const projectBuckets = new Map<string, UsageAggregateSnapshot["byProject"][number]>();
        const harnessBuckets = new Map<string, UsageAggregateSnapshot["byHarness"][number]>();
        const chronologicalRows = this.db
          .query(
            `SELECT ${USAGE_CURSOR_COLUMNS} FROM events
             WHERE ${filter.sql}
             ORDER BY ts ASC, id ASC`,
          )
          .iterate(...filter.params) as IterableIterator<UsageCursorRow>;
        for (const row of chronologicalRows) {
          addUsageCursorRow(totalUsage, row);
          totalsMinTs = totalsMinTs === null ? row.ts : Math.min(totalsMinTs, row.ts);
          totalsMaxTs = totalsMaxTs === null ? row.ts : Math.max(totalsMaxTs, row.ts);

          let daily = dailyBuckets.get(row.date);
          if (!daily) {
            daily = { date: row.date, ...emptyUsageAggregateTally() };
            dailyBuckets.set(row.date, daily);
          }
          addUsageCursorRow(daily, row);

          let model = modelBuckets.get(row.model);
          if (!model) {
            model = {
              model: row.model,
              ...emptyUsageAggregateTally(),
              minTs: row.ts,
              maxTs: row.ts,
            };
            modelBuckets.set(row.model, model);
          }
          addUsageCursorRow(model, row);
          model.minTs = Math.min(model.minTs, row.ts);
          model.maxTs = Math.max(model.maxTs, row.ts);

          let project = projectBuckets.get(row.project);
          if (!project) {
            project = {
              project: row.project,
              projectSlug: row.projectSlug,
              ...emptyUsageAggregateTally(),
              sessions: 0,
              lastTs: row.ts,
            };
            projectBuckets.set(row.project, project);
          }
          addUsageCursorRow(project, row);
          project.lastTs = Math.max(project.lastTs, row.ts);

          let harness = harnessBuckets.get(row.source);
          if (!harness) {
            harness = {
              harness: row.source,
              ...emptyUsageAggregateTally(),
              minTs: row.ts,
              maxTs: row.ts,
            };
            harnessBuckets.set(row.source, harness);
          }
          addUsageCursorRow(harness, row);
          harness.minTs = Math.min(harness.minTs, row.ts);
          harness.maxTs = Math.max(harness.maxTs, row.ts);
        }

        type RankedSession = UsageAggregateSnapshot["topSessions"][number] & {
          firstId: number;
        };
        const rankedSessions: RankedSession[] = [];
        let session: RankedSession | null = null;
        const seenModels = new Set<string>();
        const finishSession = (): void => {
          if (!session) return;
          const project = projectBuckets.get(session.project);
          if (project) project.sessions += 1;
          rankedSessions.push(session);
          rankedSessions.sort(
            (left, right) =>
              right.cost - left.cost ||
              left.firstTs - right.firstTs ||
              left.firstId - right.firstId,
          );
          if (rankedSessions.length > 25) rankedSessions.pop();
        };
        const sessionRows = this.db
          .query(usageSessionCursorQuery(filter.sql))
          .iterate(...filter.params) as IterableIterator<UsageCursorRow>;
        for (const row of sessionRows) {
          if (!session || session.sessionId !== row.sessionId) {
            finishSession();
            seenModels.clear();
            session = {
              sessionId: row.sessionId,
              project: row.project,
              projectSlug: row.projectSlug,
              models: [],
              ...emptyUsageAggregateTally(),
              firstTs: row.ts,
              lastTs: row.ts,
              firstId: row.id,
            };
          }
          addUsageCursorRow(session, row);
          session.lastTs = Math.max(session.lastTs, row.ts);
          if (!seenModels.has(row.model)) {
            seenModels.add(row.model);
            session.models.push(row.model);
          }
        }
        finishSession();

        const daily = [...dailyBuckets.values()].sort((left, right) =>
          left.date.localeCompare(right.date),
        );
        const byModel = [...modelBuckets.values()].sort(
          (left, right) => right.totalTokens - left.totalTokens,
        );
        const byProject = [...projectBuckets.values()].sort(
          (left, right) => right.totalTokens - left.totalTokens,
        );
        const byHarness = [...harnessBuckets.values()].sort(
          (left, right) => right.totalTokens - left.totalTokens,
        );
        const topSessions = rankedSessions.map(({ firstId: _firstId, ...value }) => value);
        const files =
          (
            this.db.query("SELECT COUNT(*) AS c FROM source_files").get() as
              | { c: number }
              | undefined
          )?.c ??
          (this.db.query("SELECT COUNT(*) AS c FROM file_state").get() as { c: number } | undefined)
            ?.c ??
          0;
        return {
          totals: { ...totalUsage, minTs: totalsMinTs, maxTs: totalsMaxTs },
          daily,
          byModel,
          byProject,
          topSessions,
          byHarness,
          files,
        };
      })
      .deferred();
  }

  selectEvents(opts?: { fromTs?: number; toTs?: number }): StoredEvent[] {
    const fromTs = opts?.fromTs ?? 0;
    const toTs = opts?.toTs ?? Number.MAX_SAFE_INTEGER;
    return this.mapRows(
      this.db
        .query(
          `SELECT ${EVENT_COLUMNS} FROM events WHERE ts >= ? AND ts <= ? ORDER BY ts ASC, id ASC`,
        )
        .all(fromTs, toTs) as EventRow[],
    );
  }

  /** Streams only the columns needed by the isolated rolling-rate reducer. */
  iterateRateEvents(opts: { fromTs: number; toTs: number }): IterableIterator<RateEvent> {
    return this.db
      .query(rateEventCursorQuery())
      .iterate(opts.fromTs, opts.toTs) as IterableIterator<RateEvent>;
  }

  /** Streams one compact latest timestamp per session from a SQLite grouping.
   * The isolated reducer can count exact active sessions without a JS Set/Map. */
  iterateRateSessionActivity(opts: {
    fromTs: number;
    toTs: number;
  }): IterableIterator<RateSessionActivity> {
    return this.db
      .query(rateSessionActivityCursorQuery())
      .iterate(opts.fromTs, opts.toTs) as IterableIterator<RateSessionActivity>;
  }

  selectEventsInWindow(startMs: number, endMs: number): StoredEvent[] {
    return this.mapRows(
      this.db
        .query(`SELECT ${EVENT_COLUMNS} FROM events WHERE ts >= ? AND ts < ? ORDER BY ts ASC`)
        .all(startMs, endMs) as EventRow[],
    );
  }

  getEventFilePathCounts(): FilePathCountRow[] {
    const rows = this.db
      .query(
        "SELECT file_path AS filePath, COUNT(*) AS rowCount FROM events GROUP BY file_path ORDER BY filePath",
      )
      .all() as Array<{ filePath: string; rowCount: number }>;
    return rows.map((row) => ({ filePath: row.filePath, rowCount: row.rowCount }));
  }

  deleteEventRowsByFilePaths(filePaths: string[]): number {
    if (filePaths.length === 0) return 0;
    const placeholders = filePaths.map(() => "?").join(",");
    return this.db
      .query(`DELETE FROM events WHERE file_path IN (${placeholders})`)
      .run(...filePaths).changes;
  }

  deleteFileStateRowsByFilePaths(filePaths: string[]): number {
    if (filePaths.length === 0) return 0;
    const placeholders = filePaths.map(() => "?").join(",");
    return this.db.query(`DELETE FROM file_state WHERE path IN (${placeholders})`).run(...filePaths)
      .changes;
  }

  private mapRows(rows: EventRow[]): StoredEvent[] {
    return rows.map((r) => ({
      ts: r.ts,
      date: r.date,
      model: r.model,
      project: r.project,
      projectSlug: r.project_slug,
      sessionId: r.session_id,
      inputTokens: r.input_tokens,
      outputTokens: r.output_tokens,
      cacheReadTokens: r.cache_read_tokens,
      cacheWrite5mTokens: r.cache_write_5m_tokens,
      cacheWrite1hTokens: r.cache_write_1h_tokens,
      cost: r.cost,
      filePath: r.file_path,
      lineOffset: r.line_offset,
      source: r.source,
      tokenClasses: {
        input: r.class_input_tokens,
        cachedRead: r.class_cached_read_tokens,
        cacheWrite: r.class_cache_write_tokens,
        visibleOutput: r.class_visible_output_tokens,
        reasoningOutput: r.class_reasoning_output_tokens,
        complete: r.classes_complete === 1,
      },
    }));
  }

  stats(testBetweenCountsDelayMs = 0): { events: number; files: number } {
    if (
      !Number.isSafeInteger(testBetweenCountsDelayMs) ||
      testBetweenCountsDelayMs < 0 ||
      testBetweenCountsDelayMs > 5_000
    ) {
      throw new Error("stats read delay must be a safe integer between 0 and 5000ms");
    }
    const events =
      (this.db.query("SELECT COUNT(*) AS c FROM events").get() as { c: number } | undefined)?.c ??
      0;
    // The explicit non-production seam stalls between the two real COUNTs,
    // after a deferred transaction has established its read snapshot.
    if (testBetweenCountsDelayMs > 0) Bun.sleepSync(testBetweenCountsDelayMs);
    const files =
      (this.db.query("SELECT COUNT(*) AS c FROM source_files").get() as { c: number } | undefined)
        ?.c ??
      (this.db.query("SELECT COUNT(*) AS c FROM file_state").get() as { c: number } | undefined)
        ?.c ??
      0;
    return { events, files };
  }

  /** Builds the daily (or caller-supplied) authority with one bounded SQL row
   * per provider/model.
   *
   * The long-lived HTTP process must never materialize the daily event set.
   * This roll-up runs only in the isolated ingest worker and keeps heap use
   * proportional to the number of model buckets rather than event rows.
   */
  buildAuthoritativeTokenSnapshot(
    updatedAt: number,
    window: TokenWindow = dailyTokenWindow(updatedAt),
  ): AuthoritativeTokenSnapshot {
    if (!Number.isSafeInteger(updatedAt) || updatedAt < 0) {
      throw new Error("token snapshot updatedAt must be a non-negative safe integer");
    }
    const incompleteSources = new Set<TokenSource>();
    const relevantCheckpoints = this.db
      .query(
        `WITH relevant_generations(generation_key) AS (
          SELECT generation_key
          FROM source_files
          WHERE mtime_ms >= ? AND mtime_ms < ?
          UNION
          SELECT eo.generation_key
          FROM events e
          JOIN event_occurrences eo ON eo.event_id = e.id
          WHERE e.ts >= ? AND e.ts < ?
        )
        SELECT sf.source, sf.codex_state_json
        FROM relevant_generations relevant
        JOIN source_files sf ON sf.generation_key = relevant.generation_key
        ORDER BY sf.source, sf.generation_key`,
      )
      .all(window.startMs, window.endMs, window.startMs, window.endMs) as Array<{
      source: TokenSource;
      codex_state_json: string | null;
    }>;
    for (const checkpoint of relevantCheckpoints) {
      if (!checkpointUsageComplete(checkpoint.source, checkpoint.codex_state_json)) {
        incompleteSources.add(checkpoint.source);
      }
    }
    const unresolvedLegacySources = this.db
      .query(
        "SELECT DISTINCT source FROM legacy_reconciliation_pending WHERE reason != 'expired-missing' ORDER BY source",
      )
      .all() as Array<{ source: TokenSource }>;
    for (const pending of unresolvedLegacySources) incompleteSources.add(pending.source);
    const rows = this.db
      .query(
        `SELECT
          source,
          model,
          COALESCE(SUM(input_tokens), 0) AS inputTokens,
          COALESCE(SUM(output_tokens), 0) AS outputTokens,
          COALESCE(SUM(cache_read_tokens), 0) AS cacheReadTokens,
          COALESCE(SUM(cache_write_5m_tokens), 0) AS cacheWrite5mTokens,
          COALESCE(SUM(cache_write_1h_tokens), 0) AS cacheWrite1hTokens,
          COALESCE(SUM(cost), 0) AS cost,
          COUNT(*) AS messages,
          CASE WHEN COUNT(class_input_tokens) = COUNT(*)
            THEN COALESCE(SUM(class_input_tokens), 0) ELSE NULL END AS classInputTokens,
          CASE WHEN COUNT(class_cached_read_tokens) = COUNT(*)
            THEN COALESCE(SUM(class_cached_read_tokens), 0) ELSE NULL END AS classCachedReadTokens,
          CASE WHEN COUNT(class_cache_write_tokens) = COUNT(*)
            THEN COALESCE(SUM(class_cache_write_tokens), 0) ELSE NULL END AS classCacheWriteTokens,
          CASE WHEN COUNT(class_visible_output_tokens) = COUNT(*)
            THEN COALESCE(SUM(class_visible_output_tokens), 0) ELSE NULL END AS classVisibleOutputTokens,
          CASE WHEN COUNT(class_reasoning_output_tokens) = COUNT(*)
            THEN COALESCE(SUM(class_reasoning_output_tokens), 0) ELSE NULL END AS classReasoningOutputTokens
        FROM events
        WHERE ts >= ? AND ts < ?
        GROUP BY source, model
        ORDER BY source ASC, model COLLATE BINARY ASC`,
      )
      .iterate(window.startMs, window.endMs) as IterableIterator<TokenSnapshotAggregateRow>;
    const rowsBySource: Record<TokenSource, Map<string, TokenSnapshotAggregateRow>> = {
      claude: new Map<string, TokenSnapshotAggregateRow>(),
      codex: new Map<string, TokenSnapshotAggregateRow>(),
      grok: new Map<string, TokenSnapshotAggregateRow>(),
    };
    for (const row of rows) {
      if (row.source !== "claude" && row.source !== "codex" && row.source !== "grok") {
        throw new Error(`unsupported token snapshot source: ${row.source}`);
      }
      const modelMap = rowsBySource[row.source];
      const normalizedModel = normalizeTokenModel(row.model);
      const model =
        Buffer.byteLength(normalizedModel, "utf8") > TOKEN_SNAPSHOT_MAX_MODEL_BYTES ||
        (!modelMap.has(normalizedModel) &&
          modelMap.size >= TOKEN_SNAPSHOT_MAX_MODELS_PER_PROVIDER - 1)
          ? TOKEN_SNAPSHOT_OVERFLOW_MODEL
          : normalizedModel;
      const normalizedRow = { ...row, model };
      const previous = modelMap.get(model);
      modelMap.set(
        model,
        previous ? mergeTokenSnapshotAggregateRows(previous, normalizedRow) : normalizedRow,
      );
    }
    const providerStates = new Map(this.getProviderStates().map((state) => [state.source, state]));
    const providerFor = (source: TokenSource): SnapshotProvider => {
      const state = providerStates.get(source);
      const available = state?.available ?? false;
      const discoveryComplete =
        available && (state?.complete ?? false) && !incompleteSources.has(source);
      const models = [...rowsBySource[source].values()]
        .sort((left, right) => compareTokenModelNames(left.model, right.model))
        .map(
          (row): SnapshotModel => ({
            model: row.model,
            rawTotals: aggregateRowRawTotals(row),
            classes: modelSnapshotClasses(source, row, discoveryComplete),
          }),
        );
      const rawTotals = models.reduce(
        (totals, model) => addSnapshotRawTotals(totals, model.rawTotals),
        emptySnapshotRawTotals(),
      );
      return {
        source,
        available,
        discoveryComplete,
        defectLines: state?.defectLines ?? 0,
        rawTotals,
        classes: providerSnapshotClasses(source, models, discoveryComplete),
        models,
      };
    };
    const claude = providerFor("claude");
    const codex = providerFor("codex");
    const grok = providerFor("grok");
    const providers = { claude, codex, grok };
    const rawTotals = addSnapshotRawTotals(
      addSnapshotRawTotals(claude.rawTotals, codex.rawTotals),
      grok.rawTotals,
    );
    const classes = composeSnapshotTokenClasses(snapshotClassProviders(providers));
    const snapshot: AuthoritativeTokenSnapshot = {
      source: "sigmashake-tokens",
      window,
      revision: this.getRevision(),
      updatedAt,
      totalCost: rawTotals.cost,
      calls: rawTotals.messages,
      rawTotals,
      classes,
      hud: { in: classes.input, out: classes.visibleOutput },
      providers,
    };
    if (!isAuthoritativeTokenSnapshot(snapshot)) {
      throw new Error("aggregated authoritative token snapshot is invalid");
    }
    return snapshot;
  }

  /** Atomically rolls up and publishes the current daily authority. */
  refreshAuthoritativeTokenSnapshot(
    updatedAt: number,
    expectedRevision?: number,
  ): AuthoritativeTokenSnapshot {
    return this.db
      .transaction(() => {
        if (expectedRevision !== undefined && this.getRevision() !== expectedRevision) {
          throw new Error("authoritative token snapshot revision changed before refresh");
        }
        const snapshot = this.buildAuthoritativeTokenSnapshot(updatedAt);
        if (expectedRevision !== undefined && snapshot.revision !== expectedRevision) {
          throw new Error("authoritative token snapshot revision changed during refresh");
        }
        this.writeCachedTokenSnapshot(snapshot);
        return snapshot;
      })
      .immediate();
  }

  /** Reads the tiny persisted count snapshot without touching either large table. */
  readCachedStats(): CachedTokenStats | null {
    const row = this.db
      .query("SELECT value FROM store_meta WHERE key = ?")
      .get(STATS_CACHE_META_KEY) as { value: string } | undefined;
    return row ? parseCachedTokenStats(row.value) : null;
  }

  /** Reads the bounded authoritative HUD snapshot without scanning events. */
  readCachedTokenSnapshot(): AuthoritativeTokenSnapshot | null {
    const row = this.db
      .query("SELECT value FROM store_meta WHERE key = ?")
      .get(TOKEN_SNAPSHOT_CACHE_META_KEY) as { value: string } | undefined;
    if (!row || Buffer.byteLength(row.value, "utf8") > TOKEN_SNAPSHOT_CACHE_MAX_BYTES) return null;
    try {
      const parsed: unknown = JSON.parse(row.value);
      return isAuthoritativeTokenSnapshot(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }

  /** Publishes one worker-built authoritative snapshot as a tiny metadata row. */
  writeCachedTokenSnapshot(snapshot: AuthoritativeTokenSnapshot): void {
    if (!isAuthoritativeTokenSnapshot(snapshot)) {
      throw new Error("authoritative token snapshot cache is invalid");
    }
    const encoded = JSON.stringify(snapshot);
    if (Buffer.byteLength(encoded, "utf8") > TOKEN_SNAPSHOT_CACHE_MAX_BYTES) {
      throw new Error(
        `authoritative token snapshot cache exceeds ${TOKEN_SNAPSHOT_CACHE_MAX_BYTES} bytes`,
      );
    }
    this.db
      .query(
        "INSERT INTO store_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      )
      .run(TOKEN_SNAPSHOT_CACHE_META_KEY, encoded);
  }

  /** Advances only the completion heartbeat of matching daily authority.
   * The immediate transaction prevents an older worker from overwriting a
   * newer revision published by another opener. */
  touchCachedTokenSnapshot(
    updatedAt: number,
    expectedRevision: number,
    expectedWindowKey: string,
    expectedPriorUpdatedAt: number,
  ): AuthoritativeTokenSnapshot {
    if (
      !Number.isSafeInteger(updatedAt) ||
      updatedAt < 0 ||
      !Number.isSafeInteger(expectedRevision) ||
      expectedRevision < 0 ||
      !Number.isSafeInteger(expectedPriorUpdatedAt) ||
      expectedPriorUpdatedAt < 0 ||
      expectedWindowKey.length === 0 ||
      expectedWindowKey.length > 128 ||
      Buffer.byteLength(expectedWindowKey, "utf8") > 128
    ) {
      throw new Error("authoritative token snapshot heartbeat coordinates are invalid");
    }
    // Live-revision equality is intentionally NOT re-checked here — a
    // mid-scan flush can bump the store revision without publishing a new
    // snapshot, and the CAS below (against the cached snapshot's own
    // revision/windowKey/updatedAt) is the actual staleness guard.
    const attempt = () =>
      this.db
        .transaction(() => {
          const cached = this.readCachedTokenSnapshot();
          if (
            !cached ||
            cached.revision !== expectedRevision ||
            cached.window.windowKey !== expectedWindowKey ||
            cached.updatedAt !== expectedPriorUpdatedAt
          ) {
            throw new Error("authoritative token snapshot heartbeat target is stale");
          }
          // Only the heartbeat is replaced from the current cached value. A
          // clock correction therefore cannot roll semantic authority backward,
          // and accepting the lower timestamp repairs a poisoned future value.
          const touched = { ...cached, updatedAt };
          this.writeCachedTokenSnapshot(touched);
          return touched;
        })
        .immediate();
    // Bounded immediate retry on transient write contention (the store's own
    // connection already carries a 5s busy_timeout for a single attempt;
    // this adds a couple of short, cheap re-attempts on top so one heartbeat
    // tick doesn't have to wait for the next externally-scheduled tick to
    // recover from a momentary writer collision).
    const maxAttempts = 3;
    for (let i = 1; i <= maxAttempts; i++) {
      try {
        return attempt();
      } catch (error) {
        if (i === maxAttempts || !isTransientSqliteContentionError(error)) throw error;
        Bun.sleepSync(10 * i);
      }
    }
    throw new Error("unreachable: touchCachedTokenSnapshot retry loop exhausted without result");
  }

  /** Reads one bounded worker-published rate result without touching events. */
  readCachedRateSnapshot(): CachedRateSnapshot | null {
    const row = this.db
      .query("SELECT value FROM store_meta WHERE key = ?")
      .get(RATE_SNAPSHOT_CACHE_META_KEY) as { value: string } | undefined;
    return row ? parseCachedRateSnapshot(row.value) : null;
  }

  /** Publishes a revision-pinned rate result as one bounded metadata row. */
  writeCachedRateSnapshot(
    snapshot: RateSnapshot,
    revision: number,
    publishedAt: number = Date.now(),
  ): void {
    if (
      !Number.isSafeInteger(revision) ||
      revision < 0 ||
      !Number.isSafeInteger(publishedAt) ||
      publishedAt < 0 ||
      !isRateSnapshot(snapshot)
    ) {
      throw new Error("rate snapshot cache is invalid");
    }
    const cached: CachedRateSnapshot = { revision, publishedAt, snapshot };
    const encoded = JSON.stringify(cached);
    if (Buffer.byteLength(encoded, "utf8") > RATE_SNAPSHOT_CACHE_MAX_BYTES) {
      throw new Error(`rate snapshot cache exceeds ${RATE_SNAPSHOT_CACHE_MAX_BYTES} bytes`);
    }
    this.db
      .query(
        "INSERT INTO store_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      )
      .run(RATE_SNAPSHOT_CACHE_META_KEY, encoded);
  }

  /** Computes the exact fixed windows only in the isolated ingest process.
   * The expensive hour read happens outside the writer transaction; the
   * revision checks prevent publishing it across a concurrent reconciliation. */
  refreshRateSnapshotCache(now: number, expectedRevision: number): RateSnapshot {
    if (
      !Number.isSafeInteger(now) ||
      now < 0 ||
      !Number.isSafeInteger(expectedRevision) ||
      expectedRevision < 0
    ) {
      throw new Error("rate snapshot cache coordinates are invalid");
    }
    if (this.getRevision() !== expectedRevision) {
      throw new Error("rate snapshot revision changed before computation");
    }
    const snapshot = selectRateSnapshot(this, now);
    return this.db
      .transaction(() => {
        if (this.getRevision() !== expectedRevision) {
          throw new Error("rate snapshot revision changed during computation");
        }
        // This timestamp is sampled after the expensive reducer completes, so
        // the persisted cache cannot be born stale merely because computation
        // took longer than the HTTP freshness budget.
        this.writeCachedRateSnapshot(snapshot, expectedRevision, Date.now());
        return snapshot;
      })
      .immediate();
  }

  /** Advances only the completion heartbeat of an already-published rate
   * snapshot. The isolated worker calls this as its final durable operation so
   * readiness cannot race ahead of a cache that aged during later roll-ups. */
  touchCachedRateSnapshotPublication(
    expectedRevision: number,
    expectedSnapshotNow: number,
    publishedAt?: number,
  ): CachedRateSnapshot {
    if (
      !Number.isSafeInteger(expectedRevision) ||
      expectedRevision < 0 ||
      !Number.isSafeInteger(expectedSnapshotNow) ||
      expectedSnapshotNow < 0 ||
      (publishedAt !== undefined && (!Number.isSafeInteger(publishedAt) || publishedAt < 0))
    ) {
      throw new Error("rate snapshot publication coordinates are invalid");
    }
    return this.db
      .transaction(() => {
        if (this.getRevision() !== expectedRevision) {
          throw new Error("rate snapshot revision changed before publication heartbeat");
        }
        const cached = this.readCachedRateSnapshot();
        if (
          !cached ||
          cached.revision !== expectedRevision ||
          cached.snapshot.now !== expectedSnapshotNow
        ) {
          throw new Error("rate snapshot publication heartbeat target is stale");
        }
        const completionTime = publishedAt ?? Date.now();
        // Accept a lower timestamp after wall-clock correction. The semantic
        // rate snapshot remains revision- and anchor-pinned above.
        this.writeCachedRateSnapshot(cached.snapshot, expectedRevision, completionTime);
        return { ...cached, publishedAt: completionTime };
      })
      .immediate();
  }

  /** Computes exact counts under one WAL read snapshot without reserving the
   * writer lock needed by the independent authority heartbeat. */
  computeExactStatsSnapshot(
    scannedAt: number,
    expectedRevision?: number,
    testBetweenCountsDelayMs = 0,
  ): CachedTokenStats {
    if (!Number.isSafeInteger(scannedAt) || scannedAt < 0) {
      throw new Error("stats cache scannedAt must be a non-negative safe integer");
    }
    if (
      expectedRevision !== undefined &&
      (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0)
    ) {
      throw new Error("stats cache expected revision must be a non-negative safe integer");
    }
    return this.db
      .transaction(() => {
        const revision = this.getRevision();
        if (expectedRevision !== undefined && revision !== expectedRevision) {
          throw new Error("stats cache revision changed before computation");
        }
        return {
          revision,
          ...this.stats(testBetweenCountsDelayMs),
          scannedAt,
        };
      })
      .deferred();
  }

  /** Publishes a computed count snapshot in a short revision-pinned write.
   * Contention retries this phase only; counts are never recomputed or rebased. */
  publishExactStatsCache(snapshot: CachedTokenStats): CachedTokenStats {
    for (const value of [snapshot.revision, snapshot.events, snapshot.files, snapshot.scannedAt]) {
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error("stats cache snapshot fields must be non-negative safe integers");
      }
    }
    const candidate = {
      revision: snapshot.revision,
      events: snapshot.events,
      files: snapshot.files,
      scannedAt: snapshot.scannedAt,
    };
    const publish = (): CachedTokenStats =>
      this.db
        .transaction(() => {
          if (this.getRevision() !== candidate.revision) {
            throw new Error("stats cache revision changed before publication");
          }
          const current = this.readCachedStats();
          if (
            current !== null &&
            current.revision === candidate.revision &&
            current.scannedAt > candidate.scannedAt
          ) {
            return current;
          }
          this.db
            .query(
              "INSERT INTO store_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .run(STATS_CACHE_META_KEY, JSON.stringify(candidate));
          return candidate;
        })
        .immediate();

    try {
      return publish();
    } catch (error) {
      if (!isTransientSqliteContentionError(error)) throw error;
      return publish();
    }
  }

  /** Computes then publishes exact counts inside the isolated ingest process.
   * This is intentionally never called from an HTTP path. */
  refreshExactStatsCache(
    scannedAt: number,
    expectedRevision?: number,
    testBetweenCountsDelayMs = 0,
  ): CachedTokenStats {
    const snapshot = this.computeExactStatsSnapshot(
      scannedAt,
      expectedRevision,
      testBetweenCountsDelayMs,
    );
    return this.publishExactStatsCache(snapshot);
  }

  close(): void {
    this.db.close();
  }
}

let singleton: TokenStore | null = null;
export function getStore(): TokenStore {
  if (!singleton) singleton = new TokenStore();
  return singleton;
}

export function _resetStoreForTests(): void {
  if (singleton) {
    singleton.close();
    singleton = null;
  }
}
