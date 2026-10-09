// Deterministic, token-free Tokenomics analyzer.
//
// Bounded scan: prefilters JSONL files by mtime, then filters strictly timestamped
// records within TOKENOMICS_WINDOW_H (default 72 h). It respects maxFiles (200)
// and maxBytes (256 MB) hard caps and early-exits when a cap is hit. Results are
// cached for CACHE_TTL_MS (60 s). Authoritative-read failures reject so the
// HTTP endpoint can report a non-2xx response instead of a false-green zero.
//
// SPENT and AVOIDED are always separate keys in the returned object; they are
// never combined or netted.

import { createHash } from "node:crypto";
import type { ReadStream } from "node:fs";
import { open, readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { hasReducerWrapper, isVerboseCmd } from "./governor.ts";
import { claudeProjectsDir } from "./paths.ts";
import { readSavings, type SavingsResult } from "./savings.ts";
import { spanSecFor, type Totals, tokensPerSecFor } from "./scanner.ts";
import { type EventTotalsAggregate, getStore } from "./store.ts";

// ─── Config ──────────────────────────────────────────────────────────────────

const MAX_FILES = 200;
const MAX_BYTES = 256 * 1024 * 1024; // 256 MB
const MAX_JSONL_LINE_BYTES = 1024 * 1024; // skip pathological single records
const MAX_FINDINGS = 100;
const STRICT_ISO_TIMESTAMP_RE =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-](\d{2}):(\d{2}))$/;

interface JsonlCandidate {
  path: string;
  mtime: number;
  ctime: number;
  size: number;
  dev: number;
  ino: number;
}

interface JsonlSnapshot extends JsonlCandidate {
  prefixSha256: string;
}

interface JsonlSnapshotRead {
  status: "scanned" | "changed" | "unreadable";
  bytesRead: number;
  lineCapped: boolean;
  /** Count of JSONL records skipped because a single line exceeded
   *  MAX_JSONL_LINE_BYTES. Skipped, never fatal — see forEachSnapshotLine. */
  skippedLines: number;
  /** Approximate total bytes belonging to skipped over-limit records. */
  skippedBytes: number;
}

function windowH(): number {
  const v = Number(process.env.TOKENOMICS_WINDOW_H);
  return Number.isFinite(v) && v > 0 ? v : 72;
}
function cacheTtlMs(): number {
  const raw = process.env.TOKENOMICS_CACHE_TTL_MS;
  if (raw === undefined || raw.trim() === "") return 60_000;
  const v = Number(raw);
  return Number.isFinite(v) && v >= 0 ? v : 60_000;
}
function oversizedBytes(): number {
  const v = Number(process.env.TOKENOMICS_OVERSIZED_KB);
  return (Number.isFinite(v) && v > 0 ? v : 32) * 1024;
}
function bypassBytes(): number {
  const v = Number(process.env.TOKENOMICS_BYPASS_KB);
  return (Number.isFinite(v) && v > 0 ? v : 8) * 1024;
}

// ─── Types ────────────────────────────────────────────────────────────────────

/** Per-tool call tally from the live JSONL scan (tool_use blocks). */
export interface ToolObservation {
  /** Number of tool_use invocations categorized under this bucket. */
  count: number;
  /** Rough byte estimate: sum of JSON.stringify(input).length for each call. */
  estBytes: number;
}

/** A single advisory heuristic finding. */
export interface Finding {
  kind:
    | "re-read-loop"
    | "oversized-output"
    | "reducer-bypass"
    | "ponytail-new-file"
    | "ponytail-dep-install";
  severity: "info" | "warn";
  sessionId: string;
  detail: string;
  count: number;
}

/** Advisory Ponytail/minimal-code dimension roll-up (decision-ladder signals).
 *  Mirrors the two enforced rungs: decision-ladder-new-source-file (LOG) and
 *  package-install-guard (ASK). Separate advisory counts — never a net score. */
export interface PonytailRollup {
  /** Write calls to new source files with no prior Read in the same session. */
  newFiles: number;
  /** Bash calls that install a new dependency package. */
  depInstalls: number;
}

/** ADVISORY efficiency analysis produced by scanning recent JSONL files.
 *  All findings are heuristic — label them as such when displaying. */
export interface EfficiencyReport {
  /** Tool call counts and input-byte estimates, bucketed by category. */
  observedByTool: Record<string, ToolObservation>;
  /** Advisory heuristic findings — re-read loops, oversized outputs, bypass,
   *  ponytail new-file, ponytail dep-install. */
  findings: Finding[];
  /** Advisory Ponytail/minimal-code dimension roll-up. */
  ponytail: PonytailRollup;
  windowHours: number;
  filesScanned: number;
  /** Retained for response compatibility. Successful reports are false;
   * reaching a safety cap rejects the analysis instead of returning partial data. */
  capped: boolean;
  /** True when one or more JSONL records or files were skipped during the
   *  scan (e.g. a pathological >1 MiB single-line record, or a file that
   *  mutated out from under the scan). When true, the totals below reflect
   *  everything EXCEPT the skipped data — an honest partial result, not a
   *  total outage and not a silently-wrong complete-looking number. */
  incomplete: boolean;
  /** Count of JSONL records skipped for exceeding the single-line size cap. */
  skippedRecords: number;
  /** Approximate total bytes of skipped over-limit records. */
  skippedRecordBytes: number;
  /** Count of JSONL files skipped because they mutated during the scan or
   *  became unreadable mid-read (integrity of the captured byte range could
   *  not be verified). */
  skippedFiles: number;
}

/** Token consumption from the SQLite store (SPENT). Separate from AVOIDED. */
export interface ConsumptionSummary {
  /** All-time totals from the store (SPENT). */
  allTime: Totals;
  windowHours: number;
  /** Totals within the analysis window (SPENT). */
  window: Totals;
}

export interface TokenomicsResult {
  /** SPENT — tokens/cost actually consumed, from the SQLite store. */
  consumption: ConsumptionSummary;
  /** AVOIDED — bytes/tokens saved by reducers, from the savings ledger. */
  savings: SavingsResult;
  /** ADVISORY efficiency analysis from live JSONL scan. */
  efficiency: EfficiencyReport;
}

// ─── Cache ───────────────────────────────────────────────────────────────────

let _cache: { result: TokenomicsResult; computedAt: number; revision: number } | null = null;
let _inFlight: { revision: number; promise: Promise<TokenomicsResult> } | null = null;
let _cacheGeneration = 0;

/** Call in tests to force a fresh computation on the next call. */
export function _resetTokenomicsCache(): void {
  _cacheGeneration++;
  _cache = null;
  _inFlight = null;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function totalsFromAggregate(aggregate: EventTotalsAggregate): Totals {
  const totalTokens =
    aggregate.inputTokens +
    aggregate.outputTokens +
    aggregate.cacheReadTokens +
    aggregate.cacheWrite5mTokens +
    aggregate.cacheWrite1hTokens;
  const observedSpanSec = spanSecFor(
    aggregate.minTs ?? Number.POSITIVE_INFINITY,
    aggregate.maxTs ?? Number.NEGATIVE_INFINITY,
  );
  return {
    inputTokens: aggregate.inputTokens,
    outputTokens: aggregate.outputTokens,
    cacheReadTokens: aggregate.cacheReadTokens,
    cacheWrite5mTokens: aggregate.cacheWrite5mTokens,
    cacheWrite1hTokens: aggregate.cacheWrite1hTokens,
    totalTokens,
    cost: aggregate.cost,
    messages: aggregate.messages,
    observedSpanSec,
    tokensPerSecond: tokensPerSecFor(totalTokens, observedSpanSec),
  };
}

/** Categorize a tool_use block into one of the observedByTool buckets. */
function categorize(
  toolName: string,
  input: Record<string, unknown>,
): { category: string; bashCmd: string | undefined } {
  if (toolName === "Read") return { category: "Read", bashCmd: undefined };
  if (toolName === "Edit" || toolName === "Write")
    return { category: "Edit/Write", bashCmd: undefined };
  if (toolName === "Bash") {
    const cmd = typeof input.command === "string" ? input.command : "";
    if (/^graphify\s/.test(cmd)) return { category: "graphify", bashCmd: cmd };
    if (/^oracle\s/.test(cmd)) return { category: "oracle", bashCmd: cmd };
    if (/^stk\s/.test(cmd)) return { category: "stk", bashCmd: cmd };
    if (/^headroom\s/.test(cmd)) return { category: "headroom", bashCmd: cmd };
    if (isVerboseCmd(cmd)) return { category: "verbose-Bash", bashCmd: cmd };
    return { category: "other", bashCmd: cmd };
  }
  return { category: "other", bashCmd: undefined };
}

/** Byte estimate for a tool_result content field (string or array). */
function resultBytes(content: unknown): number {
  if (typeof content === "string") return content.length;
  if (Array.isArray(content)) {
    let n = 0;
    for (const block of content) {
      if (typeof block === "object" && block !== null) {
        const b = block as Record<string, unknown>;
        if (typeof b.text === "string") n += b.text.length;
      }
    }
    return n;
  }
  return 0;
}

function projectsRoot(): string {
  return claudeProjectsDir();
}

function strictIsoTimestampMs(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const match = STRICT_ISO_TIMESTAMP_RE.exec(value);
  if (!match) return null;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] === "Z" ? 0 : Number(match[9]);
  const offsetMinute = match[8] === "Z" ? 0 : Number(match[10]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth[month - 1]! ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Source-file extensions that trigger the ponytail-new-file finding. */
const SOURCE_EXT_RE = /\.(ts|tsx|js|jsx|go|rs|ex|exs|py|c|cpp|h|zig|swift|cs)$/i;

/** Return a credential-safe dependency-install description. Arguments are
 * intentionally never copied from the raw shell command: package specs may be
 * authenticated URLs, registry tokens, or other credentials. */
function dependencyInstallSummary(cmd: string): string | null {
  const patterns: Array<[RegExp, string]> = [
    [/^pnpm\s+add\s+\S+/, "pnpm add dependency (arguments redacted)"],
    [/^bun\s+add\s+\S+/, "bun add dependency (arguments redacted)"],
    [/^npm\s+(?:install|i)\s+\S+/, "npm install dependency (arguments redacted)"],
    [/^yarn\s+add\s+\S+/, "yarn add dependency (arguments redacted)"],
    [/^cargo\s+add\s+\S+/, "cargo add dependency (arguments redacted)"],
    [/^go\s+get\s+\S+/, "go get dependency (arguments redacted)"],
    [/^pip\s+install\s+\S+/, "pip install dependency (arguments redacted)"],
    [/^poetry\s+add\s+\S+/, "poetry add dependency (arguments redacted)"],
    [/^gem\s+install\s+\S+/, "gem install dependency (arguments redacted)"],
  ];
  for (const [pattern, summary] of patterns) {
    if (pattern.test(cmd)) return summary;
  }
  return null;
}

/** Capture a bounded content identity for the byte range that tokenomics will
 * later analyze. The candidate must remain unchanged until this digest is
 * established; later appends are safe because the reader verifies this prefix
 * before accepting any lines. */
async function captureJsonlSnapshot(
  candidate: JsonlCandidate,
  maxBytes: number,
): Promise<JsonlSnapshot> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  let input: ReadStream | undefined;

  try {
    handle = await open(candidate.path, "r");
    const opened = await handle.stat();
    if (
      !opened.isFile() ||
      opened.dev !== candidate.dev ||
      opened.ino !== candidate.ino ||
      opened.size !== candidate.size ||
      opened.mtimeMs !== candidate.mtime ||
      opened.ctimeMs !== candidate.ctime
    ) {
      throw new Error("tokenomics efficiency source changed before bounded capture");
    }
    if (opened.size > maxBytes) {
      throw new Error("tokenomics efficiency scan exceeded its 256 MiB safety limit");
    }

    const hash = createHash("sha256");
    let bytesRead = 0;
    if (opened.size > 0) {
      input = handle.createReadStream({
        autoClose: false,
        start: 0,
        end: opened.size - 1,
        highWaterMark: 64 * 1024,
      });
      for await (const rawChunk of input) {
        const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
        bytesRead += chunk.length;
        hash.update(chunk);
      }
    }

    const afterRead = await handle.stat();
    if (
      bytesRead !== opened.size ||
      afterRead.dev !== opened.dev ||
      afterRead.ino !== opened.ino ||
      afterRead.size !== opened.size ||
      afterRead.mtimeMs !== opened.mtimeMs ||
      afterRead.ctimeMs !== opened.ctimeMs
    ) {
      throw new Error("tokenomics efficiency source changed during bounded capture");
    }

    return {
      path: candidate.path,
      mtime: opened.mtimeMs,
      ctime: opened.ctimeMs,
      size: opened.size,
      dev: opened.dev,
      ino: opened.ino,
      prefixSha256: hash.digest("hex"),
    };
  } finally {
    input?.destroy();
    await handle?.close().catch(() => undefined);
  }
}

/** Read exactly the inode and byte range captured during candidate discovery.
 * Appends after discovery are excluded, replacements/truncations are rejected,
 * and a pathological no-newline record can never grow the parser buffer past
 * 1 MiB. */
async function forEachSnapshotLine(
  snapshot: JsonlSnapshot,
  onLine: (line: string) => void,
): Promise<JsonlSnapshotRead> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  let input: ReadStream | undefined;
  let bytesRead = 0;
  let lineCapped = false;
  let skippedLines = 0;
  let skippedBytes = 0;
  let discardedBytesInLine = 0;

  try {
    handle = await open(snapshot.path, "r");
    const opened = await handle.stat();
    if (
      !opened.isFile() ||
      opened.dev !== snapshot.dev ||
      opened.ino !== snapshot.ino ||
      opened.size < snapshot.size ||
      (opened.size === snapshot.size &&
        (opened.mtimeMs !== snapshot.mtime || opened.ctimeMs !== snapshot.ctime))
    ) {
      return { status: "changed", bytesRead, lineCapped, skippedLines, skippedBytes };
    }
    if (snapshot.size === 0) {
      return { status: "scanned", bytesRead, lineCapped, skippedLines, skippedBytes };
    }

    input = handle.createReadStream({
      autoClose: false,
      start: 0,
      end: snapshot.size - 1,
      highWaterMark: 64 * 1024,
    });

    let buffered = Buffer.alloc(0);
    let discardingLine = false;
    const prefixHash = createHash("sha256");

    for await (const rawChunk of input) {
      const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
      bytesRead += chunk.length;
      prefixHash.update(chunk);
      if (bytesRead > snapshot.size) {
        throw new Error("JSONL snapshot exceeded its captured byte range");
      }

      let offset = 0;
      while (offset < chunk.length) {
        const newline = chunk.indexOf(0x0a, offset);
        const end = newline === -1 ? chunk.length : newline;
        const part = chunk.subarray(offset, end);

        if (!discardingLine) {
          if (buffered.length + part.length > MAX_JSONL_LINE_BYTES) {
            discardedBytesInLine = buffered.length + part.length;
            buffered = Buffer.alloc(0);
            discardingLine = true;
            lineCapped = true;
          } else if (part.length > 0) {
            buffered = buffered.length === 0 ? Buffer.from(part) : Buffer.concat([buffered, part]);
          }
        } else {
          discardedBytesInLine += part.length;
        }

        if (newline === -1) break;
        if (!discardingLine) {
          const line =
            buffered.at(-1) === 0x0d
              ? buffered.subarray(0, buffered.length - 1).toString("utf-8")
              : buffered.toString("utf-8");
          onLine(line);
        } else {
          skippedLines++;
          skippedBytes += discardedBytesInLine;
          discardedBytesInLine = 0;
        }
        buffered = Buffer.alloc(0);
        discardingLine = false;
        offset = newline + 1;
      }
    }

    const afterRead = await handle.stat();
    if (
      bytesRead !== snapshot.size ||
      afterRead.dev !== opened.dev ||
      afterRead.ino !== opened.ino ||
      afterRead.size !== opened.size ||
      afterRead.mtimeMs !== opened.mtimeMs ||
      afterRead.ctimeMs !== opened.ctimeMs ||
      prefixHash.digest("hex") !== snapshot.prefixSha256
    ) {
      return { status: "changed", bytesRead, lineCapped, skippedLines, skippedBytes };
    }
    if (discardingLine) {
      // File ended (or the record ran to EOF) while an over-limit line was
      // still being discarded — no trailing newline arrived to close it.
      // Still count it as a skipped record rather than dropping it silently.
      skippedLines++;
      skippedBytes += discardedBytesInLine;
    } else if (buffered.length > 0) {
      onLine(buffered.toString("utf-8"));
    }
    return { status: "scanned", bytesRead, lineCapped, skippedLines, skippedBytes };
  } catch {
    return { status: "unreadable", bytesRead, lineCapped, skippedLines, skippedBytes };
  } finally {
    input?.destroy();
    await handle?.close().catch(() => undefined);
  }
}

/** Narrow test hook for adversarial append/replacement snapshot regressions. */
export async function _readJsonlSnapshotForTest(
  snapshot: JsonlSnapshot,
): Promise<JsonlSnapshotRead & { lines: string[] }> {
  const lines: string[] = [];
  const result = await forEachSnapshotLine(snapshot, (line) => lines.push(line));
  return { ...result, lines: result.status === "scanned" ? lines : [] };
}

/** Narrow test hook that uses the production metadata + digest capture path. */
export async function _captureJsonlSnapshotForTest(path: string): Promise<JsonlSnapshot> {
  const captured = await stat(path);
  return captureJsonlSnapshot(
    {
      path,
      mtime: captured.mtimeMs,
      ctime: captured.ctimeMs,
      size: captured.size,
      dev: captured.dev,
      ino: captured.ino,
    },
    MAX_BYTES,
  );
}

// ─── Efficiency scanner ───────────────────────────────────────────────────────

/** Exported so the off-thread tokenomics worker (tokenomics.worker.ts) can
 * run the same CPU-bound corpus scan in a sibling process instead of on the
 * HTTP server's event loop. Reads its window/threshold/root config from
 * process.env exactly as the in-process caller does — the worker is spawned
 * with the parent's environment inherited (see tokenomics-worker.ts), so
 * TOKENOMICS_WINDOW_H / TOKENOMICS_OVERSIZED_KB / TOKENOMICS_BYPASS_KB /
 * CLAUDE_PROJECTS_DIR all resolve identically in both places. */
export async function analyzeEfficiency(wh: number): Promise<EfficiencyReport> {
  const root = projectsRoot();
  const cutoffMs = Date.now() - wh * 3_600_000;
  const oversizedThreshold = oversizedBytes();
  const bypassThreshold = bypassBytes();

  const observed = new Map<string, ToolObservation>();
  const findings: Finding[] = [];
  let filesScanned = 0;
  let totalBytesRead = 0;
  let ponytailNewFiles = 0;
  let ponytailDepInstalls = 0;
  let skippedRecords = 0;
  let skippedRecordBytes = 0;
  let skippedFiles = 0;

  function tally(category: string, inputEstBytes: number): void {
    let obs = observed.get(category);
    if (!obs) {
      obs = { count: 0, estBytes: 0 };
      observed.set(category, obs);
    }
    obs.count++;
    obs.estBytes += inputEstBytes;
  }

  function pushFinding(f: Finding): void {
    if (findings.length < MAX_FINDINGS) findings.push(f);
  }

  // Collect qualifying files (within window).
  const candidates: JsonlCandidate[] = [];

  let slugs: string[];
  try {
    const entries = await readdir(root, { withFileTypes: true });
    slugs = entries
      .filter((entry) => entry.isDirectory() || entry.isSymbolicLink())
      .map((entry) => entry.name);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      slugs = [];
    } else {
      throw new Error("tokenomics efficiency source root is unreadable", { cause: err });
    }
  }

  for (const slug of slugs) {
    const dir = join(root, slug);
    let entries: string[];
    try {
      const children = await readdir(dir, { withFileTypes: true });
      entries = children
        .filter((entry) => entry.isFile() || entry.isSymbolicLink())
        .map((entry) => entry.name);
    } catch (err) {
      throw new Error("tokenomics efficiency source directory changed or is unreadable", {
        cause: err,
      });
    }
    for (const e of entries) {
      if (!e.endsWith(".jsonl")) continue;
      const path = join(dir, e);
      try {
        const st = await stat(path);
        if (st.isFile() && st.mtimeMs >= cutoffMs) {
          candidates.push({
            path,
            mtime: st.mtimeMs,
            ctime: st.ctimeMs,
            size: st.size,
            dev: st.dev,
            ino: st.ino,
          });
        }
      } catch (err) {
        throw new Error("tokenomics efficiency source file changed or is unreadable", {
          cause: err,
        });
      }
    }
  }

  // Sort newest first so the cap discards the oldest files.
  candidates.sort((a, b) => b.mtime - a.mtime);

  if (candidates.length > MAX_FILES) {
    throw new Error(`tokenomics efficiency scan exceeded its ${MAX_FILES}-file safety limit`);
  }

  for (const candidate of candidates) {
    if (totalBytesRead + candidate.size > MAX_BYTES) {
      throw new Error("tokenomics efficiency scan exceeded its 256 MiB safety limit");
    }
    let file: JsonlSnapshot;
    try {
      file = await captureJsonlSnapshot(candidate, MAX_BYTES - totalBytesRead);
    } catch (err) {
      if (
        err instanceof Error &&
        err.message === "tokenomics efficiency scan exceeded its 256 MiB safety limit"
      ) {
        // A single-file resource-cap breach still aborts the whole scan —
        // this is a corpus-wide budget guardrail, not a per-file anomaly.
        throw err;
      }
      // The candidate disappeared, was replaced, or became unreadable
      // (permissions, ENOENT, etc.) between discovery and capture. Same
      // total-outage-over-one-file problem as the post-scan check below:
      // skip only this file and report the gap honestly.
      skippedFiles++;
      continue;
    }
    totalBytesRead += file.size;

    const sessionId = file.path
      .split(/[\\/]/)
      .pop()!
      .replace(/\.jsonl$/, "");

    // Per-session state.
    const priorReadPaths = new Set<string>();
    const freshReadPathCounts = new Map<string, number>();
    const toolById = new Map<string, { category: string; bashCmd: string | undefined }>();

    const scan = await forEachSnapshotLine(file, (raw) => {
      const line = raw.trim();
      if (!line) return;

      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        return;
      }

      if (typeof parsed !== "object" || parsed === null) return;
      const o = parsed as Record<string, unknown>;
      // The file mtime is only the coarse candidate prefilter. A strictly
      // valid top-level ISO timestamp is authoritative for each record;
      // missing or non-standard values inherit the qualifying mtime.
      const recordMs = strictIsoTimestampMs(o.timestamp) ?? file.mtime;
      const inWindow = recordMs >= cutoffMs;
      const msgType = typeof o.type === "string" ? o.type : "";
      const message =
        typeof o.message === "object" && o.message !== null
          ? (o.message as Record<string, unknown>)
          : null;
      if (!message) return;

      const contentArr = Array.isArray(message.content) ? (message.content as unknown[]) : null;
      if (!contentArr) return;

      if (msgType === "assistant") {
        for (const block of contentArr) {
          if (typeof block !== "object" || block === null) continue;
          const b = block as Record<string, unknown>;
          if (b.type !== "tool_use") continue;

          const id = typeof b.id === "string" ? b.id : "";
          const name = typeof b.name === "string" ? b.name : "";
          const input =
            typeof b.input === "object" && b.input !== null
              ? (b.input as Record<string, unknown>)
              : {};
          const inputEstBytes = JSON.stringify(input).length;

          const { category, bashCmd } = categorize(name, input);
          // Keep all preceding tool metadata so a fresh result can be
          // classified by an older exact-ID tool call. Duplicate IDs replace
          // only earlier calls; later calls cannot affect earlier results.
          if (id) toolById.set(id, { category, bashCmd });
          if (inWindow) tally(category, inputEstBytes);

          // Historical Reads suppress a fresh Write's new-file signal, but
          // only fresh Reads contribute to the re-read advisory.
          if (name === "Read") {
            const fp = typeof input.file_path === "string" ? input.file_path : null;
            if (fp) {
              priorReadPaths.add(fp);
              if (inWindow) {
                freshReadPathCounts.set(fp, (freshReadPathCounts.get(fp) ?? 0) + 1);
              }
            }
          }

          if (!inWindow) continue;

          // Ponytail: Write to a NEW source file (no prior Read in this session).
          if (name === "Write") {
            const fp = typeof input.file_path === "string" ? input.file_path : null;
            if (fp && SOURCE_EXT_RE.test(fp) && !priorReadPaths.has(fp)) {
              pushFinding({
                kind: "ponytail-new-file",
                severity: "info",
                sessionId,
                detail: fp,
                count: 1,
              });
              ponytailNewFiles++;
            }
          }

          // Ponytail: Bash command installs a named package.
          if (name === "Bash") {
            const cmd = typeof input.command === "string" ? input.command.trim() : "";
            const summary = dependencyInstallSummary(cmd);
            if (summary) {
              pushFinding({
                kind: "ponytail-dep-install",
                severity: "warn",
                sessionId,
                detail: summary,
                count: 1,
              });
              ponytailDepInstalls++;
            }
          }
        }
      } else if (msgType === "user") {
        // Result warnings are governed solely by the result record's time.
        // Older results never warn even when their tool call is fresh.
        if (!inWindow) return;
        for (const block of contentArr) {
          if (typeof block !== "object" || block === null) continue;
          const b = block as Record<string, unknown>;
          if (b.type !== "tool_result") continue;

          const tuId = typeof b.tool_use_id === "string" ? b.tool_use_id : "";
          const bytes = resultBytes(b.content);
          const toolInfo = tuId ? toolById.get(tuId) : undefined;
          const cat = toolInfo?.category ?? "unknown";

          // Oversized-output: large result NOT from stk/headroom.
          if (bytes > oversizedThreshold && cat !== "stk" && cat !== "headroom") {
            pushFinding({
              kind: "oversized-output",
              severity: "warn",
              sessionId,
              detail: `${cat} result ${Math.round(bytes / 1024)}KB exceeds ${Math.round(oversizedThreshold / 1024)}KB threshold`,
              count: 1,
            });
          }

          // Reducer-bypass: verbose Bash with large result, no stk/headroom wrapper.
          if (cat === "verbose-Bash" && bytes > bypassThreshold) {
            const cmd = toolInfo?.bashCmd ?? "";
            if (!hasReducerWrapper(cmd)) {
              pushFinding({
                kind: "reducer-bypass",
                severity: "warn",
                sessionId,
                detail: `verbose cmd produced ${Math.round(bytes / 1024)}KB without stk/headroom wrapper`,
                count: 1,
              });
            }
          }
        }
      }
    });
    // A pathological single-line record over MAX_JSONL_LINE_BYTES is skipped,
    // not fatal (see forEachSnapshotLine) — the rest of this file's records
    // were still parsed above. Surface the gap honestly on the report instead
    // of throwing the whole endpoint down over one bad line.
    if (scan.lineCapped) {
      skippedRecords += scan.skippedLines;
      skippedRecordBytes += scan.skippedBytes;
    }
    // A file that mutated mid-read (status "changed"), failed to read
    // ("unreadable"), or under-read its captured byte range cannot have its
    // partially-observed data trusted for THIS file — but on a live-tailed
    // corpus with sessions actively appending, this is routine, not an
    // integrity anomaly worth taking the whole endpoint down over. Skip only
    // this file and report the gap; every other file's data still counts.
    if (scan.status !== "scanned" || scan.bytesRead !== file.size) {
      skippedFiles++;
      continue;
    }

    // Re-read loop: same path read >= 3× within a session.
    for (const [path, count] of freshReadPathCounts) {
      if (count >= 3) {
        pushFinding({
          kind: "re-read-loop",
          severity: "info",
          sessionId,
          detail: `${path} read ${count}× in session`,
          count,
        });
      }
    }

    filesScanned++;
  }

  return {
    observedByTool: Object.fromEntries(observed),
    findings,
    ponytail: { newFiles: ponytailNewFiles, depInstalls: ponytailDepInstalls },
    windowHours: wh,
    filesScanned,
    capped: false,
    incomplete: skippedRecords > 0 || skippedFiles > 0,
    skippedRecords,
    skippedRecordBytes,
    skippedFiles,
  };
}

// ─── Consumption (SPENT) from SQLite store ────────────────────────────────────

type ConsumptionStore = Pick<ReturnType<typeof getStore>, "getRevision" | "aggregateEventTotals"> &
  Partial<Pick<ReturnType<typeof getStore>, "withReadSnapshot">>;

/** Exported for reuse by the off-thread tokenomics worker, which supplies its
 * own readonly `TokenStore` instance instead of the process-wide `getStore()`
 * singleton. Both aggregate windows run inside one read snapshot when the
 * store exposes `withReadSnapshot`, so a concurrent ingest commit cannot
 * straddle the pair. */
export async function getConsumption(
  store: ConsumptionStore,
  wh: number,
  expectedRevision: number,
): Promise<ConsumptionSummary> {
  const windowFromTs = Date.now() - wh * 3_600_000;
  const readBoth = (): ConsumptionSummary => {
    if (store.getRevision() !== expectedRevision) {
      throw new Error("tokenomics authority revision changed before consumption read");
    }
    const allTime = totalsFromAggregate(store.aggregateEventTotals());
    const win = totalsFromAggregate(store.aggregateEventTotals({ fromTs: windowFromTs }));
    if (store.getRevision() !== expectedRevision) {
      throw new Error("tokenomics authority revision changed during consumption read");
    }
    return { allTime, windowHours: wh, window: win };
  };
  if (typeof store.withReadSnapshot === "function") {
    return store.withReadSnapshot(readBoth);
  }
  return readBoth();
}

// ─── Public API ───────────────────────────────────────────────────────────────

/** Returns combined Tokenomics: SPENT consumption (store), AVOIDED savings
 *  (ledger), and ADVISORY efficiency analysis (JSONL scan). Authoritative
 *  store failures reject and are surfaced as HTTP 500 by the server. Successful
 *  results are cached for up to CACHE_TTL_MS (default 60 s). */
async function computeTokenomics(generation: number, revision: number): Promise<TokenomicsResult> {
  const wh = windowH();

  const [consumptionResult, savingsResult, efficiencyResult] = await Promise.allSettled([
    getConsumption(getStore(), wh, revision),
    readSavings(),
    analyzeEfficiency(wh),
  ]);
  if (consumptionResult.status === "rejected") throw consumptionResult.reason;
  if (savingsResult.status === "rejected") throw savingsResult.reason;
  if (efficiencyResult.status === "rejected") throw efficiencyResult.reason;

  const result: TokenomicsResult = {
    consumption: consumptionResult.value,
    savings: savingsResult.value,
    efficiency: efficiencyResult.value,
  };
  if (generation === _cacheGeneration && revision === getStore().getRevision()) {
    _cache = { result, computedAt: Date.now(), revision };
  }
  return result;
}

const CONSUMPTION_REVISION_RACE_MAX_ATTEMPTS = 10;
const CONSUMPTION_REVISION_RACE_RETRY_DELAY_MS = 20;

function isRevisionRaceError(error: unknown): boolean {
  return error instanceof Error && error.message.includes("tokenomics authority revision changed");
}

/** Retries getConsumption on the specific "revision changed before/during
 * read" race — never on any other failure. This box's ingest worker can
 * commit far more often than once per 5s poll tick under heavy concurrent
 * session activity, and the off-thread worker process (unlike the in-process
 * analyzeTokenomics() path directly below, which keeps its original
 * single-attempt behavior) adds enough spawn/IPC latency around the two
 * sequential aggregate reads that a single straddled commit became common
 * rather than rare. A fresh revision snapshot each attempt converges once a
 * quiet gap between commits lines up; only a store that is permanently
 * broken (a real aggregateEventTotals failure) still rejects immediately —
 * that path is untouched by this loop. */
async function getConsumptionWithRetry(
  store: ConsumptionStore,
  wh: number,
): Promise<ConsumptionSummary> {
  let lastError: unknown;
  for (let attempt = 0; attempt < CONSUMPTION_REVISION_RACE_MAX_ATTEMPTS; attempt++) {
    try {
      return await getConsumption(store, wh, store.getRevision());
    } catch (error) {
      if (!isRevisionRaceError(error)) throw error;
      lastError = error;
      if (attempt < CONSUMPTION_REVISION_RACE_MAX_ATTEMPTS - 1) {
        await new Promise((r) => setTimeout(r, CONSUMPTION_REVISION_RACE_RETRY_DELAY_MS));
      }
    }
  }
  throw lastError;
}

/** Computes a fresh TokenomicsResult against an explicit store instance,
 * bypassing the module-level cache entirely. Used exclusively by
 * tokenomics.worker.ts (the off-thread sibling process spawned by
 * tokenomics-worker.ts) so the CPU-bound efficiency scan never runs on the
 * HTTP server's event loop. Not used by the in-process analyzeTokenomics()
 * path above, which keeps its own cache/in-flight semantics unchanged. */
export async function computeTokenomicsWithStore(
  store: ConsumptionStore,
  wh: number = windowH(),
): Promise<TokenomicsResult> {
  const [consumptionResult, savingsResult, efficiencyResult] = await Promise.allSettled([
    getConsumptionWithRetry(store, wh),
    readSavings(),
    analyzeEfficiency(wh),
  ]);
  if (consumptionResult.status === "rejected") throw consumptionResult.reason;
  if (savingsResult.status === "rejected") throw savingsResult.reason;
  if (efficiencyResult.status === "rejected") throw efficiencyResult.reason;
  return {
    consumption: consumptionResult.value,
    savings: savingsResult.value,
    efficiency: efficiencyResult.value,
  };
}

export function analyzeTokenomics(): Promise<TokenomicsResult> {
  const revision = getStore().getRevision();
  if (_cache && _cache.revision === revision && Date.now() - _cache.computedAt < cacheTtlMs()) {
    return Promise.resolve(_cache.result);
  }
  if (_inFlight?.revision === revision) return _inFlight.promise;

  const generation = _cacheGeneration;
  let pending!: Promise<TokenomicsResult>;
  pending = computeTokenomics(generation, revision).finally(() => {
    if (_inFlight?.promise === pending) _inFlight = null;
  });
  _inFlight = { revision, promise: pending };
  return pending;
}
