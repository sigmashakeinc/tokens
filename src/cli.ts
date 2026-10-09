// Headless CLI: `bun src/cli.ts report [--days N]` prints a quick summary
// without needing the web UI. Useful for cron, hooks, or shell pipelines.

import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { appendFile, cp, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { computeGrokFamilyMix, type GrokFamilyMix } from "./governor.ts";
import {
  evaluateGovernorCli,
  formatGovernorText,
  GOVERNOR_HELP,
  parseGovernorArgs,
  readStdinText,
} from "./governor-cli.ts";
import { databasePath, dataHome } from "./paths.ts";
import { ingestOnce, loadAggregations } from "./scanner.ts";
import { TokenStore } from "./store.ts";
import { analyzeTokenomics } from "./tokenomics.ts";

function fmt(n: number): string {
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(2) + "B";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "k";
  return String(Math.round(n));
}

function fmtUsd(n: number): string {
  return "$" + n.toFixed(2);
}

function isoDaysAgo(days: number): string {
  const d = new Date(Date.now() - days * 86400 * 1000);
  return d.toISOString().slice(0, 10);
}

function resolveDbPath(): string {
  return databasePath();
}

async function isDaemonLive(): Promise<boolean> {
  const host = process.env.TOKENS_HOST || "127.0.0.1";
  const port = Number(process.env.TOKENS_PORT || "7913");
  const signal = AbortSignal.timeout(250);
  try {
    const res = await fetch(`http://${host}:${port}/healthz`, { signal });
    return res.ok;
  } catch {
    return false;
  }
}

function defaultBackupPath(dbPath: string): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `${dbPath}.${stamp}.bak`;
}

function sqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

async function backupDatabase(dbPath: string, backupPath: string): Promise<void> {
  const db = new Database(dbPath);
  try {
    try {
      db.exec(`VACUUM INTO ${sqlLiteral(backupPath)}`);
      return;
    } catch {
      db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      await cp(dbPath, backupPath);
      const walPath = `${dbPath}-wal`;
      const shmPath = `${dbPath}-shm`;
      if (existsSync(walPath)) await cp(walPath, `${backupPath}-wal`);
      if (existsSync(shmPath)) await cp(shmPath, `${backupPath}-shm`);
    }
  } finally {
    db.close();
  }
}

function dedupePercent(before: number, after: number): number {
  if (before <= 0) return 0;
  return ((before - after) / before) * 100;
}

async function runReingestDedupe(): Promise<void> {
  if (await isDaemonLive()) {
    console.error("error: refusing to run reingest-dedupe while server is live");
    console.error("stop the local token service process before re-running this command");
    process.exit(2);
  }

  const dbPath = resolveDbPath();
  const backupPath = defaultBackupPath(dbPath);
  await backupDatabase(dbPath, backupPath);

  const store = new TokenStore(dbPath);
  const rowsBefore = store.stats().events;

  const fileCounts = store.getEventFilePathCounts();
  const presentPaths: string[] = [];
  let rowsPreserved = 0;
  for (const row of fileCounts) {
    if (existsSync(row.filePath)) presentPaths.push(row.filePath);
    else rowsPreserved += row.rowCount;
  }

  const rowsDeleted = store.deleteEventRowsByFilePaths(presentPaths);
  store.deleteFileStateRowsByFilePaths(presentPaths);
  await ingestOnce(store);
  const rowsAfter = store.stats().events;
  const drop = dedupePercent(rowsBefore, rowsAfter);

  store.close();

  console.log(`rows before: ${rowsBefore}`);
  console.log(`rows deleted: ${rowsDeleted}`);
  console.log(`rows preserved: ${rowsPreserved}`);
  console.log(`rows after: ${rowsAfter}`);
  console.log(`dedupe drop %: ${drop.toFixed(2)}`);
  console.log(`backup: ${backupPath}`);
}

async function report(argv: string[]): Promise<void> {
  const daysIdx = argv.indexOf("--days");
  const days = daysIdx >= 0 ? Number(argv[daysIdx + 1]) : 0;
  const from = days > 0 ? isoDaysAgo(days - 1) : undefined;
  const agg = await loadAggregations({ from });

  const t = agg.totals;
  const header = days > 0 ? `Last ${days} days` : "All time";
  console.log(`\n${header} — ${agg.files} files, ${t.messages} assistant messages`);
  console.log(`  Input tokens          ${fmt(t.inputTokens)}`);
  console.log(`  Output tokens         ${fmt(t.outputTokens)}`);
  console.log(`  Cache read tokens     ${fmt(t.cacheReadTokens)}`);
  console.log(`  Cache write tokens    ${fmt(t.cacheWrite5mTokens + t.cacheWrite1hTokens)}`);
  console.log(`  Total tokens          ${fmt(t.totalTokens)}`);
  console.log(`  Estimated cost        ${fmtUsd(t.cost)}`);

  console.log(`\nBy model:`);
  for (const m of agg.byModel) {
    const share = t.totalTokens ? ((m.totalTokens / t.totalTokens) * 100).toFixed(1) : "0";
    console.log(
      `  ${m.model.padEnd(28)} ${fmt(m.totalTokens).padStart(8)}  (${share}%)  ${fmtUsd(m.cost)}`,
    );
  }

  console.log(`\nTop projects:`);
  for (const p of agg.byProject.slice(0, 10)) {
    console.log(
      `  ${p.project.padEnd(32)} ${fmt(p.totalTokens).padStart(8)}  ${fmtUsd(p.cost).padStart(8)}  ${p.sessions} sessions`,
    );
  }

  if (agg.daily.length > 1) {
    console.log(`\nLast 14 days:`);
    for (const d of agg.daily.slice(-14)) {
      console.log(`  ${d.date}  ${fmt(d.totalTokens).padStart(8)}  ${fmtUsd(d.cost).padStart(8)}`);
    }
  }
}

async function recordSavings(argv: string[]): Promise<void> {
  let tool = "";
  let origBytes: number | null = null;
  let compBytes: number | null = null;

  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    const next = argv[i + 1];
    if (flag === "--tool" && next !== undefined) {
      tool = next;
      i++;
    } else if (flag === "--orig" && next !== undefined) {
      origBytes = Number(next);
      i++;
    } else if (flag === "--comp" && next !== undefined) {
      compBytes = Number(next);
      i++;
    }
  }

  const errors: string[] = [];
  if (!tool) errors.push("--tool <name> is required and must be non-empty");
  if (origBytes === null || !Number.isFinite(origBytes) || origBytes < 0)
    errors.push("--orig <bytes> must be a non-negative finite number");
  if (compBytes === null || !Number.isFinite(compBytes) || compBytes < 0)
    errors.push("--comp <bytes> must be a non-negative finite number");

  if (errors.length > 0) {
    for (const e of errors) console.error(`error: ${e}`);
    console.error(
      "usage: bun src/cli.ts record-savings --tool <name> --orig <bytes> --comp <bytes>",
    );
    process.exit(1);
  }

  const home = dataHome();
  const ledgerPath = join(home, "token-savings.jsonl");
  const record = JSON.stringify({
    tool,
    ts: Math.floor(Date.now() / 1000),
    orig_bytes: origBytes,
    comp_bytes: compBytes,
  });

  try {
    await mkdir(home, { recursive: true });
    await appendFile(ledgerPath, record + "\n");
    console.log(`recorded: ${record}`);
  } catch (err) {
    console.error(`error writing to ${ledgerPath}: ${String(err)}`);
    process.exit(1);
  }
}

async function loadGrokFamilyMixForGovern(windowHours: number): Promise<GrokFamilyMix | undefined> {
  try {
    const from = new Date(Date.now() - windowHours * 3_600_000).toISOString().slice(0, 10);
    // Reuse the store aggregation path (no rescan — tokenomics already read authority).
    const agg = await loadAggregations({ from, rescan: false });
    return computeGrokFamilyMix(agg.byModel);
  } catch {
    // Store/aggregation unavailable — omit mix; evaluateTokenomics skips the check.
    return undefined;
  }
}

async function govern(argv: string[]): Promise<void> {
  const options = parseGovernorArgs(argv);
  if (options.help) {
    console.log(GOVERNOR_HELP);
    return;
  }

  const hookRaw = options.hook ? await readStdinText() : undefined;
  const tokenomics = await analyzeTokenomics();
  const grokMix = await loadGrokFamilyMixForGovern(tokenomics.consumption.windowHours);
  const report = evaluateGovernorCli(tokenomics, options, hookRaw, grokMix);
  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    const text = formatGovernorText(report);
    if (report.governor.exitCode === 2) console.error(text);
    else console.log(text);
  }
  process.exit(report.governor.exitCode);
}

async function main() {
  const cmd = process.argv[2] || "report";
  if (cmd === "report" || cmd === "scan") {
    await report(process.argv.slice(3));
  } else if (cmd === "reingest-dedupe") {
    await runReingestDedupe();
  } else if (cmd === "record-savings") {
    await recordSavings(process.argv.slice(3));
  } else if (cmd === "govern") {
    await govern(process.argv.slice(3));
  } else {
    console.error(`unknown command: ${cmd}`);
    console.error(
      `usage: bun src/cli.ts [report|scan|reingest-dedupe|govern|record-savings] [options]`,
    );
    process.exit(2);
  }
}

await main();
