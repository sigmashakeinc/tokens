#!/usr/bin/env bun
// `tokens` is the single entry point for the service, the CLI, and the MCP
// server. `bun build --compile` turns this file into one executable per
// operating system. It works the same on Windows, macOS, and Linux.
//
//   tokens serve                      run the service in the foreground
//   tokens health|usage|rate|day|session|stats|govern
//                                     query a running service over HTTP
//   tokens report|scan|reingest-dedupe|record-savings
//                                     read or change the local store directly
//   tokens mcp                        stdio MCP server for AI agents
//   tokens --version | --help
//
// `tokens __worker <kind>` is internal: the service uses it to start its child
// processes from the compiled binary. Every command module is loaded with a
// dynamic import because several of them run their work when first loaded.

import pkg from "../package.json" with { type: "json" };
import { WORKER_SUBCOMMAND, type WorkerKind } from "./worker-launch.ts";

const HELP = `tokens ${pkg.version} - token spend monitor for Claude Code, Codex, and Grok

Usage:
  tokens serve                         Run the service in the foreground (dashboard + API)
  tokens health                        Check the running service
  tokens usage [--from DATE] [--to DATE] [--models m1,m2] [--projects p1,p2]
  tokens rate                          Rolling burn-rate windows
  tokens day <YYYY-MM-DD>              Per-session breakdown for one day
  tokens session <id>                  Events for one session
  tokens stats                         Store statistics
  tokens govern [--hook] [--enforce]   Tokenomics governor for hooks
  tokens report [--days N]             Print a summary from the local store (no service needed)
  tokens scan                          Same as report
  tokens reingest-dedupe               Rebuild the store from source logs (service must be stopped)
  tokens record-savings --tool NAME --orig BYTES --comp BYTES
  tokens mcp                           Run the stdio MCP server
  tokens --version                     Print the version
  tokens --help                        Print this help

Environment:
  TOKENS_HOST, TOKENS_PORT             Listen address for "serve" (default 127.0.0.1:7913)
  TOKENS_BASE_URL                      Service URL used by the query commands
  TOKENS_DB_PATH                       SQLite store (default ~/.sigmashake/tokens.sqlite)
  CLAUDE_CONFIG_DIR, CLAUDE_PROJECTS_DIR   Claude Code logs (default ~/.claude/projects)
  CODEX_HOME, CODEX_SESSIONS_DIR           Codex logs (default ~/.codex/sessions)
  GROK_LOGS_DIR, GROK_SESSIONS_DIR         Grok logs (default ~/.grok/...)
`;

const HTTP_COMMANDS = new Set(["health", "usage", "rate", "day", "session", "stats", "govern"]);
const LOCAL_COMMANDS = new Set(["report", "scan", "reingest-dedupe", "record-savings"]);

async function runWorker(kind: string | undefined): Promise<void> {
  // Remove "__worker <kind>" so the worker sees the same argv it sees from source.
  process.argv.splice(2, 2);
  const workers: Record<WorkerKind, () => Promise<unknown>> = {
    ingest: () => import("./usage-ingest.worker.ts"),
    aggregation: () => import("./usage-aggregation.worker.ts"),
    tokenomics: () => import("./tokenomics.worker.ts"),
  };
  if (kind === undefined || !Object.hasOwn(workers, kind)) {
    console.error(`unknown worker: ${kind ?? "(none)"}`);
    process.exit(2);
  }
  await workers[kind as WorkerKind]();
}

async function main(): Promise<void> {
  const cmd = process.argv[2];
  if (cmd === "--version" || cmd === "-v" || cmd === "version") {
    console.log(pkg.version);
    return;
  }
  if (cmd === undefined || cmd === "--help" || cmd === "-h" || cmd === "help") {
    console.log(HELP);
    process.exit(cmd === undefined ? 2 : 0);
  }
  if (cmd === WORKER_SUBCOMMAND) {
    await runWorker(process.argv[3]);
  } else if (cmd === "serve") {
    const { startTokensServer } = await import("./server.ts");
    startTokensServer();
  } else if (cmd === "mcp") {
    process.argv.splice(2, 1);
    await import("../tools/sigmashake-tokens-mcp.ts");
  } else if (HTTP_COMMANDS.has(cmd)) {
    await import("../tools/sigmashake-tokens-cli.ts");
  } else if (LOCAL_COMMANDS.has(cmd)) {
    await import("./cli.ts");
  } else {
    console.error(`unknown command: ${cmd}\n`);
    console.error(HELP);
    process.exit(2);
  }
}

await main();
