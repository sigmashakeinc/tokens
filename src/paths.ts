// Single source of truth for every filesystem location the service reads or
// writes. All paths come from os.homedir() plus path.join, so the same code
// works on Windows, macOS, and Linux. Each location has an environment
// override, listed next to it.

import { homedir } from "node:os";
import { join } from "node:path";

function envPath(name: string): string | undefined {
  const value = process.env[name];
  return value === undefined || value === "" ? undefined : value;
}

/** Data directory for the SQLite store and the savings ledger. Override: SIGMASHAKE_HOME. */
export function dataHome(): string {
  return envPath("SIGMASHAKE_HOME") ?? join(homedir(), ".sigmashake");
}

/** SQLite store path. Override: TOKENS_DB_PATH. */
export function databasePath(): string {
  return envPath("TOKENS_DB_PATH") ?? join(dataHome(), "tokens.sqlite");
}

/** Claude Code config root. Override: CLAUDE_CONFIG_DIR (the variable Claude Code itself reads). */
export function claudeConfigDir(): string {
  return envPath("CLAUDE_CONFIG_DIR") ?? join(homedir(), ".claude");
}

/** Claude Code session logs. Override: CLAUDE_PROJECTS_DIR, else CLAUDE_CONFIG_DIR/projects. */
export function claudeProjectsDir(): string {
  return envPath("CLAUDE_PROJECTS_DIR") ?? join(claudeConfigDir(), "projects");
}

/** Codex home. Override: CODEX_HOME (the variable Codex itself reads). */
export function codexHome(): string {
  return envPath("CODEX_HOME") ?? join(homedir(), ".codex");
}

/** Codex session logs. Override: CODEX_SESSIONS_DIR, else CODEX_HOME/sessions. */
export function codexSessionsDir(): string {
  return envPath("CODEX_SESSIONS_DIR") ?? join(codexHome(), "sessions");
}

/** Grok log tree. Override: GROK_LOGS_DIR. */
export function grokLogsDir(): string {
  return envPath("GROK_LOGS_DIR") ?? join(homedir(), ".grok", "logs");
}

/** Grok session tree. Override: GROK_SESSIONS_DIR. */
export function grokSessionsDir(): string {
  return envPath("GROK_SESSIONS_DIR") ?? join(homedir(), ".grok", "sessions");
}

/** Normalises a stored or discovered path to forward slashes for substring checks. */
export function toPosixPath(path: string): string {
  return path.replaceAll("\\", "/");
}
