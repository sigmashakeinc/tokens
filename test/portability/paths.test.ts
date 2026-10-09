import { afterEach, describe, expect, test } from "bun:test";
import { homedir } from "node:os";
import { join } from "node:path";
import {
  claudeProjectsDir,
  codexSessionsDir,
  databasePath,
  dataHome,
  toPosixPath,
} from "../../src/paths.ts";

const KEYS = [
  "SIGMASHAKE_HOME",
  "TOKENS_DB_PATH",
  "CLAUDE_CONFIG_DIR",
  "CLAUDE_PROJECTS_DIR",
  "CODEX_HOME",
  "CODEX_SESSIONS_DIR",
] as const;
const saved = new Map<string, string | undefined>(KEYS.map((key) => [key, process.env[key]]));

function clearAll(): void {
  for (const key of KEYS) delete process.env[key];
}

afterEach(() => {
  for (const [key, value] of saved) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

describe("paths", () => {
  test("defaults come from os.homedir() and path.join", () => {
    clearAll();
    expect(dataHome()).toBe(join(homedir(), ".sigmashake"));
    expect(databasePath()).toBe(join(homedir(), ".sigmashake", "tokens.sqlite"));
    expect(claudeProjectsDir()).toBe(join(homedir(), ".claude", "projects"));
    expect(codexSessionsDir()).toBe(join(homedir(), ".codex", "sessions"));
  });

  test("CLAUDE_CONFIG_DIR and CODEX_HOME move the log roots", () => {
    clearAll();
    process.env.CLAUDE_CONFIG_DIR = join("custom", "claude");
    process.env.CODEX_HOME = join("custom", "codex");
    expect(claudeProjectsDir()).toBe(join("custom", "claude", "projects"));
    expect(codexSessionsDir()).toBe(join("custom", "codex", "sessions"));
  });

  test("the explicit directory variables win over the config-dir variables", () => {
    clearAll();
    process.env.CLAUDE_CONFIG_DIR = join("custom", "claude");
    process.env.CLAUDE_PROJECTS_DIR = join("explicit", "projects");
    process.env.CODEX_HOME = join("custom", "codex");
    process.env.CODEX_SESSIONS_DIR = join("explicit", "sessions");
    expect(claudeProjectsDir()).toBe(join("explicit", "projects"));
    expect(codexSessionsDir()).toBe(join("explicit", "sessions"));
  });

  test("SIGMASHAKE_HOME and TOKENS_DB_PATH move the store", () => {
    clearAll();
    process.env.SIGMASHAKE_HOME = join("data", "home");
    expect(databasePath()).toBe(join("data", "home", "tokens.sqlite"));
    process.env.TOKENS_DB_PATH = join("elsewhere", "t.sqlite");
    expect(databasePath()).toBe(join("elsewhere", "t.sqlite"));
  });

  test("an empty variable counts as unset", () => {
    clearAll();
    process.env.CLAUDE_PROJECTS_DIR = "";
    process.env.CLAUDE_CONFIG_DIR = "";
    expect(claudeProjectsDir()).toBe(join(homedir(), ".claude", "projects"));
  });

  test("toPosixPath turns Windows separators into slashes", () => {
    expect(toPosixPath("C:\\Users\\me\\.codex\\sessions\\a.jsonl")).toBe(
      "C:/Users/me/.codex/sessions/a.jsonl",
    );
    expect(toPosixPath("/srv/example/.grok/logs/x")).toBe("/srv/example/.grok/logs/x");
  });
});
