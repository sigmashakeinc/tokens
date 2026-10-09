import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { costFor } from "../../src/pricing.ts";
import { _resetScannerStateForTest, ingestOnce } from "../../src/scanner.ts";
import { TokenStore } from "../../src/store.ts";

test("Codex GPT-5.6 bills cached input and reasoning subsets exactly once", async () => {
  const root = await mkdtemp(join(tmpdir(), "gpt56-codex-pricing-"));
  const sessions = join(root, "sessions");
  const projects = join(root, "projects");
  const dbPath = join(root, "tokens.sqlite");
  const oldSessions = process.env.CODEX_SESSIONS_DIR;
  const oldProjects = process.env.CLAUDE_PROJECTS_DIR;
  process.env.CODEX_SESSIONS_DIR = sessions;
  process.env.CLAUDE_PROJECTS_DIR = projects;
  _resetScannerStateForTest();

  try {
    const sessionDir = join(sessions, "2026", "07", "10");
    await mkdir(sessionDir, { recursive: true });
    const lines = [
      {
        type: "session_meta",
        timestamp: "2026-07-10T01:00:00.000Z",
        payload: { session_id: "gpt56-session", cwd: "/tmp/project" },
      },
      {
        type: "turn_context",
        timestamp: "2026-07-10T01:00:01.000Z",
        payload: { model: "gpt-5.6-sol", cwd: "/tmp/project" },
      },
      {
        type: "event_msg",
        timestamp: "2026-07-10T01:00:02.000Z",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 1_000,
              cached_input_tokens: 400,
              output_tokens: 20,
              reasoning_output_tokens: 5,
            },
          },
        },
      },
    ];
    await writeFile(join(sessionDir, "gpt56.jsonl"), `${lines.map(JSON.stringify).join("\n")}\n`);

    const store = new TokenStore(dbPath);
    try {
      expect((await ingestOnce(store)).newEvents).toBe(1);
      const event = store.selectEvents()[0]!;
      expect(event.inputTokens).toBe(600);
      expect(event.cacheReadTokens).toBe(400);
      expect(event.outputTokens).toBe(20);
      expect(event.cost).toBeCloseTo(
        costFor("gpt-5.6-sol", {
          inputTokens: 600,
          cacheReadTokens: 400,
          outputTokens: 20,
          cacheWrite5mTokens: 0,
          cacheWrite1hTokens: 0,
        }),
      );
    } finally {
      store.close();
    }
  } finally {
    if (oldSessions === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = oldSessions;
    if (oldProjects === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = oldProjects;
    await rm(root, { recursive: true, force: true });
    _resetScannerStateForTest();
  }
});
