// Integration: full JSONL ingest pipeline against an on-disk fixture directory.

import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ingestOnce } from "../../src/scanner.ts";
import { TokenStore } from "../../src/store.ts";

test("ingestOnce picks up newly-written JSONL files in the projects dir", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-int-"));
  const projectsDir = join(root, "projects");
  const codexSessionsDir = join(root, "codex-sessions");
  const projectDir = join(projectsDir, "-home-user-fake");
  await mkdir(projectDir, { recursive: true });
  await mkdir(codexSessionsDir, { recursive: true });
  process.env.CLAUDE_PROJECTS_DIR = projectsDir;
  process.env.CODEX_SESSIONS_DIR = codexSessionsDir;
  const store = new TokenStore(join(root, "tokens.sqlite"));
  try {
    const sessionPath = join(projectDir, "00000000-1111-2222-3333-444444444444.jsonl");
    await writeFile(
      sessionPath,
      `${JSON.stringify({
        type: "assistant",
        timestamp: "2026-05-17T12:00:00.000Z",
        cwd: "/srv/example/fake",
        message: {
          role: "assistant",
          model: "claude-sonnet-4-6",
          usage: {
            input_tokens: 10,
            output_tokens: 20,
          },
        },
      })}\n`,
    );
    await ingestOnce(store);
    const events = store.selectEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events[0]?.model).toContain("sonnet");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
    delete process.env.CLAUDE_PROJECTS_DIR;
    delete process.env.CODEX_SESSIONS_DIR;
  }
});
