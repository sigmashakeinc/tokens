import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  _createMetadataTrustContextForTest,
  ingestOnce,
  type MetadataTrustOperations,
} from "../src/scanner.ts";
import {
  _sourceGenerationPathQueryForTest,
  type CapturedFileCommit,
  type CapturedFileCommitResult,
  type SourceReconciliationSnapshot,
  type StoredEvent,
  TokenStore,
} from "../src/store.ts";

class OperationCountingStore extends TokenStore {
  snapshotCalls = 0;
  legacyLookupCalls = 0;
  batchCommitCalls = 0;
  reconcileCalls = 0;

  override captureSourceReconciliationSnapshot(): SourceReconciliationSnapshot {
    this.snapshotCalls++;
    return super.captureSourceReconciliationSnapshot();
  }

  override getLatestSourceFile(source: "claude" | "codex", device: string, inode: string) {
    this.legacyLookupCalls++;
    return super.getLatestSourceFile(source, device, inode);
  }

  override getSourceGenerationKeys(
    source: "claude" | "codex",
    device: string,
    inode: string,
    paths: string[] = [],
  ): string[] {
    this.legacyLookupCalls++;
    return super.getSourceGenerationKeys(source, device, inode, paths);
  }

  override hasPendingLegacyPath(paths: string[]): boolean {
    this.legacyLookupCalls++;
    return super.hasPendingLegacyPath(paths);
  }

  override commitCapturedFile(batch: CapturedFileCommit): CapturedFileCommitResult {
    this.batchCommitCalls++;
    return super.commitCapturedFile(batch);
  }

  override reconcileDiscovery(...args: Parameters<TokenStore["reconcileDiscovery"]>) {
    this.reconcileCalls++;
    return super.reconcileDiscovery(...args);
  }

  resetOperationCounts(): void {
    this.snapshotCalls = 0;
    this.legacyLookupCalls = 0;
    this.batchCommitCalls = 0;
    this.reconcileCalls = 0;
  }
}

class CountingMetadataTrustOperations implements MetadataTrustOperations {
  mountId = "53";
  anchorMountId: string | null | undefined;
  filesystemType = 0xef53n;
  mountInfoCalls = 0;
  statfsCalls = 0;
  mountIdCalls = 0;
  anchorOpenCalls = 0;
  anchorCloseCalls = 0;
  throwMountInfo = false;
  throwStatfs = false;
  readonly mounts = new Map<string, { device: bigint; filesystemName: string }>();

  constructor(device: bigint) {
    this.mounts.set(this.mountId, { device, filesystemName: "ext4" });
  }

  async mountIdForFd(): Promise<string | null> {
    this.mountIdCalls++;
    return this.mountId;
  }

  async readMountInfo(): Promise<Map<string, { device: bigint; filesystemName: string }>> {
    this.mountInfoCalls++;
    if (this.throwMountInfo) throw new Error("mountinfo unavailable");
    return new Map(this.mounts);
  }

  async openAnchorFromFd(
    fd: number,
    device: bigint,
  ): Promise<{
    fd: number;
    mountId: string | null;
    device: bigint;
    close(): Promise<void>;
  }> {
    this.anchorOpenCalls++;
    return {
      fd: 100_000 + fd,
      mountId: this.anchorMountId === undefined ? this.mountId : this.anchorMountId,
      device,
      close: async () => {
        this.anchorCloseCalls++;
      },
    };
  }

  async statfsTypeForFd(): Promise<bigint> {
    this.statfsCalls++;
    if (this.throwStatfs) throw new Error("statfs unavailable");
    return this.filesystemType;
  }

  resetCounts(): void {
    this.mountInfoCalls = 0;
    this.statfsCalls = 0;
    this.mountIdCalls = 0;
    this.anchorOpenCalls = 0;
    this.anchorCloseCalls = 0;
  }
}

function unrelatedEvent(index: number): StoredEvent {
  return {
    ts: 1_700_000_000_000 + index,
    date: "2026-07-14",
    model: "claude-sonnet-4-6",
    project: "unrelated",
    projectSlug: "unrelated",
    sessionId: `unrelated-${index}`,
    inputTokens: 1,
    outputTokens: 1,
    cacheReadTokens: 0,
    cacheWrite5mTokens: 0,
    cacheWrite1hTokens: 0,
    cost: 0,
    filePath: `/unrelated/${index}.jsonl`,
    lineOffset: 0,
    source: "claude",
  };
}

async function writeEmptyJsonlFiles(dir: string, count: number): Promise<void> {
  const chunkSize = 100;
  for (let start = 0; start < count; start += chunkSize) {
    await Promise.all(
      Array.from({ length: Math.min(chunkSize, count - start) }, (_, offset) =>
        writeFile(join(dir, `session-${start + offset}.jsonl`), ""),
      ),
    );
  }
}

test("source generation lookup stays indexed at scale without losing identity or path history", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-generation-lookup-"));
  const storePath = join(root, "tokens.sqlite");
  try {
    const bootstrap = new TokenStore(storePath);
    bootstrap.close();

    const raw = new Database(storePath);
    try {
      raw.run("PRAGMA foreign_keys = ON");
      // Prove an already-versioned database receives the query-critical index
      // on reopen rather than relying only on fresh-schema creation.
      raw.run("DROP INDEX idx_source_files_representative");
      const insertSource = raw.prepare(
        "INSERT INTO source_files (generation_key, source, device, inode, representative_path, captured_size, mtime_ms, mtime_ns, ctime_ns, last_byte_offset, last_line_number, committed_prefix_sha256, codex_state_json, updated_at) VALUES (?, ?, ?, ?, ?, 0, 0, '0', '0', 0, 0, '', NULL, ?)",
      );
      const insertAlias = raw.prepare(
        "INSERT INTO source_aliases (path, generation_key, source, active, updated_at) VALUES (?, ?, ?, ?, ?)",
      );
      raw.transaction(() => {
        let order = 1;
        insertSource.run("g-path-old", "claude", "1", "1", "/target", order++);
        insertSource.run("g-physical-old", "claude", "42", "99", "/old", order++);
        insertAlias.run("/hardlink", "g-physical-old", "claude", 1, order);
        for (let i = 0; i < 2_000; i++) {
          const key = `g-unrelated-${i}`;
          insertSource.run(key, "claude", "7", String(i), `/unrelated/${i}`, order++);
          insertAlias.run(`/alias/unrelated/${i}`, key, "claude", 1, order);
        }
        insertSource.run("g-alias", "claude", "8", "8", "/renamed", order++);
        insertAlias.run("/alias", "g-alias", "claude", 1, order);
        insertSource.run("g-inactive", "claude", "9", "9", "/inactive-old", order++);
        insertAlias.run("/inactive", "g-inactive", "claude", 0, order);
        insertSource.run("g-physical-new", "claude", "42", "99", "/target", order++);
        insertSource.run("g-other-source", "codex", "42", "99", "/target", order++);
        raw
          .prepare(
            "INSERT INTO legacy_reconciliation_pending (path, source, reason) VALUES ('/target', 'claude', 'unreadable')",
          )
          .run();
      })();
      raw.run("PRAGMA user_version = 5");
    } finally {
      raw.close();
    }

    const paths = ["/target", "/hardlink", "/alias", "/inactive"];
    const store = new TokenStore(storePath);
    try {
      expect(store.getSourceGenerationKeys("claude", "42", "99", paths)).toEqual([
        "g-physical-old",
        "g-physical-new",
        "g-path-old",
        "g-alias",
        "g-inactive",
      ]);
      const snapshot = store.captureSourceReconciliationSnapshot();
      expect(snapshot.getSourceGenerationKeys("claude", "42", "99", paths)).toEqual([
        "g-physical-old",
        "g-physical-new",
        "g-path-old",
        "g-alias",
        "g-inactive",
      ]);
      expect(snapshot.hasPendingLegacyPath(paths)).toBe(true);
    } finally {
      store.close();
    }

    const inspected = new Database(storePath, { readonly: true });
    try {
      const index = inspected
        .query(
          "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_source_files_representative'",
        )
        .get() as { sql: string } | null;
      expect(index?.sql).toContain("source, representative_path");

      const plan = inspected
        .query(`EXPLAIN QUERY PLAN ${_sourceGenerationPathQueryForTest(paths.length)}`)
        .all("claude", ...paths, "claude", ...paths) as Array<{ detail: string }>;
      const details = plan.map((row) => row.detail);
      expect(details.some((detail) => detail.includes("idx_source_files_representative"))).toBe(
        true,
      );
      expect(details.some((detail) => detail.includes("sqlite_autoindex_source_aliases_1"))).toBe(
        true,
      );
      expect(details.some((detail) => detail.includes("sqlite_autoindex_source_files_1"))).toBe(
        true,
      );
      expect(details.some((detail) => /SCAN (?:sf|source_files|sa)\b/.test(detail))).toBe(false);
      expect(
        details.some(
          (detail) => detail.includes("idx_source_files_identity") && detail.includes("source=?"),
        ),
      ).toBe(false);
    } finally {
      inspected.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("steady scan operations stay constant across thousands of files and unrelated events", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-operation-bound-"));
  const projectsDir = join(root, "projects");
  const projectDir = join(projectsDir, "-home-user-ssd-scale");
  const codexDir = join(root, "codex");
  const storePath = join(root, "tokens.sqlite");
  const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
  const previousCodex = process.env.CODEX_SESSIONS_DIR;
  let store: OperationCountingStore | null = null;
  try {
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexDir, { recursive: true });
    await writeEmptyJsonlFiles(projectDir, 2_000);
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexDir;
    store = new OperationCountingStore(storePath);
    const sourceDevice = (await stat(projectDir, { bigint: true })).dev;
    const metadataTrustOperations = new CountingMetadataTrustOperations(sourceDevice);
    const hooks = { metadataTrustOperations };
    expect((await ingestOnce(store, hooks)).files).toBe(2_000);
    store.insertEvents(Array.from({ length: 10_000 }, (_, index) => unrelatedEvent(index)));
    const revision = store.getRevision();
    store.resetOperationCounts();
    metadataTrustOperations.resetCounts();

    const noop = await ingestOnce(store, hooks);

    expect(noop.files).toBe(2_000);
    expect(noop.mutated).toBe(false);
    expect(noop.revision).toBe(revision);
    expect(store.snapshotCalls).toBe(1);
    expect(store.legacyLookupCalls).toBe(0);
    expect(store.batchCommitCalls).toBe(0);
    expect(store.reconcileCalls).toBe(1);
    expect(metadataTrustOperations.mountIdCalls).toBe(2_000);
    expect(metadataTrustOperations.mountInfoCalls).toBe(1);
    expect(metadataTrustOperations.statfsCalls).toBe(1);
    expect(metadataTrustOperations.anchorOpenCalls).toBe(1);
    expect(metadataTrustOperations.anchorCloseCalls).toBe(1);
  } finally {
    store?.close();
    if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
    if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = previousCodex;
    await rm(root, { recursive: true, force: true });
  }
});

test("metadata trust pins a stable mount and rejects mount-id or mount-type changes", async () => {
  const device = 2_049n;
  const operations = new CountingMetadataTrustOperations(device);
  const context = _createMetadataTrustContextForTest(operations);
  try {
    expect(await context.supports(1, device)).toBe(true);
    let allSupported = true;
    for (let fd = 2; fd <= 2_000; fd++) {
      allSupported &&= await context.supports(fd, device);
    }
    expect(allSupported).toBe(true);
    expect(operations.mountInfoCalls).toBe(1);
    expect(operations.statfsCalls).toBe(1);
    expect(operations.anchorOpenCalls).toBe(1);

    // A cached mount ID cannot bless a descriptor from another device. The
    // retained anchor prevents this reuse in production; the comparison is a
    // second fail-closed boundary.
    expect(await context.supports(2_001, device + 1n)).toBe(false);
    expect(operations.mountInfoCalls).toBe(1);
    expect(operations.statfsCalls).toBe(1);

    operations.mountId = "60";
    operations.mounts.set("60", { device, filesystemName: "ext4" });
    operations.anchorMountId = "61";
    expect(await context.supports(2_002, device)).toBe(false);

    operations.mountId = "70";
    operations.anchorMountId = undefined;
    operations.mounts.set("70", { device, filesystemName: "overlay" });
    operations.filesystemType = 0x794c7630n;
    expect(await context.supports(2_003, device)).toBe(false);

    operations.mountId = "80";
    operations.mounts.set("80", { device, filesystemName: "ext3" });
    operations.filesystemType = 0xef53n;
    expect(await context.supports(2_004, device)).toBe(false);

    operations.mountId = "90";
    operations.throwMountInfo = true;
    expect(await context.supports(2_005, device)).toBe(false);

    operations.mountId = "100";
    operations.throwMountInfo = false;
    operations.throwStatfs = true;
    operations.mounts.set("100", { device, filesystemName: "ext4" });
    operations.filesystemType = 0xef53n;
    expect(await context.supports(2_006, device)).toBe(false);
  } finally {
    await context.close();
  }
  expect(operations.anchorCloseCalls).toBe(3);
});

test("a concurrent revision change disables no-op batch skipping", async () => {
  const root = await mkdtemp(join(tmpdir(), "tokens-revision-guard-"));
  const projectsDir = join(root, "projects");
  const projectDir = join(projectsDir, "-home-user-ssd-concurrent");
  const codexDir = join(root, "codex");
  const storePath = join(root, "tokens.sqlite");
  const sessionPath = join(projectDir, "session.jsonl");
  const previousClaude = process.env.CLAUDE_PROJECTS_DIR;
  const previousCodex = process.env.CODEX_SESSIONS_DIR;
  let store: OperationCountingStore | null = null;
  let concurrent: TokenStore | null = null;
  try {
    await mkdir(projectDir, { recursive: true });
    await mkdir(codexDir, { recursive: true });
    await writeFile(sessionPath, "");
    process.env.CLAUDE_PROJECTS_DIR = projectsDir;
    process.env.CODEX_SESSIONS_DIR = codexDir;
    store = new OperationCountingStore(storePath);
    await ingestOnce(store, { metadataFastPathSupported: () => true });
    concurrent = new TokenStore(storePath);
    store.resetOperationCounts();
    let injected = false;

    const result = await ingestOnce(store, {
      metadataFastPathSupported: () => false,
      afterCapturedRead: () => {
        if (injected) return;
        injected = true;
        concurrent?.insertEvents([unrelatedEvent(1)]);
      },
    });

    expect(injected).toBe(true);
    expect(store.snapshotCalls).toBe(1);
    expect(store.legacyLookupCalls).toBe(0);
    expect(store.batchCommitCalls).toBe(1);
    expect(result.revision).toBe(concurrent.getRevision());
  } finally {
    concurrent?.close();
    store?.close();
    if (previousClaude === undefined) delete process.env.CLAUDE_PROJECTS_DIR;
    else process.env.CLAUDE_PROJECTS_DIR = previousClaude;
    if (previousCodex === undefined) delete process.env.CODEX_SESSIONS_DIR;
    else process.env.CODEX_SESSIONS_DIR = previousCodex;
    await rm(root, { recursive: true, force: true });
  }
});
