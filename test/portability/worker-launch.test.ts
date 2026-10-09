import { describe, expect, test } from "bun:test";
import {
  detectSystemdLauncher,
  directWorkerEnv,
  isCompiledBinary,
  type SystemdProbe,
  WORKER_SUBCOMMAND,
  workerInvocation,
} from "../../src/worker-launch.ts";

function probe(overrides: Partial<SystemdProbe>): SystemdProbe {
  return {
    env: {},
    platform: "linux",
    fileExists: () => true,
    readText: () => "0::/user.slice/sigmashake-tokens.slice/sigmashake-tokens.service\n",
    ...overrides,
  };
}

describe("worker launch policy", () => {
  test("a systemd transient unit is used only inside the sigmashake-tokens service", () => {
    expect(detectSystemdLauncher(probe({ env: { INVOCATION_ID: "abc" } }))).toBe(true);
  });

  test("a foreground run on Linux uses a plain child process", () => {
    expect(detectSystemdLauncher(probe({ env: {} }))).toBe(false);
  });

  test("another systemd unit does not qualify", () => {
    expect(
      detectSystemdLauncher(
        probe({
          env: { INVOCATION_ID: "abc" },
          readText: () => "0::/system.slice/other.service\n",
        }),
      ),
    ).toBe(false);
  });

  test("macOS and Windows never use systemd, even when forced", () => {
    for (const platform of ["darwin", "win32"]) {
      expect(
        detectSystemdLauncher(
          probe({ platform, env: { INVOCATION_ID: "abc", TOKENS_WORKER_LAUNCHER: "systemd" } }),
        ),
      ).toBe(false);
    }
  });

  test("a missing systemd-run or an unreadable cgroup file means direct", () => {
    expect(
      detectSystemdLauncher(probe({ env: { INVOCATION_ID: "abc" }, fileExists: () => false })),
    ).toBe(false);
    expect(
      detectSystemdLauncher(
        probe({
          env: { INVOCATION_ID: "abc" },
          readText: () => {
            throw new Error("no /proc");
          },
        }),
      ),
    ).toBe(false);
  });

  test("TOKENS_WORKER_LAUNCHER overrides detection on Linux", () => {
    expect(detectSystemdLauncher(probe({ env: { TOKENS_WORKER_LAUNCHER: "systemd" } }))).toBe(true);
    expect(
      detectSystemdLauncher(
        probe({ env: { INVOCATION_ID: "abc", TOKENS_WORKER_LAUNCHER: "direct" } }),
      ),
    ).toBe(false);
  });

  test("from source a worker runs as a Bun script", () => {
    expect(isCompiledBinary()).toBe(false);
    expect(workerInvocation("ingest", "/x/usage-ingest.worker.ts")).toEqual([
      process.execPath,
      "/x/usage-ingest.worker.ts",
    ]);
  });

  test("the hidden worker subcommand has a stable name", () => {
    expect(WORKER_SUBCOMMAND).toBe("__worker");
  });

  test("a direct worker gets a small environment, not the parent's", () => {
    const saved = process.env.TOKENS_PRIVATE_PROBE;
    process.env.TOKENS_PRIVATE_PROBE = "secret-value";
    try {
      const env = directWorkerEnv();
      expect(env.TOKENS_PRIVATE_PROBE).toBeUndefined();
      expect(Object.values(env).every((value) => typeof value === "string")).toBe(true);
    } finally {
      if (saved === undefined) delete process.env.TOKENS_PRIVATE_PROBE;
      else process.env.TOKENS_PRIVATE_PROBE = saved;
    }
  });
});
