// Launch policy shared by the three child-process workers (ingest, usage
// aggregation, tokenomics). It decides two things that differ per platform:
//
//   1. How to start a worker. From source the worker is a TypeScript file run
//      by Bun. In a `bun build --compile` binary there is no loose file, so the
//      binary re-runs itself with the hidden `__worker <kind>` subcommand.
//   2. Whether to wrap the worker in a transient systemd unit. That is only
//      valid on Linux when this process itself runs as sigmashake-tokens.service.
//      Everywhere else (Windows, macOS, a foreground `tokens serve` on Linux)
//      the worker is a plain child process.

import { existsSync, readFileSync } from "node:fs";

export type WorkerKind = "ingest" | "aggregation" | "tokenomics";

/** Hidden subcommand the compiled binary uses to run a worker. */
export const WORKER_SUBCOMMAND = "__worker";

const SYSTEMD_RUN_PATH = "/usr/bin/systemd-run";
const SERVICE_UNIT_NAME = "sigmashake-tokens.service";

/** True when this code runs from a `bun build --compile` executable. */
export function isCompiledBinary(): boolean {
  const location = import.meta.path;
  return location.includes("$bunfs") || location.includes("~BUN");
}

/** argv that runs one worker: the Bun script from source, the self-subcommand when compiled. */
export function workerInvocation(kind: WorkerKind, sourceEntry: string): string[] {
  return isCompiledBinary()
    ? [process.execPath, WORKER_SUBCOMMAND, kind]
    : [process.execPath, sourceEntry];
}

let systemdDecision: boolean | undefined;

/**
 * True only on Linux, with systemd-run present, when this process is the
 * sigmashake-tokens.service unit (or the operator forces it). The transient
 * units bind to that service, so any other context must not use them.
 * TOKENS_WORKER_LAUNCHER=direct|systemd overrides the detection.
 */
export function useSystemdLauncher(): boolean {
  if (systemdDecision !== undefined) return systemdDecision;
  systemdDecision = detectSystemdLauncher();
  return systemdDecision;
}

export interface SystemdProbe {
  env: Record<string, string | undefined>;
  platform: string;
  fileExists: (path: string) => boolean;
  readText: (path: string) => string;
}

/** The detection behind useSystemdLauncher, with its inputs injectable for tests. */
export function detectSystemdLauncher(
  probe: SystemdProbe = {
    env: process.env,
    platform: process.platform,
    fileExists: existsSync,
    readText: (path) => readFileSync(path, "utf8"),
  },
): boolean {
  const override = probe.env.TOKENS_WORKER_LAUNCHER;
  if (override === "direct") return false;
  if (probe.platform !== "linux" || !probe.fileExists(SYSTEMD_RUN_PATH)) return false;
  if (override === "systemd") return true;
  if (!probe.env.INVOCATION_ID) return false;
  try {
    return probe.readText("/proc/self/cgroup").includes(SERVICE_UNIT_NAME);
  } catch {
    return false;
  }
}

const WINDOWS_ENV_KEYS = [
  "SystemRoot",
  "SYSTEMROOT",
  "WINDIR",
  "COMSPEC",
  "PATH",
  "PATHEXT",
  "USERPROFILE",
  "HOMEDRIVE",
  "HOMEPATH",
  "APPDATA",
  "LOCALAPPDATA",
  "TEMP",
  "TMP",
];
const POSIX_ENV_KEYS = ["HOME", "TMPDIR", "LANG", "LC_ALL"];

/**
 * Environment for a directly spawned worker. The worker receives every path it
 * needs in its request, so it gets only the small set of variables the
 * operating system and Bun need, not the parent's full environment.
 */
export function directWorkerEnv(): Record<string, string> {
  const keys = process.platform === "win32" ? WINDOWS_ENV_KEYS : POSIX_ENV_KEYS;
  const env: Record<string, string> = {};
  const seen = new Set<string>();
  for (const key of keys) {
    // Windows variable names are case-insensitive, so keep one entry per name.
    const folded = process.platform === "win32" ? key.toLowerCase() : key;
    const value = process.env[key];
    if (value === undefined || seen.has(folded)) continue;
    seen.add(folded);
    env[key] = value;
  }
  return env;
}
