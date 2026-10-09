import { describe, expect, test } from "bun:test";
import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const sourceProjectDir = resolve(import.meta.dir, "../..");

function quoted(value: string): string {
  const escaped = value.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("%", "%%");
  return `"${escaped}"`;
}

function workingDirectory(value: string): string {
  return value
    .replaceAll("\\", "\\x5c")
    .replaceAll('"', "\\x22")
    .replaceAll("'", "\\x27")
    .replaceAll(" ", "\\x20")
    .replaceAll("%", "%%");
}

async function verifyUnitIfAvailable(unitPath: string): Promise<void> {
  if (process.platform !== "linux" || !Bun.which("systemd-analyze")) return;
  const proc = Bun.spawn(["systemd-analyze", "--user", "verify", unitPath], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  expect(exitCode, `${stdout}\n${stderr}`).toBe(0);
}

async function makeFixture(root: string, projectName = "project") {
  const projectDir = join(root, projectName);
  const home = join(root, 'home $cash% "quote" \\semi;');
  const nativeBun = join(home, ".bun", "bin", "bun");
  const shimBun = join(home, ".local", "bin", "bun");
  const binDir = join(root, "bin");
  const stubSystemctl = join(binDir, "systemctl");
  const systemctlLog = join(root, "systemctl.log");
  const envBin = Bun.which("env");
  if (!envBin) throw new Error("env binary is required by the systemd installer fixture");
  await mkdir(join(projectDir, "scripts"), { recursive: true });
  await mkdir(join(projectDir, "systemd"), { recursive: true });
  await mkdir(join(home, ".bun", "bin"), { recursive: true });
  await mkdir(join(home, ".local", "bin"), { recursive: true });
  await mkdir(binDir, { recursive: true });
  await copyFile(
    join(sourceProjectDir, "scripts", "install-systemd.sh"),
    join(projectDir, "scripts", "install-systemd.sh"),
  );
  await copyFile(
    join(sourceProjectDir, "systemd", "sigmashake-tokens.service"),
    join(projectDir, "systemd", "sigmashake-tokens.service"),
  );
  await copyFile(
    join(sourceProjectDir, "systemd", "sigmashake-tokens.slice"),
    join(projectDir, "systemd", "sigmashake-tokens.slice"),
  );
  await copyFile("/bin/true", nativeBun);
  await writeFile(
    shimBun,
    `#!/usr/bin/env bash\nexec powergate.sh ${JSON.stringify(nativeBun)} "$@"\n`,
  );
  await writeFile(
    stubSystemctl,
    `#!/usr/bin/env bash
printf '%s\\n' "$*" >> ${JSON.stringify(systemctlLog)}
case " $* " in
  *" is-active "*) printf '%s\n' "\${STUB_SERVICE_STATE:-inactive}" ; exit "\${STUB_SERVICE_ACTIVE:-3}" ;;
  *" list-units "*) printf '%s' "\${STUB_WORKER_UNITS:-}" ; exit 0 ;;
  *" show "*" --property=TasksCurrent --value "*) printf '%s\\n' "\${STUB_SLICE_TASKS:-0}" ; exit 0 ;;
esac
exit 0
`,
  );
  await chmod(nativeBun, 0o755);
  await chmod(shimBun, 0o755);
  await chmod(stubSystemctl, 0o755);
  return { projectDir, home, nativeBun, shimBun, envBin, binDir, systemctlLog };
}

async function runInstaller(
  fixture: Awaited<ReturnType<typeof makeFixture>>,
  env: Record<string, string> = {},
) {
  const proc = Bun.spawn(["bash", join(fixture.projectDir, "scripts", "install-systemd.sh")], {
    cwd: fixture.projectDir,
    env: {
      ...process.env,
      HOME: fixture.home,
      BUN_INSTALL: join(fixture.home, ".bun"),
      PATH: `${fixture.binDir}:${join(fixture.home, ".local", "bin")}:${process.env.PATH ?? ""}`,
      ...env,
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  return { exitCode, stdout, stderr };
}

async function systemctlLog(fixture: Awaited<ReturnType<typeof makeFixture>>): Promise<string> {
  return readFile(fixture.systemctlLog, "utf-8").catch(() => "");
}

function expectNoRuntimePropertyWrites(log: string): void {
  expect(log).not.toContain(" set-property ");
}

// The installer is a bash script for Linux systemd. Other platforms never run it.
describe.skipIf(process.platform !== "linux")("systemd installer", () => {
  test("renders the native Bun ELF instead of the PATH powergate shim", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-"));
    try {
      const fixture = await makeFixture(root);
      const result = await runInstaller(fixture);
      expect(result.exitCode, result.stderr).toBe(0);

      const service = await readFile(
        join(fixture.home, ".config", "systemd", "user", "sigmashake-tokens.service"),
        "utf-8",
      );
      const slice = await readFile(
        join(fixture.home, ".config", "systemd", "user", "sigmashake-tokens.slice"),
        "utf-8",
      );
      expect(service).toContain(`WorkingDirectory=${workingDirectory(fixture.projectDir)}`);
      expect(service).toContain(
        `ExecStart=:${quoted(fixture.envBin)} -- ${quoted(fixture.nativeBun)} ${quoted(`${fixture.projectDir}/src/server.ts`)}`,
      );
      expect(service).not.toContain(fixture.shimBun);
      expect(service).toContain("MemoryLow=80M");
      expect(service).toContain("MemoryMax=128M");
      expect(service).toContain("CPUWeight=1000");
      expect(service).not.toMatch(/^CPUQuota=/m);
      expect(slice).toContain("MemoryMax=512M");
      expect(slice).toContain("CPUQuota=50%");

      const systemctl = await readFile(fixture.systemctlLog, "utf-8");
      expect(systemctl).toContain(
        "--user set-property --runtime sigmashake-tokens.service MemoryMax=128M",
      );
      expect(systemctl).toContain(
        "--user set-property --runtime sigmashake-tokens.slice MemoryMax=512M",
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("rejects an active parent before changing runtime properties", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-active-"));
    try {
      const fixture = await makeFixture(root);
      const result = await runInstaller(fixture, {
        STUB_SERVICE_STATE: "active",
        STUB_SERVICE_ACTIVE: "0",
      });

      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain("must be proven inactive");
      expectNoRuntimePropertyWrites(await systemctlLog(fixture));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("rejects unknown and failed parent-state probes before changing runtime properties", async () => {
    const cases = [
      { name: "unknown", state: "unknown", status: "4" },
      { name: "query-error", state: "probe-error", status: "1" },
    ];
    for (const scenario of cases) {
      const root = await mkdtemp(join(tmpdir(), `tokens-systemd-${scenario.name}-`));
      try {
        const fixture = await makeFixture(root);
        const result = await runInstaller(fixture, {
          STUB_SERVICE_STATE: scenario.state,
          STUB_SERVICE_ACTIVE: scenario.status,
        });

        expect(result.exitCode).not.toBe(0);
        expect(result.stderr).toContain("must be proven inactive");
        expectNoRuntimePropertyWrites(await systemctlLog(fixture));
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  });

  test("rejects active transient workers before changing runtime properties", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-worker-"));
    try {
      const fixture = await makeFixture(root);
      const result = await runInstaller(fixture, {
        STUB_WORKER_UNITS: "sigmashake-tokens-ingest-test.service loaded active running\n",
      });

      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain("transient workers remain active");
      expectNoRuntimePropertyWrites(await systemctlLog(fixture));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("rejects nonzero and invalid slice task counts before changing runtime properties", async () => {
    for (const tasks of ["1", "unknown"]) {
      const root = await mkdtemp(join(tmpdir(), "tokens-systemd-slice-tasks-"));
      try {
        const fixture = await makeFixture(root);
        const result = await runInstaller(fixture, { STUB_SLICE_TASKS: tasks });

        expect(result.exitCode).not.toBe(0);
        expect(result.stderr).toContain("sigmashake-tokens.slice is not empty");
        expectNoRuntimePropertyWrites(await systemctlLog(fixture));
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  });

  test("quotes spaces and systemd metacharacters without adding directives", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-meta-"));
    try {
      const fixture = await makeFixture(root, 'project $cash% "quote" \\semi; #hash');
      const result = await runInstaller(fixture);
      expect(result.exitCode, result.stderr).toBe(0);

      const unitPath = join(
        fixture.home,
        ".config",
        "systemd",
        "user",
        "sigmashake-tokens.service",
      );
      const unit = await readFile(unitPath, "utf-8");
      expect(unit).toContain(`WorkingDirectory=${workingDirectory(fixture.projectDir)}`);
      expect(unit).toContain(
        `ExecStart=:${quoted(fixture.envBin)} -- ${quoted(fixture.nativeBun)} ${quoted(`${fixture.projectDir}/src/server.ts`)}`,
      );
      expect(unit.match(/^ExecStart=/gm)).toHaveLength(1);
      expect(unit.match(/^WorkingDirectory=/gm)).toHaveLength(1);
      expect(unit).not.toContain("__WORKDIR__");
      expect(unit).not.toContain("__BUN_BIN__");
      await verifyUnitIfAvailable(unitPath);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("failed rendering preserves the previously installed unit atomically", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-preserve-"));
    try {
      const fixture = await makeFixture(root);
      const unitDir = join(fixture.home, ".config", "systemd", "user");
      const unitPath = join(unitDir, "sigmashake-tokens.service");
      await mkdir(unitDir, { recursive: true });
      await writeFile(unitPath, "previous-known-good-unit\n");
      await writeFile(
        join(fixture.projectDir, "systemd", "sigmashake-tokens.service"),
        "[Service]\nWorkingDirectory=__WORKDIR__\n",
      );

      const result = await runInstaller(fixture);

      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain("exactly one WorkingDirectory and ExecStart placeholder");
      expect(await readFile(unitPath, "utf-8")).toBe("previous-known-good-unit\n");
      expect(await readFile(fixture.systemctlLog, "utf-8").catch(() => "")).toBe("");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("control characters fail closed before replacing an installed unit", async () => {
    const root = await mkdtemp(join(tmpdir(), "tokens-systemd-control-"));
    try {
      const fixture = await makeFixture(root, "project\ninjected.service");
      const unitDir = join(fixture.home, ".config", "systemd", "user");
      const unitPath = join(unitDir, "sigmashake-tokens.service");
      await mkdir(unitDir, { recursive: true });
      await writeFile(unitPath, "previous-known-good-unit\n");

      const result = await runInstaller(fixture);

      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain("control characters");
      expect(await readFile(unitPath, "utf-8")).toBe("previous-known-good-unit\n");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
