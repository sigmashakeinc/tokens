import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const projectDir = resolve(import.meta.dir, "../..");

async function source(relativePath: string): Promise<string> {
  return readFile(resolve(projectDir, relativePath), "utf-8");
}

function activeDirectives(unit: string): string[] {
  return unit
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#") && !line.startsWith("["));
}

describe("tokens systemd process isolation", () => {
  test("the parent slice owns the aggregate memory and CPU envelope", async () => {
    const slice = activeDirectives(await source("systemd/sigmashake-tokens.slice"));

    expect(slice).toContain("MemoryMax=512M");
    expect(slice).toContain("CPUQuota=50%");
    expect(slice.filter((line) => line.startsWith("MemoryMax="))).toEqual(["MemoryMax=512M"]);
    expect(slice.filter((line) => line.startsWith("CPUQuota="))).toEqual(["CPUQuota=50%"]);
  });

  test("the HTTP owner is protected inside the shared slice", async () => {
    const service = activeDirectives(await source("systemd/sigmashake-tokens.service"));

    expect(service).toContain("Slice=sigmashake-tokens.slice");
    expect(service).toContain("MemoryLow=80M");
    expect(service).toContain("MemoryMax=128M");
    expect(service).toContain("CPUWeight=1000");
    expect(service.filter((line) => line.startsWith("MemoryMax="))).toEqual(["MemoryMax=128M"]);
    expect(service.some((line) => line.startsWith("CPUQuota="))).toBe(false);
  });

  test("each production aggregation is a bounded hardened transient unit", async () => {
    const manager = await source("src/usage-aggregation-worker.ts");
    const requiredArguments = [
      '"--user"',
      '"--quiet"',
      '"--pipe"',
      '"--wait"',
      '"--collect"',
      '"--slice=sigmashake-tokens.slice"',
      '"--service-type=exec"',
      '"--property=BindsTo=sigmashake-tokens.service"',
      '"--property=After=sigmashake-tokens.service"',
      '"--property=RuntimeMaxSec=11min"',
      '"--property=MemoryHigh=96M"',
      '"--property=MemoryMax=128M"',
      '"--property=CPUQuota=35%"',
      '"--property=CPUWeight=100"',
      '"--property=ProtectHome=read-only"',
      '"--property=ProtectSystem=full"',
      '"--property=NoNewPrivileges=yes"',
      '"--property=PrivateTmp=yes"',
      '"--property=ProtectControlGroups=yes"',
    ];
    for (const argument of requiredArguments) expect(manager).toContain(argument);
    expect(manager).toContain('const workerCommand = [ENV_BIN, "-i", ...workerInvocation(');
    expect(manager).toContain('const SYSTEMD_RUN_BIN = "/usr/bin/systemd-run"');
    expect(manager).toContain('const SYSTEMCTL_BIN = "/usr/bin/systemctl"');
    expect(manager).toContain("stopLaunch(active.launch)");
    expect(manager).not.toContain("new Worker(");
    expect(manager).not.toContain("fallback");
  });

  test("each production ingest is a bounded writable transient sibling", async () => {
    const manager = await source("src/usage-ingest-worker.ts");
    const server = await source("src/server.ts");
    const requiredArguments = [
      '"--user"',
      '"--quiet"',
      '"--pipe"',
      '"--wait"',
      '"--collect"',
      '"--slice=sigmashake-tokens.slice"',
      '"--service-type=exec"',
      '"--property=BindsTo=sigmashake-tokens.service"',
      '"--property=After=sigmashake-tokens.service"',
      "`--property=RuntimeMaxSec=${timeoutMs + MAX_STOP_TIMEOUT_MS + SYSTEMD_RUNTIME_MARGIN_MS}ms`",
      '"--property=MemoryHigh=208M"',
      '"--property=MemoryMax=256M"',
      '"--property=CPUQuota=45%"',
      '"--property=CPUWeight=100"',
      '"--property=ProtectHome=read-only"',
      '"--property=ProtectSystem=full"',
      '"--property=NoNewPrivileges=yes"',
      '"--property=PrivateTmp=yes"',
      '"--property=ProtectControlGroups=yes"',
    ];
    for (const argument of requiredArguments) expect(manager).toContain(argument);
    expect(manager).toContain("`--property=ReadWritePaths=$" + "{dirname(databasePath)}`");
    expect(manager).toContain('const workerCommand = [ENV_BIN, "-i", ...workerInvocation(');
    expect(manager).toContain('const SYSTEMD_RUN_BIN = "/usr/bin/systemd-run"');
    expect(manager).toContain('const SYSTEMCTL_BIN = "/usr/bin/systemctl"');
    expect(manager).toContain("stopLaunch(active.launch)");
    expect(manager).not.toContain("new Worker(");
    expect(manager).not.toContain("fallback");
    expect(server).toContain("ingestUsageOffThread(");
    expect(server).not.toContain("await ingestOnce(");
  });

  test("each production tokenomics job is a bounded hardened transient unit", async () => {
    const manager = await source("src/tokenomics-worker.ts");
    const requiredArguments = [
      '"--user"',
      '"--quiet"',
      '"--pipe"',
      '"--wait"',
      '"--collect"',
      '"--slice=sigmashake-tokens.slice"',
      '"--service-type=exec"',
      '"--property=BindsTo=sigmashake-tokens.service"',
      '"--property=After=sigmashake-tokens.service"',
      '"--property=RuntimeMaxSec=11min"',
      '"--property=MemoryHigh=96M"',
      '"--property=MemoryMax=128M"',
      '"--property=CPUQuota=35%"',
      '"--property=CPUWeight=100"',
    ];
    for (const argument of requiredArguments) expect(manager).toContain(argument);
    expect(manager).not.toContain("new Worker(");
    expect(manager).not.toContain("fallback");
  });

  test("serialized transients plus HTTP owner cannot overcommit the 512M slice", async () => {
    const service = activeDirectives(await source("systemd/sigmashake-tokens.service"));
    const ingest = await source("src/usage-ingest-worker.ts");
    const aggregation = await source("src/usage-aggregation-worker.ts");
    const tokenomics = await source("src/tokenomics-worker.ts");
    const server = await source("src/server.ts");
    const parsePropertyMax = (text: string): number => {
      const match = text.match(/"--property=MemoryMax=(\d+)M"/);
      expect(match).not.toBeNull();
      return Number(match![1]);
    };
    const httpMaxLine = service.find((line) => line.startsWith("MemoryMax="));
    expect(httpMaxLine).toBe("MemoryMax=128M");
    const httpMax = 128;
    const ingestMax = parsePropertyMax(ingest);
    const aggregationMax = parsePropertyMax(aggregation);
    const tokenomicsMax = parsePropertyMax(tokenomics);
    // Poll path runs at most one transient at a time (ingest → agg → tokenomics).
    expect(server).toContain("pollChain");
    expect(httpMax + Math.max(ingestMax, aggregationMax, tokenomicsMax)).toBeLessThanOrEqual(512);
    // Even HTTP + largest worker + one accidental sibling must fit the slice.
    expect(httpMax + ingestMax + Math.max(aggregationMax, tokenomicsMax)).toBeLessThanOrEqual(512);
  });

  test("the installer atomically installs and enables both unit templates", async () => {
    const installer = await source("scripts/install-systemd.sh");

    expect(installer).toContain('SLICE_SRC="$' + '{PROJECT_DIR}/systemd/sigmashake-tokens.slice"');
    expect(installer).toContain('SLICE_DST="$' + '{UNIT_DIR}/sigmashake-tokens.slice"');
    expect(installer).toContain('mv -f -- "$' + '{SLICE_TMP}" "$' + '{SLICE_DST}"');
    expect(installer).toContain('systemctl --user enable "${SLICE_UNIT}" "${SERVICE_UNIT}"');
  });
});
