import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import pkg from "../../package.json" with { type: "json" };

const MAIN = resolve(import.meta.dir, "..", "..", "src", "main.ts");

function run(args: string[]): { code: number; out: string; err: string } {
  const result = Bun.spawnSync([process.execPath, MAIN, ...args], {
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" },
  });
  return {
    code: result.exitCode,
    out: new TextDecoder().decode(result.stdout),
    err: new TextDecoder().decode(result.stderr),
  };
}

describe("tokens entry point", () => {
  test("--version prints the package version", () => {
    const result = run(["--version"]);
    expect(result.code).toBe(0);
    expect(result.out.trim()).toBe(pkg.version);
  });

  test("--help lists serve and exits 0", () => {
    const result = run(["--help"]);
    expect(result.code).toBe(0);
    expect(result.out).toContain("tokens serve");
    expect(result.out).not.toContain("systemd");
  });

  test("no command prints help and exits 2", () => {
    const result = run([]);
    expect(result.code).toBe(2);
    expect(result.out).toContain("Usage:");
  });

  test("an unknown command exits 2", () => {
    const result = run(["nonsense"]);
    expect(result.code).toBe(2);
    expect(result.err).toContain("unknown command: nonsense");
  });

  test("an unknown worker kind exits 2", () => {
    const result = run(["__worker", "nope"]);
    expect(result.code).toBe(2);
    expect(result.err).toContain("unknown worker: nope");
  });
});
