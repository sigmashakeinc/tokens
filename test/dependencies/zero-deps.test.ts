// Dependencies: sigmashake-tokens is a zero-runtime-dep Bun service.
// This regression test catches accidental additions to the dependencies block.

import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

test("package.json has no runtime dependencies", () => {
  const pkg = JSON.parse(readFileSync(join(import.meta.dir, "../../package.json"), "utf8"));
  const deps = pkg.dependencies ?? {};
  expect(Object.keys(deps).length).toBe(0);
});
