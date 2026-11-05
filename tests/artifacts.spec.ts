import { afterEach, expect, it } from "vitest";
import { mkdtemp, mkdir, rm, writeFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { Artifacts } from "../src/runtime/artifacts.js";
const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(
    dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })),
  );
});
it("atomically repairs a truncated digest object and concurrent publishers only return complete content", async () => {
  const directory = await mkdtemp(join(tmpdir(), "sa-artifact-repair-"));
  dirs.push(directory);
  const artifacts = new Artifacts(directory);
  const body = "complete source body".repeat(100_000),
    id = createHash("sha256").update(body).digest("hex");
  await mkdir(artifacts.root);
  await writeFile(join(artifacts.root, id), body.slice(0, 13));
  const published = await Promise.all(
    Array.from({ length: 8 }, async () => {
      expect(await artifacts.put(body)).toBe(id);
      return artifacts.get(id);
    }),
  );
  expect(published).toEqual(Array(8).fill(body));
  expect(await readdir(artifacts.root)).toEqual([id]);
});
