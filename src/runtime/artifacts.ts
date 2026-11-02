import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

/** Content-addressed UTF-8 bodies. IO always runs before/outside domain commits. */
export class Artifacts {
  static readonly threshold = 16 * 1024;
  readonly root: string;
  constructor(directory: string) {
    this.root = join(directory, "artifacts");
  }
  async put(text: string): Promise<string> {
    const id = createHash("sha256").update(text).digest("hex");
    await mkdir(this.root, { recursive: true });
    try {
      await writeFile(join(this.root, id), text, {
        encoding: "utf8",
        flag: "wx",
        mode: 0o600,
      });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    return id;
  }
  async get(id: string): Promise<string> {
    if (!/^[a-f0-9]{64}$/.test(id))
      throw new Error("Invalid artifact reference");
    const text = await readFile(join(this.root, id), "utf8");
    if (createHash("sha256").update(text).digest("hex") !== id)
      throw new Error("Artifact digest mismatch");
    return text;
  }
  async spill(text: string): Promise<{ text: string; artifact?: string }> {
    return Buffer.byteLength(text) > Artifacts.threshold
      ? { text: text.slice(0, 1024), artifact: await this.put(text) }
      : { text };
  }
}
/** Conservative byte budget: each UTF-8 byte counts as one estimated token, not a tokenizer. */
export const estimateTokens = (text: string): number =>
  Buffer.byteLength(text, "utf8");
