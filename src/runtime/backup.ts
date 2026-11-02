import { backup, DatabaseSync } from "node:sqlite";
import {
  mkdir,
  open,
  readFile,
  readdir,
  writeFile,
  copyFile,
  realpath,
  lstat,
  rm,
  unlink,
} from "node:fs/promises";
import { join, resolve, relative, isAbsolute, dirname } from "node:path";
import { createHash } from "node:crypto";
import fsExt from "fs-ext";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import type { Cursor, Storage } from "@earendil-works/pi-durable";
import {
  canonicalJson,
  digest,
  type RuntimeMetadata,
} from "../domain/model.js";
import { operationDigest, type PreparedReceipt } from "../environments/types.js";
import type { EnvironmentOptions } from "../services/jobs.js";
import { preflightDocuments, inspectDocuments } from "./migrations.js";
export interface EnvironmentDependency {
  kind: "local" | "remote" | "custom";
  directory?: string;
  repository?: string;
  endpointId?: string;
}
interface FileEntry {
  path: string;
  sha256: string;
  bytes: number;
}
interface ExternalFile extends FileEntry {
  source: string;
}
export interface BackupManifest {
  format: 1;
  createdAt: number;
  sourceDirectory: string;
  metadata: RuntimeMetadata;
  files: FileEntry[];
  dependency?: EnvironmentDependency;
  external: ExternalFile[];
  workspaces: { cwd: string; repository: string; markerSha256: string }[];
  releasedWorkspaces?: string[];
  inspectionOnly: boolean;
}
const hash = (bytes: Buffer) =>
  createHash("sha256").update(bytes).digest("hex");
async function paths(directory: string, prefix = ""): Promise<string[]> {
  const result: string[] = [];
  for (const item of await readdir(join(directory, prefix), {
    withFileTypes: true,
  })) {
    const path = join(prefix, item.name);
    if (item.isSymbolicLink())
      throw Error("Backup refuses symlink dependencies");
    if (item.isDirectory()) result.push(...(await paths(directory, path)));
    else if (item.isFile()) result.push(path);
    else throw Error("Backup refuses non-file dependency");
  }
  return result.sort();
}
async function domainRecords(
  storage: Storage,
): Promise<{ kind: string; key?: string; value: any }[]> {
  let cursor: Cursor | undefined;
  const result = [];
  do {
    const page = await storage.scanDocuments(
      { scope: { kind: "session" }, at: "current" },
      200,
      cursor,
      context,
    );
    for (const record of page.items) {
      const d = await storage.document(record.id, "current", context);
      if (d?.value.present)
        result.push({
          kind: record.kind,
          key: record.key,
          value: d.value.value,
        });
    }
    cursor = page.next;
  } while (cursor);
  return result;
}
async function recordFile(root: string, path: string): Promise<FileEntry> {
  const stat = await lstat(join(root, path));
  if (!stat.isFile() || stat.isSymbolicLink())
    throw Error("Backup file is not regular");
  const bytes = await readFile(join(root, path));
  return { path, sha256: hash(bytes), bytes: bytes.length };
}
async function captureDependencies(manifest: BackupManifest, target: string) {
  const dep = manifest.dependency;
  if (!dep) return;
  if (dep.kind !== "local") manifest.inspectionOnly = true;
  if (!dep.directory) return;
  const source = await realpath(dep.directory);
  const folders = await readdir(source, { withFileTypes: true });
  for (const folder of folders) {
    if (folder.isSymbolicLink())
      throw Error("Backup refuses symlink dependencies");
    if (folder.name === "worktrees") continue;
    const entries = folder.isDirectory()
      ? await paths(source, folder.name)
      : [folder.name];
    for (const path of entries) {
      if (path.endsWith(".tmp") || path.endsWith(".lock")) continue;
      const bytes = await readFile(join(source, path));
      if (path.endsWith(".json")) {
        const value = JSON.parse(bytes.toString("utf8"));
        if (value.checksum) {
          const { checksum, ...record } = value;
          const checked = "value" in value ? value.value : record;
          if (checksum !== digest(checked))
            throw Error("Endpoint ledger integrity failure");
        }
        if (value.request && value.receipt) {
          if (
            value.request.operationId !== value.receipt.operationId ||
            value.request.payloadDigest !== value.receipt.payloadDigest
          )
            throw Error("Endpoint receipt identity conflict");
          if (
            ["running", "cancelPending", "unknown"].includes(
              value.receipt.status,
            )
          )
            throw Error(
              "Unsafe active or unknown external state; reconcile endpoint before backup",
            );
        }
      }
      const destination = join("dependencies", path);
      await mkdir(dirname(join(target, destination)), {
        recursive: true,
        mode: 0o700,
      });
      await writeFile(join(target, destination), bytes, {
        flag: "wx",
        mode: 0o600,
      });
      manifest.external.push({
        path: destination,
        source: join(source, path),
        sha256: hash(bytes),
        bytes: bytes.length,
      });
    }
  }
  if (dep.kind === "local") await captureLocalWorkspaces(manifest, target, source);
}

/** Immutable descriptors survive release. Only the latest lease for a physical
 * workspace may establish its absence, with an exact, checksummed release receipt. */
async function captureLocalWorkspaces(manifest: BackupManifest, target: string, source: string) {
  const repository = await realpath(manifest.dependency!.repository!);
  const descriptors = new Map<string, PreparedReceipt>();
  const operations: any[] = [];
  const pointers = new Map<string, string>();
  for (const file of manifest.external) {
    const path = relative("dependencies", file.path);
    if (!/^(environments|operations|ticket-workspaces)\//.test(path)) continue;
    const value = JSON.parse(await readFile(join(target, file.path), "utf8"));
    if (path.startsWith("environments/")) {
      const descriptor = value as PreparedReceipt;
      if (descriptor.status !== "prepared" ||
          descriptor.environmentId !== digest({ identity: descriptor.identity, repository }) ||
          path !== join("environments", `${descriptor.environmentId}.json`) ||
          (descriptor.repository !== undefined && descriptor.repository !== repository) ||
          descriptor.cwd !== join(source, "worktrees", descriptor.workspaceId ?? descriptor.environmentId))
        throw Error("Invalid historical workspace descriptor");
      descriptors.set(descriptor.environmentId, descriptor);
    } else if (path.startsWith("operations/")) {
      const { checksum, ...record } = value;
      const { request, receipt } = record;
      if (!request || !receipt || checksum !== digest(record) ||
          path !== join("operations", `${digest(request.operationId)}.json`) ||
          request.operationId !== receipt.operationId || request.payloadDigest !== receipt.payloadDigest ||
          request.payloadDigest !== operationDigest(request.identity, request.payload) ||
          canonicalJson(request.identity) !== canonicalJson(receipt.identity))
        throw Error("Endpoint receipt identity/integrity conflict");
      operations.push(record);
    } else {
      if (value.checksum !== digest(value.value) || typeof value.value?.environmentId !== "string")
        throw Error("Invalid current workspace lease pointer");
      pointers.set(path, value.value.environmentId);
    }
  }
  for (const [path, id] of pointers) {
    const descriptor = descriptors.get(id);
    if (!descriptor || path !== join("ticket-workspaces", `${digest(digest({ ticketId: descriptor.identity.ticketId, repository }))}.json`))
      throw Error("Invalid current workspace lease pointer");
  }
  const groups = new Map<string, PreparedReceipt[]>();
  for (const descriptor of descriptors.values()) {
    const group = groups.get(descriptor.cwd) ?? [];
    group.push(descriptor); groups.set(descriptor.cwd, group);
  }
  for (const [cwd, leases] of groups) {
    leases.sort((a, b) => b.identity.epoch - a.identity.epoch);
    const latest = leases[0];
    if (leases.some(d => d.identity.ticketId !== latest.identity.ticketId) ||
        leases.slice(1).some(d => d.identity.epoch >= latest.identity.epoch))
      throw Error("Ambiguous historical workspace leases");
    const released = operations.some(({ request, receipt }) =>
      request.payload.kind === "release" && request.payload.environmentId === latest.environmentId &&
      canonicalJson(request.identity) === canonicalJson(latest.identity) && receipt.status === "released");
    if (released) {
      if ([...pointers.values()].some(id => leases.some(d => d.environmentId === id)))
        throw Error("Released workspace retains a current lease pointer");
      await requireAbsentWorkspace(cwd);
      (manifest.releasedWorkspaces ??= []).push(cwd);
      continue;
    }
    if (await realpath(cwd) !== cwd) throw Error("Workspace identity changed");
    const marker = await readFile(join(cwd, ".super-agent-environment.json"));
    if (canonicalJson(JSON.parse(marker.toString("utf8"))) !== canonicalJson(latest))
      throw Error("Current workspace lease marker conflict");
    manifest.workspaces.push({ cwd, repository, markerSha256: hash(marker) });
  }
}
async function requireAbsentWorkspace(cwd: string): Promise<void> {
  try { await lstat(cwd); }
  catch (error: any) { if (error.code === "ENOENT") return; throw error; }
  throw Error("Released workspace path reappeared; reconciliation required");
}

/** Stop the service first. Takes the same permanent advisory writer lock as runtime startup. */
export async function backupRuntime(
  directory: string,
  destination: string,
): Promise<BackupManifest> {
  directory = await realpath(directory);
  destination = resolve(destination);
  await lstat(join(directory, "session.sqlite"));
  const lock = await open(join(directory, ".writer.lock"), "a+", 0o600);
  let created = false;
  try {
    try {
      fsExt.flockSync(lock.fd, "exnb");
    } catch {
      throw Error("Primary writer lock is held; stop service before backup");
    }
    const storage = await openNodeSqliteStorage(
      join(directory, "session.sqlite"),
    );
    let records: Awaited<ReturnType<typeof domainRecords>>;
    const documentValues: unknown[] = [];
    try {
      await preflightDocuments(storage);
      records = await domainRecords(storage);
      await inspectDocuments(storage, (d) => documentValues.push(d.value));
    } finally {
      await storage.close(context);
    }
    const metadata = records.find(
      (r) => r.kind === "sa.metadata" && r.key === "runtime",
    )?.value as RuntimeMetadata;
    if (!metadata || metadata.schemaVersion !== 1)
      throw Error("Unsupported or missing runtime metadata");
    const dependency = records.find(
      (r) => r.kind === "sa.metadata" && r.key === "environment",
    )?.value as EnvironmentDependency | undefined;
    if (
      !dependency &&
      records.some((r) => r.kind === "sa.job" && r.value.payloadDigest)
    )
      throw Error(
        "Missing endpoint dependency metadata; reconcile original endpoint first",
      );
    for (const source of [directory, dependency?.directory].filter(
      (p): p is string => !!p,
    )) {
      const rel = relative(resolve(source), destination);
      if (!rel || (!rel.startsWith("..") && !isAbsolute(rel)))
        throw Error(
          "Backup destination must be outside primary and endpoint directories",
        );
    }
    await mkdir(destination, { mode: 0o700 });
    created = true;
    const manifest: BackupManifest = {
      format: 1,
      createdAt: Date.now(),
      sourceDirectory: directory,
      metadata,
      files: [],
      external: [],
      workspaces: [],
      inspectionOnly: false,
      ...(dependency ? { dependency } : {}),
    };
    const database = new DatabaseSync(join(directory, "session.sqlite"), {
      readOnly: true,
    });
    try {
      await backup(database, join(destination, "session.sqlite"));
    } finally {
      database.close();
    }
    manifest.files.push(await recordFile(destination, "session.sqlite"));
    const references = new Set<string>();
    function artifacts(value: any): void {
      if (!value || typeof value !== "object") return;
      for (const [key, item] of Object.entries(value)) {
        if (
          /^(bodyArtifact|goalArtifact|artifact|artifactId)$/.test(key) &&
          typeof item === "string" &&
          /^[a-f0-9]{64}$/.test(item)
        )
          references.add(item);
        else artifacts(item);
      }
    }
    documentValues.forEach(artifacts);
    const names: string[] = await readdir(join(directory, "artifacts")).catch(
      (e: any) => {
        if (e.code === "ENOENT") return [] as string[];
        throw e;
      },
    );
    for (const id of references)
      if (!names.includes(id)) throw Error("Referenced artifact is missing");
    for (const id of names.filter((n) => /^[a-f0-9]{64}$/.test(n))) {
      const bytes = await readFile(join(directory, "artifacts", id));
      if (hash(bytes) !== id) throw Error("Artifact integrity failure");
      await mkdir(join(destination, "artifacts"), {
        recursive: true,
        mode: 0o700,
      });
      await writeFile(join(destination, "artifacts", id), bytes, {
        flag: "wx",
        mode: 0o600,
      });
      manifest.files.push(await recordFile(destination, join("artifacts", id)));
    }
    await captureDependencies(manifest, destination);
    await validateDependencies(manifest);
    await writeFile(
      join(destination, "manifest.json"),
      canonicalJson({ manifest, checksum: digest(manifest) }),
      { flag: "wx", mode: 0o600 },
    );
    return manifest;
  } catch (error) {
    if (created) await rm(destination, { recursive: true, force: true });
    throw error;
  } finally {
    await lock.close();
  }
}
function safePath(root: string, path: string): string {
  if (
    typeof path !== "string" ||
    isAbsolute(path) ||
    relative(root, resolve(root, path)).startsWith("..")
  )
    throw Error("Invalid backup path");
  return join(root, path);
}
export async function verifyBackup(directory: string): Promise<BackupManifest> {
  const { manifest, checksum } = JSON.parse(
    await readFile(join(directory, "manifest.json"), "utf8"),
  ) as { manifest: BackupManifest; checksum: string };
  if (
    checksum !== digest(manifest) ||
    manifest.format !== 1 ||
    manifest.metadata?.schemaVersion !== 1 ||
    !manifest.files.some((f) => f.path === "session.sqlite")
  )
    throw Error("Backup manifest integrity/version failure");
  for (const file of [...manifest.files, ...manifest.external]) {
    const bytes = await readFile(safePath(directory, file.path));
    if (hash(bytes) !== file.sha256 || bytes.length !== file.bytes)
      throw Error(`Backup integrity failure: ${file.path}`);
  }
  return manifest;
}
async function validateDependencies(manifest: BackupManifest): Promise<void> {
  for (const cwd of manifest.releasedWorkspaces ?? []) await requireAbsentWorkspace(cwd);
  if (manifest.dependency?.directory) {
    const directory = await realpath(manifest.dependency.directory);
    const current: string[] = [];
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      if (entry.isSymbolicLink())
        throw Error("Original endpoint dependency path changed");
      if (entry.name === "worktrees") continue;
      const entries = entry.isDirectory()
        ? await paths(directory, entry.name)
        : [entry.name];
      for (const path of entries)
        if (!path.endsWith(".tmp") && !path.endsWith(".lock"))
          current.push(join(directory, path));
    }
    if (
      canonicalJson(current.sort()) !==
      canonicalJson(manifest.external.map((file) => file.source).sort())
    )
      throw Error(
        "Missing original endpoint dependency or inventory changed; reconciliation required",
      );
  }

  for (const file of manifest.external) {
    let bytes: Buffer;
    try {
      bytes = await readFile(file.source);
    } catch {
      throw Error(`Missing original endpoint dependency: ${file.source}`);
    }
    if (hash(bytes) !== file.sha256)
      throw Error(
        "Original endpoint dependency changed; reconciliation required",
      );
  }
  for (const workspace of manifest.workspaces) {
    if (
      (await realpath(workspace.cwd)) !== workspace.cwd ||
      (await realpath(workspace.repository)) !== workspace.repository ||
      hash(
        await readFile(join(workspace.cwd, ".super-agent-environment.json")),
      ) !== workspace.markerSha256
    )
      throw Error("Workspace identity changed; reconciliation required");
  }
}
/** Never restores/rewinds a broker, process or Git worktree. Original external dependencies stay authoritative. */
export async function restoreRuntime(
  backupDirectory: string,
  destination: string,
): Promise<BackupManifest> {
  const manifest = await verifyBackup(backupDirectory);
  destination = resolve(destination);
  await mkdir(destination, { mode: 0o700 });
  try {
    for (const file of manifest.files) {
      const target = safePath(destination, file.path);
      await mkdir(dirname(target), { recursive: true, mode: 0o700 });
      await copyFile(safePath(backupDirectory, file.path), target);
    }
    await writeFile(
      join(destination, ".restore.json"),
      canonicalJson({ manifest, checksum: digest(manifest) }),
      { flag: "wx", mode: 0o600 },
    );
    return manifest;
  } catch (error) {
    await rm(destination, { recursive: true, force: true });
    throw error;
  }
}
/** Invoked before Harness initialization; no missing ledger can become authoritative absence. */
export async function preflightRestore(
  directory: string,
  environment?: EnvironmentOptions,
): Promise<boolean> {
  let value: any;
  try {
    value = JSON.parse(
      await readFile(join(directory, ".restore.json"), "utf8"),
    );
  } catch (error: any) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
  const manifest = value.manifest as BackupManifest;
  if (value.checksum !== digest(manifest) || manifest.format !== 1)
    throw Error("Restore manifest integrity failure");
  if (manifest.inspectionOnly)
    throw Error(
      "Restore is inspection-only: independent remote/custom endpoint reconciliation required",
    );
  const dep = manifest.dependency;
  if (dep) {
    if (
      dep.kind !== "local" ||
      !environment ||
      "broker" in environment ||
      !environment.directory ||
      (await realpath(environment.directory)) !== dep.directory ||
      (await realpath(environment.repository)) !== dep.repository
    )
      throw Error(
        "Restore requires explicit original local broker/repository paths",
      );
    await validateDependencies(manifest);
  }
  return true;
}
export async function finishRestore(directory: string) {
  await unlink(join(directory, ".restore.json"));
}
