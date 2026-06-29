#!/usr/bin/env node
import { createHash } from "node:crypto";
import { copyFile, mkdir, readdir, readFile, rename, stat } from "node:fs/promises";
import path from "node:path";

const repoRoot = process.cwd();
const sourceRoot = repoRoot;
const targetRoot = "/root/.hermes/plugins/opencode-hermes-commands";
const backupRoot = "/root/backups/opencode-hermes-commands";
const dryRun = process.argv.includes("--dry-run");

const requiredFiles = [
  "__init__.py",
  "README.md",
  "handler.py",
  "opencode_bridge.py",
  "opencode-hermes-commands.js",
  "opencode-hermes-commands.ts",
  "plugin.yaml",
  "LICENSE",
  ".gitignore",
];

const skippedNames = new Set(["state.db", "state.db-shm", "state.db-wal", "errors.log"]);
const skippedDirs = new Set(["__pycache__", ".slim", ".git", "scripts"]);

async function listFiles(dir, prefix = "") {
  const entries = await readdir(dir, { withFileTypes: true });
  const out = [];
  for (const entry of entries) {
    if (skippedNames.has(entry.name)) continue;
    const rel = path.join(prefix, entry.name);
    if (entry.isDirectory()) {
      if (skippedDirs.has(entry.name)) continue;
      out.push(...(await listFiles(path.join(dir, entry.name), rel)));
    } else {
      out.push(rel);
    }
  }
  return out;
}

async function exists(filePath) {
  try {
    await stat(filePath);
    return true;
  } catch {
    return false;
  }
}

async function sha256(filePath) {
  const data = await readFile(filePath);
  return createHash("sha256").update(data).digest("hex");
}

function backupPathFor(relPath) {
  return path.join(backupRoot, new Date().toISOString().replace(/[:.]/g, "-"), relPath);
}

async function verifyRequiredSources(files) {
  for (const required of requiredFiles) {
    if (!files.includes(required)) {
      throw new Error(`Missing required source file: ${required}`);
    }
  }
}

async function backupIfNeeded(relPath) {
  const dest = path.join(targetRoot, relPath);
  if (!(await exists(dest))) return;
  const backup = backupPathFor(relPath);
  if (dryRun) {
    console.log(`backup ${dest} -> ${backup}`);
    return;
  }
  await mkdir(path.dirname(backup), { recursive: true });
  await copyFile(dest, backup);
}

async function atomicCopy(relPath) {
  const src = path.join(sourceRoot, relPath);
  const dest = path.join(targetRoot, relPath);
  const tmp = `${dest}.tmp-${process.pid}`;
  if (dryRun) {
    console.log(`copy ${src} -> ${dest}`);
    return;
  }
  await mkdir(path.dirname(dest), { recursive: true });
  await copyFile(src, tmp);
  await rename(tmp, dest);
}

async function verifyDestinations(files) {
  for (const relPath of files) {
    const src = path.join(sourceRoot, relPath);
    const dest = path.join(targetRoot, relPath);
    if (!(await exists(dest))) throw new Error(`Missing deployed file: ${dest}`);
    const [srcHash, destHash] = await Promise.all([sha256(src), sha256(dest)]);
    console.log(`sha256 ${relPath} src=${srcHash} dest=${destHash}`);
    if (srcHash !== destHash) {
      throw new Error(`Checksum mismatch for ${relPath}`);
    }
  }
}

async function main() {
  const files = (await listFiles(sourceRoot)).sort();
  await verifyRequiredSources(files);
  if (files.some((file) => file.includes("state.db"))) {
    throw new Error("Refusing to touch state.db* files");
  }

  console.log(dryRun ? "dry-run" : "deploy");
  console.log(`source: ${sourceRoot}`);
  console.log(`target: ${targetRoot}`);

  for (const relPath of files) {
    console.log(`source-checksum ${relPath} ${await sha256(path.join(sourceRoot, relPath))}`);
  }

  if (!dryRun) await mkdir(backupRoot, { recursive: true });
  for (const relPath of files) {
    await backupIfNeeded(relPath);
    await atomicCopy(relPath);
  }

  if (!dryRun) {
    await verifyDestinations(files);
    console.log(`deployed ${files.length} files`);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
