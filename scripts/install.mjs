#!/usr/bin/env node
import { existsSync, lstatSync, mkdirSync, readlinkSync, symlinkSync, unlinkSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const hermesHome = process.env.HERMES_HOME || "/root/.hermes";
const dryRun = process.argv.includes("--dry-run");
const restart = process.argv.includes("--restart-gateway");

const links = [
  {
    label: "OpenCode plugin",
    target: `${repoRoot}/opencode-hermes-commands.js`,
    link: `${process.env.HOME || "/root"}/.config/opencode/plugins/opencode-hermes-commands.js`,
  },
  {
    label: "Bridge command",
    target: `${repoRoot}/opencode_bridge.py`,
    link: "/usr/local/bin/opencode_bridge.py",
  },
];

function log(message) {
  console.log(message);
}

function ensureSymlink({ label, target, link }) {
  if (!existsSync(target)) {
    throw new Error(`${label} target missing: ${target}`);
  }
  mkdirSync(dirname(link), { recursive: true });

  if (existsSync(link)) {
    const stat = lstatSync(link);
    if (stat.isSymbolicLink()) {
      const current = resolve(dirname(link), readlinkSync(link));
      if (current === target) {
        log(`✓ ${label} symlink already correct: ${link} -> ${target}`);
        return;
      }
      log(`${dryRun ? "would replace" : "replace"} ${label} symlink: ${link} -> ${current}`);
      if (!dryRun) unlinkSync(link);
    } else {
      throw new Error(`${label} path exists and is not a symlink: ${link}`);
    }
  }

  log(`${dryRun ? "would link" : "link"} ${label}: ${link} -> ${target}`);
  if (!dryRun) symlinkSync(target, link);
}

function run(command, args, options = {}) {
  log(`${dryRun ? "would run" : "run"}: ${command} ${args.join(" ")}`);
  if (dryRun) return { status: 0, stdout: "", stderr: "" };
  const result = spawnSync(command, args, {
    text: true,
    encoding: "utf8",
    stdio: options.capture ? "pipe" : "inherit",
    env: { ...process.env, HERMES_HOME: hermesHome },
  });
  if (result.status !== 0 && !options.allowFailure) {
    throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}`);
  }
  return result;
}

function main() {
  log("Install opencode-hermes-commands as one package with two runtimes");
  log(`repo: ${repoRoot}`);
  log(`HERMES_HOME: ${hermesHome}`);

  for (const link of links) ensureSymlink(link);

  run("hermes", ["plugins", "enable", "opencode-hermes-commands"]);

  log("\nHealth check:");
  run("python3", [`${repoRoot}/opencode_bridge.py`, "/oc", "health"], { allowFailure: true });

  if (restart) {
    log("\nRestarting Hermes gateway because --restart-gateway was supplied.");
    run("hermes", ["gateway", "restart"]);
  } else {
    log("\nIf /oc was just enabled or health says restart required, run from a shell outside the gateway:");
    log(`HERMES_HOME=${hermesHome} hermes gateway restart`);
  }
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
