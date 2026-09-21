#!/usr/bin/env node
const { spawnSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const PLATFORMS = {
  "linux-x64": { pkg: "@x7ssss/dbdrain-linux-x64", bin: "dbdrain" },
  "darwin-arm64": { pkg: "@x7ssss/dbdrain-darwin-arm64", bin: "dbdrain" },
  "darwin-x64": { pkg: "@x7ssss/dbdrain-darwin-x64", bin: "dbdrain" },
  "win32-x64": { pkg: "@x7ssss/dbdrain-win32-x64", bin: "dbdrain.exe" },
};

const key = `${process.platform}-${process.arch}`;
const target = PLATFORMS[key];

if (!target) {
  console.error(`[dbdrain] Unsupported platform/architecture: ${process.platform} ${process.arch}`);
  console.error(`Supported platforms: linux (x64), darwin (arm64, x64), win32 (x64)`);
  process.exit(1);
}

function resolveBinary() {
  // 1. Try resolving from node_modules (production npm install)
  try {
    const pkgPath = require.resolve(`${target.pkg}/package.json`);
    const binPath = path.join(path.dirname(pkgPath), "bin", target.bin);
    if (fs.existsSync(binPath)) {
      return binPath;
    }
  } catch {}

  // 2. Try resolving relative to repository monorepo structure (development/local)
  const localRepoPath = path.resolve(__dirname, "../../platforms", key, "bin", target.bin);
  if (fs.existsSync(localRepoPath)) {
    return localRepoPath;
  }

  // 3. Try dist folder if running directly from repo root
  const distName =
    process.platform === "win32"
      ? "dbdrain-windows-amd64.exe"
      : process.platform === "darwin"
      ? process.arch === "arm64"
        ? "dbdrain-darwin-arm64"
        : "dbdrain-darwin-amd64"
      : "dbdrain-linux-amd64";

  const distPath = path.resolve(__dirname, "../../../dist", distName);
  if (fs.existsSync(distPath)) {
    return distPath;
  }

  return null;
}

const binPath = resolveBinary();

if (!binPath) {
  console.error(`[dbdrain] Could not find native binary for ${key}.`);
  console.error(`Please ensure the platform package ${target.pkg} is installed.`);
  console.error(`Run: npm install --save-optional ${target.pkg}`);
  process.exit(1);
}

const result = spawnSync(binPath, process.argv.slice(2), {
  stdio: "inherit",
  windowsVerbatimArguments: true,
  windowsHide: true,
});

if (result.error) {
  console.error(`[dbdrain] Failed to execute ${binPath}:`, result.error.message);
  process.exit(1);
}

if (result.signal) {
  process.kill(process.pid, result.signal);
} else {
  process.exit(result.status ?? 0);
}
