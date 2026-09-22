#!/usr/bin/env node
const { spawnSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const PLATFORMS = {
  "linux-x64": { pkgs: ["@dbdrain/linux-x64", "@x7ssss/dbdrain-linux-x64"], bin: "dbdrain" },
  "linux-arm64": { pkgs: ["@dbdrain/linux-arm64", "@x7ssss/dbdrain-linux-arm64"], bin: "dbdrain" },
  "darwin-arm64": { pkgs: ["@dbdrain/darwin-arm64", "@x7ssss/dbdrain-darwin-arm64"], bin: "dbdrain" },
  "darwin-x64": { pkgs: ["@dbdrain/darwin-x64", "@x7ssss/dbdrain-darwin-x64"], bin: "dbdrain" },
  "win32-x64": { pkgs: ["@dbdrain/win32-x64", "@x7ssss/dbdrain-win32-x64"], bin: "dbdrain.exe" },
  "win32-arm64": { pkgs: ["@dbdrain/win32-arm64", "@x7ssss/dbdrain-win32-arm64"], bin: "dbdrain.exe" },
};

const key = `${process.platform}-${process.arch}`;
const target = PLATFORMS[key];

if (!target) {
  console.error(`[dbdrain] Unsupported platform/architecture: ${process.platform} ${process.arch}`);
  console.error(`Supported platforms: linux (x64, arm64), darwin (arm64, x64), win32 (x64, arm64)`);
  process.exit(1);
}

function resolveBinary() {
  // 1. Try resolving from node_modules (production npm install)
  for (const pkg of target.pkgs) {
    try {
      const pkgPath = require.resolve(`${pkg}/package.json`);
      const binPath = path.join(path.dirname(pkgPath), "bin", target.bin);
      if (fs.existsSync(binPath)) {
        return binPath;
      }
    } catch {}
  }

  // 2. Try resolving relative to repository monorepo structure (development/local)
  const localRepoPath = path.resolve(__dirname, "../../platforms", key, "bin", target.bin);
  if (fs.existsSync(localRepoPath)) {
    return localRepoPath;
  }
  const scopedRepoPath = path.resolve(__dirname, "../../platforms/@dbdrain", key, "bin", target.bin);
  if (fs.existsSync(scopedRepoPath)) {
    return scopedRepoPath;
  }

  // 3. Try dist folder if running directly from repo root
  const distName =
    process.platform === "win32"
      ? process.arch === "arm64"
        ? "dbdrain-windows-arm64.exe"
        : "dbdrain-windows-amd64.exe"
      : process.platform === "darwin"
      ? process.arch === "arm64"
        ? "dbdrain-darwin-arm64"
        : "dbdrain-darwin-amd64"
      : process.arch === "arm64"
      ? "dbdrain-linux-arm64"
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
  console.error(`Please ensure one of the platform packages is installed: ${target.pkgs.join(", ")}`);
  console.error(`Run: npm install --save-optional ${target.pkgs[0]}`);
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
