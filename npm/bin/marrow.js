#!/usr/bin/env node

"use strict";

const { spawnSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const BINARY_NAME = process.platform === "win32" ? "marrow.exe" : "marrow";
const binaryPath = path.resolve(__dirname, "..", "dist", BINARY_NAME);

if (!fs.existsSync(binaryPath)) {
  console.error("[marrow] Binary not found at:", binaryPath);
  console.error("[marrow] Try reinstalling: npm install -g @nickm-swe/marrow@alpha");
  process.exit(1);
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: "inherit",
  env: process.env,
});

if (result.error) {
  console.error("[marrow] Failed to launch binary:", result.error.message);
  process.exit(1);
}

process.exit(result.status ?? 1);
