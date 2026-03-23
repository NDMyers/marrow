#!/usr/bin/env node

"use strict";

const axios = require("axios");
const tar = require("tar");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");

const DIST_DIR = path.join(__dirname, "..", "dist");
const REPO = "NDMyers/marrow";
const BINARY_NAME = process.platform === "win32" ? "marrow.exe" : "marrow";

function getTargetTriple() {
  const platform = process.platform;
  const arch = process.arch;

  const matrix = {
    darwin: {
      arm64: "aarch64-apple-darwin",
      x64: "x86_64-apple-darwin",
    },
    linux: {
      arm64: "aarch64-unknown-linux-gnu",
      x64: "x86_64-unknown-linux-gnu",
    },
    win32: {
      arm64: "aarch64-pc-windows-msvc",
      x64: "x86_64-pc-windows-msvc",
    },
  };

  const platformTargets = matrix[platform];
  if (!platformTargets) {
    console.error(`[marrow] Unsupported platform: ${platform}`);
    console.error("[marrow] Please build from source: https://github.com/" + REPO);
    process.exit(1);
  }

  const target = platformTargets[arch];
  if (!target) {
    console.error(`[marrow] Unsupported architecture: ${arch} on ${platform}`);
    console.error("[marrow] Please build from source: https://github.com/" + REPO);
    process.exit(1);
  }

  return target;
}

async function download(url, destPath) {
  console.log(`[marrow] Downloading from: ${url}`);

  let response;
  try {
    response = await axios.get(url, {
      responseType: "stream",
      timeout: 60_000,
      maxRedirects: 5,
    });
  } catch (err) {
    if (err.response) {
      console.error(`[marrow] Download failed with HTTP ${err.response.status}: ${url}`);
    } else {
      console.error(`[marrow] Download failed: ${err.message}`);
    }
    throw err;
  }

  const writer = fs.createWriteStream(destPath);
  await pipeline(response.data, writer);
}

async function main() {
  const version = require("../package.json").version;
  const target = getTargetTriple();
  const archiveName = `marrow-${target}.tar.gz`;
  const url = `https://github.com/${REPO}/releases/download/v${version}/${archiveName}`;
  const archivePath = path.join(DIST_DIR, archiveName);

  fs.mkdirSync(DIST_DIR, { recursive: true });

  await download(url, archivePath);

  console.log(`[marrow] Extracting to: ${DIST_DIR}`);
  await tar.extract({
    file: archivePath,
    cwd: DIST_DIR,
    // Release archives ship the binary as marrow / marrow.exe (see .github/workflows/release.yml)
    filter: (filePath) => path.basename(filePath).startsWith("marrow"),
  });

  fs.unlinkSync(archivePath);

  const extractedBinaryName =
    process.platform === "win32" ? "marrow.exe" : "marrow";
  const extractedPath = path.join(DIST_DIR, extractedBinaryName);
  const binaryPath = path.join(DIST_DIR, BINARY_NAME);

  if (fs.existsSync(extractedPath) && extractedPath !== binaryPath) {
    fs.renameSync(extractedPath, binaryPath);
  }

  if (!fs.existsSync(binaryPath)) {
    console.error(`[marrow] Binary not found after extraction: ${binaryPath}`);
    console.error("[marrow] The release archive may have an unexpected layout.");
    process.exit(1);
  }

  fs.chmodSync(binaryPath, 0o755);
  console.log(`[marrow] Installed successfully: ${binaryPath}`);
}

main().catch((err) => {
  console.error("[marrow] Installation failed:", err.message);
  process.exit(1);
});
