#!/usr/bin/env node

"use strict";

const axios = require("axios");
const tar = require("tar");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { pipeline } = require("stream/promises");

const DIST_DIR = path.join(__dirname, "..", "dist");
const REPO = "NDMyers/marrow";
const BINARY_NAME = process.platform === "win32" ? "marrow.exe" : "marrow";
const CHECKSUM_FILENAME = "checksums.sha256";

function getTargetTriple() {
  const platform = process.platform;
  const arch = process.arch;

  // M-16 FIX: Only map targets that actually have published release artifacts.
  const matrix = {
    darwin: {
      arm64: "aarch64-apple-darwin",
      x64: "x86_64-apple-darwin",
    },
    linux: {
      x64: "x86_64-unknown-linux-gnu",
    },
    win32: {
      x64: "x86_64-pc-windows-msvc",
    },
  };

  const platformTargets = matrix[platform];
  if (!platformTargets) {
    console.error(`[marrow] Unsupported platform: ${platform}`);
    console.error("[marrow] Supported platforms: macOS (arm64, x64), Linux (x64), Windows (x64)");
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

/**
 * Download and parse the checksums file. Returns a Map<filename, sha256>.
 * Fails closed: throws if the file is missing or malformed.
 */
async function downloadChecksums(baseUrl) {
  const url = `${baseUrl}/${CHECKSUM_FILENAME}`;
  console.log(`[marrow] Downloading checksums from: ${url}`);

  let response;
  try {
    response = await axios.get(url, {
      responseType: "text",
      timeout: 30_000,
      maxRedirects: 5,
    });
  } catch (err) {
    if (err.response?.status === 404) {
      throw new Error(`Checksum file not found: ${url}`);
    }
    throw new Error(`Failed to download checksums: ${err.message}`);
  }

  const checksums = new Map();
  const lines = response.data.trim().split("\n");
  for (const line of lines) {
    // Format: <sha256_hash>  <filename> (two spaces between hash and filename)
    const match = line.match(/^([a-fA-F0-9]{64})\s+(.+)$/);
    if (!match) {
      throw new Error(`Malformed checksum line: ${line}`);
    }
    checksums.set(match[2], match[1].toLowerCase());
  }

  if (checksums.size === 0) {
    throw new Error("Checksum file is empty");
  }

  return checksums;
}

/**
 * Compute SHA256 hash of a file.
 */
async function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const stream = fs.createReadStream(filePath);
    stream.on("error", reject);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

/**
 * Verify archive SHA256 against expected checksum. Fails closed on mismatch.
 */
async function verifyChecksum(archivePath, archiveName, checksums) {
  const expected = checksums.get(archiveName);
  if (!expected) {
    throw new Error(`No checksum found for ${archiveName}`);
  }

  console.log(`[marrow] Verifying checksum for ${archiveName}...`);
  const actual = await hashFile(archivePath);

  if (actual !== expected) {
    throw new Error(
      `Checksum mismatch for ${archiveName}:\n` +
      `  Expected: ${expected}\n` +
      `  Actual:   ${actual}\n` +
      `Archive may be corrupted or tampered with.`
    );
  }

  console.log(`[marrow] Checksum verified: ${actual}`);
}

/**
 * Hardened tar extraction.
 * Rejects:
 *   - Path traversal (.. components, absolute paths)
 *   - Symlinks and hardlinks
 *   - Ambiguous entries (multiple files matching expected binary prefix)
 * Extracts only the exact expected binary.
 */
async function extractSecurely(archivePath, destDir, expectedBinaryName) {
  const entries = [];

  // First pass: validate all entries and find the binary
  await tar.list({
    file: archivePath,
    onentry: (entry) => {
      const entryPath = entry.path;
      const basename = path.basename(entryPath);

      // Reject symlinks and hardlinks
      if (entry.type === "SymbolicLink" || entry.type === "Link") {
        throw new Error(
          `Security violation: archive contains ${entry.type.toLowerCase()}: ${entryPath}`
        );
      }

      // Reject path traversal
      if (entryPath.includes("..") || path.isAbsolute(entryPath)) {
        throw new Error(
          `Security violation: path traversal detected: ${entryPath}`
        );
      }

      // Reject entries that would escape the destination
      const resolvedPath = path.resolve(destDir, entryPath);
      if (!resolvedPath.startsWith(path.resolve(destDir) + path.sep) && resolvedPath !== path.resolve(destDir)) {
        throw new Error(
          `Security violation: entry would escape destination: ${entryPath}`
        );
      }

      if (basename === expectedBinaryName && entry.type === "File") {
        entries.push(entryPath);
      }
    },
  });

  // Reject ambiguous archives (multiple matching entries)
  if (entries.length === 0) {
    throw new Error(
      `Binary not found in archive: expected ${expectedBinaryName}`
    );
  }
  if (entries.length > 1) {
    throw new Error(
      `Ambiguous archive: multiple entries match ${expectedBinaryName}: ${entries.join(", ")}`
    );
  }

  const binaryEntryPath = entries[0];
  console.log(`[marrow] Extracting: ${binaryEntryPath}`);

  // Second pass: extract only the validated binary entry
  await tar.extract({
    file: archivePath,
    cwd: destDir,
    filter: (entryPath, entry) => {
      // Only extract the exact binary we validated
      if (entryPath !== binaryEntryPath) {
        return false;
      }
      // Final safety check: reject non-file types
      if (entry.type !== "File") {
        throw new Error(
          `Security violation: expected file, got ${entry.type}: ${entryPath}`
        );
      }
      return true;
    },
    // Additional hardening: strip leading path components to extract flat
    strip: binaryEntryPath.split("/").length - 1,
  });
}

async function main() {
  const version = require("../package.json").version;
  const target = getTargetTriple();
  const archiveName = `marrow-${target}.tar.gz`;
  const baseUrl = `https://github.com/${REPO}/releases/download/v${version}`;
  const url = `${baseUrl}/${archiveName}`;
  const archivePath = path.join(DIST_DIR, archiveName);

  fs.mkdirSync(DIST_DIR, { recursive: true });

  // Download and verify checksums before extraction
  const checksums = await downloadChecksums(baseUrl);
  await download(url, archivePath);
  await verifyChecksum(archivePath, archiveName, checksums);

  const expectedBinaryName = process.platform === "win32" ? "marrow.exe" : "marrow";
  console.log(`[marrow] Extracting to: ${DIST_DIR}`);
  await extractSecurely(archivePath, DIST_DIR, expectedBinaryName);

  fs.unlinkSync(archivePath);

  const binaryPath = path.join(DIST_DIR, BINARY_NAME);

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
