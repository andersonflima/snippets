#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pipeline } from "node:stream/promises";

function die(message) {
  console.error(`[github-zip-download-extract] erro: ${message}`);
  process.exit(1);
}

function ensureOkResponse(response, url) {
  if (response.ok) {
    return;
  }
  die(`falha no download (${response.status}) para ${url}`);
}

function extractZip(zipPath, extractRoot) {
  const unzipResult = spawnSync("unzip", ["-q", zipPath, "-d", extractRoot], {
    stdio: "pipe",
    encoding: "utf8",
  });
  if (unzipResult.status !== 0) {
    const reason = (unzipResult.stderr || unzipResult.stdout || "").trim();
    die(`nao foi possivel extrair zip com unzip: ${reason || "erro desconhecido"}`);
  }
}

async function main() {
  const [url, zipPath, extractRoot, destination] = process.argv.slice(2);
  if (!url || !zipPath || !extractRoot || !destination) {
    die("uso: node scripts/github_zip_download_extract.mjs <url> <zipPath> <extractRoot> <destination>");
  }

  fs.mkdirSync(path.dirname(zipPath), { recursive: true });
  fs.mkdirSync(extractRoot, { recursive: true });

  const headers = { "User-Agent": "nvim-zip-bootstrap/1.0" };
  const token = (process.env.GITHUB_TOKEN || "").trim();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(url, { headers });
  ensureOkResponse(response, url);
  if (!response.body) {
    die(`resposta sem body para ${url}`);
  }

  await pipeline(response.body, fs.createWriteStream(zipPath));
  extractZip(zipPath, extractRoot);

  const entries = fs
    .readdirSync(extractRoot, { withFileTypes: true })
    .filter((entry) => !entry.name.startsWith(".__"))
    .map((entry) => path.join(extractRoot, entry.name));
  const dirs = entries.filter((entryPath) => fs.statSync(entryPath).isDirectory());
  if (dirs.length !== 1) {
    die(`arquivo zip com formato inesperado: ${url}`);
  }

  const source = dirs[0];
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.rmSync(destination, { recursive: true, force: true });
  fs.renameSync(source, destination);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  die(message || os.constants.errno.EIO);
});
