#!/usr/bin/env node

import fs from "node:fs";
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toErrorMessage(error) {
  if (!(error instanceof Error)) {
    return String(error);
  }
  const cause = error.cause;
  const causeMessage =
    cause && typeof cause === "object"
      ? [cause.code, cause.errno, cause.syscall, cause.hostname, cause.message]
          .filter(Boolean)
          .join(" | ")
      : "";
  return causeMessage ? `${error.message} (${causeMessage})` : error.message;
}

function buildCandidateUrls(primaryUrl) {
  return [primaryUrl];
}

async function downloadWithRetryAndFallback(urls, headers) {
  let lastError = "";
  for (const url of urls) {
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        const response = await fetch(url, { headers });
        ensureOkResponse(response, url);
        if (!response.body) {
          throw new Error(`resposta sem body para ${url}`);
        }
        return response;
      } catch (error) {
        lastError = `${url} [tentativa ${attempt}/3]: ${toErrorMessage(error)}`;
        if (attempt < 3) {
          await sleep(attempt * 500);
        }
      }
    }
  }
  throw new Error(lastError || "falha desconhecida de download");
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

  const candidateUrls = buildCandidateUrls(url);
  try {
    const response = await downloadWithRetryAndFallback(candidateUrls, headers);
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
    return;
  } catch (downloadError) {
    const message = toErrorMessage(downloadError);
    die(
      [
        `falha no download por ZIP: ${message}`,
        "fallback por git clone foi desabilitado para ambientes corporativos com restricao de organizacao.",
        "configure um espelho interno e use --github-base <url> (ou GITHUB_BASE_URL) para apontar para esse host.",
      ].join(" "),
    );
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  die(message || "erro desconhecido");
});
