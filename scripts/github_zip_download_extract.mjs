#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import https from "node:https";
import { spawnSync } from "node:child_process";

function die(message) {
  console.error(`[github-zip-download-extract] erro: ${message}`);
  process.exit(1);
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

function buildHttpsAgentFromProxyEnv() {
  try {
    return new https.Agent({ proxyEnv: process.env });
  } catch {
    return undefined;
  }
}

function downloadUrlToFile(url, outputPath, headers, redirectDepth = 0) {
  return new Promise((resolve, reject) => {
    if (redirectDepth > 5) {
      reject(new Error(`redirecionamento excessivo para ${url}`));
      return;
    }

    const request = https.request(
      url,
      {
        method: "GET",
        headers,
        timeout: 120000,
        agent: buildHttpsAgentFromProxyEnv(),
      },
      (response) => {
        const statusCode = response.statusCode || 0;
        const location = response.headers.location;
        if (statusCode >= 300 && statusCode < 400 && location) {
          response.resume();
          const redirectedUrl = new URL(location, url).toString();
          downloadUrlToFile(redirectedUrl, outputPath, headers, redirectDepth + 1)
            .then(resolve)
            .catch(reject);
          return;
        }
        if (statusCode < 200 || statusCode >= 300) {
          response.resume();
          reject(new Error(`falha no download (${statusCode}) para ${url}`));
          return;
        }

        const fileStream = fs.createWriteStream(outputPath);
        response.pipe(fileStream);
        fileStream.on("finish", () => {
          fileStream.close(() => resolve());
        });
        fileStream.on("error", (error) => reject(error));
      },
    );

    request.on("timeout", () => {
      request.destroy(new Error(`timeout ao baixar ${url}`));
    });
    request.on("error", (error) => reject(error));
    request.end();
  });
}

async function downloadWithRetry(urls, zipPath, headers) {
  let lastError = "";
  for (const url of urls) {
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      try {
        await downloadUrlToFile(url, zipPath, headers);
        return;
      } catch (error) {
        lastError = `${url} [tentativa ${attempt}/4]: ${toErrorMessage(error)}`;
        fs.rmSync(zipPath, { force: true });
        if (attempt < 4) {
          await sleep(attempt * 1000);
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
    await downloadWithRetry(candidateUrls, zipPath, headers);
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
