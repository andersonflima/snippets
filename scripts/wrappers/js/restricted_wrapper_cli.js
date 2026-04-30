#!/usr/bin/env node
"use strict";

const fs = require("fs");
const http = require("http");
const https = require("https");
const net = require("net");
const tls = require("tls");
const { URL } = require("url");
const path = require("path");
const os = require("os");
const { spawnSync } = require("child_process");

const commandName = process.argv[2] || "";
const args = process.argv.slice(3);

const truthy = (value) => ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());

const log = (message) => {
  process.stderr.write(`[restricted-js-wrapper] ${message}\n`);
};

const fail = (message, code = 1) => {
  log(`erro: ${message}`);
  process.exit(code);
};

const unique = (items) => [...new Set(items.filter(Boolean))];

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const run = (command, commandArgs, options = {}) => {
  const result = spawnSync(command, commandArgs, {
    stdio: options.stdio || "pipe",
    encoding: "utf8",
    env: options.env || process.env,
    cwd: options.cwd || process.cwd(),
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const stderr = (result.stderr || "").trim();
    throw new Error(stderr || `${command} saiu com status ${result.status}`);
  }
  return result.stdout || "";
};

const splitShortOptions = (items) => {
  const normalized = [];
  for (const item of items) {
    if (!item.startsWith("-") || item.startsWith("--") || item === "-") {
      normalized.push(item);
      continue;
    }

    const payload = item.slice(1);
    if (payload.length <= 1) {
      normalized.push(item);
      continue;
    }

    const valueOptions = new Set(["o", "A", "H", "x", "X"]);
    for (let index = 0; index < payload.length; index += 1) {
      const option = payload[index];
      if (valueOptions.has(option)) {
        normalized.push(`-${option}`);
        if (index + 1 < payload.length) {
          normalized.push(payload.slice(index + 1));
        }
        break;
      }
      normalized.push(`-${option}`);
    }
  }
  return normalized;
};

const parseCurl = (rawArgs) => {
  const parsedArgs = splitShortOptions(rawArgs);
  const request = {
    url: "",
    output: "",
    headers: {},
    userAgent: "",
    proxy: "",
    allowRedirects: true,
    insecure: false,
    remoteName: false,
    outputDir: "",
    createDirs: false,
    connectTimeoutMs: 20_000,
    maxTimeMs: 300_000,
  };

  const positional = [];
  for (let index = 0; index < parsedArgs.length; index += 1) {
    const arg = parsedArgs[index];
    const next = () => {
      index += 1;
      if (index >= parsedArgs.length) fail(`faltou valor para ${arg}`);
      return parsedArgs[index];
    };

    if (arg === "-o" || arg === "--output") request.output = next();
    else if (arg.startsWith("--output=")) request.output = arg.slice("--output=".length);
    else if (arg === "-O" || arg === "--remote-name" || arg === "--remote-name-all") request.remoteName = true;
    else if (arg === "--output-dir") request.outputDir = next();
    else if (arg.startsWith("--output-dir=")) request.outputDir = arg.slice("--output-dir=".length);
    else if (arg === "--create-dirs") request.createDirs = true;
    else if (arg === "-A" || arg === "--user-agent") request.userAgent = next();
    else if (arg.startsWith("--user-agent=")) request.userAgent = arg.slice("--user-agent=".length);
    else if (arg === "-H" || arg === "--header") addHeader(request, next());
    else if (arg.startsWith("--header=")) addHeader(request, arg.slice("--header=".length));
    else if (arg === "-x" || arg === "--proxy") request.proxy = next();
    else if (arg.startsWith("--proxy=")) request.proxy = arg.slice("--proxy=".length);
    else if (arg === "-L" || arg === "--location") request.allowRedirects = true;
    else if (arg === "-k" || arg === "--insecure") request.insecure = true;
    else if (arg === "--connect-timeout") request.connectTimeoutMs = secondsToMs(next(), 20_000);
    else if (arg.startsWith("--connect-timeout=")) request.connectTimeoutMs = secondsToMs(arg.slice("--connect-timeout=".length), 20_000);
    else if (arg === "--max-time") request.maxTimeMs = secondsToMs(next(), 300_000);
    else if (arg.startsWith("--max-time=")) request.maxTimeMs = secondsToMs(arg.slice("--max-time=".length), 300_000);
    else if (["-f", "--fail", "-s", "--silent", "-S", "--show-error", "-4", "--http1.1", "--retry", "--retry-delay", "--retry-all-errors", "--tlsv1.2"].includes(arg)) {
      if (arg === "--retry" || arg === "--retry-delay") next();
    } else if (arg === "--") {
      positional.push(...parsedArgs.slice(index + 1));
      break;
    } else if (arg.startsWith("-")) {
      fail(`argumento curl não suportado pelo motor JS: ${arg}`, 2);
    } else {
      positional.push(arg);
    }
  }

  request.url = positional[positional.length - 1] || "";
  finalizeOutput(request);
  return request;
};

const parseWget = (rawArgs) => {
  const request = {
    url: "",
    output: "",
    headers: {},
    userAgent: "",
    proxy: "",
    allowRedirects: true,
    insecure: false,
    remoteName: true,
    outputDir: "",
    createDirs: false,
    connectTimeoutMs: 20_000,
    maxTimeMs: 300_000,
  };

  const positional = [];
  for (let index = 0; index < rawArgs.length; index += 1) {
    const arg = rawArgs[index];
    const next = () => {
      index += 1;
      if (index >= rawArgs.length) fail(`faltou valor para ${arg}`);
      return rawArgs[index];
    };

    if (arg === "-O" || arg === "--output-document") request.output = next();
    else if (arg.startsWith("--output-document=")) request.output = arg.slice("--output-document=".length);
    else if (arg === "-o" || arg === "--output-file") next();
    else if (arg.startsWith("-o") && arg.length > 2) {}
    else if (arg.startsWith("--output-file=")) {}
    else if (arg === "-P" || arg === "--directory-prefix") {
      request.outputDir = next();
      request.createDirs = true;
    } else if (arg.startsWith("--directory-prefix=")) {
      request.outputDir = arg.slice("--directory-prefix=".length);
      request.createDirs = true;
    } else if (arg === "--user-agent") request.userAgent = next();
    else if (arg.startsWith("--user-agent=")) request.userAgent = arg.slice("--user-agent=".length);
    else if (arg === "--header") addHeader(request, next());
    else if (arg.startsWith("--header=")) addHeader(request, arg.slice("--header=".length));
    else if (arg === "--connect-timeout") request.connectTimeoutMs = secondsToMs(next(), 20_000);
    else if (arg.startsWith("--connect-timeout=")) request.connectTimeoutMs = secondsToMs(arg.slice("--connect-timeout=".length), 20_000);
    else if (arg === "--timeout" || arg === "-T") request.maxTimeMs = secondsToMs(next(), 300_000);
    else if (arg.startsWith("--timeout=")) request.maxTimeMs = secondsToMs(arg.slice("--timeout=".length), 300_000);
    else if (arg.startsWith("-T") && arg.length > 2) request.maxTimeMs = secondsToMs(arg.slice(2), 300_000);
    else if (arg === "--tries") next();
    else if (arg.startsWith("--tries=")) {}
    else if (arg === "--method") {
      const method = next().toUpperCase();
      if (method !== "GET") fail(`método wget não suportado pelo motor JS: ${method}`, 2);
    }
    else if (arg.startsWith("--method=")) {
      const method = arg.slice("--method=".length).toUpperCase();
      if (method !== "GET") fail(`método wget não suportado pelo motor JS: ${method}`, 2);
    }
    else if (arg === "--no-check-certificate") request.insecure = true;
    else if (["--retry-connrefused", "--no-verbose", "-q", "--quiet", "-nv"].includes(arg)) {}
    else if (arg === "--") {
      positional.push(...rawArgs.slice(index + 1));
      break;
    } else if (arg.startsWith("-")) {
      fail(`argumento wget não suportado pelo motor JS: ${arg}`, 2);
    } else {
      positional.push(arg);
    }
  }

  request.url = positional[positional.length - 1] || "";
  finalizeOutput(request);
  return request;
};

function addHeader(request, headerLine) {
  const separator = headerLine.indexOf(":");
  if (separator < 0) return;
  const key = headerLine.slice(0, separator).trim();
  const value = headerLine.slice(separator + 1).trim();
  if (key) request.headers[key] = value;
}

function secondsToMs(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.ceil(parsed * 1000) : fallback;
}

function finalizeOutput(request) {
  if (!request.url) fail("URL não informada", 2);
  if (!request.output && request.remoteName) {
    const cleanUrl = request.url.split("?")[0].split("#")[0];
    request.output = path.basename(cleanUrl) || "download.bin";
  }
  if (request.outputDir && request.output) {
    request.output = path.join(request.outputDir, request.output);
  }
}

function resolveProxy(command, request) {
  if (request.proxy) return request.proxy;
  const commandProxy = command === "wget"
    ? process.env.WGET_WRAPPER_PROXY
    : command === "git"
      ? process.env.GIT_ZIP_WRAPPER_PROXY
      : process.env.CURL_WRAPPER_PROXY;
  return commandProxy || process.env.HTTPS_PROXY || process.env.https_proxy || process.env.ALL_PROXY || process.env.all_proxy || process.env.HTTP_PROXY || process.env.http_proxy || "";
}

function archiveRefCandidateUrls(owner, repo, ref) {
  const githubBaseUrl = (process.env.RESTRICTED_GITHUB_BASE_URL || "https://github.com").replace(/\/$/, "");
  const codeloadBaseUrl = (process.env.RESTRICTED_CODELOAD_BASE_URL || "https://codeload.github.com").replace(/\/$/, "");
  const urls = [];
  const add = (candidate) => {
    if (!urls.includes(candidate)) urls.push(candidate);
  };

  if (ref.startsWith("refs/heads/")) {
    const shortRef = ref.slice("refs/heads/".length);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/${shortRef}.zip`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/heads/${shortRef}.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/heads/${shortRef}`);
  } else if (ref.startsWith("refs/tags/")) {
    const shortRef = ref.slice("refs/tags/".length);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/${shortRef}.zip`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/tags/${shortRef}.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/tags/${shortRef}`);
  } else if (ref === "HEAD") {
    add(`${githubBaseUrl}/${owner}/${repo}/archive/main.zip`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/heads/main.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/heads/main`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/master.zip`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/heads/master.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/heads/master`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/HEAD.zip`);
  } else {
    add(`${githubBaseUrl}/${owner}/${repo}/archive/${ref}.zip`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/heads/${ref}.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/heads/${ref}`);
    add(`${githubBaseUrl}/${owner}/${repo}/archive/refs/tags/${ref}.zip`);
    add(`${codeloadBaseUrl}/${owner}/${repo}/zip/refs/tags/${ref}`);
  }

  return urls;
}

function archiveCandidates(rawUrl) {
  const cleanUrl = rawUrl.split("?")[0].split("#")[0];
  const githubBaseUrl = (process.env.RESTRICTED_GITHUB_BASE_URL || "https://github.com").replace(/\/$/, "");
  const codeloadBaseUrl = (process.env.RESTRICTED_CODELOAD_BASE_URL || "https://codeload.github.com").replace(/\/$/, "");
  const githubArchive = cleanUrl.match(new RegExp(`^${escapeRegExp(githubBaseUrl)}\\/([^/]+)\\/([^/]+)\\/archive\\/(.+)\\.zip$`));
  if (githubArchive) {
    const [, owner, repo, ref] = githubArchive;
    return archiveRefCandidateUrls(owner, repo, ref);
  }

  const codeload = cleanUrl.match(new RegExp(`^${escapeRegExp(codeloadBaseUrl)}\\/([^/]+)\\/([^/]+)\\/zip\\/(.+)$`));
  if (codeload) {
    const [, owner, repo, ref] = codeload;
    return archiveRefCandidateUrls(owner, repo, ref);
  }
  return [];
}

function normalizeCloneUrlForHttpTransport(repoUrl) {
  if (/^https?:\/\//.test(repoUrl)) return repoUrl;
  if (repoUrl.startsWith("git://")) return `https://${repoUrl.slice("git://".length)}`;
  if (repoUrl.startsWith("ssh://")) {
    const url = new URL(repoUrl);
    return `https://${url.hostname}${url.pathname}`;
  }
  const scpLike = repoUrl.match(/^[^@]+@([^:]+):(.+)$/);
  if (scpLike) return `https://${scpLike[1]}/${scpLike[2]}`;
  return "";
}

function extractGithubSlug(repoUrl) {
  const normalizedUrl = normalizeCloneUrlForHttpTransport(repoUrl) || repoUrl;
  const githubUrl = normalizedUrl.match(/^https?:\/\/github\.com\/([^/]+\/[^/#?]+?)(?:\.git)?(?:[/?#].*)?$/);
  return githubUrl ? githubUrl[1].replace(/\.git$/, "") : "";
}

function repoSourceRequiresPlainGit(repoUrl) {
  const normalizedUrl = normalizeCloneUrlForHttpTransport(repoUrl) || repoUrl;
  const owner = normalizedUrl.match(/^https?:\/\/[^/]+\/([^/]+)/)?.[1] || "";
  const prefixes = (process.env.RESTRICTED_GIT_PLAIN_OWNER_PREFIXES || "itau-")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  return prefixes.some((prefix) => owner.toLowerCase().startsWith(prefix));
}

function parseGitInvocation(rawArgs) {
  const globalArgs = [];
  for (let index = 0; index < rawArgs.length; index += 1) {
    const arg = rawArgs[index];
    if (["-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--config-env"].includes(arg)) {
      if (index + 1 >= rawArgs.length) fail(`faltou valor para ${arg}`, 2);
      globalArgs.push(arg, rawArgs[index + 1]);
      index += 1;
      continue;
    }
    if (/^(--git-dir|--work-tree|--namespace|--exec-path|--config-env)=/.test(arg) || ["--bare", "--no-pager", "--paginate", "--literal-pathspecs", "--no-literal-pathspecs", "--optional-locks", "--no-optional-locks"].includes(arg)) {
      globalArgs.push(arg);
      continue;
    }
    if (arg === "--") return { globalArgs, subcommand: "", subcommandArgs: rawArgs.slice(index + 1) };
    if (arg.startsWith("-")) {
      globalArgs.push(arg);
      continue;
    }
    return { globalArgs, subcommand: arg, subcommandArgs: rawArgs.slice(index + 1) };
  }
  return { globalArgs, subcommand: "", subcommandArgs: [] };
}

function parseGitClone(rawArgs) {
  const optionsWithValues = new Set(["-b", "--branch", "-c", "--config", "-o", "--origin", "-u", "--upload-pack", "-j", "--jobs", "--depth", "--filter", "--template", "--reference", "--reference-if-able", "--server-option", "--separate-git-dir", "--bundle-uri"]);
  const positional = [];
  let branch = "";

  for (let index = 0; index < rawArgs.length; index += 1) {
    const arg = rawArgs[index];
    const next = () => {
      index += 1;
      if (index >= rawArgs.length) fail(`faltou valor para ${arg}`, 2);
      return rawArgs[index];
    };

    if (arg === "--") {
      positional.push(...rawArgs.slice(index + 1));
      break;
    }
    if (arg === "-b" || arg === "--branch") {
      branch = next();
      continue;
    }
    if (arg.startsWith("--branch=")) {
      branch = arg.slice("--branch=".length);
      continue;
    }
    if (optionsWithValues.has(arg)) {
      next();
      continue;
    }
    if (/^(--config|--jobs|--depth|--filter|--origin|--upload-pack|--template|--reference|--reference-if-able|--server-option|--separate-git-dir|--bundle-uri|--recurse-submodules)=/.test(arg)) continue;
    if (arg.startsWith("-")) continue;
    positional.push(arg);
  }

  if (positional.length < 1 || positional.length > 2) fail("uso inválido: git clone <repo> [destino]", 2);
  const repoUrl = positional[0];
  const destination = positional[1] || defaultDestinationFromRepo(repoUrl);
  return { repoUrl, destination, branch };
}

function defaultDestinationFromRepo(repoUrl) {
  const cleaned = repoUrl.replace(/[/?#].*$/, "").replace(/\/$/, "");
  const repoName = path.basename(cleaned).replace(/\.git$/, "");
  if (!repoName) fail(`não foi possível inferir destino para clone: ${repoUrl}`, 2);
  return repoName;
}

function normalizeArchiveRef(ref) {
  if (!ref || ref === "HEAD") return "";
  return ref
    .replace(/^\+/, "")
    .replace(/:.+$/, "")
    .replace(/^refs\/heads\//, "")
    .replace(/^heads\//, "")
    .replace(/^refs\/remotes\/origin\//, "")
    .replace(/^remotes\/origin\//, "")
    .replace(/^origin\//, "")
    .replace(/^refs\/tags\//, "")
    .replace(/^tags\//, "");
}

function sanitizeBranchName(ref) {
  const normalized = normalizeArchiveRef(ref) || "main";
  const sanitized = normalized.replace(/[^A-Za-z0-9._/-]/g, "-").replace(/^[-/]+|[-/]+$/g, "");
  return sanitized || "main";
}

function resolveArchiveBaseUrl() {
  return (process.env.RESTRICTED_GIT_ARCHIVE_BASE_URL || "https://github.com").replace(/\/$/, "");
}

function resolveCodeloadBaseUrl() {
  return (process.env.RESTRICTED_CODELOAD_BASE_URL || "https://codeload.github.com").replace(/\/$/, "");
}

function resolveGithubApiBaseUrl() {
  return (process.env.RESTRICTED_GITHUB_API_BASE_URL || "https://api.github.com").replace(/\/$/, "");
}

function splitGithubSlug(slug) {
  const [owner, repo] = slug.split("/");
  if (!owner || !repo) fail(`slug GitHub inválido: ${slug}`, 2);
  return { owner, repo };
}

function githubCloneArchiveCandidatesForRef(slug, rawRef) {
  const { owner, repo } = splitGithubSlug(slug);
  const githubBaseUrl = resolveArchiveBaseUrl();
  const codeloadBaseUrl = resolveCodeloadBaseUrl();
  const ref = normalizeArchiveRef(rawRef);
  const add = (items, candidate) => {
    if (!items.some((item) => item.url === candidate.url)) items.push(candidate);
    return items;
  };

  if (!ref) {
    return [
      { url: `${githubBaseUrl}/${slug}/archive/HEAD.zip`, ref: "HEAD", refType: "head" },
      { url: `${codeloadBaseUrl}/${owner}/${repo}/zip/HEAD`, ref: "HEAD", refType: "head" },
    ];
  }

  if (/^[0-9a-fA-F]{7,40}$/.test(ref)) {
    return [
      { url: `${githubBaseUrl}/${slug}/archive/${ref}.zip`, ref, refType: "commit" },
      { url: `${codeloadBaseUrl}/${owner}/${repo}/zip/${ref}`, ref, refType: "commit" },
    ];
  }

  return [
    { url: `${githubBaseUrl}/${slug}/archive/${ref}.zip`, ref, refType: "branch" },
    { url: `${githubBaseUrl}/${slug}/archive/refs/heads/${ref}.zip`, ref, refType: "branch" },
    { url: `${codeloadBaseUrl}/${owner}/${repo}/zip/refs/heads/${ref}`, ref, refType: "branch" },
    { url: `${githubBaseUrl}/${slug}/archive/refs/tags/${ref}.zip`, ref, refType: "tag" },
    { url: `${codeloadBaseUrl}/${owner}/${repo}/zip/refs/tags/${ref}`, ref, refType: "tag" },
  ].reduce(add, []);
}

async function resolveGithubDefaultBranch(slug) {
  const request = {
    url: `${resolveGithubApiBaseUrl()}/repos/${slug}`,
    output: "",
    headers: { Accept: "application/vnd.github+json" },
    userAgent: "restricted-js-wrapper",
    proxy: process.env.GIT_ZIP_WRAPPER_PROXY || "",
    allowRedirects: true,
    insecure: truthy(process.env.GIT_ZIP_WRAPPER_CURL_INSECURE),
    remoteName: false,
    outputDir: "",
    createDirs: false,
    connectTimeoutMs: 20_000,
    maxTimeMs: 60_000,
  };

  return new Promise((resolve, reject) => {
    requestUrl(request.url, request, resolveProxy("git", request), 0, (error, response) => {
      if (error) {
        reject(error);
        return;
      }
      try {
        const payload = JSON.parse(response.body.toString("utf8"));
        const defaultBranch = String(payload.default_branch || "").trim();
        if (!defaultBranch) throw new Error("default_branch ausente");
        resolve(defaultBranch);
      } catch (parseError) {
        reject(parseError);
      }
    });
  });
}

async function githubCloneArchiveCandidates(slug, requestedRef) {
  const refs = [];
  if (requestedRef) {
    refs.push(requestedRef);
  } else {
    try {
      const defaultBranch = await resolveGithubDefaultBranch(slug);
      log(`default branch resolvida para ${slug}: ${defaultBranch}`);
      refs.push(defaultBranch);
    } catch (error) {
      log(`não foi possível resolver default branch de ${slug}: ${error.message}`);
    }
    refs.push("main", "master", "HEAD");
  }

  return unique(refs)
    .flatMap((ref) => githubCloneArchiveCandidatesForRef(slug, ref))
    .filter((candidate, index, candidates) => candidates.findIndex((item) => item.url === candidate.url) === index);
}

function validateCloneDestination(destination) {
  if (fs.existsSync(destination) && !fs.statSync(destination).isDirectory()) {
    fail(`destino existe e não é diretório: ${destination}`, 2);
  }
  if (fs.existsSync(destination) && fs.readdirSync(destination).length > 0) {
    fail(`destino já existe e não está vazio: ${destination}`, 2);
  }
  fs.mkdirSync(destination, { recursive: true });
}

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

async function downloadWithCandidates(command, request) {
  const proxy = resolveProxy(command, request);
  const candidates = unique([request.url, ...archiveCandidates(request.url)]);
  let lastError = null;

  for (const candidate of candidates) {
    try {
      await download(candidate, request, proxy);
      return candidate;
    } catch (error) {
      lastError = error;
      log(`${candidate} falhou: ${error.message}`);
    }
  }

  throw lastError || new Error("download falhou");
}

async function downloadArchiveCandidate(candidate, outputPath) {
  const request = {
    url: candidate.url,
    output: outputPath,
    headers: { Accept: "application/octet-stream,*/*" },
    userAgent: "restricted-js-wrapper",
    proxy: process.env.GIT_ZIP_WRAPPER_PROXY || "",
    allowRedirects: true,
    insecure: truthy(process.env.GIT_ZIP_WRAPPER_CURL_INSECURE),
    remoteName: false,
    outputDir: "",
    createDirs: true,
    connectTimeoutMs: 20_000,
    maxTimeMs: 300_000,
  };
  await download(candidate.url, request, resolveProxy("git", request));
  return candidate;
}

async function downloadFirstArchiveCandidate(candidates, outputPath) {
  let lastError = null;
  for (const candidate of candidates) {
    try {
      return await downloadArchiveCandidate(candidate, outputPath);
    } catch (error) {
      lastError = error;
      log(`${candidate.url} falhou: ${error.message}`);
    }
  }
  throw lastError || new Error("download do archive falhou");
}

function extractZipArchive(archivePath, destination) {
  const extractDir = createTempDir("restricted-git-extract-");
  try {
    run("unzip", ["-q", archivePath, "-d", extractDir]);
    const entries = fs.readdirSync(extractDir).filter((entry) => entry !== "__MACOSX");
    const topDir = entries
      .map((entry) => path.join(extractDir, entry))
      .find((entryPath) => fs.statSync(entryPath).isDirectory());
    if (!topDir) throw new Error("archive não possui diretório raiz extraído");
    fs.cpSync(topDir, destination, { recursive: true, force: true });
  } finally {
    fs.rmSync(extractDir, { recursive: true, force: true });
  }
}

function resolveRealGit() {
  if (process.env.GIT_ZIP_WRAPPER_REAL_GIT) return process.env.GIT_ZIP_WRAPPER_REAL_GIT;
  const candidates = ["/usr/bin/git", "/opt/homebrew/bin/git", "/usr/local/bin/git", "/bin/git"];
  const selected = candidates.find((candidate) => fs.existsSync(candidate));
  if (!selected) fail("não foi possível localizar git real. Defina GIT_ZIP_WRAPPER_REAL_GIT.", 2);
  return selected;
}

function configureArchiveGitRepository({ realGit, repoUrl, destination, ref, refType }) {
  const branchName = sanitizeBranchName(ref);
  run(realGit, ["init", "--quiet", destination]);
  run(realGit, ["-C", destination, "remote", "add", "origin", repoUrl]);
  run(realGit, ["-C", destination, "symbolic-ref", "HEAD", `refs/heads/${branchName}`]);
  run(realGit, ["-C", destination, "add", "-A"]);
  try {
    run(realGit, ["-C", destination, "-c", "user.name=restricted-js-wrapper", "-c", "user.email=restricted-js-wrapper@local", "commit", "--quiet", "-m", `Archive snapshot of ${repoUrl} (${ref || "HEAD"})`]);
  } catch (error) {
    log(`commit local vazio ou indisponível: ${error.message}`);
  }

  const commit = run(realGit, ["-C", destination, "rev-parse", "HEAD"]).trim();
  if (refType === "tag") {
    run(realGit, ["-C", destination, "tag", "-f", branchName, commit]);
    run(realGit, ["-C", destination, "checkout", "--quiet", "--detach", commit]);
    return;
  }
  run(realGit, ["-C", destination, "update-ref", `refs/remotes/origin/${branchName}`, commit]);
  run(realGit, ["-C", destination, "symbolic-ref", "refs/remotes/origin/HEAD", `refs/remotes/origin/${branchName}`]);
  run(realGit, ["-C", destination, "checkout", "--quiet", "-B", branchName, `refs/remotes/origin/${branchName}`]);
}

function execRealGit(rawArgs) {
  const realGit = resolveRealGit();
  const result = spawnSync(realGit, rawArgs, { stdio: "inherit", env: process.env });
  if (result.error) throw result.error;
  process.exit(result.status || 0);
}

async function runGit(rawArgs) {
  const invocation = parseGitInvocation(rawArgs);
  if (invocation.subcommand !== "clone") {
    execRealGit(rawArgs);
    return;
  }

  const clone = parseGitClone(invocation.subcommandArgs);
  if (repoSourceRequiresPlainGit(clone.repoUrl)) {
    log("source corporativo detectado; usando git real");
    execRealGit(rawArgs);
    return;
  }

  const slug = extractGithubSlug(clone.repoUrl);
  if (!slug) {
    execRealGit(rawArgs);
    return;
  }

  validateCloneDestination(clone.destination);
  const tempDir = createTempDir("restricted-git-clone-");
  const archivePath = path.join(tempDir, "repo.zip");
  try {
    const candidates = await githubCloneArchiveCandidates(slug, clone.branch);
    const source = await downloadFirstArchiveCandidate(candidates, archivePath);
    extractZipArchive(archivePath, clone.destination);
    configureArchiveGitRepository({
      realGit: resolveRealGit(),
      repoUrl: clone.repoUrl,
      destination: clone.destination,
      ref: source.ref,
      refType: source.refType,
    });
    log(`git clone via zip concluído: ${clone.repoUrl} -> ${clone.destination} (source: ${source.url})`);
  } catch (error) {
    fs.rmSync(clone.destination, { recursive: true, force: true });
    throw error;
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

function download(url, request, proxy) {
  return new Promise((resolve, reject) => {
    requestUrl(url, request, proxy, 0, (error, response) => {
      if (error) {
        reject(error);
        return;
      }
      if (request.output && request.output !== "-") {
        const outputDir = path.dirname(request.output);
        if (outputDir && outputDir !== ".") fs.mkdirSync(outputDir, { recursive: true });
        fs.writeFileSync(request.output, response.body);
      } else {
        process.stdout.write(response.body);
      }
      resolve();
    });
  });
}

function requestUrl(rawUrl, request, proxy, redirectCount, done) {
  if (redirectCount > 10) {
    done(new Error("redirecionamentos excedidos"));
    return;
  }

  const target = new URL(rawUrl);
  const headers = { ...request.headers };
  if (request.userAgent && !headers["User-Agent"]) headers["User-Agent"] = request.userAgent;
  if (!headers.Accept) headers.Accept = "*/*";

  const onResponse = (response) => {
    const statusCode = response.statusCode || 0;
    const location = response.headers.location;
    if (request.allowRedirects && location && [301, 302, 303, 307, 308].includes(statusCode)) {
      const nextUrl = new URL(location, target).toString();
      response.resume();
      requestUrl(nextUrl, request, proxy, redirectCount + 1, done);
      return;
    }
    if (statusCode >= 400) {
      response.resume();
      done(new Error(`HTTP ${statusCode}`));
      return;
    }

    const chunks = [];
    response.on("data", (chunk) => chunks.push(chunk));
    response.on("end", () => done(null, { body: Buffer.concat(chunks) }));
  };

  const onError = (error) => done(error);
  if (proxy) {
    requestViaProxy(target, headers, request, proxy, onResponse, onError);
    return;
  }
  requestDirect(target, headers, request, onResponse, onError);
}

function requestDirect(target, headers, request, onResponse, onError) {
  const client = target.protocol === "https:" ? https : http;
  const options = {
    method: "GET",
    hostname: target.hostname,
    port: target.port || (target.protocol === "https:" ? 443 : 80),
    path: `${target.pathname}${target.search}`,
    headers,
    timeout: request.maxTimeMs,
    rejectUnauthorized: !request.insecure,
  };

  const req = client.request(options, onResponse);
  req.on("timeout", () => req.destroy(new Error("timeout")));
  req.on("error", onError);
  req.end();
}

function requestViaProxy(target, headers, request, proxyValue, onResponse, onError) {
  const proxy = new URL(proxyValue);
  const proxyHost = proxy.hostname;
  const proxyPort = Number(proxy.port || 8080);
  const proxyAuth = proxy.username ? `Basic ${Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString("base64")}` : "";

  if (target.protocol === "http:") {
    const proxyHeaders = { ...headers, Host: target.host };
    if (proxyAuth) proxyHeaders["Proxy-Authorization"] = proxyAuth;
    const req = http.request({
      method: "GET",
      hostname: proxyHost,
      port: proxyPort,
      path: target.toString(),
      headers: proxyHeaders,
      timeout: request.maxTimeMs,
    }, onResponse);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", onError);
    req.end();
    return;
  }

  const socket = net.connect(proxyPort, proxyHost);
  socket.setTimeout(request.connectTimeoutMs);
  socket.once("connect", () => {
    const connectHeaders = [`CONNECT ${target.hostname}:${target.port || 443} HTTP/1.1`, `Host: ${target.hostname}:${target.port || 443}`];
    if (proxyAuth) connectHeaders.push(`Proxy-Authorization: ${proxyAuth}`);
    socket.write(`${connectHeaders.join("\r\n")}\r\n\r\n`);
  });
  socket.once("timeout", () => socket.destroy(new Error("proxy connect timeout")));
  socket.once("error", onError);
  socket.once("data", (chunk) => {
    const header = chunk.toString("latin1");
    if (!/^HTTP\/1\.[01] 2\d\d/.test(header)) {
      socket.destroy();
      onError(new Error(`proxy CONNECT falhou: ${header.split("\r\n")[0] || "sem resposta"}`));
      return;
    }

    socket.removeAllListeners("timeout");
    socket.removeAllListeners("error");
    const tlsSocket = tls.connect({
      socket,
      servername: target.hostname,
      rejectUnauthorized: !request.insecure,
    });
    const req = https.request({
      method: "GET",
      hostname: target.hostname,
      port: target.port || 443,
      path: `${target.pathname}${target.search}`,
      headers,
      timeout: request.maxTimeMs,
      createConnection: () => tlsSocket,
    }, onResponse);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", onError);
    req.end();
  });
}

async function main() {
  if (!["curl", "wget", "git"].includes(commandName)) {
    fail(`comando não suportado: ${commandName}`, 2);
  }

  if (commandName === "git") {
    await runGit(args);
    return;
  }

  const request = commandName === "curl" ? parseCurl(args) : parseWget(args);
  if (request.insecure) process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
  const source = await downloadWithCandidates(commandName, request);
  log(`download concluído via JS: ${source}`);
}

main().catch((error) => {
  fail(error.message || String(error), 22);
});
