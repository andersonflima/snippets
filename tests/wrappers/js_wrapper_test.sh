#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
SERVER_PIDS=()

cleanup() {
  local pid
  for pid in "${SERVER_PIDS[@]+"${SERVER_PIDS[@]}"}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

HTTP_ROOT="${TMP_DIR}/http-root"
PORT_FILE="${TMP_DIR}/http-port"
mkdir -p "${HTTP_ROOT}/assets"
printf '%s\n' "download via js" > "${HTTP_ROOT}/assets/file.txt"

python3 - "${HTTP_ROOT}" "${PORT_FILE}" <<'PY' &
import functools
import http.server
import socketserver
import sys

root, port_file = sys.argv[1:3]
handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=root)

with socketserver.TCPServer(("127.0.0.1", 0), handler) as server:
    with open(port_file, "w", encoding="utf-8") as handle:
        handle.write(str(server.server_address[1]))
    server.serve_forever()
PY
SERVER_PIDS+=("$!")

for _ in $(seq 1 50); do
  [[ -s "${PORT_FILE}" ]] && break
  sleep 0.1
done
[[ -s "${PORT_FILE}" ]]

PORT="$(cat "${PORT_FILE}")"
URL="http://127.0.0.1:${PORT}/assets/file.txt"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
    curl -fsSL "${URL}" -o "${TMP_DIR}/curl-js.txt"

test "$(cat "${TMP_DIR}/curl-js.txt")" = "download via js"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
    wget -q -O "${TMP_DIR}/wget-js.txt" "${URL}"

test "$(cat "${TMP_DIR}/wget-js.txt")" = "download via js"

WGET_STDOUT="$(
  env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
    node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
      wget -nv -o /dev/null -O - --timeout=30 --method=GET "${URL}"
)"

test "${WGET_STDOUT}" = "download via js"

FAKE_CURL="${TMP_DIR}/curl"
cat > "${FAKE_CURL}" <<'EOF2'
#!/usr/bin/env bash
exit 22
EOF2
chmod +x "${FAKE_CURL}"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  CURL_WRAPPER_USE_JS_ENGINE=1 \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    -fsSL "${URL}" -o "${TMP_DIR}/curl-wrapper-js.txt"

test "$(cat "${TMP_DIR}/curl-wrapper-js.txt")" = "download via js"

WRAPPER_WGET_STDOUT="$(
  env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
    WGET_WRAPPER_USE_JS_ENGINE=1 \
    "${REPO_ROOT}/scripts/wrappers/wget_wrapper.sh" \
      -nv -o /dev/null -O - --timeout=30 --method=GET "${URL}"
)"

test "${WRAPPER_WGET_STDOUT}" = "download via js"

FAKE_RELEASE_CURL="${TMP_DIR}/release-curl"
FAKE_RELEASE_CURL_LOG="${TMP_DIR}/release-curl-args"
FAKE_RELEASE_OUTPUT="${TMP_DIR}/registry-via-wget.zip"
cat > "${FAKE_RELEASE_CURL}" <<EOF2
#!/usr/bin/env bash
set -euo pipefail

output=""
printf '%s\n' "\$*" > "${FAKE_RELEASE_CURL_LOG}"
[[ "\${CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK:-}" == "0" ]]

while [[ \$# -gt 0 ]]; do
  case "\$1" in
    -o)
      output="\${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

python3 - "\${output}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("registry.json", "[]\\n")
PY
EOF2
chmod +x "${FAKE_RELEASE_CURL}"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  WGET_WRAPPER_USE_JS_ENGINE=1 \
  WGET_WRAPPER_CURL_BIN="${FAKE_RELEASE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/wget_wrapper.sh" \
    --header "User-Agent: mason.nvim test" \
    -o /dev/null \
    -O "${FAKE_RELEASE_OUTPUT}" \
    -T 30 \
    "https://github.com/mason-org/mason-registry/releases/download/2026-04-30-stable-registry/registry.json.zip"

grep -Fq 'https://github.com/mason-org/mason-registry/releases/download/2026-04-30-stable-registry/registry.json.zip' "${FAKE_RELEASE_CURL_LOG}"
python3 - "${FAKE_RELEASE_OUTPUT}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path) as archive:
    assert archive.read("registry.json") == b"[]\n"
PY

MASON_REGISTRY_ARCHIVE_ROOT="${HTTP_ROOT}/mason-org/mason-registry/archive/refs/heads"
mkdir -p "${MASON_REGISTRY_ARCHIVE_ROOT}"
python3 - "${MASON_REGISTRY_ARCHIVE_ROOT}/main.zip" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("mason-registry-main/", "")
    archive.writestr("mason-registry-main/registry.json", "{}\n")
PY

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  RESTRICTED_GITHUB_BASE_URL="http://127.0.0.1:${PORT}" \
  node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
    curl -fsSL "http://127.0.0.1:${PORT}/mason-org/mason-registry/archive/main.zip" \
    -o "${TMP_DIR}/mason-registry-js.zip"

python3 - "${TMP_DIR}/mason-registry-js.zip" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path) as archive:
    assert "mason-registry-main/registry.json" in archive.namelist()
PY

ARCHIVE_ROOT="${HTTP_ROOT}/folke/lazy.nvim/archive/refs/heads/feature"
mkdir -p "${ARCHIVE_ROOT}"
python3 - "${ARCHIVE_ROOT}/foo.zip" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("lazy.nvim-feature-foo/", "")
    archive.writestr("lazy.nvim-feature-foo/README.md", "lazy via js zip\n")
PY

GIT_DESTINATION="${TMP_DIR}/lazy-js.nvim"
REAL_GIT="$(command -v git)"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  RESTRICTED_GIT_ARCHIVE_BASE_URL="http://127.0.0.1:${PORT}" \
  GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT}" \
  node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
    git clone --branch feature/foo https://github.com/folke/lazy.nvim "${GIT_DESTINATION}"

test -f "${GIT_DESTINATION}/README.md"
test "$(cat "${GIT_DESTINATION}/README.md")" = "lazy via js zip"
test -d "${GIT_DESTINATION}/.git"
test "$(git -C "${GIT_DESTINATION}" branch --show-current)" = "feature/foo"

GIT_WRAPPER_DESTINATION="${TMP_DIR}/lazy-wrapper-js.nvim"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  RESTRICTED_GIT_ARCHIVE_BASE_URL="http://127.0.0.1:${PORT}" \
  GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT}" \
  GIT_ZIP_WRAPPER_USE_JS_ENGINE=1 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch feature/foo https://github.com/folke/lazy.nvim "${GIT_WRAPPER_DESTINATION}"

test -f "${GIT_WRAPPER_DESTINATION}/README.md"
test "$(git -C "${GIT_WRAPPER_DESTINATION}" branch --show-current)" = "feature/foo"

DEFAULT_BRANCH_API_ROOT="${HTTP_ROOT}/repos/acme/not-main"
DEFAULT_BRANCH_CODELOAD_ROOT="${HTTP_ROOT}/acme/not-main/zip/refs/heads"
mkdir -p "${DEFAULT_BRANCH_API_ROOT}" "${DEFAULT_BRANCH_CODELOAD_ROOT}"
printf '%s\n' '{"default_branch":"trunk"}' > "${DEFAULT_BRANCH_API_ROOT}/index.html"
python3 - "${DEFAULT_BRANCH_CODELOAD_ROOT}/trunk" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]
with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("not-main-trunk/", "")
    archive.writestr("not-main-trunk/README.md", "default branch via codeload\n")
PY

DEFAULT_BRANCH_DESTINATION="${TMP_DIR}/not-main-js"

env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY -u all_proxy \
  RESTRICTED_GIT_ARCHIVE_BASE_URL="http://127.0.0.1:${PORT}" \
  RESTRICTED_CODELOAD_BASE_URL="http://127.0.0.1:${PORT}" \
  RESTRICTED_GITHUB_API_BASE_URL="http://127.0.0.1:${PORT}" \
  GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT}" \
  node "${REPO_ROOT}/scripts/wrappers/js/restricted_wrapper_cli.js" \
    git clone https://github.com/acme/not-main "${DEFAULT_BRANCH_DESTINATION}"

test "$(cat "${DEFAULT_BRANCH_DESTINATION}/README.md")" = "default branch via codeload"
test "$(git -C "${DEFAULT_BRANCH_DESTINATION}" branch --show-current)" = "trunk"
