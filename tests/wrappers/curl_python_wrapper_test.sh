#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
SERVER_PIDS=()
export CURL_WRAPPER_USE_JS_ENGINE=0

cleanup() {
  local pid
  for pid in "${SERVER_PIDS[@]+"${SERVER_PIDS[@]}"}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

FAKE_CURL="${TMP_DIR}/curl"
OUTPUT_TARBALL="${TMP_DIR}/tree-sitter-lua.tar.gz"
CURL_LOG="${TMP_DIR}/curl-urls"

cat > "${FAKE_CURL}" <<EOF2
#!/usr/bin/env bash
set -euo pipefail

url=""
output=""

while [[ \$# -gt 0 ]]; do
  case "\$1" in
    -o|--output)
      output="\${2:-}"
      shift 2
      ;;
    --output=*)
      output="\${1#--output=}"
      shift
      ;;
    http://*|https://*)
      url="\$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s\n' "\${url}" >> "${CURL_LOG}"

case "\${url}" in
  https://github.com/tree-sitter-grammars/tree-sitter-lua/archive/v0.1.0.tar.gz)
    printf '%s\n' 'curl: (22) The requested URL returned error: 404' >&2
    exit 22
    ;;
  https://github.com/tree-sitter-grammars/tree-sitter-lua/archive/v0.1.0.zip)
    python3 - "\${output}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("tree-sitter-lua-0.1.0/", "")
    archive.writestr("tree-sitter-lua-0.1.0/grammar.js", "module.exports = grammar({ name: 'lua' })\\n")
PY
    exit 0
    ;;
  https://github.com/mason-org/mason-registry/archive/refs/heads/main.zip)
    printf '%s\n' 'curl: (22) The requested URL returned error: 403' >&2
    exit 22
    ;;
  https://github.com/mason-org/mason-registry/archive/main.zip)
    python3 - "\${output}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("mason-registry-main/", "")
    archive.writestr("mason-registry-main/registry.json", "{}\n")
PY
    exit 0
    ;;
  https://github.com/mason-org/mason-registry/archive/dev.zip)
    printf '%s\n' 'curl: (22) The requested URL returned error: 403' >&2
    exit 22
    ;;
  https://github.com/mason-org/mason-registry/archive/refs/heads/dev.zip)
    python3 - "\${output}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("mason-registry-dev/", "")
    archive.writestr("mason-registry-dev/registry.json", "{}\n")
PY
    exit 0
    ;;
  https://github.com/example/project/archive/feature/foo.zip)
    printf '%s\n' 'curl: (22) The requested URL returned error: 404' >&2
    exit 22
    ;;
  https://github.com/example/project/archive/refs/heads/feature/foo.zip)
    python3 - "\${output}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("project-feature-foo/", "")
    archive.writestr("project-feature-foo/README.md", "feature archive\n")
PY
    exit 0
    ;;
  http://127.0.0.1:*/repos/mason-org/mason-registry/releases/latest)
    printf '%s\n' 'curl: (22) The requested URL returned error: 403' >&2
    exit 22
    ;;
  *)
    printf 'unexpected url: %s\n' "\${url}" >&2
    exit 22
    ;;
esac
EOF2

chmod +x "${FAKE_CURL}"

CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    --silent \
    --fail \
    --show-error \
    --retry 7 \
    -L \
    https://github.com/tree-sitter-grammars/tree-sitter-lua/archive/v0.1.0.tar.gz \
    --output "${OUTPUT_TARBALL}"

test -s "${OUTPUT_TARBALL}"
tar -tzf "${OUTPUT_TARBALL}" | grep -Fq "tree-sitter-lua-0.1.0/grammar.js"
test "$(sed -n '1p' "${CURL_LOG}")" = "https://github.com/tree-sitter-grammars/tree-sitter-lua/archive/v0.1.0.tar.gz"
test "$(sed -n '2p' "${CURL_LOG}")" = "https://github.com/tree-sitter-grammars/tree-sitter-lua/archive/v0.1.0.zip"

OUTPUT_REGISTRY_ZIP="${TMP_DIR}/mason-registry.zip"

CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    --silent \
    --fail \
    --show-error \
    -L \
    https://github.com/mason-org/mason-registry/archive/refs/heads/main.zip \
    --output "${OUTPUT_REGISTRY_ZIP}"

python3 - "${OUTPUT_REGISTRY_ZIP}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path) as archive:
    assert "mason-registry-main/registry.json" in archive.namelist()
PY
test "$(sed -n '3p' "${CURL_LOG}")" = "https://github.com/mason-org/mason-registry/archive/refs/heads/main.zip"
test "$(sed -n '4p' "${CURL_LOG}")" = "https://github.com/mason-org/mason-registry/archive/main.zip"

OUTPUT_REGISTRY_FROM_CODELOAD_ZIP="${TMP_DIR}/mason-registry-from-codeload.zip"

CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    --silent \
    --fail \
    --show-error \
    -L \
    https://codeload.github.com/mason-org/mason-registry/zip/refs/heads/main \
    --output "${OUTPUT_REGISTRY_FROM_CODELOAD_ZIP}"

python3 - "${OUTPUT_REGISTRY_FROM_CODELOAD_ZIP}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path) as archive:
    assert "mason-registry-main/registry.json" in archive.namelist()
PY
test "$(sed -n '5p' "${CURL_LOG}")" = "https://github.com/mason-org/mason-registry/archive/main.zip"

OUTPUT_REGISTRY_PLAIN_REF_ZIP="${TMP_DIR}/mason-registry-plain-ref.zip"

CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    --silent \
    --fail \
    --show-error \
    -L \
    https://github.com/mason-org/mason-registry/archive/dev.zip \
    --output "${OUTPUT_REGISTRY_PLAIN_REF_ZIP}"

python3 - "${OUTPUT_REGISTRY_PLAIN_REF_ZIP}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path) as archive:
    assert "mason-registry-dev/registry.json" in archive.namelist()
PY
test "$(sed -n '6p' "${CURL_LOG}")" = "https://github.com/mason-org/mason-registry/archive/dev.zip"
test "$(sed -n '7p' "${CURL_LOG}")" = "https://github.com/mason-org/mason-registry/archive/refs/heads/dev.zip"

OUTPUT_BRANCH_WITH_SLASH_ZIP="${TMP_DIR}/branch-with-slash.zip"

CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD=1 \
CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
  "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
    --silent \
    --fail \
    --show-error \
    -L \
    https://codeload.github.com/example/project/zip/feature/foo \
    --output "${OUTPUT_BRANCH_WITH_SLASH_ZIP}"

python3 - "${OUTPUT_BRANCH_WITH_SLASH_ZIP}" <<'PY'
import sys
import zipfile

output_path = sys.argv[1]

with zipfile.ZipFile(output_path) as archive:
    assert "project-feature-foo/README.md" in archive.namelist()
PY
test "$(sed -n '8p' "${CURL_LOG}")" = "https://github.com/example/project/archive/feature/foo.zip"
test "$(sed -n '9p' "${CURL_LOG}")" = "https://github.com/example/project/archive/refs/heads/feature/foo.zip"

API_ROOT="${TMP_DIR}/api-root"
API_PORT_FILE="${TMP_DIR}/api-port"
mkdir -p "${API_ROOT}/repos/mason-org/mason-registry/releases"
printf '%s\n' '{"tag_name":"2026-04-30-stable-registry"}' > "${API_ROOT}/repos/mason-org/mason-registry/releases/latest"

python3 - "${API_ROOT}" "${API_PORT_FILE}" <<'PY' &
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
  [[ -s "${API_PORT_FILE}" ]] && break
  sleep 0.1
done
[[ -s "${API_PORT_FILE}" ]]

API_PORT="$(cat "${API_PORT_FILE}")"
API_RESPONSE="$(
  CURL_WRAPPER_REAL_CURL="${FAKE_CURL}" \
    "${REPO_ROOT}/scripts/wrappers/curl_python_wrapper.sh" \
      -fsSL \
      -X GET \
      "http://127.0.0.1:${API_PORT}/repos/mason-org/mason-registry/releases/latest"
)"

test "${API_RESPONSE}" = '{"tag_name":"2026-04-30-stable-registry"}'
