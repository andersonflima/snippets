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
