#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

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
