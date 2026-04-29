#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FAKE_CURL="${TMP_DIR}/curl"
FAKE_GIT="${TMP_DIR}/git"
DESTINATION="${TMP_DIR}/lazy.nvim"
CURL_CALLED="${TMP_DIR}/curl-called"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'printf "%s\n" "$*" > "'"${CURL_CALLED}"'"' \
  'exit 22' \
  > "${FAKE_CURL}"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'if [[ "${1:-}" == "clone" ]]; then' \
  '  destination="${@: -1}"' \
  '  mkdir -p "${destination}/.git"' \
  '  exit 0' \
  'fi' \
  'exit 1' \
  > "${FAKE_GIT}"

chmod +x "${FAKE_CURL}" "${FAKE_GIT}"

GIT_ZIP_WRAPPER_REAL_GIT="${FAKE_GIT}" \
CURL="${FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=git-first \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch main https://github.com/folke/lazy.nvim "${DESTINATION}"

test -d "${DESTINATION}/.git"
test ! -e "${CURL_CALLED}"

ARCHIVE_PARENT="${TMP_DIR}/archive-source"
ARCHIVE_TOP="${ARCHIVE_PARENT}/lazy.nvim-main"
ARCHIVE_DESTINATION="${TMP_DIR}/lazy-archive.nvim"
ARCHIVE_CURL_LOG="${TMP_DIR}/archive-curl-urls"
ARCHIVE_FAKE_CURL="${TMP_DIR}/archive-curl"
ARCHIVE_FAKE_GIT="${TMP_DIR}/archive-git"
REAL_GIT="$(command -v git)"

mkdir -p "${ARCHIVE_TOP}"
printf '%s\n' 'archive fallback content' > "${ARCHIVE_TOP}/README.md"

cat > "${ARCHIVE_FAKE_CURL}" <<EOF2
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

printf '%s\n' "\${url}" >> "${ARCHIVE_CURL_LOG}"

case "\${url}" in
  https://github.com/folke/lazy.nvim/archive/refs/heads/main.tar.gz)
    tar -czf "\${output}" -C "${ARCHIVE_PARENT}" lazy.nvim-main
    exit 0
    ;;
  https://codeload.github.com/folke/lazy.nvim/tar.gz/main)
    exit 22
    ;;
  *)
    exit 22
    ;;
esac
EOF2

cat > "${ARCHIVE_FAKE_GIT}" <<EOF2
#!/usr/bin/env bash
set -euo pipefail

if [[ "\${1:-}" == "clone" ]]; then
  exit 1
fi

exec "${REAL_GIT}" "\$@"
EOF2

chmod +x "${ARCHIVE_FAKE_CURL}" "${ARCHIVE_FAKE_GIT}"

GIT_ZIP_WRAPPER_REAL_GIT="${ARCHIVE_FAKE_GIT}" \
CURL="${ARCHIVE_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=local-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=tar.gz \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch main https://github.com/folke/lazy.nvim "${ARCHIVE_DESTINATION}"

test -d "${ARCHIVE_DESTINATION}/.git"
test -f "${ARCHIVE_DESTINATION}/README.md"
test "$(sed -n '1p' "${ARCHIVE_CURL_LOG}")" = "https://github.com/folke/lazy.nvim/archive/refs/heads/main.tar.gz"
