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
  https://github.com/folke/lazy.nvim/archive/main.zip)
    python3 - "\${output}" "${ARCHIVE_PARENT}" <<'PY'
import os
import sys
import zipfile

output_path, source_parent = sys.argv[1:3]
source_root = os.path.join(source_parent, "lazy.nvim-main")

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    for root, dirs, files in os.walk(source_root):
        dirs.sort()
        files.sort()
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, source_parent)
            archive.write(full_path, rel_path)
PY
    exit 0
    ;;
  https://github.com/folke/lazy.nvim/archive/HEAD.zip)
    python3 - "\${output}" "${ARCHIVE_PARENT}" <<'PY'
import os
import sys
import zipfile

output_path, source_parent = sys.argv[1:3]
source_root = os.path.join(source_parent, "lazy.nvim-main")

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    for root, dirs, files in os.walk(source_root):
        dirs.sort()
        files.sort()
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, source_parent)
            archive.write(full_path, rel_path)
PY
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
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch main https://github.com/folke/lazy.nvim "${ARCHIVE_DESTINATION}"

test -d "${ARCHIVE_DESTINATION}/.git"
test -f "${ARCHIVE_DESTINATION}/README.md"
test "$(sed -n '1p' "${ARCHIVE_CURL_LOG}")" = "https://github.com/folke/lazy.nvim/archive/main.zip"

ARCHIVE_GIT_FIRST_DESTINATION="${TMP_DIR}/lazy-archive-git-first.nvim"

GIT_ZIP_WRAPPER_REAL_GIT="${ARCHIVE_FAKE_GIT}" \
CURL="${ARCHIVE_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=git-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch main https://github.com/folke/lazy.nvim "${ARCHIVE_GIT_FIRST_DESTINATION}"

test -d "${ARCHIVE_GIT_FIRST_DESTINATION}/.git"
test -f "${ARCHIVE_GIT_FIRST_DESTINATION}/README.md"
test "$(sed -n '2p' "${ARCHIVE_CURL_LOG}")" = "https://github.com/folke/lazy.nvim/archive/main.zip"

ARCHIVE_GLOBAL_CONFIG_DESTINATION="${TMP_DIR}/lazy-archive-global-config.nvim"
ARCHIVE_GLOBAL_CONFIG_FAKE_GIT="${TMP_DIR}/archive-global-config-git"

cat > "${ARCHIVE_GLOBAL_CONFIG_FAKE_GIT}" <<EOF2
#!/usr/bin/env bash
set -euo pipefail

for arg in "\$@"; do
  if [[ "\${arg}" == "clone" ]]; then
    exit 1
  fi
done

exec "${REAL_GIT}" "\$@"
EOF2

chmod +x "${ARCHIVE_GLOBAL_CONFIG_FAKE_GIT}"

GIT_ZIP_WRAPPER_REAL_GIT="${ARCHIVE_GLOBAL_CONFIG_FAKE_GIT}" \
CURL="${ARCHIVE_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=local-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    -c protocol.version=2 clone --branch main https://github.com/folke/lazy.nvim "${ARCHIVE_GLOBAL_CONFIG_DESTINATION}"

test -d "${ARCHIVE_GLOBAL_CONFIG_DESTINATION}/.git"
test -f "${ARCHIVE_GLOBAL_CONFIG_DESTINATION}/README.md"
test "$(sed -n '3p' "${ARCHIVE_CURL_LOG}")" = "https://github.com/folke/lazy.nvim/archive/main.zip"

ARCHIVE_DEFAULT_BRANCH_DESTINATION="${TMP_DIR}/lazy-archive-default-branch.nvim"

GIT_ZIP_WRAPPER_REAL_GIT="${ARCHIVE_FAKE_GIT}" \
CURL="${ARCHIVE_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=local-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --filter=blob:none https://github.com/folke/lazy.nvim "${ARCHIVE_DEFAULT_BRANCH_DESTINATION}"

test -d "${ARCHIVE_DEFAULT_BRANCH_DESTINATION}/.git"
test -f "${ARCHIVE_DEFAULT_BRANCH_DESTINATION}/README.md"
test "$(git -C "${ARCHIVE_DEFAULT_BRANCH_DESTINATION}" branch --show-current)" = "main"
test "$(sed -n '4p' "${ARCHIVE_CURL_LOG}")" = "https://github.com/folke/lazy.nvim/archive/main.zip"

PERMANENT_404_DESTINATION="${TMP_DIR}/lazy-permanent-404.nvim"
PERMANENT_404_CURL_LOG="${TMP_DIR}/permanent-404-curl-urls"
PERMANENT_404_FAKE_CURL="${TMP_DIR}/permanent-404-curl"
PERMANENT_404_FAKE_GIT="${TMP_DIR}/permanent-404-git"

cat > "${PERMANENT_404_FAKE_CURL}" <<EOF2
#!/usr/bin/env bash
set -euo pipefail

url=""

while [[ \$# -gt 0 ]]; do
  case "\$1" in
    http://*|https://*)
      url="\$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s\n' "\${url}" >> "${PERMANENT_404_CURL_LOG}"
printf '%s\n' 'curl: (22) The requested URL returned error: 404' >&2
exit 22
EOF2

cat > "${PERMANENT_404_FAKE_GIT}" <<'EOF2'
#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "clone" ]]; then
  destination="${@: -1}"
  mkdir -p "${destination}/.git"
  exit 0
fi

exit 1
EOF2

chmod +x "${PERMANENT_404_FAKE_CURL}" "${PERMANENT_404_FAKE_GIT}"

set +e
GIT_ZIP_WRAPPER_REAL_GIT="${PERMANENT_404_FAKE_GIT}" \
CURL="${PERMANENT_404_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=local-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=0 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch missing-ref https://github.com/folke/lazy.nvim "${PERMANENT_404_DESTINATION}"
permanent_404_status=$?
set -e

test "${permanent_404_status}" -ne 0
test ! -d "${PERMANENT_404_DESTINATION}/.git"
test "$(wc -l < "${PERMANENT_404_CURL_LOG}" | tr -d ' ')" = "1"
! grep -q 'https://codeload.github.com/' "${PERMANENT_404_CURL_LOG}"

PERMANENT_404_FALLBACK_DESTINATION="${TMP_DIR}/lazy-permanent-404-fallback.nvim"

GIT_ZIP_WRAPPER_REAL_GIT="${PERMANENT_404_FAKE_GIT}" \
CURL="${PERMANENT_404_FAKE_CURL}" \
GIT_ZIP_WRAPPER_STRICT=0 \
GIT_ZIP_WRAPPER_CLONE_ORDER=local-first \
GIT_ZIP_WRAPPER_ARCHIVE_FORMAT=zip \
GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=1 \
  "${REPO_ROOT}/scripts/wrappers/git_zip_clone_wrapper.sh" \
    clone --branch missing-ref https://github.com/folke/lazy.nvim "${PERMANENT_404_FALLBACK_DESTINATION}"

test -d "${PERMANENT_404_FALLBACK_DESTINATION}/.git"
