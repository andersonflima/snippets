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
