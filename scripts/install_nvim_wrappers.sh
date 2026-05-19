#!/usr/bin/env sh
set -eu

TARGET_DIR="${HOME}/.local/share/nvim/wrappers/bin"
FORCE=0
AUTO_SHELL_PROFILE=1
SHELL_RC_FILE=""
EC2_HOST=""
EC2_INSTANCE_ID=""
EC2_S3_URI=""
CLONE_ORDER="local-first"

log() {
  printf '[nvim-wrappers] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  sh scripts/install_nvim_wrappers.sh [opcoes]

Opcoes:
  --target-dir <dir>  Diretorio onde os shims git/curl serao instalados.
                      Default: $HOME/.local/share/nvim/wrappers/bin
  --force             Sobrescreve shims existentes.
  --rc-file <arquivo> Define explicitamente o arquivo de perfil para receber o PATH.
  --ec2-host <host>   Host SSH da EC2 usado para git clone de repositorios externos.
                      Quando definido, usa clone order ec2-first.
  --ec2-instance-id <id>
                      Instance ID da EC2 gerenciada pelo SSM.
  --ec2-s3-uri <uri>  Prefixo S3 usado para transportar clones da EC2 para a maquina.
                      Quando definido junto com --ec2-instance-id, usa ec2-s3-first.
  --clone-order <ordem>
                      Ordem do wrapper git: local-first, git-first, ec2-first ou ec2-s3-first.
                      Default: local-first
  --no-shell-profile  Nao altera arquivo de perfil; apenas instala os shims.
  -h, --help          Mostra esta ajuda.

Resultado:
  - instala shims "git" e "curl" para wrappers locais do repositorio
  - por padrao, grava automaticamente o PATH no perfil do shell
USAGE
}

detect_shell_rc_file() {
  shell_name="$(basename "${SHELL:-}")"
  case "${shell_name}" in
    zsh)
      printf '%s\n' "${HOME}/.zshrc"
      ;;
    bash)
      printf '%s\n' "${HOME}/.bashrc"
      ;;
    *)
      printf '%s\n' "${HOME}/.profile"
      ;;
  esac
}

upsert_path_block() {
  rc_file="$1"
  marker_begin="# >>> nvim-wrappers PATH >>>"
  marker_end="# <<< nvim-wrappers PATH <<<"
  temp_file="$(mktemp)"
  mkdir -p "$(dirname "${rc_file}")"

  if [ -f "${rc_file}" ]; then
    awk -v begin="${marker_begin}" -v end="${marker_end}" '
      $0 == begin { in_block=1; next }
      $0 == end { in_block=0; next }
      !in_block { print }
    ' "${rc_file}" > "${temp_file}"
  fi

  {
    cat "${temp_file}"
    printf '%s\n' "${marker_begin}"
    printf '%s\n' "export PATH=\"${TARGET_DIR}:\$PATH\""
    printf '%s\n' "${marker_end}"
  } > "${rc_file}"

  rm -f "${temp_file}"
}

resolve_real_binary() {
  bin_name="$1"
  shim_path="${TARGET_DIR%/}/${bin_name}"

  direct_candidate="$(command -v "${bin_name}" 2>/dev/null || true)"

  case "${direct_candidate}" in
    "${shim_path}"|"")
      ;;
    *)
      printf '%s\n' "${direct_candidate}"
      return 0
      ;;
  esac

  path_without_shim="$(printf '%s' "${PATH}" | awk -F: -v skip="${TARGET_DIR}" '
    BEGIN { first = 1 }
    {
      for (i = 1; i <= NF; i++) {
        if ($i == "" || $i == skip) {
          continue
        }
        if (!first) {
          printf(":")
        }
        printf("%s", $i)
        first = 0
      }
    }
  ')"

  fallback_candidate="$(PATH="${path_without_shim}" command -v "${bin_name}" 2>/dev/null || true)"
  if [ -n "${fallback_candidate}" ] && [ "${fallback_candidate}" != "${shim_path}" ]; then
    printf '%s\n' "${fallback_candidate}"
    return 0
  fi

  for known_path in "/usr/bin/${bin_name}" "/bin/${bin_name}" "/usr/local/bin/${bin_name}"; do
    if [ -x "${known_path}" ] && [ "${known_path}" != "${shim_path}" ]; then
      printf '%s\n' "${known_path}"
      return 0
    fi
  done

  return 1
}

escape_double_quoted_value() {
  printf '%s' "$1" | sed 's/[\\$"`]/\\&/g'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target-dir)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --rc-file)
      SHELL_RC_FILE="${2:-}"
      shift 2
      ;;
    --ec2-host)
      EC2_HOST="${2:-}"
      CLONE_ORDER="ec2-first"
      shift 2
      ;;
    --ec2-instance-id)
      EC2_INSTANCE_ID="${2:-}"
      if [ -n "${EC2_S3_URI}" ]; then
        CLONE_ORDER="ec2-s3-first"
      fi
      shift 2
      ;;
    --ec2-s3-uri)
      EC2_S3_URI="${2:-}"
      if [ -n "${EC2_INSTANCE_ID}" ]; then
        CLONE_ORDER="ec2-s3-first"
      fi
      shift 2
      ;;
    --clone-order)
      CLONE_ORDER="${2:-}"
      shift 2
      ;;
    --no-shell-profile)
      AUTO_SHELL_PROFILE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "parametro invalido: $1"
      ;;
  esac
done

[ -n "${TARGET_DIR}" ] || die "--target-dir nao pode ser vazio"
case "${CLONE_ORDER}" in
  local-first|git-first|ec2-first|ec2-s3-first)
    ;;
  *)
    die "--clone-order invalido: ${CLONE_ORDER}"
    ;;
esac
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WRAPPER_DIR="${REPO_ROOT}/scripts/wrappers"

[ -x "${WRAPPER_DIR}/git_zip_clone_wrapper.sh" ] || die "wrapper git ausente em ${WRAPPER_DIR}"
[ -x "${WRAPPER_DIR}/curl_python_wrapper.sh" ] || die "wrapper curl ausente em ${WRAPPER_DIR}"

REAL_GIT_BIN="$(resolve_real_binary git || true)"
REAL_CURL_BIN="$(resolve_real_binary curl || true)"
[ -n "${REAL_GIT_BIN}" ] || die "git real nao encontrado no PATH (sem recursao de shim)"
[ -n "${REAL_CURL_BIN}" ] || die "curl real nao encontrado no PATH (sem recursao de shim)"
EC2_HOST_ESCAPED="$(escape_double_quoted_value "${EC2_HOST}")"
EC2_INSTANCE_ID_ESCAPED="$(escape_double_quoted_value "${EC2_INSTANCE_ID}")"
EC2_S3_URI_ESCAPED="$(escape_double_quoted_value "${EC2_S3_URI}")"

mkdir -p "${TARGET_DIR}"

if [ -e "${TARGET_DIR}/git" ] && [ "${FORCE}" != "1" ]; then
  die "shim git ja existe em ${TARGET_DIR}/git (use --force para sobrescrever)"
fi

if [ -e "${TARGET_DIR}/curl" ] && [ "${FORCE}" != "1" ]; then
  die "shim curl ja existe em ${TARGET_DIR}/curl (use --force para sobrescrever)"
fi

# Escrever o shim final de forma segura com escaped dollars.
cat > "${TARGET_DIR}/git" <<EOF2
#!/usr/bin/env sh
set -eu

export GIT_ZIP_WRAPPER_REAL_GIT="${REAL_GIT_BIN}"
export GIT_ZIP_WRAPPER_CLONE_ORDER="\${GIT_ZIP_WRAPPER_CLONE_ORDER:-${CLONE_ORDER}}"
export GIT_ZIP_WRAPPER_EC2_HOST="\${GIT_ZIP_WRAPPER_EC2_HOST:-${EC2_HOST_ESCAPED}}"
export GIT_ZIP_WRAPPER_EC2_INSTANCE_ID="\${GIT_ZIP_WRAPPER_EC2_INSTANCE_ID:-${EC2_INSTANCE_ID_ESCAPED}}"
export GIT_ZIP_WRAPPER_EC2_S3_URI="\${GIT_ZIP_WRAPPER_EC2_S3_URI:-${EC2_S3_URI_ESCAPED}}"
export GIT_ZIP_WRAPPER_ARCHIVE_FORMAT="\${GIT_ZIP_WRAPPER_ARCHIVE_FORMAT:-zip}"
export GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK="\${GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK:-1}"
export GIT_ZIP_WRAPPER_STRICT="\${GIT_ZIP_WRAPPER_STRICT:-0}"
exec "${WRAPPER_DIR}/git_zip_clone_wrapper.sh" "\$@"
EOF2

cat > "${TARGET_DIR}/curl" <<EOF2
#!/usr/bin/env sh
set -eu

export CURL_WRAPPER_REAL_CURL="${REAL_CURL_BIN}"
exec "${WRAPPER_DIR}/curl_python_wrapper.sh" "\$@"
EOF2

chmod +x "${TARGET_DIR}/git" "${TARGET_DIR}/curl"

log "wrappers instalados em ${TARGET_DIR}"

if [ "${AUTO_SHELL_PROFILE}" = "1" ]; then
  if [ -z "${SHELL_RC_FILE}" ]; then
    SHELL_RC_FILE="$(detect_shell_rc_file)"
  fi
  upsert_path_block "${SHELL_RC_FILE}"
  log "PATH configurado automaticamente em ${SHELL_RC_FILE}"
else
  log "adicione no ambiente da maquina do servico:"
  printf 'export PATH="%s:$PATH"\n' "${TARGET_DIR}"
fi
