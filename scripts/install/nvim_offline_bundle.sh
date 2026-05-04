#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[nvim-offline-bundle] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

usage() {
  cat <<'USAGE'
Uso:
  scripts/install/nvim_offline_bundle.sh <snapshot|restore> [opções]

Ações:
  snapshot   Copia a configuração e instalados atuais do Neovim para o bundle.
  restore    Restaura a configuração e os instalados do bundle para o usuário atual.

Opções:
  --bundle-dir <dir>            Diretório destino/origem do bundle.
  --source-config <dir>         Caminho de origem do config nvim (snapshot).
  --source-data <dir>           Caminho de origem do data nvim (snapshot).
  --target-config <dir>         Caminho de destino do config nvim (restore).
  --target-data <dir>           Caminho de destino do data nvim (restore).
  --data-components <lista>     Componentes do data a incluir (snapshot).
                                Padrão: lazy,mason.
  --no-bootstrap                Não executar bootstrap automático do Lazy/Mason após restore.
  --bootstrap-timeout <segundos> Timeout por pacote no bootstrap. Padrão: 1200.
  --no-config                   Não copiar/configurar .config/nvim.
  --no-data                     Não copiar/configurar dados de runtime.
  --backup                      Faz backup dos diretórios existentes no restore.
  --overwrite                   Substitui diretórios existentes sem backup.
  --copy-wrapper-env            Copia também ~/.config/wrapper-envs.sh e estado do restricted-dev-env.
  -h, --help                    Mostra esta ajuda.

Observações:
  snapshot:
    --source-config e --source-data usam valores padrão do usuário atual.
    o bundle é salvo em $BUNDLE_DIR.
  restore:
    --target-config e --target-data usam valores padrão do usuário atual.
    diretórios existentes geram backup com sufixo .offline-backup.<timestamp>.
  snapshot:
    ao usar --no-data, o script apenas captura a lista de pacotes Mason para bootstrap.
USAGE
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)"

MODE=""
BUNDLE_DIR="${REPO_ROOT}/nvim-offline-bundle"
SOURCE_CONFIG_DIR="${HOME}/.config/nvim"
SOURCE_DATA_DIR="${HOME}/.local/share/nvim"
TARGET_CONFIG_DIR="${HOME}/.config/nvim"
TARGET_DATA_DIR="${HOME}/.local/share/nvim"
DATA_COMPONENTS="lazy,mason"
DO_CONFIG="1"
DO_DATA="1"
DO_BACKUP="1"
DO_OVERWRITE="0"
COPY_WRAPPER_ENV="0"
DO_BOOTSTRAP="1"
BOOTSTRAP_TIMEOUT_SECONDS="1200"

copy_tree() {
  local source_dir target_dir source_path target_path
  source_path="${1}"
  target_path="${2}"

  if [[ ! -d "${source_path}" ]]; then
    die "diretório origem inválido: ${source_path}"
  fi

  mkdir -p "${target_path%/*}"
  rm -rf "${target_path}"

  if command -v rsync >/dev/null 2>&1; then
    rsync -a "${source_path}/" "${target_path}/"
    return 0
  fi

  mkdir -p "${target_path}"
  cp -a "${source_path}/." "${target_path}/"
}

copy_file() {
  local source_file target_file
  source_file="${1}"
  target_file="${2}"

  if [[ ! -f "${source_file}" ]]; then
    return 0
  fi

  mkdir -p "${target_file%/*}"
  cp -a "${source_file}" "${target_file}"
}

copy_data_components() {
  local bundle_data_dir source_data_dir component candidate source_path target_path
  bundle_data_dir="${1}"
  source_data_dir="${2}"

  IFS=',' read -r -a data_components <<< "${DATA_COMPONENTS}"
  for component in "${data_components[@]}"; do
    component="${component//[$'\t\r\n ']/}"
    [[ -n "${component}" ]] || continue
    source_path="${source_data_dir}/${component}"
    target_path="${bundle_data_dir}/${component}"

    if [[ -d "${source_path}" ]]; then
      copy_tree "${source_path}" "${target_path}"
      continue
    fi

    log "componente inexistente no source-data e ignorado: ${source_path}"
  done
}

collect_mason_packages_snapshot() {
  local bundle_data_dir source_mason_dir output_file entry package_name
  source_mason_dir="${1}"
  output_file="${2}"

  mkdir -p "$(dirname "${output_file}")"
  rm -f "${output_file}"

  [[ -d "${source_mason_dir}" ]] || return 0

  for entry in "${source_mason_dir}"/*; do
    [[ -e "${entry}" ]] || continue
    [[ -d "${entry}" ]] || continue
    package_name="$(basename "${entry}")"
    [[ -n "${package_name}" ]] || continue
    printf '%s\n' "${package_name}" >> "${output_file}"
  done

  sort -u "${output_file}" -o "${output_file}" 2>/dev/null || true
}

build_packages_from_bundle_file() {
  local file_path="${1}"

  [[ -f "${file_path}" ]] || { echo ""; return 0; }

  awk 'NF {printf "%s,", $0}' "${file_path}" | sed 's/,$//'
}

run_restore_bootstrap() {
  local packages_file packages_list bootstrap_args env_file

  [[ -x "${SCRIPT_DIR}/bootstrap_lazyvim_mason.sh" ]] || return 0
  command -v nvim >/dev/null 2>&1 || {
    log "nvim não encontrado; bootstrap de lazy/mason não executado"
    return 0
  }

  env_file="${HOME}/.config/wrapper-envs.sh"
  if [[ -f "${env_file}" ]]; then
    # shellcheck disable=SC1090
    . "${env_file}"
  fi

  packages_file="${BUNDLE_DIR}/mason-packages.txt"
  packages_list="$(build_packages_from_bundle_file "${packages_file}")"

  bootstrap_args=(--env-file "${env_file}" --bootstrap-timeout "${BOOTSTRAP_TIMEOUT_SECONDS}")
  [[ -n "${packages_list}" ]] && bootstrap_args+=(--mason-packages "${packages_list}")

  log "executando bootstrap lazy/mason na máquina atual"
  sh "${SCRIPT_DIR}/bootstrap_lazyvim_mason.sh" "${bootstrap_args[@]}"
}

restore_item() {
  local source_path target_path
  source_path="${1}"
  target_path="${2}"

  [[ -e "${source_path}" ]] || return 0

  if [[ -e "${target_path}" ]]; then
    if [[ "${DO_OVERWRITE}" == "1" ]]; then
      rm -rf "${target_path}"
    elif [[ "${DO_BACKUP}" == "1" ]]; then
      local backup_path timestamp
      timestamp="$(date +%Y%m%d_%H%M%S)"
      backup_path="${target_path}.offline-backup.${timestamp}"
      mv "${target_path}" "${backup_path}"
      log "backup criado: ${backup_path}"
    else
      die "destino já existe e --overwrite não foi informado: ${target_path}"
    fi
  fi

  mkdir -p "${target_path%/*}"
  if [[ -d "${source_path}" ]]; then
    copy_tree "${source_path}" "${target_path}"
    return 0
  fi

  cp -a "${source_path}" "${target_path}"
}

generate_manifest() {
  local manifest_path bundle_config_dir bundle_data_dir lazy_dir mason_dir
  manifest_path="${BUNDLE_DIR}/MANIFEST.md"
  bundle_config_dir="${1}"
  bundle_data_dir="${2}"

  lazy_dir="${bundle_data_dir}/lazy"
  mason_dir="${bundle_data_dir}/mason"

  {
    printf '# Neovim Offline Bundle\\n'
    printf 'Gerado: %s\\n' "$(date -R)"
    printf 'Origem config: %s\\n' "${SOURCE_CONFIG_DIR}"
    printf 'Origem data: %s\\n' "${SOURCE_DATA_DIR}"
    printf 'Componentes de dados: %s\\n' "${DATA_COMPONENTS}"
    printf '\\n'
    printf '## Conteúdo do bundle\\n'
    printf '- config/nvim: %s\\n' "${bundle_config_dir}"
    printf '- data/nvim: %s\\n' "${bundle_data_dir}"
    printf '- dados opcionais: wrapper-envs/state (se coletados): yes\\n'
    printf '\\n'
    printf '## Resumo\\n'
    if [[ -f "${bundle_config_dir}/lazy-lock.json" ]]; then
      printf 'lazy-lock.json: presente\\n'
    else
      printf 'lazy-lock.json: ausente\\n'
    fi
    if [[ -d "${lazy_dir}" ]]; then
      printf 'plugins em lazy: %s\\n' "$(find "${lazy_dir}" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | tr -d ' ')"
    else
      printf 'plugins em lazy: 0\\n'
    fi
    if [[ -d "${mason_dir}" ]]; then
      printf 'pacotes Mason: %s\\n' "$(find "${mason_dir}" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | grep -E '/packages/[^/]+' | wc -l | tr -d ' ')"
    else
      printf 'pacotes Mason: 0\\n'
    fi
  } > "${manifest_path}"
}

snapshot_bundle() {
  local bundle_config_dir bundle_data_dir
  bundle_config_dir="${BUNDLE_DIR}/config/nvim"
  bundle_data_dir="${BUNDLE_DIR}/data/nvim"

  mkdir -p "${BUNDLE_DIR}"
  rm -rf "${bundle_config_dir}" "${bundle_data_dir}"

  if [[ "${DO_CONFIG}" == "1" ]]; then
    copy_tree "${SOURCE_CONFIG_DIR}" "${bundle_config_dir}"
  fi

  if [[ "${DO_DATA}" == "1" ]]; then
    copy_data_components "${bundle_data_dir}" "${SOURCE_DATA_DIR}"
  fi

  collect_mason_packages_snapshot \
    "${SOURCE_DATA_DIR}/mason/packages" \
    "${BUNDLE_DIR}/mason-packages.txt"

  if [[ "${COPY_WRAPPER_ENV}" == "1" ]]; then
    copy_file "${HOME}/.config/wrapper-envs.sh" "${BUNDLE_DIR}/wrapper-envs.sh"
    copy_file "${HOME}/.config/restricted-dev-env/state.sh" "${BUNDLE_DIR}/restricted-dev-env-state.sh"
  fi

  generate_manifest "${bundle_config_dir}" "${bundle_data_dir}"
  if command -v du >/dev/null 2>&1; then
    log "bundle criado em ${BUNDLE_DIR} ($(du -sh "${BUNDLE_DIR}" | awk '{print $1}'))"
  else
    log "bundle criado em ${BUNDLE_DIR}"
  fi
}

restore_bundle() {
  local bundle_config_dir bundle_data_dir
  bundle_config_dir="${BUNDLE_DIR}/config/nvim"
  bundle_data_dir="${BUNDLE_DIR}/data/nvim"

  [[ "${DO_CONFIG}" == "1" ]] || bundle_config_dir=""
  [[ "${DO_DATA}" == "1" ]] || bundle_data_dir=""

  [[ -d "${bundle_config_dir:-${BUNDLE_DIR}}" ]] || [[ -d "${bundle_data_dir:-${BUNDLE_DIR}}" ]] || die "bundle vazio ou inválido: ${BUNDLE_DIR}"
  [[ -f "${BUNDLE_DIR}/MANIFEST.md" ]] || log "manifest não encontrado; prosseguindo sem validação"

  if [[ "${DO_CONFIG}" == "1" ]]; then
    restore_item "${bundle_config_dir}" "${TARGET_CONFIG_DIR}"
  fi

  if [[ "${DO_DATA}" == "1" ]]; then
    restore_item "${bundle_data_dir}" "${TARGET_DATA_DIR}"
  fi

  if [[ "${COPY_WRAPPER_ENV}" == "1" ]]; then
    restore_item "${BUNDLE_DIR}/wrapper-envs.sh" "${HOME}/.config/wrapper-envs.sh"
    restore_item "${BUNDLE_DIR}/restricted-dev-env-state.sh" "${HOME}/.config/restricted-dev-env/state.sh"
  fi

  log "restore concluído."
  if [[ "${DO_BOOTSTRAP}" == "1" ]]; then
    run_restore_bootstrap
  else
    log "próximo passo recomendado: executar nvim --headless '+Lazy! sync' +qa e validar."
  fi
}

while [[ $# -gt 0 ]]; do
  case "${1}" in
    snapshot|restore)
      MODE="${1}"
      shift
      ;;
    --bundle-dir)
      BUNDLE_DIR="${2:-}"
      shift 2
      ;;
    --source-config)
      SOURCE_CONFIG_DIR="${2:-}"
      shift 2
      ;;
    --source-data)
      SOURCE_DATA_DIR="${2:-}"
      shift 2
      ;;
    --target-config)
      TARGET_CONFIG_DIR="${2:-}"
      shift 2
      ;;
    --target-data)
      TARGET_DATA_DIR="${2:-}"
      shift 2
      ;;
    --data-components)
      DATA_COMPONENTS="${2:-}"
      shift 2
      ;;
    --no-config)
      DO_CONFIG="0"
      shift
      ;;
    --no-data)
      DO_DATA="0"
      shift
      ;;
    --overwrite)
      DO_OVERWRITE="1"
      DO_BACKUP="0"
      shift
      ;;
    --backup)
      DO_BACKUP="1"
      DO_OVERWRITE="0"
      shift
      ;;
    --copy-wrapper-env)
      COPY_WRAPPER_ENV="1"
      shift
      ;;
    --no-bootstrap)
      DO_BOOTSTRAP="0"
      shift
      ;;
    --bootstrap-timeout)
      BOOTSTRAP_TIMEOUT_SECONDS="${2:-1200}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "opção inválida: ${1}"
      ;;
  esac
done

[[ -n "${MODE}" ]] || {
  usage
  exit 1
}

case "${MODE}" in
  snapshot)
    [[ "${DO_CONFIG}" == "1" ]] && [[ -d "${SOURCE_CONFIG_DIR}" ]] || {
      [[ "${DO_CONFIG}" == "1" ]] && die "source-config não encontrado: ${SOURCE_CONFIG_DIR}"
    }
    [[ "${DO_DATA}" == "1" ]] && [[ -d "${SOURCE_DATA_DIR}" ]] || {
      [[ "${DO_DATA}" == "1" ]] && die "source-data não encontrado: ${SOURCE_DATA_DIR}"
    }
    snapshot_bundle
    ;;
  restore)
    [[ -d "${BUNDLE_DIR}" ]] || die "bundle-dir não encontrado: ${BUNDLE_DIR}"
    restore_bundle
    ;;
  *)
    die "ação inválida: ${MODE}"
    ;;
esac
