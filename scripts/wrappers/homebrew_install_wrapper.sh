#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[homebrew-install-wrapper] %s\n' "$*" >&2
}

die() {
  log "erro: $*"
  exit 1
}

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on)
      return 0
      ;;
  esac
  return 1
}

resolve_real_brew() {
  local self_path shell_path candidate
  self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  if [[ -n "${BREW_WRAPPER_REAL_BREW:-}" ]]; then
    [[ -x "${BREW_WRAPPER_REAL_BREW}" ]] || die "BREW_WRAPPER_REAL_BREW inválido: ${BREW_WRAPPER_REAL_BREW}"
    [[ "${BREW_WRAPPER_REAL_BREW}" != "${self_path}" ]] || die "BREW_WRAPPER_REAL_BREW não pode apontar para o wrapper instalado"
    printf '%s\n' "${BREW_WRAPPER_REAL_BREW}"
    return 0
  fi

  shell_path="$(command -v -p brew 2>/dev/null || true)"
  if [[ -n "${shell_path}" && "${shell_path}" != "${self_path}" ]]; then
    printf '%s\n' "${shell_path}"
    return 0
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" != "${self_path}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done < <(which -a brew 2>/dev/null || true)

  for candidate in /opt/homebrew/bin/brew /usr/local/bin/brew /home/linuxbrew/.linuxbrew/bin/brew; do
    if [[ -x "${candidate}" && "${candidate}" != "${self_path}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

extract_brew_subcommand() {
  local arg
  for arg in "$@"; do
    case "${arg}" in
      --)
        return 1
        ;;
      -*)
        ;;
      *)
        printf '%s\n' "${arg}"
        return 0
        ;;
    esac
  done

  return 1
}

prepend_path_dir() {
  local dir current
  dir="$1"
  current="${2:-}"

  [[ -n "${dir}" ]] || {
    printf '%s\n' "${current}"
    return 0
  }

  if [[ -z "${current}" ]]; then
    printf '%s\n' "${dir}"
    return 0
  fi

  case ":${current}:" in
    *":${dir}:"*)
      printf '%s\n' "${current}"
      ;;
    *)
      printf '%s:%s\n' "${dir}" "${current}"
      ;;
  esac
}

resolve_wrapper_binary() {
  local explicit_value default_value
  explicit_value="$1"
  default_value="$2"

  if [[ -n "${explicit_value}" ]]; then
    printf '%s\n' "${explicit_value}"
    return 0
  fi

  printf '%s\n' "${default_value}"
}

should_wrap_command() {
  local subcommand
  subcommand="$(extract_brew_subcommand "$@" 2>/dev/null || true)"
  [[ "${subcommand}" == "install" ]]
}

configure_backend_failopen_policy() {
  local curl_ec2_required git_ec2_required

  curl_ec2_required="$(resolve_wrapper_binary "${BREW_WRAPPER_CURL_EC2_REQUIRED:-}" "0")"
  git_ec2_required="$(resolve_wrapper_binary "${BREW_WRAPPER_GIT_EC2_REQUIRED:-}" "0")"

  export CURL_WRAPPER_EC2_REQUIRED="${curl_ec2_required}"
  export GIT_ZIP_WRAPPER_EC2_REQUIRED="${git_ec2_required}"
}

configure_install_environment() {
  local curl_bin git_bin curl_dir git_dir

  curl_bin="$(resolve_wrapper_binary "${BREW_WRAPPER_CURL_BIN:-}" "${CURL:-${HOME}/.local/share/curl-python-wrapper/bin/curl}")"
  git_bin="$(resolve_wrapper_binary "${BREW_WRAPPER_GIT_BIN:-}" "${GIT:-${HOME}/.local/share/git-zip-wrapper/bin/git}")"
  curl_dir="$(dirname "${curl_bin}")"
  git_dir="$(dirname "${git_bin}")"

  if [[ -x "${curl_bin}" ]]; then
    export CURL="${curl_bin}"
    export HOMEBREW_CURL_PATH="${curl_bin}"
    PATH="$(prepend_path_dir "${curl_dir}" "${PATH:-}")"
  fi

  if [[ -x "${git_bin}" ]]; then
    export GIT="${git_bin}"
    export HOMEBREW_GIT_PATH="${git_bin}"
    PATH="$(prepend_path_dir "${git_dir}" "${PATH:-}")"
  fi

  export PATH
  configure_backend_failopen_policy

  if is_truthy "${BREW_WRAPPER_NO_AUTO_UPDATE:-0}"; then
    export HOMEBREW_NO_AUTO_UPDATE="1"
  fi
}

main() {
  local real_brew
  real_brew="$(resolve_real_brew || true)"
  [[ -n "${real_brew}" ]] || die "não foi possível localizar brew no PATH"

  if should_wrap_command "$@"; then
    configure_install_environment
  fi

  exec "${real_brew}" "$@"
}

main "$@"
