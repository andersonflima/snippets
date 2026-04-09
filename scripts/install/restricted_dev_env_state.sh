#!/usr/bin/env bash

set -euo pipefail

RESTRICTED_DEV_ENV_STATE_DIR="${HOME}/.config/restricted-dev-env"
RESTRICTED_DEV_ENV_STATE_FILE="${RESTRICTED_DEV_ENV_STATE_DIR}/state.sh"
RESTRICTED_DEV_ENV_HEX_BACKUP_FILE="${RESTRICTED_DEV_ENV_STATE_DIR}/hex.config.backup"
RESTRICTED_DEV_ENV_SHELL_RC_BEGIN="# >>> restricted-dev-env >>>"
RESTRICTED_DEV_ENV_SHELL_RC_END="# <<< restricted-dev-env <<<"
RESTRICTED_DEV_ENV_ELIXIR_LS_BEGIN="# >>> restricted-dev-env-elixir-ls >>>"
RESTRICTED_DEV_ENV_ELIXIR_LS_END="# <<< restricted-dev-env-elixir-ls <<<"

restricted_dev_env_shell_quote() {
  printf "%q" "$1"
}

restricted_dev_env_ensure_state_dir() {
  mkdir -p "${RESTRICTED_DEV_ENV_STATE_DIR}"
}

restricted_dev_env_load_state() {
  if [[ -f "${RESTRICTED_DEV_ENV_STATE_FILE}" ]]; then
    # shellcheck disable=SC1090
    . "${RESTRICTED_DEV_ENV_STATE_FILE}"
  fi
}

restricted_dev_env_remove_exact_block() {
  local target_file begin_marker end_marker tmp_file tmp_suffix
  target_file="$1"
  begin_marker="$2"
  end_marker="$3"
  tmp_suffix="$4"

  [[ -f "${target_file}" ]] || return 0

  tmp_file="$(mktemp "/tmp/restricted-dev-env-${tmp_suffix}.XXXXXX")"
  awk \
    -v begin="${begin_marker}" \
    -v end="${end_marker}" '
    $0 == begin {
      inside_managed_block = 1
      next
    }

    $0 == end {
      inside_managed_block = 0
      next
    }

    inside_managed_block == 1 {
      next
    }

    {
      print
    }
  ' "${target_file}" > "${tmp_file}"

  mv "${tmp_file}" "${target_file}"
}

restricted_dev_env_cleanup_legacy_shell_rc_lines() {
  local rc_file tmp_file
  rc_file="$1"
  [[ -f "${rc_file}" ]] || return 0

  tmp_file="$(mktemp "/tmp/restricted-dev-env-shell-rc.XXXXXX")"
  awk \
    -v begin_marker="${RESTRICTED_DEV_ENV_SHELL_RC_BEGIN}" \
    -v end_marker="${RESTRICTED_DEV_ENV_SHELL_RC_END}" '
    $0 == begin_marker {
      inside_managed_block = 1
      next
    }

    $0 == end_marker {
      inside_managed_block = 0
      next
    }

    inside_managed_block == 1 {
      next
    }

    index($0, ".config/mix-via-ec2-envs.sh") > 0 {
      next
    }

    index($0, ".config/wrapper-envs.sh") > 0 {
      next
    }

    index($0, ".config/restricted-dev-env.fish") > 0 {
      next
    }

    index($0, ".config/mix-hex-envs.sh") > 0 {
      next
    }

    index($0, "# wrapper do mix via EC2") > 0 {
      next
    }

    index($0, "# wrappers de curl/git para ambiente restrito") > 0 {
      next
    }

    {
      print
    }
  ' "${rc_file}" > "${tmp_file}"

  mv "${tmp_file}" "${rc_file}"
}

restricted_dev_env_apply_shell_rc_block() {
  local rc_file env_file
  rc_file="$1"
  shift

  mkdir -p "$(dirname "${rc_file}")"
  touch "${rc_file}"

  restricted_dev_env_cleanup_legacy_shell_rc_lines "${rc_file}"

  {
    printf '\n%s\n' "${RESTRICTED_DEV_ENV_SHELL_RC_BEGIN}"
    for env_file in "$@"; do
      [[ -n "${env_file}" ]] || continue
      printf '[ -f %s ] && . %s\n' \
        "$(restricted_dev_env_shell_quote "${env_file}")" \
        "$(restricted_dev_env_shell_quote "${env_file}")"
    done
    printf '%s\n' "${RESTRICTED_DEV_ENV_SHELL_RC_END}"
  } >> "${rc_file}"
}

restricted_dev_env_apply_shell_rc_fish_block() {
  local rc_file fish_env_file
  rc_file="$1"
  fish_env_file="$2"

  mkdir -p "$(dirname "${rc_file}")"
  touch "${rc_file}"

  restricted_dev_env_cleanup_legacy_shell_rc_lines "${rc_file}"

  {
    printf '\n%s\n' "${RESTRICTED_DEV_ENV_SHELL_RC_BEGIN}"
    printf 'if test -f %s\n' "$(restricted_dev_env_shell_quote "${fish_env_file}")"
    printf '    source %s\n' "$(restricted_dev_env_shell_quote "${fish_env_file}")"
    printf 'end\n'
    printf '%s\n' "${RESTRICTED_DEV_ENV_SHELL_RC_END}"
  } >> "${rc_file}"
}

restricted_dev_env_remove_shell_rc_block() {
  local rc_file
  rc_file="$1"
  restricted_dev_env_cleanup_legacy_shell_rc_lines "${rc_file}"
}

restricted_dev_env_apply_elixir_ls_setup_sh_block() {
  local setup_file env_file
  setup_file="$1"
  shift

  mkdir -p "$(dirname "${setup_file}")"
  {
    cat <<EOF
#!/usr/bin/env sh
# Gerado por scripts/install/setup_restricted_dev_env.sh

case ":\${PATH:-}:" in
  *:/usr/bin:*) ;;
  *)
    if [ -n "\${PATH:-}" ]; then
      PATH="/usr/bin:/bin:/usr/sbin:/sbin:\${PATH}"
    else
      PATH="/usr/bin:/bin:/usr/sbin:/sbin"
    fi
    ;;
esac

export PATH

${RESTRICTED_DEV_ENV_ELIXIR_LS_BEGIN}
EOF
    for env_file in "$@"; do
      [[ -n "${env_file}" ]] || continue
      printf '[ -f "%s" ] && . "%s"\n' "${env_file}" "${env_file}"
    done
    cat <<EOF
${RESTRICTED_DEV_ENV_ELIXIR_LS_END}
EOF
  } > "${setup_file}"
  chmod 0644 "${setup_file}"
}

restricted_dev_env_apply_elixir_ls_setup_fish_block() {
  local setup_file fish_env_file
  setup_file="$1"
  fish_env_file="${2:-}"

  mkdir -p "$(dirname "${setup_file}")"
  {
    cat <<EOF
#!/usr/bin/env fish
# Gerado por scripts/install/setup_restricted_dev_env.sh

if not contains -- /usr/bin \$PATH
    set -gx PATH /usr/bin /bin /usr/sbin /sbin \$PATH
end

${RESTRICTED_DEV_ENV_ELIXIR_LS_BEGIN}
EOF
    if [[ -n "${fish_env_file}" ]]; then
      cat <<EOF
if test -f "${fish_env_file}"
    source "${fish_env_file}"
end
EOF
    fi
    cat <<EOF
${RESTRICTED_DEV_ENV_ELIXIR_LS_END}
EOF
  } > "${setup_file}"
  chmod 0644 "${setup_file}"
}

restricted_dev_env_remove_elixir_ls_setup_sh_block() {
  local setup_file
  setup_file="$1"
  restricted_dev_env_remove_exact_block \
    "${setup_file}" \
    "${RESTRICTED_DEV_ENV_ELIXIR_LS_BEGIN}" \
    "${RESTRICTED_DEV_ENV_ELIXIR_LS_END}" \
    "elixir-ls-setup-sh"
}

restricted_dev_env_remove_elixir_ls_setup_fish_block() {
  local setup_file
  setup_file="$1"
  restricted_dev_env_remove_exact_block \
    "${setup_file}" \
    "${RESTRICTED_DEV_ENV_ELIXIR_LS_BEGIN}" \
    "${RESTRICTED_DEV_ENV_ELIXIR_LS_END}" \
    "elixir-ls-setup-fish"
}

restricted_dev_env_write_state() {
  restricted_dev_env_ensure_state_dir

  cat > "${RESTRICTED_DEV_ENV_STATE_FILE}" <<EOF
#!/usr/bin/env bash
export RESTRICTED_DEV_ENV_STATE_VERSION="1"
export RESTRICTED_DEV_ENV_MANAGED_SHELL_RC=$(restricted_dev_env_shell_quote "${RESTRICTED_DEV_ENV_MANAGED_SHELL_RC:-}")
export RESTRICTED_DEV_ENV_HEX_MANAGED=$(restricted_dev_env_shell_quote "${RESTRICTED_DEV_ENV_HEX_MANAGED:-0}")
export RESTRICTED_DEV_ENV_HEX_CONFIG_PATH=$(restricted_dev_env_shell_quote "${RESTRICTED_DEV_ENV_HEX_CONFIG_PATH:-}")
export RESTRICTED_DEV_ENV_HEX_BACKUP_PATH=$(restricted_dev_env_shell_quote "${RESTRICTED_DEV_ENV_HEX_BACKUP_PATH:-}")
export RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE=$(restricted_dev_env_shell_quote "${RESTRICTED_DEV_ENV_HEX_CONFIG_EXISTED_BEFORE:-0}")
EOF

  chmod 0644 "${RESTRICTED_DEV_ENV_STATE_FILE}"
}

restricted_dev_env_clear_state() {
  rm -f "${RESTRICTED_DEV_ENV_STATE_FILE}" "${RESTRICTED_DEV_ENV_HEX_BACKUP_FILE}"
  rmdir "${RESTRICTED_DEV_ENV_STATE_DIR}" 2>/dev/null || true
}
