#!/bin/sh
set -eu

strip_path_entry() {
  target="$1"
  current_path="${2:-}"

  printf '%s' "${current_path}" | awk -v target="${target}" '
    BEGIN {
      split("", parts)
      count = 0
    }
    {
      n = split($0, raw_parts, ":")
      for (i = 1; i <= n; i++) {
        if (raw_parts[i] != "" && raw_parts[i] != target) {
          count++
          parts[count] = raw_parts[i]
        }
      }
    }
    END {
      for (i = 1; i <= count; i++) {
        if (i > 1) {
          printf ":"
        }
        printf "%s", parts[i]
      }
    }
  '
}

PATH="$(strip_path_entry "${HOME}/.local/share/mix-ec2-wrapper/bin" "${PATH:-}")"
PATH="$(strip_path_entry "${HOME}/.local/share/curl-python-wrapper/bin" "${PATH:-}")"
PATH="$(strip_path_entry "${HOME}/.local/share/git-zip-wrapper/bin" "${PATH:-}")"
PATH="$(strip_path_entry "${HOME}/.local/share/nvim-ec2-wrapper/bin" "${PATH:-}")"
export PATH

unset MIX 2>/dev/null || true
unset CURL 2>/dev/null || true
unset GIT 2>/dev/null || true
unset NVIM 2>/dev/null || true

unset MIX_WRAPPER_REAL_MIX 2>/dev/null || true
unset MIX_WRAPPER_REMOTE_COMMANDS 2>/dev/null || true
unset MIX_WRAPPER_DISABLE_REMOTE 2>/dev/null || true
unset MIX_WRAPPER_FORCE_REMOTE 2>/dev/null || true

unset MIX_VIA_EC2_INSTANCE_NAME 2>/dev/null || true
unset MIX_VIA_EC2_AWS_PROFILE 2>/dev/null || true
unset MIX_VIA_EC2_AWS_REGION 2>/dev/null || true
unset MIX_VIA_EC2_S3_BUCKET 2>/dev/null || true
unset MIX_VIA_EC2_S3_PREFIX 2>/dev/null || true
unset MIX_VIA_EC2_SSH_IDENTITY 2>/dev/null || true
unset MIX_VIA_EC2_TRANSPORT 2>/dev/null || true
unset MIX_VIA_EC2_SSH_USER 2>/dev/null || true
unset MIX_VIA_EC2_REMOTE_PROJECT_PATH 2>/dev/null || true
unset MIX_VIA_EC2_CACHE_ROOT 2>/dev/null || true
unset MIX_VIA_EC2_ENTRYPOINT 2>/dev/null || true

unset WRAPPERS_VIA_EC2_ENABLED 2>/dev/null || true
unset WRAPPERS_VIA_EC2_INSTANCE_NAME 2>/dev/null || true
unset WRAPPERS_VIA_EC2_AWS_PROFILE 2>/dev/null || true
unset WRAPPERS_VIA_EC2_AWS_REGION 2>/dev/null || true
unset WRAPPERS_VIA_EC2_S3_BUCKET 2>/dev/null || true
unset WRAPPERS_VIA_EC2_S3_PREFIX 2>/dev/null || true
unset WRAPPERS_VIA_EC2_ALL_URLS 2>/dev/null || true
unset WRAPPERS_VIA_EC2_PROXY 2>/dev/null || true

unset CURL_WRAPPER_REAL_CURL 2>/dev/null || true
unset CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES 2>/dev/null || true
unset CURL_WRAPPER_RELEASE_FALLBACK_REPOS 2>/dev/null || true
unset CURL_WRAPPER_RELEASE_CACHE_DIR 2>/dev/null || true
unset CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS 2>/dev/null || true
unset CURL_WRAPPER_MASON_BUILDERS 2>/dev/null || true
unset CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS 2>/dev/null || true
unset CURL_WRAPPER_MASON_SEED_DIR 2>/dev/null || true
unset CURL_WRAPPER_USE_EC2 2>/dev/null || true
unset CURL_WRAPPER_EC2_ALL_URLS 2>/dev/null || true
unset CURL_WRAPPER_EC2_REQUIRED 2>/dev/null || true
unset CURL_WRAPPER_EC2_PROXY 2>/dev/null || true
unset CURL_WRAPPER_PROXY 2>/dev/null || true
unset CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR 2>/dev/null || true

unset GIT_ZIP_WRAPPER_REAL_GIT 2>/dev/null || true
unset GIT_ZIP_WRAPPER_ARCHIVE_FORMAT 2>/dev/null || true
unset GIT_ZIP_WRAPPER_USE_EC2 2>/dev/null || true
unset GIT_ZIP_WRAPPER_EC2_ALL_URLS 2>/dev/null || true
unset GIT_ZIP_WRAPPER_EC2_REQUIRED 2>/dev/null || true
unset GIT_ZIP_WRAPPER_EC2_PROXY 2>/dev/null || true
unset GIT_ZIP_WRAPPER_PROXY 2>/dev/null || true
unset GIT_ZIP_WRAPPER_CURL_CACERT 2>/dev/null || true
unset GIT_ZIP_WRAPPER_CURL_INSECURE 2>/dev/null || true

unset NVIM_WRAPPER_REAL_NVIM 2>/dev/null || true

rehash 2>/dev/null || true
hash -r 2>/dev/null || true

printf '[deactivate-restricted-dev-env] ambiente removido da sessão atual\n' >&2
