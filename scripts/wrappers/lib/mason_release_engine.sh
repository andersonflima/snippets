CURL_WRAPPER_RELEASE_CACHE_DIR="${CURL_WRAPPER_RELEASE_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME:-/tmp}/.cache}/curl-python-wrapper/releases}"
CURL_WRAPPER_MASON_BUILDERS="${CURL_WRAPPER_MASON_BUILDERS:-elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish}"
CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS="${CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS:-tar.gz,tgz,tar}"
CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS="${CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS:-elixir-lsp/elixir-ls,omnisharp/omnisharp-roslyn}"

mason_release_cache_path() {
  local slug tag asset cache_root asset_name
  slug="$(normalize_repo_slug "${1:-}")"
  tag="${2:-}"
  asset_name="$(basename "${3:-}")"
  cache_root="${CURL_WRAPPER_RELEASE_CACHE_DIR%/}"
  printf '%s/%s/%s/%s\n' "${cache_root}" "${slug}" "${tag}" "${asset_name}"
}

mason_release_restore_cached_artifact() {
  local slug tag asset output_path cache_path
  slug="$1"
  tag="$2"
  asset="$3"
  output_path="$4"
  cache_path="$(mason_release_cache_path "${slug}" "${tag}" "${asset}")"

  [[ -s "${cache_path}" ]] || return 1
  mkdir -p "$(dirname "${output_path}")"
  cp "${cache_path}" "${output_path}"
}

mason_release_store_cached_artifact() {
  local slug tag asset source_path cache_path
  slug="$1"
  tag="$2"
  asset="$3"
  source_path="$4"
  cache_path="$(mason_release_cache_path "${slug}" "${tag}" "${asset}")"

  mkdir -p "$(dirname "${cache_path}")"
  cp "${source_path}" "${cache_path}"
}

mason_release_fetch_metadata_json() {
  local owner repo tag output_path api_path api_url
  owner="$1"
  repo="$2"
  tag="$3"
  output_path="$4"
  api_path="/repos/${owner}/${repo}/releases/tags/${tag}"
  api_url="https://api.github.com${api_path}"

  mkdir -p "$(dirname "${output_path}")"

  if (
       CURL_FALLBACK_URL="${api_url}"
       CURL_FALLBACK_OUTPUT="${output_path}"
       CURL_FALLBACK_USER_AGENT="${CURL_FALLBACK_USER_AGENT:-curl-python-wrapper}"
       CURL_FALLBACK_CONNECT_TIMEOUT="${CURL_FALLBACK_CONNECT_TIMEOUT:-20}"
       CURL_FALLBACK_MAX_TIME="${CURL_FALLBACK_MAX_TIME:-300}"
       CURL_FALLBACK_HEADERS=$'Accept: application/vnd.github+json\nX-GitHub-Api-Version: 2022-11-28'
       CURL_FALLBACK_PROXY="${CURL_FALLBACK_PROXY:-${CURL_WRAPPER_ACTIVE_PROXY:-}}"
       CURL_FALLBACK_ALLOW_REDIRECTS="1"
       CURL_FALLBACK_CREATE_DIRS="1"
       CURL_FALLBACK_INSECURE="${CURL_FALLBACK_INSECURE:-0}"
       download_url_with_real_curl "${api_url}" "${output_path}" "1"
     ); then
    return 0
  fi

  if command -v gh >/dev/null 2>&1; then
    if gh api \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "${api_path}" > "${output_path}" 2>/dev/null; then
      return 0
    fi
  fi

  (
    CURL_FALLBACK_URL="${api_url}"
    CURL_FALLBACK_OUTPUT="${output_path}"
    CURL_FALLBACK_USER_AGENT="${CURL_FALLBACK_USER_AGENT:-curl-python-wrapper}"
    CURL_FALLBACK_CONNECT_TIMEOUT="${CURL_FALLBACK_CONNECT_TIMEOUT:-20}"
    CURL_FALLBACK_MAX_TIME="${CURL_FALLBACK_MAX_TIME:-300}"
    CURL_FALLBACK_HEADERS=$'Accept: application/vnd.github+json\nX-GitHub-Api-Version: 2022-11-28'
    CURL_FALLBACK_PROXY="${CURL_FALLBACK_PROXY:-${CURL_WRAPPER_ACTIVE_PROXY:-}}"
    CURL_FALLBACK_ALLOW_REDIRECTS="1"
    CURL_FALLBACK_CREATE_DIRS="1"
    CURL_FALLBACK_INSECURE="${CURL_FALLBACK_INSECURE:-0}"
    download_with_python_requests
  )
}

mason_release_extract_asset_id_from_metadata() {
  local metadata_path asset_name
  metadata_path="$1"
  asset_name="$2"

  python3 - "${metadata_path}" "${asset_name}" <<'PY'
import json
import sys

metadata_path, asset_name = sys.argv[1:3]

with open(metadata_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

for asset in payload.get("assets", []) if isinstance(payload, dict) else []:
    if not isinstance(asset, dict):
        continue
    if str(asset.get("name", "")).strip() == asset_name:
        print(asset.get("id", ""))
        raise SystemExit(0)

raise SystemExit(1)
PY
}

mason_release_fetch_asset_id() {
  local owner repo tag asset_name tmp_dir metadata_path asset_id
  owner="$1"
  repo="$2"
  tag="$3"
  asset_name="$4"
  tmp_dir="$(mktemp -d -t mason-release-asset-id-XXXXXX)"
  metadata_path="${tmp_dir}/release.json"

  if ! mason_release_fetch_metadata_json "${owner}" "${repo}" "${tag}" "${metadata_path}"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  asset_id="$(
    mason_release_extract_asset_id_from_metadata "${metadata_path}" "${asset_name}" 2>/dev/null || true
  )"
  rm -rf "${tmp_dir}"
  [[ -n "${asset_id}" ]] || return 1
  printf '%s\n' "${asset_id}"
}

mason_release_discover_archive_asset_name() {
  local owner repo tag requested_asset slug tmp_dir metadata_path discovered_asset derived_asset
  owner="$1"
  repo="$2"
  tag="$3"
  requested_asset="$4"
  slug="$(normalize_repo_slug "${owner}/${repo}")"
  tmp_dir="$(mktemp -d -t mason-release-meta-XXXXXX)"
  metadata_path="${tmp_dir}/release.json"

  if mason_release_fetch_metadata_json "${owner}" "${repo}" "${tag}" "${metadata_path}"; then
    discovered_asset="$(
      python3 - "${metadata_path}" "${requested_asset}" "${CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS}" <<'PY'
import json
import os
import re
import sys
from difflib import SequenceMatcher

metadata_path, requested_asset, raw_extensions = sys.argv[1:4]
allowed_extensions = [item.strip().lower() for item in raw_extensions.split(",") if item.strip()]

ALIASES = {
    "amd64": "x64",
    "x86_64": "x64",
    "x64": "x64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "windows": "win",
    "win32": "win",
    "macos": "darwin",
    "osx": "darwin",
    "darwin": "darwin",
    "linux": "linux",
    "musl": "musl",
    "gnu": "gnu",
    "glibc": "gnu",
}

CATEGORIES = {
    "linux": "os",
    "darwin": "os",
    "win": "os",
    "x64": "arch",
    "arm64": "arch",
    "arm": "arch",
    "musl": "libc",
    "gnu": "libc",
}

EXTENSION_ORDER = {
    "tar.gz": 80,
    "tgz": 75,
    "tar": 65,
}


def split_extension(name: str):
    lower = name.lower()
    for suffix in (".tar.gz", ".tgz", ".tar", ".zip"):
        if lower.endswith(suffix):
            return name[: -len(suffix)], suffix[1:]
    stem, ext = os.path.splitext(name)
    return stem, ext[1:].lower()


def tokenize(value: str):
    return [segment for segment in re.split(r"[^a-z0-9]+", value.lower()) if segment]


def canonicalize_tokens(tokens):
    normalized = []
    for token in tokens:
      normalized.append(ALIASES.get(token, token))
    return normalized


def categorize(tokens):
    categories = {}
    for token in tokens:
        category = CATEGORIES.get(token)
        if category and category not in categories:
            categories[category] = token
    return categories


with open(metadata_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

requested_base, _ = split_extension(requested_asset)
requested_tokens = canonicalize_tokens(tokenize(requested_base))
requested_token_set = set(requested_tokens)
requested_categories = categorize(requested_tokens)

assets = payload.get("assets", []) if isinstance(payload, dict) else []
asset_names = {}

for asset in assets:
    if not isinstance(asset, dict):
        continue
    candidate_name = str(asset.get("name", "")).strip()
    if candidate_name:
        asset_names[candidate_name.lower()] = candidate_name

for extension in allowed_extensions:
    direct_twin = f"{requested_base}.{extension}".lower()
    if direct_twin in asset_names:
        print(asset_names[direct_twin])
        raise SystemExit(0)

best_name = ""
best_score = None

for asset in assets:
    if not isinstance(asset, dict):
        continue
    candidate_name = str(asset.get("name", "")).strip()
    if not candidate_name:
        continue

    candidate_base, candidate_extension = split_extension(candidate_name)
    if candidate_extension not in allowed_extensions:
        continue

    candidate_tokens = canonicalize_tokens(tokenize(candidate_base))
    candidate_token_set = set(candidate_tokens)
    candidate_categories = categorize(candidate_tokens)

    score = 0
    if candidate_base == requested_base:
        score += 1200
    if candidate_base.startswith(requested_base) or requested_base.startswith(candidate_base):
        score += 250

    score += int(SequenceMatcher(None, requested_asset.lower(), candidate_name.lower()).ratio() * 100)
    score += len(requested_token_set & candidate_token_set) * 45
    score -= abs(len(candidate_tokens) - len(requested_tokens)) * 4
    score += EXTENSION_ORDER.get(candidate_extension, 0)

    for category, requested_value in requested_categories.items():
        candidate_value = candidate_categories.get(category)
        if candidate_value == requested_value:
            score += 100
        elif candidate_value is None:
            score -= 10
        else:
            score -= 140

    if best_score is None or score > best_score:
        best_score = score
        best_name = candidate_name

print(best_name)
PY
    )"
  else
    discovered_asset=""
  fi

  rm -rf "${tmp_dir}"

  if [[ -n "${discovered_asset}" ]]; then
    printf '%s\n' "${discovered_asset}"
    return 0
  fi

  derived_asset="$(mason_release_derive_archive_asset_name "${slug}" "${requested_asset}" 2>/dev/null || true)"
  [[ -n "${derived_asset}" ]] || return 1
  printf '%s\n' "${derived_asset}"
}

mason_release_derive_archive_asset_name() {
  local slug asset
  slug="$(normalize_repo_slug "${1:-}")"
  asset="$2"

  [[ "${asset}" == *.zip ]] || return 1

  case "${slug}" in
    */*)
      printf '%s\n' "${asset%.zip}.tar.gz"
      return 0
      ;;
  esac

  return 1
}

mason_release_extract_archive() {
  local archive_path destination_path
  archive_path="$1"
  destination_path="$2"

  mkdir -p "${destination_path}"

  case "${archive_path}" in
    *.tar.gz|*.tgz)
      tar -xzf "${archive_path}" -C "${destination_path}"
      return 0
      ;;
    *.tar)
      tar -xf "${archive_path}" -C "${destination_path}"
      return 0
      ;;
  esac

  return 1
}

resolve_single_extracted_root() {
  local base_dir
  base_dir="$1"

  mapfile -t extracted_entries < <(find "${base_dir}" -mindepth 1 -maxdepth 1)

  if [[ "${#extracted_entries[@]}" -eq 1 && -d "${extracted_entries[0]}" ]]; then
    printf '%s\n' "${extracted_entries[0]}"
    return 0
  fi

  printf '%s\n' "${base_dir}"
}

pack_directory_as_zip() {
  local source_dir output_path
  source_dir="$1"
  output_path="$2"

  command -v python3 >/dev/null 2>&1 || die "python3 é obrigatório para empacotar artefato zip local"
  mkdir -p "$(dirname "${output_path}")"

  python3 - "${source_dir}" "${output_path}" <<'PY'
import os
import sys
import zipfile

source_dir, output_path = sys.argv[1], sys.argv[2]

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as archive:
    for root, dirs, files in os.walk(source_dir):
        dirs.sort()
        files.sort()
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, source_dir)
            archive.write(full_path, rel_path)
PY
}

mason_release_repackage_archive_as_zip() {
  local owner repo tag requested_asset output_path resolved_asset tmp_dir archive_path extracted_dir package_root
  owner="$1"
  repo="$2"
  tag="$3"
  requested_asset="$4"
  output_path="$5"

  resolved_asset="$(mason_release_discover_archive_asset_name "${owner}" "${repo}" "${tag}" "${requested_asset}" 2>/dev/null || true)"
  [[ -n "${resolved_asset}" ]] || return 1

  tmp_dir="$(mktemp -d -t mason-release-artifact-XXXXXX)"
  archive_path="${tmp_dir}/$(basename "${resolved_asset}")"
  extracted_dir="${tmp_dir}/extract"

  if ! download_release_asset_by_name "${owner}" "${repo}" "${tag}" "${resolved_asset}" "${archive_path}"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  if ! mason_release_extract_archive "${archive_path}" "${extracted_dir}"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  package_root="$(resolve_single_extracted_root "${extracted_dir}")"
  pack_directory_as_zip "${package_root}" "${output_path}"
  rm -rf "${tmp_dir}"
}

build_elixir_ls_release_zip() {
  local owner repo tag output_path tmp_dir source_tarball source_extract source_root release_dir release_task
  owner="$1"
  repo="$2"
  tag="$3"
  output_path="$4"

  command -v elixir >/dev/null 2>&1 || return 1
  command -v mix >/dev/null 2>&1 || return 1
  command -v tar >/dev/null 2>&1 || return 1

  tmp_dir="$(mktemp -d -t curl-wrapper-elixirls-XXXXXX)"
  source_tarball="${tmp_dir}/source.tar.gz"
  source_extract="${tmp_dir}/source"
  release_dir="${tmp_dir}/release"
  mkdir -p "${source_extract}" "${release_dir}"

  if ! download_source_tarball_for_tag "${owner}" "${repo}" "${tag}" "${source_tarball}"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  tar -xzf "${source_tarball}" -C "${source_extract}"
  source_root="$(resolve_single_extracted_root "${source_extract}")"
  release_task="elixir_ls.release"

  if [[ -f "${source_root}/apps/elixir_ls_utils/lib/mix.tasks.elixir_ls.release2.ex" ]]; then
    release_task="elixir_ls.release2 --destination"
  fi

  (
    cd "${source_root}"
    mix local.hex --force >/dev/null 2>&1 || true
    mix local.rebar --force >/dev/null 2>&1 || true
    mix deps.get >/dev/null 2>&1 || true
    case "${release_task}" in
      "elixir_ls.release2 --destination")
        mix elixir_ls.release2 --destination "${release_dir}"
        ;;

      *)
        mix elixir_ls.release -o "${release_dir}"
        ;;
    esac
  ) >/dev/null 2>&1 || {
    rm -rf "${tmp_dir}"
    return 1
  }

  pack_directory_as_zip "${release_dir}" "${output_path}"
  rm -rf "${tmp_dir}"
}

mason_release_prefers_source_builder() {
  local slug candidate normalized_candidate
  slug="$(normalize_repo_slug "${1:-}")"
  [[ -n "${slug}" ]] || return 1

  IFS=',' read -r -a source_build_repos <<< "${CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS}"
  for candidate in "${source_build_repos[@]}"; do
    normalized_candidate="$(normalize_repo_slug "${candidate}")"
    [[ -n "${normalized_candidate}" ]] || continue
    if [[ "${normalized_candidate}" == "${slug}" ]]; then
      return 0
    fi
  done

  return 1
}

parse_omnisharp_requested_asset() {
  local requested_asset base_without_extension framework runtime_id
  requested_asset="$1"

  case "${requested_asset}" in
    omnisharp-*-net*.zip)
      base_without_extension="${requested_asset%.zip}"
      framework="${base_without_extension##*-}"
      runtime_id="${base_without_extension#omnisharp-}"
      runtime_id="${runtime_id%-${framework}}"
      ;;

    *)
      return 1
      ;;
  esac

  [[ -n "${runtime_id}" && -n "${framework}" ]] || return 1
  printf '%s\t%s\n' "${runtime_id}" "${framework}"
}

build_omnisharp_source_zip() {
  local owner repo tag requested_asset output_path tmp_dir source_tarball source_extract source_root release_dir runtime_id framework project_file
  owner="$1"
  repo="$2"
  tag="$3"
  requested_asset="$4"
  output_path="$5"

  command -v dotnet >/dev/null 2>&1 || return 1
  command -v tar >/dev/null 2>&1 || return 1

  read -r runtime_id framework <<EOF
$(parse_omnisharp_requested_asset "${requested_asset}" 2>/dev/null || printf 'linux-x64\tnet6.0')
EOF

  tmp_dir="$(mktemp -d -t curl-wrapper-omnisharp-XXXXXX)"
  source_tarball="${tmp_dir}/source.tar.gz"
  source_extract="${tmp_dir}/source"
  release_dir="${tmp_dir}/release"
  mkdir -p "${source_extract}" "${release_dir}"

  if ! download_source_tarball_for_tag "${owner}" "${repo}" "${tag}" "${source_tarball}"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  tar -xzf "${source_tarball}" -C "${source_extract}"
  source_root="$(resolve_single_extracted_root "${source_extract}")"
  project_file="${source_root}/src/OmniSharp.Stdio.Driver/OmniSharp.Stdio.Driver.csproj"
  [[ -f "${project_file}" ]] || {
    rm -rf "${tmp_dir}"
    return 1
  }

  (
    cd "${source_root}"
    export DOTNET_CLI_TELEMETRY_OPTOUT=1
    export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
    export NUGET_XMLDOC_MODE=skip

    dotnet restore "${project_file}" --runtime "${runtime_id}" >/dev/null 2>&1 || true

    dotnet publish "${project_file}" \
      --configuration Release \
      --framework "${framework}" \
      --runtime "${runtime_id}" \
      --output "${release_dir}" \
      --self-contained false \
      -p:PublishReadyToRun=false \
      -p:UseAppHost=false \
      -p:RollForward=LatestMajor

    cp license.md "${release_dir}/license.md" 2>/dev/null || true
  ) >/dev/null 2>&1 || {
    rm -rf "${tmp_dir}"
    return 1
  }

  pack_directory_as_zip "${release_dir}" "${output_path}"
  rm -rf "${tmp_dir}"
}

mason_release_resolve_builder() {
  local slug entry repo_part builder_part normalized_repo
  slug="$(normalize_repo_slug "${1:-}")"
  [[ -n "${slug}" ]] || return 1

  IFS=',' read -r -a builder_entries <<< "${CURL_WRAPPER_MASON_BUILDERS}"
  for entry in "${builder_entries[@]}"; do
    repo_part="${entry%%=*}"
    builder_part="${entry#*=}"
    normalized_repo="$(normalize_repo_slug "${repo_part}")"
    [[ -n "${normalized_repo}" && -n "${builder_part}" ]] || continue
    if [[ "${normalized_repo}" == "${slug}" ]]; then
      printf '%s\n' "${builder_part}"
      return 0
    fi
  done

  return 1
}

mason_release_run_builder() {
  local builder_id owner repo tag output_path requested_asset
  builder_id="$1"
  owner="$2"
  repo="$3"
  tag="$4"
  output_path="$5"
  requested_asset="${6:-}"

  case "${builder_id}" in
    elixir_ls_release)
      build_elixir_ls_release_zip "${owner}" "${repo}" "${tag}" "${output_path}"
      return $?
      ;;

    omnisharp_source_publish)
      build_omnisharp_source_zip "${owner}" "${repo}" "${tag}" "${requested_asset}" "${output_path}"
      return $?
      ;;
  esac

  return 1
}

handle_smart_release_asset() {
  local output_path tmp_dir generated_artifact builder_id prefer_source_builder
  output_path="${CURL_FALLBACK_OUTPUT:-}"

  [[ -n "${output_path}" ]] || return 1
  [[ "${output_path}" == *.zip ]] || return 1
  parse_github_release_asset_url "${CURL_FALLBACK_URL:-}" || return 1

  if mason_release_restore_cached_artifact \
    "${GITHUB_RELEASE_SLUG}" \
    "${GITHUB_RELEASE_TAG}" \
    "${GITHUB_RELEASE_ASSET}" \
    "${output_path}"; then
    log "artefato Mason restaurado do cache local para ${GITHUB_RELEASE_SLUG}"
    return 0
  fi

  tmp_dir="$(mktemp -d -t mason-release-smart-XXXXXX)"
  generated_artifact="${tmp_dir}/$(basename "${output_path}")"

  builder_id="$(mason_release_resolve_builder "${GITHUB_RELEASE_SLUG}" 2>/dev/null || true)"
  prefer_source_builder="0"

  if mason_release_prefers_source_builder "${GITHUB_RELEASE_SLUG}"; then
    prefer_source_builder="1"
  fi

  if [[ -n "${builder_id}" ]] && [[ "${prefer_source_builder}" == "1" ]] && mason_release_run_builder \
    "${builder_id}" \
    "${GITHUB_RELEASE_OWNER}" \
    "${GITHUB_RELEASE_REPO}" \
    "${GITHUB_RELEASE_TAG}" \
    "${generated_artifact}" \
    "${GITHUB_RELEASE_ASSET}"; then
    mkdir -p "$(dirname "${output_path}")"
    cp "${generated_artifact}" "${output_path}"
    mason_release_store_cached_artifact \
      "${GITHUB_RELEASE_SLUG}" \
      "${GITHUB_RELEASE_TAG}" \
      "${GITHUB_RELEASE_ASSET}" \
      "${generated_artifact}"
    rm -rf "${tmp_dir}"
    log "artefato zip gerado localmente por builder ${builder_id} para ${GITHUB_RELEASE_SLUG}"
    return 0
  fi

  if [[ "${prefer_source_builder}" == "1" ]]; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  if mason_release_repackage_archive_as_zip \
    "${GITHUB_RELEASE_OWNER}" \
    "${GITHUB_RELEASE_REPO}" \
    "${GITHUB_RELEASE_TAG}" \
    "${GITHUB_RELEASE_ASSET}" \
    "${generated_artifact}"; then
    mkdir -p "$(dirname "${output_path}")"
    cp "${generated_artifact}" "${output_path}"
    mason_release_store_cached_artifact \
      "${GITHUB_RELEASE_SLUG}" \
      "${GITHUB_RELEASE_TAG}" \
      "${GITHUB_RELEASE_ASSET}" \
      "${generated_artifact}"
    rm -rf "${tmp_dir}"
    log "artefato zip gerado localmente a partir de archive alternativo da release para ${GITHUB_RELEASE_SLUG}"
    return 0
  fi

  if [[ -n "${builder_id}" ]] && mason_release_run_builder \
    "${builder_id}" \
    "${GITHUB_RELEASE_OWNER}" \
    "${GITHUB_RELEASE_REPO}" \
    "${GITHUB_RELEASE_TAG}" \
    "${generated_artifact}" \
    "${GITHUB_RELEASE_ASSET}"; then
    mkdir -p "$(dirname "${output_path}")"
    cp "${generated_artifact}" "${output_path}"
    mason_release_store_cached_artifact \
      "${GITHUB_RELEASE_SLUG}" \
      "${GITHUB_RELEASE_TAG}" \
      "${GITHUB_RELEASE_ASSET}" \
      "${generated_artifact}"
    rm -rf "${tmp_dir}"
    log "artefato zip gerado localmente por builder ${builder_id} para ${GITHUB_RELEASE_SLUG}"
    return 0
  fi

  rm -rf "${tmp_dir}"
  return 1
}
