# Wrappers

Esta pasta contém os wrappers reais usados para adaptar downloads e clones em ambientes com restrições corporativas, sempre com fluxo local.

## Arquivos

- `curl_python_wrapper.sh`: wrapper de `curl` com fallback para Python, `gh release` e estratégias inteligentes para Mason.
- `wget_wrapper.sh`: wrapper de `wget` que delega assets `.zip` de release do GitHub para o wrapper de `curl` quando o fluxo do Mason precisa disso.
- `git_zip_clone_wrapper.sh`: wrapper de `git` que delega `git clone` GitHub para o motor Node por archive `.zip`, cai para o fluxo shell local quando necessário e mantém fallback local para cache do `Mix.install`.

## Instalação

### Bootstrap único

Fluxo público recomendado:

```bash
sh scripts/configure.sh
```

Esse fluxo instala e configura:

- wrapper do `curl`
- wrapper do `wget`
- wrapper do `git`
- envs compartilhadas locais
- bloco gerenciado no shell rc
- manifesto de estado em `~/.config/restricted-dev-env/state.sh`

Se você quiser ativar só na sessão atual, sem persistir:

```bash
sh scripts/configure.sh --no-shell-rc
. "$HOME/.config/wrapper-envs.sh"
```

Para diagnosticar se o Mason está vendo os wrappers:

```bash
sh scripts/install/validate_wrappers.sh
```

Se você quiser mudar o rc de destino:

```bash
sh scripts/configure.sh --shell-rc "$HOME/.config/fish/config.fish"
```

Para zerar tudo depois:

```bash
sh scripts/reset.sh
```

### Curl wrapper

```bash
sh scripts/install/install_curl_python_wrapper.sh
```

Opcionalmente:

```bash
sh scripts/install/install_curl_python_wrapper.sh \
  --install-dir "$HOME/.local/share/curl-python-wrapper/bin" \
  --real-curl "$(command -v curl)"
```

### Git wrapper

```bash
sh scripts/install/install_git_zip_wrapper.sh
```

Opcionalmente:

```bash
sh scripts/install/install_git_zip_wrapper.sh \
  --install-dir "$HOME/.local/share/git-zip-wrapper/bin" \
  --real-git "$(command -v git)"
```

## Shell

Se você não usar o configurador automático, exporte manualmente os paths e variáveis principais no shell:

```bash
export CURL_WRAPPER_REAL_CURL="$(command -v curl)"
export WGET_WRAPPER_REAL_WGET="$(command -v wget)"
export GIT_ZIP_WRAPPER_REAL_GIT="$(command -v git)"
export PATH="$HOME/.local/share/curl-python-wrapper/bin:$HOME/.local/share/git-zip-wrapper/bin:$PATH"
```

## LazyVim / Mason

Exemplo de configuração por ambiente:

```lua
vim.env.CURL_WRAPPER_REAL_CURL = "/usr/bin/curl"
vim.env.WGET_WRAPPER_REAL_WGET = "/usr/bin/wget"
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "/usr/bin/git"
vim.env.PATH = table.concat({
  vim.fn.expand("~/.local/share/curl-python-wrapper/bin"),
  vim.fn.expand("~/.local/share/git-zip-wrapper/bin"),
  vim.env.PATH,
}, ":")

vim.env.CURL_WRAPPER_RELEASE_FALLBACK_REPOS = "elixir-lsp/elixir-ls,johnnymorganz/stylua,luals/lua-language-server,omnisharp/omnisharp-roslyn"
vim.env.CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK = "1"
vim.env.CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES = "1"
vim.env.CURL_WRAPPER_RELEASE_CACHE_DIR = vim.fn.expand("~/.cache/curl-python-wrapper/releases")
vim.env.CURL_WRAPPER_MASON_BUILDERS = "elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"
vim.env.CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS = "omnisharp/omnisharp-roslyn"
vim.env.GIT_ZIP_WRAPPER_ARCHIVE_FORMAT = "zip"
vim.env.GIT_ZIP_WRAPPER_USE_JS_ENGINE = "1"
vim.env.GIT_ZIP_WRAPPER_CLONE_ORDER = "local-first"
vim.env.GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK = "0"
vim.env.GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK = "1"
vim.env.GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS = "1"
vim.env.GIT_ZIP_WRAPPER_LFS_MODE = "local"
vim.env.GIT_ZIP_WRAPPER_STRICT = "0"
```

## Variáveis de ambiente

### `curl_python_wrapper.sh`

- `CURL_WRAPPER_REAL_CURL`: caminho do `curl` real.
- `CURL_WRAPPER_PROXY`: proxy explícito do wrapper; tem precedência sobre `HTTPS_PROXY`, `ALL_PROXY` e `HTTP_PROXY`.
- `CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD`: libera download direto de `.zip` quando necessário. Padrão: `0`.
- `CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR`: tenta novamente sem validação TLS quando o erro for de certificado. Padrão: `0`.
- `CURL_WRAPPER_RELEASE_FALLBACK_REPOS`: lista CSV de repositórios GitHub tratados como releases restritas.
- `CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK`: reabilita tentativa direta do asset remoto da release. Padrão: `0`.
- `CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES`: ativa a estratégia inteligente do Mason. Padrão: `1`.
- `CURL_WRAPPER_RELEASE_CACHE_DIR`: diretório de cache dos artefatos gerados localmente.
- `CURL_WRAPPER_MASON_BUILDERS`: registro CSV `repo=builder` para builders especiais.
- `CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS`: lista CSV de repositórios que devem preferir build local a partir do source tarball.
- `CURL_WRAPPER_MASON_SEED_DIR`: diretório opcional com artefatos `.zip` já gerados fora da máquina restrita.
- `CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS`: extensões candidatas que a engine dinâmica pode baixar e reempacotar em `.zip`.
- `CURL_WRAPPER_STRICT`: desativa fallbacks e faz o wrapper retornar o erro do `curl` real.

### `git_zip_clone_wrapper.sh`

- `GIT_ZIP_WRAPPER_REAL_GIT`: caminho do `git` real.
- `GIT_ZIP_WRAPPER_PROXY`: proxy explícito para os downloads do wrapper.
- `GIT_ZIP_WRAPPER_ARCHIVE_FORMAT`: formato preferido do archive. Valores válidos: `tar.gz`, `tgz`, `tar`, `zip`. Padrão: `zip`.
- `GIT_ZIP_WRAPPER_USE_JS_ENGINE`: usa o motor Node para transformar `git clone` GitHub em download/extracao de `.zip` antes de cair no fluxo shell. Padrão: `1`.
- `GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK`: libera fallback ou uso primário de `.zip`.
- `GIT_ZIP_WRAPPER_CLONE_ORDER`: política do clone do wrapper. Valores suportados: `git-first`, `local-first`. Padrão: `local-first`.
- `GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK`: permite usar `git clone/fetch/checkout` remoto real se o archive falhar. Padrão: `0`.
- `GIT_ZIP_WRAPPER_FORCE_LOCAL_DOWNLOADS`: mantém a tentativa de archive local como preferência. Padrão: `1`.
- `GIT_ZIP_WRAPPER_CURL_CACERT`: caminho para CA customizada em ambiente corporativo.
- `GIT_ZIP_WRAPPER_CURL_INSECURE`: desativa validação TLS do `curl` usado pelo wrapper.
- `GIT_ZIP_WRAPPER_LFS_MODE`: modo do Git LFS. Valor suportado: `local`.
- `GIT_ZIP_WRAPPER_STRICT`: impede fallback para `git clone` normal quando o archive local falhar.

Comportamento adicional para LazyVim:

- em `local-first`, o wrapper usa archive do GitHub antes de qualquer Git remoto
- com `GIT_ZIP_WRAPPER_USE_JS_ENGINE=1`, `git clone` GitHub baixa `.zip`, remove o diretório raiz do archive, escreve no destino solicitado por LazyVim/Mason e inicializa metadados Git locais sem acessar o remoto Git
- por padrão, se o archive falhar, o wrapper não tenta `git clone` real para repositórios GitHub
- para liberar Git remoto real como último fallback, defina `GIT_ZIP_WRAPPER_ALLOW_REMOTE_GIT_FALLBACK=1`
- em `git-first`, o wrapper ainda pode tentar `git clone` normal antes do archive, então esse modo não é adequado para ambientes onde Git remoto externo é bloqueado

Comportamento adicional para ElixirLS/Mix.install:

- quando `git fetch` falha dentro do cache `mix/installs`, o wrapper tenta fallback local por archive do GitHub para materializar o tag ou branch solicitado.

## Mason inteligente

No `curl` wrapper existe uma engine adicional para pacotes do Mason que falham em ambiente corporativo por dependerem de asset `.zip` de release.

Comportamento atual:

- quando a URL é de GitHub release e o Mason pede `.zip`, a engine tenta descobrir assets equivalentes da release via API
- para repositórios marcados em `CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS`, a engine tenta primeiro gerar o artefato localmente a partir do source tarball
- se existir twin exato em `.tar.gz`, `.tgz` ou `.tar`, ele é preferido antes da heurística de similaridade
- se encontrar `.tar.gz`, `.tgz` ou `.tar` compatível, baixa, extrai e reempacota localmente em `.zip`
- se não encontrar asset equivalente, consulta o registro de builders especiais
- o artefato gerado fica em cache local para reutilização automática nas próximas instalações
- se a estratégia inteligente falhar, o wrapper ainda tenta `gh release download`

## Pré-requisitos

Para o `curl` wrapper:

- `python3`
- `tar`

Para a engine dinâmica do `elixir-ls`:

- `elixir`
- `mix`

Para a engine dinâmica do `omnisharp`:

- `dotnet` SDK

Opcional:

- `gh` autenticado (`gh auth status`)

### Seed local para hosts restritos

Se a máquina do serviço não consegue rodar `mix deps.get` ou `dotnet restore` de forma confiável, gere o artefato em outra máquina e copie para o host restrito.
