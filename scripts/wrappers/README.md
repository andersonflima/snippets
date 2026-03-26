# Wrappers

Esta pasta contém os wrappers reais usados para adaptar downloads e clones em ambientes com restrições corporativas.

Arquivos:

- `curl_python_wrapper.sh`: wrapper de `curl` com fallback para Python, `gh release` e estratégias inteligentes para Mason
- `git_zip_clone_wrapper.sh`: wrapper de `git clone` que baixa tarball/zip de repositório e monta o diretório localmente
- `fetch-url-via-ec2`: helper instalado junto dos wrappers para buscar URLs via EC2 + S3 quando a máquina local estiver restrita

Os entrypoints antigos em `scripts/` raiz continuam existindo como wrappers finos por compatibilidade.

## Estrutura

- implementação real do `curl`: `scripts/wrappers/curl_python_wrapper.sh`
- implementação real do `git`: `scripts/wrappers/git_zip_clone_wrapper.sh`
- instalador do wrapper de `curl`: `scripts/install/install_curl_python_wrapper.sh`
- instalador do wrapper de `git`: `scripts/install/install_git_zip_wrapper.sh`
- configurador de ambiente: `scripts/install/configure_wrapper_envs.sh`

## Instalação

### Bootstrap único

Se você não quiser rodar instalador por instalador, use o bootstrap único:

```bash
sh scripts/setup_restricted_dev_env.sh --s3-bucket "<bucket>"
```

Ou, no atalho mais simples:

```bash
sh scripts/configure_restricted_dev_env.sh "<bucket>"
```

Esse fluxo instala e configura:

- wrapper do `mix`
- wrapper do `curl`
- wrapper do `git`
- envs compartilhadas do EC2/S3

Por padrão, o bootstrap não altera mais o `~/.zshrc`. Para ativar na sessão atual:

```bash
. scripts/activate_restricted_dev_env.sh
```

Se você quiser persistir no shell rc de propósito:

```bash
sh scripts/setup_restricted_dev_env.sh \
  --s3-bucket "<bucket>" \
  --apply-shell-rc \
  --shell-rc "$HOME/.zshrc"
```

Para zerar tudo depois:

```bash
sh scripts/reset_restricted_dev_env.sh
```

Opcionalmente, ele também pode aplicar `mix hex.config`:

```bash
sh scripts/setup_restricted_dev_env.sh \
  --s3-bucket "<bucket>" \
  --configure-hex \
  --hex-unsafe-https
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

### Configurar envs do ambiente

Depois de instalar os wrappers, gere e conecte as envs ao shell:

```bash
sh scripts/install/configure_wrapper_envs.sh
```

Se você usa `zsh` e quer aplicar direto no `~/.zshrc`:

```bash
sh scripts/configure_wrapper_envs_zsh.sh
```

Opcionalmente:

```bash
sh scripts/install/configure_wrapper_envs.sh \
  --shell-rc "$HOME/.zshrc" \
  --proxy "http://proxy.seu-dominio:3128" \
  --ca-cert "/etc/pki/ca-trust/source/anchors/corp-ca.pem"
```

## Shell

Se você não usar o configurador automático, exporte manualmente os paths e variáveis principais no shell:

```bash
export CURL_WRAPPER_REAL_CURL="$(command -v curl)"
export GIT_ZIP_WRAPPER_REAL_GIT="$(command -v git)"

export PATH="$HOME/.local/share/curl-python-wrapper/bin:$HOME/.local/share/git-zip-wrapper/bin:$PATH"
```

## LazyVim / Mason

Exemplo de configuração por ambiente:

```lua
vim.env.CURL_WRAPPER_REAL_CURL = "/usr/bin/curl"
vim.env.GIT_ZIP_WRAPPER_REAL_GIT = "/usr/bin/git"
vim.env.PATH = table.concat({
  vim.fn.expand("~/.local/share/curl-python-wrapper/bin"),
  vim.fn.expand("~/.local/share/git-zip-wrapper/bin"),
  vim.env.PATH,
}, ":")

vim.env.CURL_WRAPPER_RELEASE_FALLBACK_REPOS = "elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn"
vim.env.CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES = "1"
vim.env.CURL_WRAPPER_RELEASE_CACHE_DIR = vim.fn.expand("~/.cache/curl-python-wrapper/releases")
vim.env.CURL_WRAPPER_MASON_BUILDERS = "elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish"
vim.env.CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS = "elixir-lsp/elixir-ls,omnisharp/omnisharp-roslyn"
vim.env.GIT_ZIP_WRAPPER_ARCHIVE_FORMAT = "tar.gz"
```

## Variáveis de ambiente

### `curl_python_wrapper.sh`

Principais variáveis:

- `CURL_WRAPPER_REAL_CURL`
  Caminho do `curl` real.

- `CURL_WRAPPER_USE_EC2`
  Quando `1`, delega downloads suportados para o helper remoto via EC2.
  Padrão: herda `WRAPPERS_VIA_EC2_ENABLED`.

- `WRAPPERS_VIA_EC2_INSTANCE_NAME`
  Instância EC2 compartilhada usada pelos wrappers.
  Padrão: `Dander`.

- `WRAPPERS_VIA_EC2_AWS_REGION`
  Region AWS do backend remoto compartilhado.
  Padrão: `sa-east-1`.

- `WRAPPERS_VIA_EC2_S3_BUCKET`
  Bucket S3 intermediário compartilhado com o backend remoto.

- `WRAPPERS_VIA_EC2_S3_PREFIX`
  Prefixo S3 compartilhado.
  Padrão: `wrappers-via-ec2`.

- `CURL_WRAPPER_PROXY`
  Proxy explícito do wrapper. Tem precedência sobre `HTTPS_PROXY`, `ALL_PROXY` e `HTTP_PROXY`.

- `CURL_WRAPPER_ALLOW_ZIP_DOWNLOAD`
  Libera download direto de `.zip` quando necessário.
  Padrão: `0`.

- `CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR`
  Se ativado, o fallback tenta novamente sem validação TLS quando o erro for de certificado.
  Padrão: `0`.

- `CURL_WRAPPER_RELEASE_FALLBACK_REPOS`
  Lista CSV de repositórios GitHub tratados como releases restritas.
  Padrão:
  `elixir-lsp/elixir-ls,luals/lua-language-server,omnisharp/omnisharp-roslyn`

- `CURL_WRAPPER_ALLOW_DIRECT_RELEASE_FALLBACK`
  Reabilita tentativa direta do asset remoto da release, mesmo para repositórios restritos.
  Padrão: `0`.

- `CURL_WRAPPER_ENABLE_MASON_SMART_RELEASES`
  Ativa a estratégia inteligente do Mason para montar artefatos localmente.
  Padrão: `1`.

- `CURL_WRAPPER_RELEASE_CACHE_DIR`
  Diretório de cache dos artefatos gerados localmente.
  Padrão: `$XDG_CACHE_HOME/curl-python-wrapper/releases`.

- `CURL_WRAPPER_MASON_BUILDERS`
  Registro CSV `repo=builder` para builders especiais quando não houver asset alternativo.
  Padrão: `elixir-lsp/elixir-ls=elixir_ls_release,omnisharp/omnisharp-roslyn=omnisharp_source_publish`.

- `CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS`
  Lista CSV de repositórios que devem preferir build local a partir do source tarball, sem cair em asset de release.
  Padrão: `elixir-lsp/elixir-ls,omnisharp/omnisharp-roslyn`.

- `CURL_WRAPPER_MASON_SEED_DIR`
  Diretório opcional com artefatos `.zip` já gerados fora da máquina restrita.
  Formato esperado: `<seed-dir>/<owner>/<repo>/<tag>/<asset>` ou `<seed-dir>/<asset>`.
  Quando definido, o wrapper tenta esse diretório antes de buildar localmente.

- `CURL_WRAPPER_MASON_REPACKAGE_EXTENSIONS`
  Extensões candidatas que a engine dinâmica pode baixar e reempacotar em `.zip`.
  Padrão: `tar.gz,tgz,tar`.

- `CURL_WRAPPER_STRICT`
  Desativa fallbacks e faz o wrapper retornar o erro do `curl` real.

### `git_zip_clone_wrapper.sh`

Principais variáveis:

- `GIT_ZIP_WRAPPER_REAL_GIT`
  Caminho do `git` real.

- `GIT_ZIP_WRAPPER_USE_EC2`
  Quando `1`, delega downloads dos archives suportados para o helper remoto via EC2.
  Padrão: herda `WRAPPERS_VIA_EC2_ENABLED`.

- `GIT_ZIP_WRAPPER_PROXY`
  Proxy explícito para os downloads do wrapper.

- `GIT_ZIP_WRAPPER_ARCHIVE_FORMAT`
  Formato preferido do archive.
  Valores válidos: `tar.gz`, `tgz`, `tar`, `zip`.
  Padrão: `tar.gz`.

- `GIT_ZIP_WRAPPER_ALLOW_ZIP_FALLBACK`
  Libera fallback para `.zip` quando o `.tar.gz` não estiver disponível.

- `GIT_ZIP_WRAPPER_CURL_CACERT`
  Caminho para CA customizada em ambiente corporativo.

- `GIT_ZIP_WRAPPER_CURL_INSECURE`
  Desativa validação TLS do `curl` usado pelo wrapper.

- `GIT_ZIP_WRAPPER_STRICT`
  Impede fallback para `git clone` normal.

## Mason inteligente

## Backend compartilhado via EC2

Os wrappers de `curl` e `git` agora podem usar o mesmo backend remoto do `mix`, compartilhando:

- instância EC2
- region AWS
- bucket S3 intermediário
- prefixo S3

Isso é controlado pelas envs:

```bash
export WRAPPERS_VIA_EC2_ENABLED=1
export WRAPPERS_VIA_EC2_INSTANCE_NAME="Dander"
export WRAPPERS_VIA_EC2_AWS_REGION="sa-east-1"
export WRAPPERS_VIA_EC2_S3_BUCKET="<bucket-compartilhado>"
export WRAPPERS_VIA_EC2_S3_PREFIX="wrappers-via-ec2"
```

Quando esse backend está ativo:

- o `curl` wrapper tenta buscar assets suportados via EC2 antes do download local
- o `git` wrapper tenta baixar archives GitHub via EC2 antes das tentativas locais
- o `curl` do Mason pode aproveitar o EC2 para baixar `.zip` oficiais de release quando a máquina local não consegue

No `curl` wrapper existe uma engine adicional para pacotes do Mason que falham em ambiente corporativo por dependerem de asset `.zip` de release.

Comportamento atual:

- quando a URL é de GitHub release e o Mason pede `.zip`, a engine tenta descobrir assets equivalentes da release via API
- para repositórios marcados em `CURL_WRAPPER_MASON_SOURCE_BUILD_REPOS`, a engine tenta primeiro gerar o artefato localmente a partir do source tarball (`archive/refs/tags/*.tar.gz`)
- se existir twin exato em `.tar.gz`, `.tgz` ou `.tar`, ele é preferido antes da heurística de similaridade
- se encontrar `.tar.gz`, `.tgz` ou `.tar` compatível, baixa, extrai e reempacota localmente em `.zip`
- se não encontrar asset equivalente, consulta o registro de builders especiais
- os builders padrão atuais cobrem:
  - `elixir-lsp/elixir-ls`, gerando o release localmente com `mix elixir_ls.release2` ou caindo para `mix elixir_ls.release`
  - `omnisharp/omnisharp-roslyn`, gerando o pacote localmente com `dotnet publish` a partir do source tarball
- quando o pacote só publica `.zip`, o wrapper também tenta o endpoint de assets da API do GitHub antes de desistir
- o artefato gerado fica em cache local para reutilização automática nas próximas instalações

Se a estratégia inteligente falhar:

- o wrapper ainda tenta `gh release download`
- se isso também falhar, retorna erro claro

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

Gerando o seed:

```bash
sh scripts/build_mason_seed_artifact.sh \
  --release-url https://github.com/elixir-lsp/elixir-ls/releases/download/v0.30.0/elixir-ls-v0.30.0.zip \
  --seed-dir "$HOME/.cache/mason-seeds"
```

No host restrito:

```bash
export CURL_WRAPPER_MASON_SEED_DIR="$HOME/.cache/mason-seeds"
```

Depois disso, o wrapper tenta o seed local antes de chamar o builder.

Se quiser persistir isso no `zsh`:

```bash
sh scripts/configure_wrapper_envs_zsh.sh \
  --mason-seed-dir "$HOME/.cache/mason-seeds"
```

## Ambientes com proxy/certificado

Exemplo:

```bash
export HTTPS_PROXY="http://proxy.seu-dominio:3128"
export HTTP_PROXY="http://proxy.seu-dominio:3128"
export ALL_PROXY="http://proxy.seu-dominio:3128"

export CURL_WRAPPER_PROXY="http://proxy.seu-dominio:3128"
export GIT_ZIP_WRAPPER_PROXY="http://proxy.seu-dominio:3128"

export GIT_ZIP_WRAPPER_CURL_CACERT="/etc/pki/ca-trust/source/anchors/corp-ca.pem"
```

Se o ambiente for muito restrito:

```bash
export CURL_WRAPPER_AUTO_INSECURE_ON_CERT_ERROR=1
```

## Testes rápidos

### Curl wrapper

```bash
curl -fsSL https://github.com/neovim/neovim/archive/refs/heads/master.tar.gz -o /tmp/neovim.tar.gz
```

### Git wrapper

```bash
git clone https://github.com/neovim/neovim ~/tmp/neovim-zip-clone
```

## Observações

- A descoberta de asset alternativo agora é genérica para releases do GitHub que tenham formato compatível.
- O ponto de extensão para builders especiais está em `scripts/wrappers/lib/mason_release_engine.sh`.
- Para adicionar outro builder especial, registre `repo=builder` em `CURL_WRAPPER_MASON_BUILDERS` e implemente o builder na engine.
