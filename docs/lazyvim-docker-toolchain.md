# LazyVim/Mason com Docker no macOS corporativo

Fluxo para máquina macOS com restrições de rede/permissão, mantendo o `nvim` local e movendo o runtime de ferramentas para um container Linux.

## O que este fluxo resolve

- `git clone`/downloads bloqueados ou limitados no host;
- falhas de Mason para baixar binários no macOS corporativo;
- necessidade de manter o editor no Mac, mas executar LSPs/formatadores dentro do Docker.

## O que este fluxo **não** faz

- não faz o macOS executar binário Linux diretamente;
- não evita bloqueio de rede corporativa por si só;
- não substitui o Docker Desktop/Colima — ele depende de um daemon Docker funcional.

## Arquivos

- `config/lazyvim/Dockerfile.nvim-toolchain`
- `config/lazyvim/setup_lazyvim_docker_toolchain.sh`

## Imagem base

A imagem usa `node:22-trixie-slim` (imagem oficial do Node sobre Debian 13
slim): base mínima, CLI-only, sem X11/UI. Node e npm vêm prontos da própria
imagem (sem `apt install nodejs npm`), o restante entra via apt com
`--no-install-recommends`, retries e limpeza de caches em cada camada.

Não há compilação Rust no build: `stylua` é instalado pelo pacote npm oficial
`@johnnymorganz/stylua-bin` (binário pronto) e o lint de Lua fica com
`luacheck` — isso remove o `cargo install` (etapa lenta e sensível a bloqueio
de rede corporativa, ex.: crates.io).

Por que Debian slim e não Alpine: o bootstrap usa Mason, que baixa binários
pré-compilados linkados em **glibc** (`lua-language-server`, `omnisharp`,
`rust-analyzer`, `elixir-ls`) — em Alpine (musl) esses binários quebram.
Por que a variante trixie e não bookworm: o LazyVim exige Neovim >= 0.9 e o
`gopls` atual exige Go recente; o bookworm empacota nvim 0.7 e go 1.19.

## Como funciona

1. builda uma imagem Docker com runtimes e ferramentas comuns;
2. sobe um container persistente `nvim-toolchain`;
3. cria um XDG isolado em `~/.local/share/nvim-docker-toolchain/xdg-*`;
4. gera wrappers locais em `~/.local/share/nvim-docker-toolchain/bin`;
5. os wrappers fazem `docker exec` no container e chamam:
   - primeiro `~/.local/share/nvim-docker-toolchain/xdg/data/nvim/mason/bin/<cmd>` se existir;
   - senão o executável do PATH do container.

Resultado: o seu `nvim` local continua no macOS, mas `gopls`, `lua-language-server`, `stylua`, `shellcheck`, `pyright-langserver` e demais comandos passam a rodar dentro do container.

## Instalação

```bash
cd ~/.../snippets
bash config/lazyvim/setup_lazyvim_docker_toolchain.sh
source ~/.local/share/nvim-docker-toolchain/env.sh
```

Se quiser também copiar a config versionada do Neovim para `~/.config/nvim`, o script já faz isso por default. Para evitar isso:

```bash
bash config/lazyvim/setup_lazyvim_docker_toolchain.sh --skip-config
```

## Verificação

```bash
command -v gopls
command -v lua-language-server
command -v stylua
gopls version
lua-language-server --version
stylua --version
```

Se os wrappers estiverem ativos, os executáveis vão apontar para `~/.local/share/nvim-docker-toolchain/bin/...`.

## Atualização

Rode novamente:

```bash
bash config/lazyvim/setup_lazyvim_docker_toolchain.sh
```

Isso:

- rebuilda a imagem;
- atualiza o container;
- recopia a config para o XDG isolado;
- reescreve os wrappers.

## Git dentro do container

O git de dentro do container usa a **config global de git do host**: o setup
detecta o arquivo (`GIT_CONFIG_GLOBAL` exportado > `~/.gitconfig` >
`~/.config/git/config`) e o injeta como `GIT_CONFIG_GLOBAL` no `docker run`,
no exec do bootstrap e nos wrappers — necessário porque o `XDG_CONFIG_HOME`
isolado esconderia o local XDG do host, e um `docker exec` manual cairia em
`HOME=/root`. Assim proxy/SSL/credenciais que fazem o git funcionar na rede
corporativa valem também lá dentro.

Por cima da config do host entram só três redes de segurança via env (no
`docker run` e nos execs — valem até para `docker exec` manual):

- URLs ssh do GitHub são reescritas para https (`url.….insteadOf`) — a rede
  corporativa bloqueia SSH e o container não tem agente/known_hosts;
- `safe.directory=*` — o HOME montado tem dono ≠ root do container, o que
  sem isso dá `detected dubious ownership`;
- `http.sslBackend=gnutls` — o git da imagem (Debian) só suporta GnuTLS; se
  a config do host força `sslBackend = openssl` (comum com git do Homebrew),
  qualquer operação https morre com `Unsupported SSL backend 'openssl'`.

Knobs relacionados: `GIT_INSECURE=1` exporta `GIT_SSL_NO_VERIFY=1` no exec
(proxy MITM sem CA instalada) e `CONTAINER_PROXY=1` repassa `HTTP(S)_PROXY`
ao exec (opt-in: o proxy do npm devolve 403 para github.com).

## Instalação das dependências (plugins)

**Default: ZIP da main.** O setup baixa o ZIP de cada dependência
(`archive/HEAD.zip` para branch default — a main —, `refs/heads/<branch>.zip`
quando pinada) via Python/urllib com proxy, sem git nem curl: é o único canal
que passa em rede corporativa que devolve 403 para as rotas do git
(`expected flush after ref listing`) e bloqueia SSH (`could not read from
remote repository`). Instala lazy.nvim + todos os plugins do manifesto no
**data dir do nvim do HOST** (`~/.local/share/nvim/lazy`) — é o nvim local
que carrega os plugins; o XDG isolado do toolchain fica só com o Mason/LSPs
Linux do container. O `Lazy! sync` dentro do container fica desligado
(opt-in `CONTAINER_SYNC=1`, exige rede liberada).

Com o toolchain presente na máquina (detecção por filesystem —
`~/.local/share/nvim-docker-toolchain` existe — independe de `env.sh`
sourceado), a config do nvim entra em **modo offline**: o lazy não clona
plugin faltante nem roda checker de updates, e o auto-update do pingu não
faz fetch — nenhum `git clone`/`git fetch` na abertura do editor (era a
fonte do `Could not read from remote repository` em todos os plugins no
startup).

Alternativa opt-in: `PLUGINS_FROM_GIT=1` clona pelo git do host.

## Clones pelo git do host (PLUGINS_FROM_GIT=1)

Os clones feitos pelo HOST:

- honram `GIT_INSECURE=1` também (`-c http.sslVerify=false` — proxy MITM);
- tentam as URLs em ordem: primeiro **o mesmo formato do remote origin deste
  repo** (o transporte do `git pull` do snippets é o canal provado na rede),
  depois https, ssh na porta 22 e por fim **ssh over 443**
  (`ssh://git@ssh.github.com:443/...`) — cobre proxy que devolve 403 para
  github (`expected flush after ref listing`) com porta 22 também bloqueada;
- plugins privados (ex.: `pingu_ai_codding_pair_programming`) falham sem
  abortar o setup quando a credencial não está disponível — instale-os
  manualmente depois em `~/.local/share/nvim/lazy/<nome>`;
- rodam sem prompt interativo (`GIT_TERMINAL_PROMPT=0`, ssh em BatchMode) —
  um prompt no meio do loop travaria o setup;
- não engolem o erro: a linha fatal de cada falha aparece no log e o stderr
  completo da última tentativa fica em
  `~/.local/share/nvim-docker-toolchain/host-git-clone.err`.

## Observações operacionais

- O XDG do container é separado do `~/.local/share/nvim` do host para não misturar Mason Linux com Mason macOS.
- A config do `snippets/config/nvim` passa a preferir os wrappers quando `NVIM_DOCKER_WRAPPER_BIN` existir.
- `omnisharp` e `elixir-ls` continuam sendo tratados como casos especiais via wrapper/PATH, sem hardcode absoluto do Mason do host.
