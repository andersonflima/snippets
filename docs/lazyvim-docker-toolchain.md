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

## Observações operacionais

- O XDG do container é separado do `~/.local/share/nvim` do host para não misturar Mason Linux com Mason macOS.
- A config do `snippets/config/nvim` passa a preferir os wrappers quando `NVIM_DOCKER_WRAPPER_BIN` existir.
- `omnisharp` e `elixir-ls` continuam sendo tratados como casos especiais via wrapper/PATH, sem hardcode absoluto do Mason do host.
