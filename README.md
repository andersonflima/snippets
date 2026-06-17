# snippets

Repositório genérico para **scripts utilitários e automações**.

Este repo é intencionalmente flexível: pode conter scripts independentes, helpers de operação, POCs, jobs AWS e utilitários locais.

## Objetivo

- Centralizar scripts reutilizáveis em um só lugar.
- Manter automações pequenas, objetivas e fáceis de executar.
- Permitir evolução incremental sem acoplar tudo em um único projeto.

## Estrutura

- `*.py`: scripts Python (utilitários, automações, integrações AWS etc.).
- `*.sh`: scripts shell para operação e setup.
- `scripts/`: automações de setup/rollback.
- `tests/` e `test_*.py`: testes automatizados.

## Requisitos gerais

- Python `3.10+`.
- Dependências variam por script (ex.: `boto3` para integrações AWS).
- Em scripts AWS, use credenciais/região válidas no ambiente de execução.

## Scripts de setup de máquina (LazyVim + Mason)

A pasta `scripts/` mantém:

- `scripts/setup_lazyvim_mason_from_zip.sh`
- `scripts/undo_lazyvim_mason_from_zip.sh`
- `scripts/setup_homebrew_proxy.sh`

### O que o setup faz

- baixa dependências usando rota ZIP do GitHub (`/archive/refs/heads/<branch>.zip`);
- resolve dinamicamente a branch padrão quando o manifesto usa `default`;
- não usa `curl` nem `wget` (download por helper Python com biblioteca padrão e retry);
- instala wrappers locais de `curl`/`wget` em `~/.local/share/nvim/wrappers/bin` para runtime do LazyVim/Mason em ambientes bloqueados;
- instala wrapper local de `git` para reescrever `https://github.com/...` como `git@github.com:...` no runtime do Neovim;
- por padrão, preserva `~/.config/nvim` (não altera sua config da empresa);
- instala plugins do manifesto em `~/.local/share/nvim/lazy`;
- instala registry local do Mason em `~/.cache/nvim/mason-registry-main`;
- com `--manage-config`, copia a config versionada em `nvim-config/` do repositório para `~/.config/nvim` e aplica os overrides.
- instala comandos `lazy-check`, `lazy-install` e `lazy-update` (ZIP-only) para fluxo de instalação/verificação/atualização sem `git fetch/clone`.
- com `--manage-config --config-source-dir <dir>`, sobrescreve a origem padrão e copia a config desse diretório para `~/.config/nvim`.

### Execução

```bash
bash scripts/setup_lazyvim_mason_from_zip.sh --force
bash scripts/setup_lazyvim_mason_from_zip.sh --force --manage-config
bash scripts/setup_lazyvim_mason_from_zip.sh --force --manage-config --config-source-dir ~/.config/nvim
bash scripts/undo_lazyvim_mason_from_zip.sh
```

Depois do setup, abra um novo terminal (ou rode `source ~/.zshrc`) para refletir o `PATH` também no shell.
A config do Neovim usa o shell definido em `$SHELL`; em máquinas com zsh, não é necessário instalar fish.

Para instalar, verificar e atualizar plugins sem `:Lazy install/check/update` (sem git externo):

```bash
lazy-check
lazy-install
lazy-update
```

Observacao:
- `lazy-update` usa download por ZIP (`github.com/.../archive/HEAD.zip` para `default` e `github.com/.../archive/refs/heads/...zip` para branches explicitas), sem `api.github.com`.
- Se `archive/HEAD.zip` falhar, o fluxo tenta `main` e `master` como fallback.
- `lazy-check` em modo offline nao calcula delta remoto; use `lazy-update` para aplicar refresh por ZIP.
- Antes de `lazy-install`, `lazy-update` e `lazy-sync`, o Neovim atualiza o manifesto ZIP com os plugins carregados pelo Lazy. Plugins novos declarados na config passam a entrar no fluxo ZIP.
- A config versionada força `blink.cmp` a usar fuzzy em Lua para evitar falha em `blink.lib`/biblioteca nativa em máquinas com proxy, sem Rust ou sem acesso a assets de release.
- Se o download retornar HTML no lugar de ZIP, o setup falha com a URL e o inicio da resposta. Isso normalmente indica proxy, bloqueio, pagina de login, rate limit ou URL base incorreta em `--github-base`.
- O Mason ainda depende das receitas do registry e pode baixar assets de GitHub Releases, npm, pip, cargo ou outros canais. Se a rede bloquear o media type do pacote, o wrapper local de `curl`/`wget` nao consegue contornar a politica do proxy.

No popup do `:Lazy`, quando `--manage-config` for usado, as teclas ficam remapeadas para ZIP:
- `I` -> `lazy-install`
- `U` -> `lazy-update`
- `S` -> `lazy-update` (sync offline)
- `C` -> `lazy-check`

Além das teclas, os subcomandos `:Lazy install`, `:Lazy update`, `:Lazy sync` e `:Lazy check` também são sobrescritos para usar o fluxo ZIP.
Esse override também é escrito em `~/.config/nvim/after/plugin/` para garantir aplicação mesmo fora do popup.
As ações ZIP abrem um terminal flutuante para exibir progresso e evitar timeout do comando dentro do Neovim.

## Homebrew atrás de proxy (corrigir 403)

`scripts/setup_homebrew_proxy.sh` analisa e melhora o `~/.zshrc` (ou outro rc) para
permitir `brew install` atrás de proxy corporativo sem erro `403` nos bottles/artefatos.

### O que faz

- analisa o rc atual: detecta proxy/no_proxy e variáveis `HOMEBREW_*` existentes;
- normaliza `NO_PROXY`/`no_proxy` (mescla defaults — `localhost,127.0.0.1,::1,*.local` —
  com o valor atual e extras, deduplicando) e espelha proxy em maiúsculas e minúsculas;
- garante as variáveis de mirror interno usadas para contornar o `403`:
  `HOMEBREW_ARTIFACT_DOMAIN`, `HOMEBREW_BOTTLE_DOMAIN`, `HOMEBREW_API_DOMAIN`,
  `HOMEBREW_DOCKER_REGISTRY_TOKEN`, `HOMEBREW_BREW_GIT_REMOTE`, `HOMEBREW_CORE_GIT_REMOTE`;
- escreve tudo num bloco gerenciado e idempotente (`# >>> homebrew-proxy (managed) >>>`),
  com backup timestamped do rc antes de gravar;
- precedência de valores: flag > ambiente atual > valor já presente no rc (re-rodar sem
  flags preserva o que já estava configurado);
- variáveis importantes sem valor entram comentadas como template a preencher;
- `--doctor` testa a conectividade dos endpoints (API e artifact/ghcr) e aponta a causa
  provável do `403` (token ausente/mirror não configurado).

O `403` típico do Homebrew vem de pedir bottles ao `ghcr.io` anonimamente atrás do proxy:
apontar `HOMEBREW_ARTIFACT_DOMAIN` para o mirror interno e definir
`HOMEBREW_DOCKER_REGISTRY_TOKEN` resolve. O bloco é adicionado no fim do rc, então
prevalece sobre exports anteriores (é carregado por último).

### Execução

```bash
# 1) Analisar (dry-run): mostra o bloco que seria aplicado; token é mascarado.
bash scripts/setup_homebrew_proxy.sh

# 2) Aplicar com os valores da empresa (cria backup do rc):
bash scripts/setup_homebrew_proxy.sh --apply \
  --https-proxy "http://proxy.empresa:porta" \
  --artifact-domain "https://<mirror-interno>" \
  --bottle-domain   "https://<mirror-interno>" \
  --registry-token  "<token-do-ghcr-mirror>"

# 3) Diagnosticar conectividade / causa do 403:
bash scripts/setup_homebrew_proxy.sh --doctor

# 4) (opcional) instalar Homebrew e o conjunto p/ LazyVim/Mason já com proxy:
bash scripts/setup_homebrew_proxy.sh --apply --install-brew --install-packages

source ~/.zshrc
```

Para encadear no setup do Neovim, use `--with-homebrew-proxy` (aplica o bloco antes):

```bash
bash scripts/setup_lazyvim_mason_from_zip.sh --force --with-homebrew-proxy
```

## Como usar

1. Entre no repositório.
2. Abra o script alvo para ver os parâmetros disponíveis.
3. Rode localmente com `--help` quando suportado.

Exemplos:

```bash
python3 upgrade_resource_version.py --help
python3 dynamodb_snapshot_lambda.py --help
bash scripts/setup_lazyvim_mason_from_zip.sh --help
bash scripts/setup_homebrew_proxy.sh --help
```

## Testes

Para validar mudanças, execute os testes relacionados ao script alterado.

Exemplos:

```bash
python3 -m unittest test_crate_tables.py
python3 -m unittest test_kms_lambda.py
```

## Convenções para novos scripts

- Nome descritivo (`verbo_contexto.py` ou `contexto_acao.sh`).
- Entrada por argumentos/flags (evitar valores mágicos no código).
- Erros claros e exit code consistente.
- README e exemplos atualizados quando houver mudança relevante de uso.
- Sempre que possível, adicionar teste junto da automação.

## Observações

- Este README é propositalmente **genérico** para refletir o papel do repositório.
- Documentação específica de um script pode ficar no próprio arquivo, em comentários de uso, ou em docs dedicadas quando necessário.
