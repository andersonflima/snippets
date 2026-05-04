# Scripts

## Fluxo público

- `scripts/configure.sh`
  Configura tudo: instala wrappers locais, gera envs, atualiza shell rc, integra setup do ElixirLS, salva estado e ainda executa bootstrap automático do LazyVim/Mason no `nvim --headless`.
- `scripts/reset.sh`
  Remove wrappers/envs, limpa o bloco gerenciado do shell rc, apaga cache local do wrapper, limpa estado completo do Mason (ou apenas `elixir-ls`), e restaura configuração persistida do Hex.

## Fluxo recomendado

Configuração completa:

```bash
sh scripts/configure.sh
```

Bootstrap automático padrão (Lazy/Mason):

```bash
sh scripts/configure.sh \
  --proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem" \
  --mason-packages "lua-language-server,pyright,elixir-ls"
```

Se `git fetch` em repositórios não-itau retornar “remote disabled”, use:

```bash
sh scripts/configure.sh \
  --disable-remote-git-fallback \
  --proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem"
```

Com proxy ou CA corporativa:

```bash
sh scripts/configure.sh \
  --proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem"
``` 

Remoção completa:

```bash
sh scripts/reset.sh
```

## Snapshot do Neovim para outra máquina

Para clonar seu estado atual do Neovim (config + plugins já instalados do Lazy/Mason):

```bash
sh scripts/install/nvim_offline_bundle.sh snapshot --copy-wrapper-env
```

No snapshot, o `--no-data` mantém a pasta `.local/share/nvim` fora e registra apenas `mason-packages.txt` para bootstrap posterior.

Diretório gerado:

```bash
./nvim-offline-bundle
```

Para restaurar em outra máquina:

```bash
sh scripts/install/nvim_offline_bundle.sh restore --bundle-dir ./nvim-offline-bundle
```

Opções úteis:

- `--overwrite` para substituir diretórios existentes sem criar backup.
- `--data-components "lazy,mason"` para escolher componentes de runtime.
- `--no-config` e `--no-data` para reduzir escopo.
- `--copy-wrapper-env` para levar `wrapper-envs.sh` e `restricted-dev-env/state.sh` junto com a configuração.

Após restore na máquina nova, rode seu fluxo de wrapper para reativar o ambiente:

```bash
sh scripts/configure.sh --skip-lazy-bootstrap --skip-mason-bootstrap
```

Assim os wrappers ficam instalados e os pacotes copiados permanecem prontos para bootstrap.
Se quiser evitar qualquer alteração de rede na restauração, rode:

```bash
sh scripts/install/nvim_offline_bundle.sh restore --bundle-dir ./nvim-offline-bundle --no-bootstrap
```

## Diagnóstico

Validação rápida:

```bash
sh scripts/install/validate_wrappers.sh
```

## Ferramentas operacionais

- `scripts/install/build_mason_seed_artifact.sh`
- `scripts/install/configure_hex_config.sh`

## Organização interna

- `scripts/install/`: instalação, configuração, reset e validação.
- `scripts/wrappers/`: wrappers reais de `curl`, `wget` e `git`.
