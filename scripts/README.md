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
