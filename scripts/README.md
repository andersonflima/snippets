# Scripts

## Fluxo público

- `scripts/dev-env.sh`
  Entry point único. Sem subcomando, configura tudo: reset limpo, instalação dos wrappers, geração de envs, atualização do shell rc, integração do ElixirLS, estado persistido e bootstrap automático do LazyVim/Mason no `nvim --headless`.
- `scripts/configure.sh`
  Alias de compatibilidade para `scripts/dev-env.sh setup`.
- `scripts/reset.sh`
  Alias de compatibilidade para `scripts/dev-env.sh reset`.

## Fluxo recomendado

Configuração completa:

```bash
sh scripts/dev-env.sh
```

Bootstrap automático padrão (Lazy/Mason):

```bash
sh scripts/dev-env.sh \
  --proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem" \
  --mason-packages "lua-language-server,pyright,elixir-ls"
```

Com proxy ou CA corporativa:

```bash
sh scripts/dev-env.sh \
  --proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem"
``` 

Remoção completa:

```bash
sh scripts/dev-env.sh reset
```

## Diagnóstico

Validação rápida:

```bash
sh scripts/dev-env.sh validate
```

## Ferramentas operacionais

- `scripts/install/build_mason_seed_artifact.sh`
- `scripts/install/configure_hex_config.sh`

## Organização interna

- `scripts/install/`: instalação, configuração, reset e validação.
- `scripts/wrappers/`: wrappers reais de `curl`, `wget` e `git`.
