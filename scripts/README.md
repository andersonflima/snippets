# Scripts

## Fluxo público

- `scripts/configure.sh`
  Configura tudo: instala wrappers locais, gera envs, atualiza shell rc, integra setup do ElixirLS e salva estado.
- `scripts/reset.sh`
  Remove wrappers/envs, limpa o bloco gerenciado do shell rc, apaga cache local do wrapper, limpa artefatos locais do `elixir-ls` no Mason e restaura a configuração persistida do Hex.

## Fluxo recomendado

Configuração completa:

```bash
sh scripts/configure.sh
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
