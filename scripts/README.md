# Scripts

## Entrypoints canônicos

- `scripts/configure_restricted_dev_env.sh`
  Bootstrap completo: instala wrappers, gera envs, atualiza shell rc e salva estado.
- `scripts/reinstall_wrappers.sh`
  Reinstala wrappers de `curl/git/brew` (e opcionalmente `mix`) sem precisar resetar tudo.
- `scripts/validate_wrappers.sh`
  Valida wrappers ativos no `PATH`, binários reais e política de fallback EC2.
- `scripts/doctor_restricted_dev_env.sh`
  Diagnóstico detalhado (inclui `validate_wrappers` + visão de `nvim`).
- `scripts/reset_restricted_dev_env.sh`
  Remove wrappers/envs/bloco do shell rc e restaura Hex persistido.
- `scripts/activate_restricted_dev_env.sh`
  Carrega envs na sessão atual.
- `scripts/deactivate_restricted_dev_env.sh`
  Remove envs da sessão atual.

## Fluxo recomendado

Configuração inicial:

```bash
sh scripts/configure_restricted_dev_env.sh "<bucket>"
```

Com backend EC2 dos wrappers habilitado explicitamente:

```bash
sh scripts/configure_restricted_dev_env.sh "<bucket>" --enable-ec2-backend
```

Reinstalação rápida dos wrappers:

```bash
sh scripts/reinstall_wrappers.sh
```

Validação rápida:

```bash
sh scripts/validate_wrappers.sh
```

Diagnóstico completo:

```bash
sh scripts/doctor_restricted_dev_env.sh
```

## Ferramentas operacionais

- `scripts/list_github_repos_by_prefix.sh`
- `scripts/build_mason_seed_artifact.sh`
- `scripts/configure_hex_config.sh`
- `scripts/fetch_url_via_ec2.sh`
- `scripts/fetch_mix_hex_cache_from_ec2.sh`
- `scripts/docdb_stream_backup.exs`

## Organização interna

- `scripts/install/`: implementação canônica de instalação/configuração/validação.
- `scripts/wrappers/`: wrappers reais (`curl`, `wget`, `git`, `brew`).
- `scripts/ec2/`: helpers e automações EC2 (assets, git, elixir, go, mongodb).

## Compatibilidade

Scripts antigos como `scripts/install_*` e `scripts/*_wrapper.sh` continuam como entrypoints de compatibilidade, mas a manutenção principal está nos canônicos acima.
