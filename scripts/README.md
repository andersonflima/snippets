# Scripts

## Fluxo principal (2 comandos)

- `scripts/configure.sh`
  Configura tudo: instala wrappers, gera envs, atualiza shell rc e salva estado.
- `scripts/reset.sh`
  Remove tudo: wrappers/envs/bloco do shell rc e restaura Hex persistido.

## Fluxo recomendado

Configuração completa:

```bash
sh scripts/configure.sh "<bucket>"
```

Com backend EC2 dos wrappers habilitado explicitamente:

```bash
sh scripts/configure.sh "<bucket>" --enable-ec2-backend
```

Remoção completa:

```bash
sh scripts/reset.sh
```

Reconfiguração completa reaproveitando bucket salvo (após primeira configuração):

```bash
sh scripts/configure.sh
```

## Diagnóstico opcional

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

Scripts antigos como `scripts/configure_restricted_dev_env.sh`, `scripts/reset_restricted_dev_env.sh`, `scripts/reinstall_wrappers.sh`, `scripts/install_*` e `scripts/*_wrapper.sh` continuam por compatibilidade, mas o fluxo recomendado fica restrito aos dois comandos acima.
