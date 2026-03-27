# Scripts

Interface pública para ambiente restrito:

- `scripts/configure_restricted_dev_env.sh`: instala e configura `mix`, `curl` e `git` com backend EC2 compartilhado
- `scripts/activate_restricted_dev_env.sh`: carrega os env-files do ambiente restrito só na sessão atual
- `scripts/deactivate_restricted_dev_env.sh`: remove da sessão atual as envs e paths do ambiente restrito
- `scripts/doctor_restricted_dev_env.sh`: valida se `mix`, `curl`, `git` e `nvim` estão vendo os wrappers
- `scripts/reset_restricted_dev_env.sh`: remove wrappers, env-files e referências no shell rc para zerar o ambiente restrito

Ferramentas operacionais:

- `scripts/docdb_stream_backup.exs`: backup DocumentDB em Elixir
- `scripts/configure_hex_config.sh`: persiste proxy/TLS diretamente no Hex via `mix hex.config`
- `scripts/fetch_url_via_ec2.sh`: baixa uma URL pelo EC2 via SSM e devolve o artefato por S3
- `scripts/fetch_mix_hex_cache_from_ec2.sh`: importa `~/.mix`, `~/.hex` e cache de projeto de um EC2
- `scripts/build_mason_seed_artifact.sh`: gera um artefato seed do Mason fora da máquina restrita

Implementação interna:

- `scripts/install/`: bootstrap, reset e instaladores/configuradores internos
- `scripts/wrappers/`: wrappers reais de `mix`, `curl` e `git`
- `scripts/ec2/elixir/`: scripts específicos de runtime Elixir/Erlang e backend remoto do `mix`
- `scripts/ec2/assets/`: helpers remotos para download via EC2
- `scripts/ec2/go/`: instalação de Go no EC2
- `scripts/ec2/mongodb/`: instalação de MongoDB Database Tools no EC2

Uso recomendado:

```bash
sh scripts/configure_restricted_dev_env.sh "<bucket>"
. scripts/activate_restricted_dev_env.sh
```

Para descarregar o ambiente da sessão atual:

```bash
. scripts/deactivate_restricted_dev_env.sh
```

Para validar o ambiente:

```bash
sh scripts/doctor_restricted_dev_env.sh
```
