# Scripts

Estrutura canônica:

- `scripts/setup_restricted_dev_env.sh`: bootstrap único para instalar e configurar `mix`, `curl` e `git` com backend EC2 compartilhado
- `scripts/configure_restricted_dev_env.sh`: atalho simples para o bootstrap completo, aceitando o bucket como primeiro argumento posicional
- `scripts/activate_restricted_dev_env.sh`: carrega os env-files do ambiente restrito só na sessão atual
- `scripts/reset_restricted_dev_env.sh`: remove wrappers, env-files e referências no shell rc para zerar o ambiente restrito
- `scripts/docdb_stream_backup.exs`: backup DocumentDB em Elixir
- `scripts/ec2/elixir/`: instalação e correção de runtime Elixir/Erlang no EC2
- `scripts/ec2/elixir/configure_mix_hex_envs.sh`: configura proxy/CA/Hex/Mix para ambiente restrito
- `scripts/ec2/elixir/configure_hex_config.sh`: persiste proxy/TLS diretamente no Hex via `mix hex.config`
- `scripts/ec2/elixir/fetch_mix_hex_cache_from_ec2.sh`: importa `~/.mix`, `~/.hex` e cache de projeto de um EC2
- `scripts/ec2/elixir/mix_via_ec2.sh`: executa `mix` no EC2 e sincroniza de volta deps/cache úteis
- `scripts/ec2/assets/fetch_url_via_ec2.sh`: baixa uma URL pelo EC2 via SSM e devolve o artefato por S3
- `scripts/install/install_mix_ec2_wrapper.sh`: instala um wrapper `mix` que delega comandos de dependência para o EC2
- `scripts/install/configure_mix_via_ec2_envs.sh`: configura as envs do wrapper `mix` via EC2
- `scripts/ec2/go/`: instalação de Go no EC2
- `scripts/ec2/mongodb/`: instalação de MongoDB Database Tools no EC2
- `scripts/install/`: instaladores dos wrappers locais
- `scripts/install/build_mason_seed_artifact.sh`: gera um artefato seed do Mason fora da máquina restrita
- `scripts/install/configure_wrapper_envs.sh`: gera e conecta as envs dos wrappers ao shell
- `scripts/install/configure_wrapper_envs_zsh.sh`: gera e conecta as envs dos wrappers diretamente ao `~/.zshrc`
- `scripts/wrappers/`: wrappers reais de `curl` e `git`

Compatibilidade:

- Os nomes antigos continuam existindo em `scripts/` raiz como wrappers finos.
- Isso preserva comandos antigos enquanto a organização nova passa a ser por domínio.
