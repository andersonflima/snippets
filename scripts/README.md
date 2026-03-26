# Scripts

Estrutura canônica:

- `scripts/docdb_stream_backup.exs`: backup DocumentDB em Elixir
- `scripts/ec2/elixir/`: instalação e correção de runtime Elixir/Erlang no EC2
- `scripts/ec2/go/`: instalação de Go no EC2
- `scripts/ec2/mongodb/`: instalação de MongoDB Database Tools no EC2
- `scripts/install/`: instaladores dos wrappers locais
- `scripts/install/configure_wrapper_envs.sh`: gera e conecta as envs dos wrappers ao shell
- `scripts/wrappers/`: wrappers reais de `curl` e `git`

Compatibilidade:

- Os nomes antigos continuam existindo em `scripts/` raiz como wrappers finos.
- Isso preserva comandos antigos enquanto a organização nova passa a ser por domínio.
