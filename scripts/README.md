# Scripts

## Fluxo público (2 comandos)

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

## Diagnóstico opcional (implementação interna)

Validação rápida:

```bash
sh scripts/install/validate_wrappers.sh
```

## Ferramentas operacionais

- `scripts/install/build_mason_seed_artifact.sh`
- `scripts/ec2/elixir/configure_hex_config.sh`
- `scripts/ec2/assets/fetch_url_via_ec2.sh`
- `scripts/ec2/elixir/fetch_mix_hex_cache_from_ec2.sh`

## Organização interna

- `scripts/install/`: implementação canônica de instalação/configuração/validação.
- `scripts/wrappers/`: wrappers reais (`curl`, `wget`, `git`, `brew`).
- `scripts/ec2/`: helpers e automações EC2 (assets, git, elixir, go, mongodb).
