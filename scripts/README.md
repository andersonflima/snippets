# Scripts

## Fluxo público (2 comandos)

- `scripts/configure.sh`
  Configura tudo: instala wrappers, gera envs, atualiza shell rc, integra setup do ElixirLS e salva estado.
- `scripts/reset.sh`
  Remove tudo: wrappers/envs/bloco do shell rc, cache do wrapper, artefatos locais do elixir-ls no Mason e restaura Hex persistido.

## Fluxo recomendado

Configuração completa:

```bash
sh scripts/configure.sh "<bucket>"
```

Se o bucket informado não existir, o bootstrap cria automaticamente no `--aws-region` efetivo.
No fluxo público acima, o backend EC2 dos wrappers já é habilitado por padrão.

Para desligar o backend EC2 dos wrappers:

```bash
sh scripts/configure.sh "<bucket>" --disable-ec2-backend
```

Com proxy/CA corporativo no remoto (mix + wrappers via EC2):

```bash
sh scripts/configure.sh "<bucket>" \
  --ec2-proxy "http://proxy.corp:3128" \
  --ca-cert "/etc/ssl/certs/corp-ca.pem"
```

Remoção completa:

```bash
sh scripts/reset.sh
```

Após o reset completo, a próxima configuração deve informar o bucket novamente:

```bash
sh scripts/configure.sh "<bucket>"
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
