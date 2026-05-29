# snippets

Repositório genérico para **scripts utilitários e automações**.

Este repo é intencionalmente flexível: pode conter scripts independentes, helpers de operação, POCs, jobs AWS e utilitários locais.

## Objetivo

- Centralizar scripts reutilizáveis em um só lugar.
- Manter automações pequenas, objetivas e fáceis de executar.
- Permitir evolução incremental sem acoplar tudo em um único projeto.

## Estrutura

- `*.py`: scripts Python (utilitários, automações, integrações AWS etc.).
- `*.sh`: scripts shell para operação e setup.
- `scripts/`: automações de setup/rollback.
- `tests/` e `test_*.py`: testes automatizados.

## Requisitos gerais

- Python `3.10+`.
- Dependências variam por script (ex.: `boto3` para integrações AWS).
- Em scripts AWS, use credenciais/região válidas no ambiente de execução.

## Scripts de setup de máquina (LazyVim + Mason)

A pasta `scripts/` mantém dois scripts:

- `scripts/setup_lazyvim_mason_from_zip.sh`
- `scripts/undo_lazyvim_mason_from_zip.sh`

### O que o setup faz

- baixa dependências usando rota ZIP do GitHub (`/archive/refs/heads/<branch>.zip`);
- não usa `curl` nem `wget` (download por helper Python com biblioteca padrão e retry);
- instala wrappers locais de `curl`/`wget` em `~/.local/share/nvim/wrappers/bin` para runtime do LazyVim/Mason em ambientes bloqueados;
- injeta esse diretório no `PATH` do Neovim no startup (via spec local do LazyVim);
- instala configuração do `LazyVim/starter`;
- instala plugins do manifesto em `~/.local/share/nvim/lazy`;
- instala registry local do Mason em `~/.cache/nvim/mason-registry-main`;
- cria override para Mason priorizar registry local `file:`.

### Execução

```bash
bash scripts/setup_lazyvim_mason_from_zip.sh --force
bash scripts/undo_lazyvim_mason_from_zip.sh
```

Depois do setup, abra um novo terminal (ou rode `source ~/.zshrc`) para refletir o `PATH` também no shell.

## Como usar

1. Entre no repositório.
2. Abra o script alvo para ver os parâmetros disponíveis.
3. Rode localmente com `--help` quando suportado.

Exemplos:

```bash
python3 upgrade_resource_version.py --help
python3 dynamodb_snapshot_lambda.py --help
bash scripts/setup_lazyvim_mason_from_zip.sh --help
```

## Testes

Para validar mudanças, execute os testes relacionados ao script alterado.

Exemplos:

```bash
python3 -m unittest test_crate_tables.py
python3 -m unittest test_kms_lambda.py
```

## Convenções para novos scripts

- Nome descritivo (`verbo_contexto.py` ou `contexto_acao.sh`).
- Entrada por argumentos/flags (evitar valores mágicos no código).
- Erros claros e exit code consistente.
- README e exemplos atualizados quando houver mudança relevante de uso.
- Sempre que possível, adicionar teste junto da automação.

## Observações

- Este README é propositalmente **genérico** para refletir o papel do repositório.
- Documentação específica de um script pode ficar no próprio arquivo, em comentários de uso, ou em docs dedicadas quando necessário.
