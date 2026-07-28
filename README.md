# snippets

Repositório genérico para **scripts, ferramentas e arquivos de configuração** reutilizáveis.

É intencionalmente flexível: pode receber qualquer coisa útil — automações, helpers de operação, POCs, jobs AWS, configs de ambiente/editor e diagramas. Cada item é independente e autocontido.

## Estrutura

```
config/        # scripts e arquivos de CONFIGURAÇÃO / setup de ambiente
  homebrew/    #   setup de proxy/mirror do Homebrew (corrigir 403 atrás de proxy)
  lazyvim/     #   setup/rollback do LazyVim + Mason e helpers relacionados
  nvim/        #   configuração versionada do Neovim (LazyVim)

automation/    # scripts e ferramentas de AUTOMAÇÃO (executam tarefas)
  aws/         #   utilitários AWS (Lambda, IAM, KMS, DynamoDB, DocDB, EKS, ...)
  media/       #   automações de mídia
  misc/        #   utilitários diversos
  git-yaml-ref-bump/  # ferramenta Go para bump de refs YAML em branches

diagrams/      # diagramas (Mermaid)
docs/          # documentação específica de itens quando necessário

microservicos/ # microserviços action-driven (FastAPI) p/ pipeline de mascaramento
  docs/        #   arquitetura (diagrama 4K) + contratos
  api-gateway/ #   contratos OpenAPI do API Gateway (REST, VPC Link -> NLB -> EKS)
  <ação>/      #   um serviço autocontido por ação (Dockerfile + app FastAPI)
```

A divisão principal é por propósito: **`config/`** (preparar uma máquina/ambiente) versus **`automation/`** (rodar uma tarefa). Dentro de cada uma, subpastas por domínio.

## Como usar

Cada script é independente. O fluxo padrão:

1. Localize o item na pasta de propósito/domínio correspondente.
2. Veja os parâmetros disponíveis (a maioria suporta `--help`).
3. Rode localmente.

```bash
# Automação (exemplos)
python3 automation/aws/aws-lambda-exists-check.py --help
python3 automation/misc/upgrade_resource_version.py --help

# Configuração (exemplos)
bash config/lazyvim/setup_lazyvim_mason_from_zip.sh --help
bash config/lazyvim/setup_lazyvim_docker_toolchain.sh --help
bash config/homebrew/setup_homebrew_proxy.sh --help
```

## LazyVim/Mason com Docker

Para máquinas corporativas onde o Neovim local é macOS mas os LSPs/ferramentas precisam rodar em Linux, existe um fluxo dedicado:

- imagem Docker: `config/lazyvim/Dockerfile.nvim-toolchain`
- bootstrap: `config/lazyvim/setup_lazyvim_docker_toolchain.sh`
- documentação: `docs/lazyvim-docker-toolchain.md`

Esse fluxo mantém o `nvim` no host e move `gopls`, `lua-language-server`, `pyright-langserver`, `stylua`, `shellcheck` e outros comandos para wrappers locais que executam via `docker exec`.

## Requisitos

- Variam por item. Scripts Python geralmente pedem `python3` (3.10+) e, quando AWS, `boto3` (+ `openpyxl` para relatórios Excel).
- Scripts AWS usam credenciais/região válidas no ambiente de execução.
- A ferramenta `git-yaml-ref-bump` requer Go.

Detalhes de uso e flags de cada script ficam no próprio arquivo (docstring/`--help`) ou em `docs/` quando houver documentação dedicada.

## Convenções para novos itens

- Coloque em `config/` ou `automation/`, na subpasta de domínio adequada (crie uma nova se fizer sentido).
- Nome descritivo e orientado a intenção.
- Entrada por argumentos/flags; evite valores mágicos no código.
- Erros claros e exit code consistente.
- Atualize esta estrutura/README quando adicionar uma categoria nova.
