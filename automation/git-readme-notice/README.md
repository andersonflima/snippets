# git-readme-notice

Percorre **todas as branches** de um repositório (locais + remotas), adiciona um
aviso em **UPPERCASE** no topo do `README.md`, faz `git add` + `git commit` +
`git push` branch a branch e, ao final, gera um relatório Markdown que mostra —
entre outras coisas — **quais branches já tinham o aviso**.

O aviso padrão deixa claro que qualquer alteração feita no template Terraform
revoga a obrigação do time DATADEVOPS de prestar suporte a erros e problemas
decorrentes dessas mudanças.

## Uso

```sh
# ver o plano sem tocar no repositório
node index.js --repo /caminho/do/repo --dry-run

# execução real: atualiza, commita e faz push em todas as branches
node index.js --repo /caminho/do/repo --report relatorio.md
```

Requisitos: Node >= 18 e credenciais de push já configuradas para o remote.

## Comportamento

- Descobre branches locais e remotas (deduplicadas), na ordem alfabética.
- Para cada branch: checkout (criando tracking branch quando só existe no
  remote), fast-forward para a ponta do remote, prepend do aviso, commit apenas
  do arquivo alvo e push — antes de seguir para a próxima branch.
- **Idempotente**: branch que já contém a mensagem é pulada e listada na seção
  "Branches que já têm o aviso" do relatório.
- Branch sem `README.md` é pulada e reportada; branch com erro (ex.: local
  divergente do remote) não interrompe as demais.
- Ao final o repositório volta para a branch original.

## Flags

| Flag | Default | Descrição |
| --- | --- | --- |
| `--repo <path>` | `.` | repositório alvo |
| `--file <path>` | `README.md` | arquivo a atualizar em cada branch |
| `--message <text>` | aviso DATADEVOPS | texto do aviso (uppercased automaticamente) |
| `--commit-message <text>` | `docs: adiciona aviso...` | mensagem de commit |
| `--pattern <re>` | `.*` | regexp para filtrar branches |
| `--remote <name>` | `origin` | remote usado para descoberta e push |
| `--dry-run` | off | só inspeciona e reporta, sem escrever/commitar/pushar |
| `--no-fetch` | fetch ligado | pula o `git fetch --prune` inicial |
| `--report <path>` | — | grava o relatório Markdown também em arquivo |
| `--quiet` | off | silencia o log de progresso no stderr |

## Relatório

O relatório (stdout e, com `--report`, também em arquivo) agrupa as branches em:

1. **Branches que já têm o aviso** — nada a fazer (pedido central do relatório);
2. **Branches atualizadas** — com o hash do commit e status do push;
3. **Branches sem o arquivo** — puladas;
4. **Branches com erro** — com a causa, sem interromper o restante.
