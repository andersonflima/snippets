# Mapeamento de Roles e Permissões IAM

> Documento derivado diretamente do código dos microserviços em `microservicos/`.
> Cada permissão listada corresponde a uma chamada boto3 encontrada no código.
> Permissões marcadas com **(confirmar)** são inferidas (não vistas literalmente numa
> chamada, mas necessárias na prática) e devem ser validadas antes de produção.

## Duas camadas de identidade

Cada microserviço opera com **duas identidades distintas**:

### 1. Role de execução da plataforma (IRSA / Pod Identity no EKS)

É a identidade com que o pod do microserviço roda dentro do cluster. Ela é
propositalmente **mínima** e **não** carrega permissões sobre os recursos do
cliente. Ela só precisa de:

- **`sts:AssumeRole`** sobre os ARNs das roles das contas-cliente (o `roleArn`
  que chega em cada request). Toda chamada AWS de negócio é feita com as
  credenciais temporárias resultantes desse assume-role — nunca com a identidade
  da plataforma.
- **Leitura do backend de regras** (RULES) — feita com a identidade da
  plataforma (IRSA), não com a role assumida:
  - `s3:GetObject` no `RULES_BUCKET` (quando `RULES_BACKEND=s3`), ou
  - `dynamodb:GetItem` na `RULES_TABLE` (quando `RULES_BACKEND=dynamodb`).

O padrão de assume-role é idêntico em todos os serviços (`app/aws.py`):

```python
sts = boto3.client("sts")
resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"ms-{uuid}")
cred = resp["Credentials"]
session = boto3.Session(
    aws_access_key_id=cred["AccessKeyId"],
    aws_secret_access_key=cred["SecretAccessKey"],
    aws_session_token=cred["SessionToken"],
    region_name=region,
)
```

Cada request carrega `account` (12 dígitos), `roleArn`
(`arn:aws:iam::<account>:role/...`) e `region`. O serviço assume essa role e
executa as chamadas-alvo com as credenciais temporárias.

### 2. Role-alvo na conta-cliente (a role assumida)

É a role apontada pelo `roleArn` de cada request. **É ela que precisa conceder
as permissões reais sobre os recursos** (RDS, KMS, EC2, etc.). A trust policy
dessa role deve confiar na role de execução da plataforma. As tabelas abaixo
indicam, por microserviço, quais ações essa role assumida precisa conceder.

---

## Permissões por microserviço

Legenda da coluna **Camada**:
- **assumida** = permissão da role-alvo na conta-cliente (via `roleArn`).
- **plataforma** = permissão da role de execução (IRSA) do próprio pod.

### create

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| create | sts | `sts:AssumeRole` | plataforma |
| create | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| create | rds | `rds:CreateDBInstance` | assumida |
| create | rds | `rds:CreateDBSubnetGroup` | assumida |

> `gmud.py` **não faz chamada AWS** — só um POST HTTP para o serviço `servicenow`
> (`SERVICENOW_SERVICE_URL`) para o gate de GMUD em ambiente `prod`.

### destroy

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| destroy | sts | `sts:AssumeRole` | plataforma |
| destroy | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| destroy | rds | `rds:DeleteDBInstance` | assumida |
| destroy | rds | `rds:DeleteDBSnapshot` | assumida |
| destroy | ec2 | `ec2:DeleteVpcEndpoints` | assumida |
| destroy | ec2 | `ec2:DeleteSecurityGroup` | assumida |

### modify

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| modify | sts | `sts:AssumeRole` | plataforma |
| modify | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| modify | rds | `rds:ModifyDBInstance` | assumida |
| modify | ec2 | `ec2:ModifyInstanceAttribute` | assumida |

### start-stop

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| start-stop | sts | `sts:AssumeRole` | plataforma |
| start-stop | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| start-stop | rds | `rds:StartDBInstance`, `rds:StopDBInstance` | assumida |
| start-stop | rds | `rds:StartDBCluster`, `rds:StopDBCluster` | assumida |
| start-stop | ec2 | `ec2:StartInstances`, `ec2:StopInstances` | assumida |

### storage

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| storage | sts | `sts:AssumeRole` | plataforma |
| storage | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| storage | rds | `rds:ModifyDBInstance` | assumida |
| storage | ec2 | `ec2:ModifyVolume` | assumida |

### replicate

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| replicate | sts | `sts:AssumeRole` | plataforma |
| replicate | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| replicate | rds | `rds:ModifyDBSnapshotAttribute` | assumida |

> Compartilha o snapshot com a conta destino (`ValuesToAdd=[destinationAccount]`).
> A cópia/re-encriptação sob a KMS de destino é feita **pela conta de destino**,
> fora deste serviço.

### restore

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| restore | sts | `sts:AssumeRole` | plataforma |
| restore | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| restore | rds | `rds:CreateDBSnapshot` | assumida |
| restore | rds | `rds:RestoreDBInstanceFromDBSnapshot` | assumida |
| restore | rds | `rds:CreateDBClusterSnapshot` (Aurora) | assumida |
| restore | rds | `rds:RestoreDBClusterFromSnapshot` (Aurora) | assumida |
| restore | rds | `rds:CreateDBInstance` (membros do cluster Aurora) | assumida |

### db-password

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| db-password | sts | `sts:AssumeRole` | plataforma |
| db-password | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| db-password | rds | `rds:DescribeDBInstances` | assumida |
| db-password | secretsmanager | `secretsmanager:GetSecretValue` (MasterUserSecret + `newPasswordSecretArn`) | assumida |
| db-password | (rede) | acesso de rede à porta do banco + `kms:Decrypt` no CMK do secret **(confirmar)** | assumida |

> **Importante:** a troca de senha **não** usa `rds:ModifyDBInstance`. O serviço
> lê a senha admin do `MasterUserSecret` e a nova senha do `newPasswordSecretArn`
> via Secrets Manager, conecta diretamente no banco (PostgreSQL via `psycopg` /
> MySQL via `pymysql`) e executa `ALTER ROLE`/`ALTER USER`. Não há API AWS que
> altere a senha; ela é aplicada in-database. O `kms:Decrypt` só é necessário se
> os secrets estiverem cifrados com CMK gerenciada pelo cliente.

### vpc-link

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| vpc-link | sts | `sts:AssumeRole` | plataforma |
| vpc-link | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| vpc-link | ec2 | `ec2:ModifyVpcEndpointServicePermissions` | assumida |

> Autoriza a conta consumidora no VPC Endpoint Service (PrivateLink). **Não** usa
> `elbv2:*` nem `apigateway:*` — apenas a permissão de EC2/PrivateLink acima.

### kms

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| kms | sts | `sts:AssumeRole` | plataforma |
| kms | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| kms | kms | `kms:CreateKey` | assumida |
| kms | kms | `kms:CreateAlias` | assumida |
| kms | kms | `kms:PutKeyPolicy` (quando `keyPolicyJson`) | assumida |
| kms | rds | `rds:CopyDBSnapshot` (quando `targetResourceType=db-snapshot`) | assumida |

> O código **não** chama `kms:ScheduleKeyDeletion` nem `kms:EnableKeyRotation`.
> Para `db-instance`, o serviço não re-encripta in-place (retorna nota
> orientando snapshot/restore). A re-encriptação de snapshot usa `rds:CopyDBSnapshot`
> com `KmsKeyId` da nova key.

### finops

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| finops | sts | `sts:AssumeRole` | plataforma |
| finops | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES/preços) | plataforma |
| finops | rds | `rds:DescribeDBInstances`, `rds:DescribeDBSnapshots` | assumida |
| finops | ec2 | `ec2:DescribeInstances`, `ec2:DescribeVolumes`, `ec2:DescribeAddresses`, `ec2:DescribeSnapshots` | assumida |
| finops | elasticloadbalancing | `elasticloadbalancing:DescribeLoadBalancers`, `elasticloadbalancing:DescribeTargetGroups`, `elasticloadbalancing:DescribeTargetHealth` (ELBv2 + Classic) | assumida |
| finops | cloudwatch | `cloudwatch:GetMetricStatistics` | assumida |

> **Correção importante:** o finops **não** usa Cost Explorer (`ce:GetCostAndUsage`)
> nem `compute-optimizer:*`. O custo é calculado por heurística, aplicando uma
> **tabela de preços** (fornecida via regras/env) sobre métricas do CloudWatch e
> os inventários das chamadas `Describe*`. Portanto **não** conceda `ce:*` nem
> `compute-optimizer:*` a menos que o código passe a usá-los.

### insights

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| insights | sts | `sts:AssumeRole` | plataforma |
| insights | rds | `rds:DescribeDBInstances` | assumida |
| insights | ec2 | `ec2:DescribeInstances`, `ec2:DescribeVolumes`, `ec2:DescribeAddresses`, `ec2:DescribeSnapshots`, `ec2:DescribeVpcEndpoints` | assumida |
| insights | elasticloadbalancing | `elasticloadbalancing:DescribeLoadBalancers` | assumida |
| insights | kms | `kms:ListKeys`, `kms:DescribeKey` | assumida |
| insights | cloudwatch | `cloudwatch:GetMetricData`, `cloudwatch:GetMetricStatistics` | assumida |
| insights | logs | `logs:FilterLogEvents` | assumida |
| insights | secretsmanager | `secretsmanager:GetSecretValue` | assumida |
| insights | (kms) | `kms:Decrypt` para secrets cifrados com CMK **(confirmar)** | assumida |

> `INSIGHTS_MODE=mock` (default) usa dados sintéticos e não faz chamadas AWS. As
> permissões acima valem para `INSIGHTS_MODE=aws`. Serviço somente leitura
> (`Describe*`/`Get*`/`List*`).

### rds-data

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| rds-data | sts | `sts:AssumeRole` | plataforma |
| rds-data | s3 / dynamodb | `s3:GetObject` (`RULES_BUCKET`/`RULES_KEY`) \| `dynamodb:GetItem` (RULES) | plataforma |
| rds-data | rds-data | `rds-data:ExecuteStatement` | assumida |
| rds-data | secretsmanager | `secretsmanager:GetSecretValue` no `secretArn` do Data API **(confirmar)** | assumida |
| rds-data | (kms) | `kms:Decrypt` no CMK do secret/cluster **(confirmar)** | assumida |

> O Data API precisa de `resourceArn` (cluster Aurora) + `secretArn`. O código não
> chama `secretsmanager:GetSecretValue` diretamente, mas o próprio serviço
> `rds-data` resolve o secret — por isso a permissão de leitura do secret é
> necessária na prática (marcada como confirmar).

### servicenow

| Microserviço | Serviço AWS | Ações IAM necessárias | Camada |
| --- | --- | --- | --- |
| servicenow | s3 / dynamodb | `s3:GetObject` \| `dynamodb:GetItem` (RULES) | plataforma |
| servicenow | — | **nenhuma permissão AWS sobre recursos** (mudança via API externa) | — |

> O `servicenow` chama a ServiceNow Table API por HTTP; a mudança em si não usa
> nenhuma API AWS. Ele **não** faz `sts:AssumeRole` para executar a operação — só
> lê o backend de regras com a identidade da plataforma. Detalhes na seção do
> gate de GMUD abaixo.

---

## Políticas IAM de menor privilégio (exemplos)

> Recursos abaixo aparecem como `"*"` para legibilidade. **Em produção, restrinja
> por ARN e/ou por tag** (ex.: `Condition` com `aws:ResourceTag`, escopo de conta
> e região, ARNs de bucket/tabela específicos).

### (a) Role de execução da plataforma (IRSA)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeCustomerRoles",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": [
        "arn:aws:iam::*:role/plataforma-rds-actions-*"
      ]
    },
    {
      "Sid": "RulesBackendS3",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::RULES_BUCKET/rules/*"
    },
    {
      "Sid": "RulesBackendDynamoDB",
      "Effect": "Allow",
      "Action": "dynamodb:GetItem",
      "Resource": "arn:aws:dynamodb:*:*:table/RULES_TABLE"
    }
  ]
}
```

> Use **apenas** o statement `RulesBackendS3` **ou** o `RulesBackendDynamoDB`,
> conforme `RULES_BACKEND`. Restrinja `Resource` do `sts:AssumeRole` aos ARNs
> reais das roles-alvo (evite `role/*`).

### (b) Role-alvo na conta-cliente (frota de ações)

Agrupada por serviço. Inclua apenas os blocos correspondentes aos microserviços
que a role precisa atender.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RdsWrite",
      "Effect": "Allow",
      "Action": [
        "rds:CreateDBInstance",
        "rds:CreateDBSubnetGroup",
        "rds:DeleteDBInstance",
        "rds:DeleteDBSnapshot",
        "rds:ModifyDBInstance",
        "rds:StartDBInstance",
        "rds:StopDBInstance",
        "rds:StartDBCluster",
        "rds:StopDBCluster",
        "rds:CreateDBSnapshot",
        "rds:CreateDBClusterSnapshot",
        "rds:RestoreDBInstanceFromDBSnapshot",
        "rds:RestoreDBClusterFromSnapshot",
        "rds:CopyDBSnapshot",
        "rds:ModifyDBSnapshotAttribute"
      ],
      "Resource": "*"
    },
    {
      "Sid": "RdsRead",
      "Effect": "Allow",
      "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBSnapshots"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Ec2Write",
      "Effect": "Allow",
      "Action": [
        "ec2:DeleteVpcEndpoints",
        "ec2:DeleteSecurityGroup",
        "ec2:ModifyInstanceAttribute",
        "ec2:ModifyVolume",
        "ec2:StartInstances",
        "ec2:StopInstances",
        "ec2:ModifyVpcEndpointServicePermissions"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Ec2Read",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "ec2:DescribeAddresses",
        "ec2:DescribeSnapshots",
        "ec2:DescribeVpcEndpoints"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Kms",
      "Effect": "Allow",
      "Action": [
        "kms:CreateKey",
        "kms:CreateAlias",
        "kms:PutKeyPolicy",
        "kms:ListKeys",
        "kms:DescribeKey",
        "kms:Decrypt"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ElbRead",
      "Effect": "Allow",
      "Action": [
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTargetGroups",
        "elasticloadbalancing:DescribeTargetHealth"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Observability",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricData",
        "logs:FilterLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Secrets",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "*"
    },
    {
      "Sid": "RdsDataApi",
      "Effect": "Allow",
      "Action": [
        "rds-data:ExecuteStatement"
      ],
      "Resource": "*"
    }
  ]
}
```

> Notas de escopo:
> - Prefira dividir esta role por conjunto de microserviços em vez de conceder
>   tudo a uma única role (ex.: role de leitura para `finops`/`insights`, role de
>   escrita para `create`/`modify`/`restore`).
> - `kms:Decrypt`, `secretsmanager:GetSecretValue` e `rds-data`
>   (`secretsmanager`/`kms`) estão marcados como **(confirmar)** nas tabelas —
>   inclua conforme o uso real de CMK e do secret do Data API.
> - Restrinja `Resource` por ARN/tag e por região em produção.

---

## Gate de GMUD (ServiceNow)

O microserviço `servicenow` implementa o **gate de mudança (GMUD)** consultando a
**ServiceNow Table API** por HTTP. Para a mudança em si **não precisa de nenhuma
permissão AWS sobre recursos**; a única permissão AWS é a leitura do backend de
regras (`s3:GetObject` no `RULES_BUCKET` ou `dynamodb:GetItem` na `RULES_TABLE`),
com a identidade da plataforma (IRSA).

Em ambiente `prod`, cada microserviço de ação chama o `servicenow` (via
`SERVICENOW_SERVICE_URL`, `gmud.py`) com `operation=validate` antes de executar;
só prossegue se `detail.allowed == true`.

### Credenciais / ambiente (lado ServiceNow)

| Variável | Uso |
| --- | --- |
| `SERVICENOW_INSTANCE_URL` | Base da instância ServiceNow (obrigatória) |
| `SERVICENOW_TOKEN` | Bearer token (`Authorization: Bearer <token>`) — preferencial |
| `SERVICENOW_USER` / `SERVICENOW_PASSWORD` | HTTP Basic auth (fallback quando não há token) |
| `SERVICENOW_SERVICE_URL` | URL interna do serviço servicenow, usada pelo gate em `gmud.py` |
| `SERVICENOW_CHANGE_TABLE` | Tabela de changes (default `change_request`) |
| `SERVICENOW_TASK_TABLE` | Tabela de tasks (default `change_task`) |

### Campos e checagens de change-management lidos

A operação `validate` avalia (cada checagem é opt-out por flag; ausência de flag
mantém o default seguro):

| Checagem | Campo ServiceNow | Flag / config |
| --- | --- | --- |
| Estado liberado p/ execução | `state` | `SERVICENOW_ALLOWED_STATES` (default `-1,implement`) |
| Dentro da janela planejada | `start_date`/`end_date` (fallback `work_start`/`work_end`) | (sempre requerido) |
| Aprovações corretas | `approval` | `SERVICENOW_REQUIRE_APPROVAL`, `SERVICENOW_APPROVED_STATES` (default `approved`) |
| Sem conflitos | `conflict_status` | `SERVICENOW_REQUIRE_NO_CONFLICT`, `SERVICENOW_CONFLICT_OK` (default `no conflict`) |
| Tarefas registradas | contagem em `change_task` (via `change_request=<sys_id>`) | `SERVICENOW_REQUIRE_TASKS`, `SERVICENOW_MIN_TASKS` (default `1`) |

> A validação recente passou a **exigir também** aprovações, ausência de conflitos
> e tarefas registradas — não apenas o `state` e a janela. Todas as checagens
> requeridas precisam passar (`reasons == []`) para `allowed=true`.
>
> Operações: `validate` (gate/booleano `allowed`), `status` (retrato completo, sem
> gatear) e `register` (PATCH em `work_notes` da change; requer permissão de escrita
> na change no ServiceNow, não em AWS).
