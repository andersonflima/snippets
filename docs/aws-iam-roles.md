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

As policies abaixo já vêm **escopadas por ARN** (least-privilege). Cada ação que
suporta resource-level permissions aponta para um ARN construído com placeholders;
as ações que **não** suportam resource-level ficam isoladas num statement próprio
com `"Resource": "*"` (com nota explicando o porquê e, quando aplicável, uma
`Condition` recomendada).

### Placeholders

Substitua estes marcadores pelos valores reais ao materializar a policy:

| Placeholder | Significado |
| --- | --- |
| `${account}` | ID da conta AWS (12 dígitos) da conta-cliente (o `account` do request) |
| `${region}` | Região do recurso-alvo (o `region` do request) |
| `${resource}` | Nome/identificador do recurso-alvo (ex.: `DBInstanceIdentifier`, `VpcEndpointId`, `GroupId`) |
| `${RULES_BUCKET}` | Bucket do backend de regras (`RULES_BUCKET`, quando `RULES_BACKEND=s3`) |
| `${RULES_PREFIX}` | Prefixo das chaves de regras (`RULES_KEY_PREFIX`, default `rules`) |
| `${RULES_TABLE}` | Tabela do backend de regras (`RULES_TABLE`, quando `RULES_BACKEND=dynamodb`) |

> Quando o recurso é dinâmico por request (nome variável), use `${resource}` como
> curinga controlado (ex.: `db:*` restrito por tag) ou materialize a policy por
> recurso/frota. Não deixe `Resource: "*"` amplo em ações de escrita.

### (a) Role de execução da plataforma (IRSA)

Confirmado no código (`app/aws.py` + `app/rules.py`): apenas `sts:AssumeRole` +
leitura do backend de regras (`s3:GetObject` na chave `${RULES_PREFIX}/<service>.json`,
**ou** `dynamodb:GetItem`). Não há `s3:ListBucket` no código.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeCustomerRoles",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": [
        "arn:aws:iam::*:role/ms-actions-*"
      ]
    },
    {
      "Sid": "RulesBackendS3",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${RULES_BUCKET}/${RULES_PREFIX}/*"
    },
    {
      "Sid": "RulesBackendDynamoDB",
      "Effect": "Allow",
      "Action": "dynamodb:GetItem",
      "Resource": "arn:aws:dynamodb:${region}:${account}:table/${RULES_TABLE}"
    }
  ]
}
```

> - `AssumeCustomerRoles`: `sts:AssumeRole` só aceita ARN de role como `Resource`
>   (a conta-alvo varia por request, por isso `iam::*`). Restrinja
>   `*:role/ms-actions-*` à **convenção de nome real** das roles-alvo — evite
>   `role/*`. Se quiser fixar também a conta, liste as contas-cliente conhecidas.
> - Use **apenas** `RulesBackendS3` **ou** `RulesBackendDynamoDB`, conforme
>   `RULES_BACKEND`. O código lê `${RULES_PREFIX}/<service>.json`, então
>   `${RULES_PREFIX}/*` cobre exatamente os objetos de regra (só `s3:GetObject`).

### (b) Role-alvo na conta-cliente (frota de ações)

Agrupada por serviço. Inclua apenas os blocos correspondentes aos microserviços
que a role precisa atender. Statements de escrita/leitura por recurso usam ARN de
recurso; statements de `Describe*`/`List*` ficam separados em `"Resource": "*"`
(ver bloco e nota adiante).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RdsInstanceWrite",
      "Effect": "Allow",
      "Action": [
        "rds:CreateDBInstance",
        "rds:DeleteDBInstance",
        "rds:ModifyDBInstance",
        "rds:StartDBInstance",
        "rds:StopDBInstance",
        "rds:CreateDBSnapshot",
        "rds:RestoreDBInstanceFromDBSnapshot"
      ],
      "Resource": [
        "arn:aws:rds:${region}:${account}:db:${resource}"
      ]
    },
    {
      "Sid": "RdsSubnetGroup",
      "Effect": "Allow",
      "Action": "rds:CreateDBSubnetGroup",
      "Resource": [
        "arn:aws:rds:${region}:${account}:subgrp:${resource}"
      ]
    },
    {
      "Sid": "RdsClusterWrite",
      "Effect": "Allow",
      "Action": [
        "rds:StartDBCluster",
        "rds:StopDBCluster",
        "rds:CreateDBClusterSnapshot",
        "rds:RestoreDBClusterFromSnapshot"
      ],
      "Resource": [
        "arn:aws:rds:${region}:${account}:cluster:${resource}"
      ]
    },
    {
      "Sid": "RdsSnapshotWrite",
      "Effect": "Allow",
      "Action": [
        "rds:DeleteDBSnapshot",
        "rds:CopyDBSnapshot",
        "rds:ModifyDBSnapshotAttribute"
      ],
      "Resource": [
        "arn:aws:rds:${region}:${account}:snapshot:${resource}"
      ]
    },
    {
      "Sid": "RdsClusterSnapshotWrite",
      "Effect": "Allow",
      "Action": [
        "rds:CreateDBClusterSnapshot",
        "rds:RestoreDBClusterFromSnapshot"
      ],
      "Resource": [
        "arn:aws:rds:${region}:${account}:cluster-snapshot:${resource}"
      ]
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
      "Resource": [
        "arn:aws:ec2:${region}:${account}:instance/${resource}",
        "arn:aws:ec2:${region}:${account}:volume/${resource}",
        "arn:aws:ec2:${region}:${account}:security-group/${resource}",
        "arn:aws:ec2:${region}:${account}:vpc-endpoint/${resource}",
        "arn:aws:ec2:${region}:${account}:vpc-endpoint-service/*"
      ]
    },
    {
      "Sid": "KmsKeyWrite",
      "Effect": "Allow",
      "Action": [
        "kms:CreateAlias",
        "kms:PutKeyPolicy",
        "kms:DescribeKey",
        "kms:Decrypt"
      ],
      "Resource": [
        "arn:aws:kms:${region}:${account}:key/*",
        "arn:aws:kms:${region}:${account}:alias/*"
      ]
    },
    {
      "Sid": "Secrets",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:${region}:${account}:secret:${secretName}-*"
      ]
    },
    {
      "Sid": "RdsDataApi",
      "Effect": "Allow",
      "Action": [
        "rds-data:ExecuteStatement"
      ],
      "Resource": [
        "arn:aws:rds:${region}:${account}:cluster:${cluster}"
      ]
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": [
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${region}:${account}:log-group:${resource}:*"
      ]
    }
  ]
}
```

#### Statement separado — ações SEM resource-level (`Resource: "*"`)

As ações abaixo **não suportam resource-level permissions na AWS**, então precisam
ficar num statement próprio com `"Resource": "*"`. Reduza o risco com `Condition`
(região e, para RDS/EC2, tag).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DescribeListNoResourceLevel",
      "Effect": "Allow",
      "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBSnapshots",
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "ec2:DescribeAddresses",
        "ec2:DescribeSnapshots",
        "ec2:DescribeVpcEndpoints",
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTargetGroups",
        "elasticloadbalancing:DescribeTargetHealth",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricData",
        "cloudwatch:ListMetrics",
        "kms:ListKeys",
        "kms:CreateKey",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": { "aws:RequestedRegion": "${region}" }
      }
    }
  ]
}
```

> Notas de escopo:
> - **`Resource: "*"` é obrigatório** para o statement `DescribeListNoResourceLevel`:
>   `rds:Describe*`, `ec2:Describe*`, `elasticloadbalancing:Describe*` e
>   `cloudwatch:GetMetric*`/`ListMetrics` não aceitam ARN de recurso; `kms:CreateKey`
>   também não (a key ainda não existe no momento da chamada), `kms:ListKeys` é uma
>   listagem global e `sts:GetCallerIdentity` não tem recurso-alvo. Inclua no
>   `Action` só as ações realmente usadas pelos serviços que a role atende. O
>   `sts:AssumeRole` da role de plataforma segue a mesma lógica de não ter escopo
>   por recurso próprio.
> - **`Condition` recomendada:** `aws:RequestedRegion` limita as chamadas à região
>   do request. Para RDS/EC2, se os recursos forem etiquetados, adicione também
>   `aws:ResourceTag/<chave>` (ex.: `"aws:ResourceTag/managed-by": "plataforma"`)
>   como filtro extra — recomendação a validar conforme a estratégia de tags do
>   cliente. `elasticloadbalancing:Describe*` e `cloudwatch:GetMetric*` não filtram
>   por tag de recurso, então ficam limitados só pela região.
> - `${resource}` no bloco EC2 (`Ec2Write`) deve casar com o tipo real usado por
>   cada request: `instance/*` (`modify_instance_attribute`, `start/stop_instances`),
>   `volume/*` (`modify_volume`), `security-group/*` (`delete_security_group`),
>   `vpc-endpoint/*` (`delete_vpc_endpoints`). `vpc-endpoint-service/*` cobre
>   `ec2:ModifyVpcEndpointServicePermissions` (vpc-link).
> - `KmsKeyWrite` cobre apenas as ações **com** resource-level (`CreateAlias`,
>   `PutKeyPolicy`, `DescribeKey`, `Decrypt`); `kms:CreateKey` fica no statement `*`.
> - `Secrets` (`secretsmanager:GetSecretValue`): troque `${secretName}` pelo nome
>   real do secret (Secrets Manager anexa um sufixo aleatório, daí o `-*`). Cobre o
>   `MasterUserSecret`/`newPasswordSecretArn` do `db-password`, o secret lido pelo
>   `insights` e o secret do Data API do `rds-data` **(confirmar)**.
> - `RdsDataApi` (`rds-data:ExecuteStatement`) escopa ao cluster Aurora
>   (`${cluster}` = `resourceArn` do request). O `rds-data` também depende, na
>   prática, de leitura do `secretArn` via Secrets Manager (statement `Secrets`) e
>   possivelmente `kms:Decrypt` no CMK do secret/cluster **(confirmar)**.
> - `kms:Decrypt` e `secretsmanager:GetSecretValue` estão marcados como
>   **(confirmar)** nas tabelas — inclua conforme o uso real de CMK e do secret do
>   Data API.
> - Prefira dividir esta role por conjunto de microserviços em vez de conceder tudo
>   a uma única role (ex.: role somente-leitura para `finops`/`insights`, role de
>   escrita para `create`/`modify`/`restore`).

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
