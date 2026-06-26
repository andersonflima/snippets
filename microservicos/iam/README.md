# IAM — role dos microserviços action-driven

Role que os 10 microserviços assumem (via `STS:AssumeRole`) **na conta-alvo**
para executar as ações sobre os recursos. Deve ser **deployada na conta do
cliente**; quem a assume é a sua **role de plataforma** (a que já tem permissão
de assumir esta role quando deployada).

## Cadeia de assume-role

```
Pod no EKS (IRSA, conta da plataforma)
  └─ assume → role de plataforma  (platform_role_arn)   ← já existe
        └─ assume → microservicos-actions-role (esta)   ← deploy na conta do cliente
              └─ executa as ações (RDS / EC2 / KMS / Secrets Manager)
```

A `microservicos-actions-role` **confia** (trust policy) na
`platform_role_arn`; as **permissões** estão em `permissions-policy.json`.

## Arquivos

| Arquivo | O quê |
|---------|-------|
| `trust-policy.json` | Trust policy (quem pode assumir). Substitua `<PLATFORM_ACCOUNT_ID>` e `<PLATFORM_ASSUME_ROLE>`. |
| `permissions-policy.json` | Permissões (todas as ações dos microserviços). |
| `role.tf` | Mesma role via Terraform (trust parametrizado + permissões do JSON). |

## Ações cobertas (por serviço / microserviço)

| Serviço AWS | Ações | Usado por |
|-------------|-------|-----------|
| **RDS** (describe) | DescribeDBInstances/Clusters/Snapshots/ClusterSnapshots/SubnetGroups, ListTagsForResource | todos |
| **RDS** (snapshot) | Create/Copy/Restore/ModifyAttribute/Delete de DB e DBCluster snapshots | `restore`, `kms`, `replicate`, `destroy` |
| **RDS** (provision/modify/power) | CreateDBInstance, CreateDBSubnetGroup, ModifyDBInstance, ModifyDBCluster, DeleteDBInstance, Start/StopDBInstance, Start/StopDBCluster, Add/RemoveTags | `create`, `modify`, `storage`, `destroy`, `start-stop` |
| **EC2** (describe) | DescribeInstances/Volumes/SecurityGroups/VpcEndpoints/EndpointServices/Subnets/Vpcs | `modify`, `storage`, `vpc-link`, `destroy` |
| **EC2** (modify/power/PrivateLink) | ModifyInstanceAttribute, ModifyVolume, Start/StopInstances, Create/Modify/Delete VpcEndpointServiceConfiguration, ModifyVpcEndpointServicePermissions, DeleteVpcEndpoints, DeleteSecurityGroup, Create/DeleteTags | `modify`, `storage`, `start-stop`, `vpc-link`, `destroy` |
| **KMS** (gestão) | CreateKey, Create/Update/DeleteAlias, ListAliases/Keys, DescribeKey, Put/GetKeyPolicy, TagResource, EnableKeyRotation, ScheduleKeyDeletion | `kms` |
| **KMS** (cripto/grants) | Encrypt, Decrypt, ReEncrypt*, GenerateDataKey*, CreateGrant, List/Retire/RevokeGrant | `kms`, `restore`, `replicate` (snapshots encriptados) |
| **Secrets Manager** | GetSecretValue, DescribeSecret, ListSecrets | `db-password` (master secret + nova senha) |
| **IAM** | PassRole (condicionado a `monitoring.rds.amazonaws.com`) | `create`/`restore`/`modify` quando usam Enhanced Monitoring |
| **RDS Data API** | rds-data: Execute/BatchExecute/Begin/Commit/RollbackTransaction | `rds-data` (wrapper seguro de SQL) |

> **Nota (`rds-data`):** as **regras de SQL** são lidas de um bucket S3 com a
> **identidade da plataforma (IRSA)** — não com esta role da conta-alvo. Portanto
> a role IRSA do pod precisa de `s3:GetObject` no bucket de regras
> (`RULES_BUCKET`). Esta role (conta-alvo) só precisa do `rds-data:*` acima e do
> `secretsmanager:GetSecretValue` (já incluído) do segredo do banco.

## Criar via AWS CLI (na conta do cliente)

```bash
# 1) Edite trust-policy.json: troque <PLATFORM_ACCOUNT_ID> e <PLATFORM_ASSUME_ROLE>.

# 2) Crie a role com a trust policy:
aws iam create-role \
  --role-name microservicos-actions-role \
  --assume-role-policy-document file://trust-policy.json \
  --max-session-duration 3600

# 3) Anexe as permissões (inline):
aws iam put-role-policy \
  --role-name microservicos-actions-role \
  --policy-name microservicos-actions-policy \
  --policy-document file://permissions-policy.json
```

A partir daí os microserviços fazem:
`sts:assume-role --role-arn arn:aws:iam::<CLIENT_ACCOUNT_ID>:role/microservicos-actions-role`.

## Criar via Terraform

```bash
terraform init
terraform apply \
  -var 'platform_role_arn=arn:aws:iam::<PLATFORM_ACCOUNT_ID>:role/<PLATFORM_ASSUME_ROLE>'
# opcional: -var 'external_id=<id>'
```

## Endurecimento (recomendado p/ produção)

`permissions-policy.json` usa `Resource: "*"` para simplicidade (e porque
`kms:CreateKey` exige `*`). Para least-privilege, considere:

- **Escopo por recurso/tag**: restrinja `Resource` aos ARNs do pipeline, ou use
  `Condition` com `aws:ResourceTag/<chave>` (ex.: só recursos `pipeline=masking`).
- **Restrição de região**: adicione `Condition` `aws:RequestedRegion` com a lista
  de regiões permitidas em cada statement.
- **ExternalId no trust**: exija `sts:ExternalId` (descomente no Terraform via
  `-var external_id=...`, ou adicione o bloco `Condition` no `trust-policy.json`):

  ```json
  "Condition": { "StringEquals": { "sts:ExternalId": "<EXTERNAL_ID>" } }
  ```

- **Sessão curta**: `max_session_duration` 3600s (1h) já é o default aqui.
- **Separar leitura/escrita**: se quiser, quebre em duas roles (read-only para
  describe; write para as ações mutáveis) e atribua por microserviço.
