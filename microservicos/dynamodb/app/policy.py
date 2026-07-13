"""Motor de decisão: aplica as regras externas a uma requisição DynamoDB.

Ordem de avaliação (tudo vindo do JSON externo, nada hardcoded de negócio):

1. Região (allowlist global).
2. Exceções: se houver uma exceção casando conta + ambiente + recurso (quando
   informado) que libere a operação/categoria, ela **autoriza** a ação — ignora o
   allow/deny do ambiente e dispensa a GMUD. Exceções expiradas são ignoradas.
3. Ambiente (dev/homol/prod...): allow/deny por operação e por categoria.
4. Modelagem/configuração: naming, billing, capacidade, GSIs, criptografia,
   proteção contra deleção, PITR e tags obrigatórias.

Tudo é **opt-in**: chave ausente/lista vazia não restringe. `rule_violation`
(HTTP 403) quando algo é barrado. Retorna a decisão (exceção casada + se exige
GMUD) para o handler seguir com o gate de produção.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .aws import ActionError
from .operations import Operation, resource_of

# Operações que criam/nomeiam uma tabela nova — alvo das regras de naming.
_CREATE_OPS = {"CreateTable", "CreateGlobalTable", "RestoreTableFromBackup", "RestoreTableToPointInTime"}


@dataclass(frozen=True)
class Decision:
    resource: str | None
    exception_id: str | None
    gmud_required: bool


def _deny(message: str) -> None:
    raise ActionError("rule_violation", message, 403)


def _parse_ts(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _exception_active(exc: dict) -> bool:
    expires = _parse_ts(exc.get("expiresAt"))
    if expires is None:
        return True
    now = datetime.now(timezone.utc)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now <= expires


def _matching_exception(rules: dict, req, op: Operation, resource: str | None) -> dict | None:
    for exc in rules.get("exceptions") or []:
        if not isinstance(exc, dict):
            continue
        if exc.get("account") != req.account:
            continue
        if exc.get("environment") not in (None, req.environment):
            continue
        exc_resource = exc.get("resource")
        if exc_resource and exc_resource != resource:
            continue
        if not _exception_active(exc):
            continue
        allow_ops = exc.get("allowOperations") or []
        allow_cats = exc.get("allowCategories") or []
        wildcard = "*" in allow_ops or "*" in allow_cats
        if wildcard or op.name in allow_ops or op.category in allow_cats:
            return exc
    return None


def _enforce_environment(env_cfg: dict, op: Operation) -> bool:
    """Aplica allow/deny do ambiente. Retorna se mutações exigem GMUD."""
    denied_ops = env_cfg.get("deniedOperations") or []
    if op.name in denied_ops:
        _deny(f"operação bloqueada neste ambiente: {op.name}")

    allowed_ops = env_cfg.get("allowedOperations") or []
    if allowed_ops and op.name not in allowed_ops:
        _deny(f"operação não permitida neste ambiente: {op.name} (permitidas: {allowed_ops})")

    denied_cats = env_cfg.get("deniedCategories") or []
    if op.category in denied_cats:
        _deny(f"categoria bloqueada neste ambiente: {op.category}")

    allowed_cats = env_cfg.get("allowedCategories") or []
    if allowed_cats and op.category not in allowed_cats:
        _deny(f"categoria não permitida neste ambiente: {op.category} (permitidas: {allowed_cats})")

    return _gmud_required(env_cfg, op)


def _gmud_required(env_cfg: dict, op: Operation) -> bool:
    """GMUD por categoria (ex.: só control-plane) tem precedência; senão, por mutação."""
    cats = env_cfg.get("gmudForCategories")
    if isinstance(cats, list) and cats:
        return op.category in cats
    return bool(env_cfg.get("requireGmudForMutations", False)) and op.mutating


def _enforce_naming(env_cfg: dict, op: Operation, resource: str | None) -> None:
    prefixes = env_cfg.get("tableNamePrefixes") or []
    if not prefixes or op.name not in _CREATE_OPS or not resource:
        return
    if not any(resource.startswith(px) for px in prefixes):
        _deny(f"nome de tabela fora do padrão do ambiente: '{resource}' (prefixos: {prefixes})")


def _cap(env_cfg: dict, key: str) -> float | None:
    v = env_cfg.get(key)
    return v if isinstance(v, (int, float)) else None


def _enforce_throughput(env_cfg: dict, args: dict) -> None:
    max_r = _cap(env_cfg, "maxReadCapacityUnits")
    max_w = _cap(env_cfg, "maxWriteCapacityUnits")
    if max_r is None and max_w is None:
        return

    def check(pt: dict, where: str) -> None:
        if not isinstance(pt, dict):
            return
        r = pt.get("ReadCapacityUnits")
        w = pt.get("WriteCapacityUnits")
        if max_r is not None and isinstance(r, (int, float)) and r > max_r:
            _deny(f"ReadCapacityUnits acima do limite ({where}): {r} > {max_r}")
        if max_w is not None and isinstance(w, (int, float)) and w > max_w:
            _deny(f"WriteCapacityUnits acima do limite ({where}): {w} > {max_w}")

    check(args.get("ProvisionedThroughput") or {}, "tabela")
    for gsi in args.get("GlobalSecondaryIndexes") or []:
        if isinstance(gsi, dict):
            check(gsi.get("ProvisionedThroughput") or {}, f"GSI {gsi.get('IndexName', '?')}")


def _enforce_create_modeling(env_cfg: dict, args: dict) -> None:
    billing = env_cfg.get("allowedBillingModes") or []
    mode = args.get("BillingMode", "PROVISIONED")
    if billing and mode not in billing:
        _deny(f"BillingMode não permitido: {mode} (permitidos: {billing})")

    max_gsi = _cap(env_cfg, "maxGlobalSecondaryIndexes")
    gsis = args.get("GlobalSecondaryIndexes") or []
    if max_gsi is not None and len(gsis) > max_gsi:
        _deny(f"número de GSIs acima do limite: {len(gsis)} > {max_gsi}")

    if env_cfg.get("requireEncryption"):
        sse = args.get("SSESpecification") or {}
        if not sse.get("Enabled"):
            _deny("criptografia obrigatória: envie SSESpecification.Enabled=true")

    if env_cfg.get("requireDeletionProtection") and args.get("DeletionProtectionEnabled") is not True:
        _deny("proteção contra deleção obrigatória: envie DeletionProtectionEnabled=true")

    required_tags = env_cfg.get("requireTags") or []
    if required_tags:
        present = {t.get("Key") for t in args.get("Tags") or [] if isinstance(t, dict)}
        missing = [k for k in required_tags if k not in present]
        if missing:
            _deny(f"tags obrigatórias ausentes: {missing}")

    _enforce_throughput(env_cfg, args)


def _enforce_update_modeling(env_cfg: dict, args: dict) -> None:
    billing = env_cfg.get("allowedBillingModes") or []
    mode = args.get("BillingMode")
    if billing and mode is not None and mode not in billing:
        _deny(f"BillingMode não permitido: {mode} (permitidos: {billing})")
    if env_cfg.get("requireDeletionProtection") and args.get("DeletionProtectionEnabled") is False:
        _deny("proteção contra deleção obrigatória: não é permitido desabilitar neste ambiente")
    _enforce_throughput(env_cfg, args)


def _enforce_pitr(env_cfg: dict, args: dict) -> None:
    if not env_cfg.get("requirePITR"):
        return
    spec = args.get("PointInTimeRecoverySpecification") or {}
    if spec.get("PointInTimeRecoveryEnabled") is False:
        _deny("PITR obrigatório neste ambiente: não é permitido desabilitar Point-in-Time Recovery")


def _enforce_modeling(env_cfg: dict, op: Operation, args: dict, resource: str | None) -> None:
    _enforce_naming(env_cfg, op, resource)
    if op.name in ("CreateTable", "RestoreTableFromBackup", "RestoreTableToPointInTime"):
        _enforce_create_modeling(env_cfg, args)
    elif op.name == "UpdateTable":
        _enforce_update_modeling(env_cfg, args)
    elif op.name == "UpdateContinuousBackups":
        _enforce_pitr(env_cfg, args)


def evaluate(rules: dict, req, op: Operation, args: dict) -> Decision:
    """Avalia a requisição contra as regras. Levanta ActionError se barrada."""
    allowed_regions = rules.get("allowedRegions") or []
    if allowed_regions and req.region not in allowed_regions:
        _deny(f"região não permitida: {req.region} (permitidas: {allowed_regions})")

    resource = resource_of(op, args) or (req.resource if req.resource != "*" else None)

    exc = _matching_exception(rules, req, op, resource)
    env_cfg = (rules.get("environments") or {}).get(req.environment) or {}

    if exc is not None:
        # Exceção autoriza a ação: pula allow/deny do ambiente e dispensa GMUD.
        if not exc.get("skipModeling"):
            _enforce_modeling(env_cfg, op, args, resource)
        return Decision(resource=resource, exception_id=exc.get("id"), gmud_required=False)

    gmud_required = _enforce_environment(env_cfg, op)
    _enforce_modeling(env_cfg, op, args, resource)
    return Decision(resource=resource, exception_id=None, gmud_required=gmud_required)
