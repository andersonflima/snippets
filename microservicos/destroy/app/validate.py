"""Validação dos args contra o input shape real da operação (botocore, offline).

Sem rede e sem credenciais: usa apenas os service models locais do botocore
(embarcado no boto3) para checar tipos e campos obrigatórios ANTES do dispatch.
Assim o dryRun também valida os params, não só o caminho real.
"""
from __future__ import annotations

from botocore.session import get_session
from botocore.validate import ParamValidator

from .aws import ActionError
from .operations import Operation

_session = get_session()
_shapes: dict[tuple[str, str], object] = {}


def _input_shape(client: str, op_name: str):
    key = (client, op_name)
    if key not in _shapes:
        model = _session.get_service_model(client).operation_model(op_name)
        _shapes[key] = model.input_shape
    return _shapes[key]


def validate_args(op: Operation, args: dict) -> None:
    shape = _input_shape(op.client, op.name)
    if shape is None:
        if args:
            raise ActionError("validation_error", f"{op.key} não aceita argumentos", 400)
        return
    report = ParamValidator().validate(args, shape)
    if report.has_errors():
        raise ActionError("validation_error", f"args inválidos p/ {op.key}: {report.generate_report()}", 400)
