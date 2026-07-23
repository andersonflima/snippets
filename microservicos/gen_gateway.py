"""Monta contratos do API Gateway a partir dos contratos por serviço.

Mantém dois artefatos:

- `api-gateway/contracts/<servico>/openapi.yaml`: contrato segregado do serviço.
- `api-gateway/openapi.yaml`: contrato consolidado, derivado dos segregados.

O consolidado une `paths` e `components` (schemas/securitySchemes) e o
request-validator. Desacoplado dos geradores: reflete exatamente os serviços
presentes no repo (o que foi removido não aparece).

Uso: python gen_gateway.py
"""
from __future__ import annotations

import glob
import os

import yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "api-gateway", "openapi.yaml")
SEGREGATED_ROOT = os.path.join(ROOT, "api-gateway", "contracts")


def load_contract(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def dump_contract(path: str, doc: dict, header: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(header)
        yaml.safe_dump(doc, fh, sort_keys=False, allow_unicode=True, width=100)


def clean_stale_contracts(active_services: set[str]) -> None:
    if not os.path.isdir(SEGREGATED_ROOT):
        return
    for entry in os.listdir(SEGREGATED_ROOT):
        path = os.path.join(SEGREGATED_ROOT, entry)
        if entry not in active_services and os.path.isdir(path):
            contract = os.path.join(path, "openapi.yaml")
            if os.path.exists(contract):
                os.remove(contract)
            try:
                os.rmdir(path)
            except OSError:
                pass


def main() -> None:
    contracts = sorted(glob.glob(os.path.join(ROOT, "*", "contract", "openapi.yaml")))
    gateway = {
        "openapi": "3.0.3",
        "info": {
            "title": "microservicos — API Gateway (consolidado)",
            "version": "1.0.0",
            "description": "Contrato consolidado de todos os action-services. Gerado por gen_gateway.py.",
        },
        "servers": [{"url": "https://${stageVariables.apiDomain}/${stageVariables.basePath}"}],
        "x-amazon-apigateway-request-validators": {
            "all": {"validateRequestBody": True, "validateRequestParameters": True}
        },
        "paths": {},
        "components": {"securitySchemes": {}, "schemas": {}},
    }
    services = []
    for path in contracts:
        svc = os.path.basename(os.path.dirname(os.path.dirname(path)))
        doc = load_contract(path)
        dump_contract(
            os.path.join(SEGREGATED_ROOT, svc, "openapi.yaml"),
            doc,
            "# Gerado por gen_gateway.py — contrato API Gateway segregado por microserviço.\n",
        )
        for p, spec in (doc.get("paths") or {}).items():
            if p in gateway["paths"]:
                raise SystemExit(f"conflito de path {p} (serviço {svc})")
            gateway["paths"][p] = spec
        comps = doc.get("components") or {}
        gateway["components"]["securitySchemes"].update(comps.get("securitySchemes") or {})
        gateway["components"]["schemas"].update(comps.get("schemas") or {})
        services.append(svc)

    clean_stale_contracts(set(services))
    dump_contract(OUT, gateway, "# Gerado por gen_gateway.py — NÃO editar à mão.\n")
    print(f"api-gateway/openapi.yaml: {len(services)} serviços, {len(gateway['paths'])} paths, "
          f"{len(gateway['components']['schemas'])} schemas")
    print(f"api-gateway/contracts: {len(services)} contratos segregados")
    print("serviços:", ", ".join(services))


if __name__ == "__main__":
    main()
