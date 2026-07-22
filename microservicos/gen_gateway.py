"""Monta o contrato consolidado do API Gateway a partir dos contratos por serviço.

Faz o merge de todos os `*/contract/openapi.yaml` (verb-services gerados +
serviços especiais) num único `api-gateway/openapi.yaml`: une `paths` e
`components` (schemas/securitySchemes) e o request-validator. Desacoplado dos
geradores — reflete exatamente os serviços presentes no repo (o que foi removido
não aparece).

Uso: python gen_gateway.py
"""
from __future__ import annotations

import glob
import os

import yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "api-gateway", "openapi.yaml")


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
        doc = yaml.safe_load(open(path))
        for p, spec in (doc.get("paths") or {}).items():
            if p in gateway["paths"]:
                raise SystemExit(f"conflito de path {p} (serviço {svc})")
            gateway["paths"][p] = spec
        comps = doc.get("components") or {}
        gateway["components"]["securitySchemes"].update(comps.get("securitySchemes") or {})
        gateway["components"]["schemas"].update(comps.get("schemas") or {})
        services.append(svc)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as fh:
        fh.write("# Gerado por gen_gateway.py — NÃO editar à mão.\n")
        yaml.safe_dump(gateway, fh, sort_keys=False, allow_unicode=True, width=100)
    print(f"api-gateway/openapi.yaml: {len(services)} serviços, {len(gateway['paths'])} paths, "
          f"{len(gateway['components']['schemas'])} schemas")
    print("serviços:", ", ".join(services))


if __name__ == "__main__":
    main()
