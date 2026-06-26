"""Configuração do BFF, carregada de variáveis de ambiente (imutável)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # --- sessão / JWT próprio do BFF -----------------------------------------
    jwt_secret: str
    jwt_issuer: str = "bff"
    jwt_ttl_seconds: int = 1800
    cookie_name: str = "bff_session"
    cookie_secure: bool = True
    cookie_samesite: str = "strict"  # strict | lax | none
    cookie_domain: str | None = None

    # --- store de usuários (JSON em env/secret; senhas em bcrypt) ------------
    users_json: str = "[]"

    # --- M2M para o API Gateway (client_credentials no Cognito) --------------
    gateway_base_url: str = ""
    m2m_token_url: str = ""
    m2m_client_id: str = ""
    m2m_client_secret: str = ""
    m2m_scope: str = ""
    upstream_timeout: float = 15.0

    # --- CORS (origem do frontend) -------------------------------------------
    cors_origins: list[str] = field(default_factory=list)


def load_settings() -> Settings:
    """Resolve as settings a partir do ambiente. Falha cedo se faltar segredo crítico."""
    secret = os.getenv("BFF_JWT_SECRET", "")
    if not secret:
        # Em produção isto deve vir de um secret; sem ele não há sessão segura.
        raise RuntimeError("BFF_JWT_SECRET é obrigatório")

    return Settings(
        jwt_secret=secret,
        jwt_issuer=os.getenv("BFF_JWT_ISSUER", "bff"),
        jwt_ttl_seconds=int(os.getenv("BFF_JWT_TTL_SECONDS", "1800")),
        cookie_name=os.getenv("BFF_COOKIE_NAME", "bff_session"),
        cookie_secure=os.getenv("BFF_COOKIE_SECURE", "true").lower() != "false",
        cookie_samesite=os.getenv("BFF_COOKIE_SAMESITE", "strict").lower(),
        cookie_domain=os.getenv("BFF_COOKIE_DOMAIN") or None,
        users_json=os.getenv("BFF_USERS", "[]"),
        gateway_base_url=os.getenv("API_GATEWAY_BASE_URL", "").rstrip("/"),
        m2m_token_url=os.getenv("BFF_M2M_TOKEN_URL", ""),
        m2m_client_id=os.getenv("BFF_M2M_CLIENT_ID", ""),
        m2m_client_secret=os.getenv("BFF_M2M_CLIENT_SECRET", ""),
        m2m_scope=os.getenv("BFF_M2M_SCOPE", ""),
        upstream_timeout=float(os.getenv("BFF_UPSTREAM_TIMEOUT", "15.0")),
        cors_origins=_split_csv(os.getenv("BFF_CORS_ORIGINS", "")),
    )
