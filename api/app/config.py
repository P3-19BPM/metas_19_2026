from dataclasses import dataclass
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

# Allows local config from project `.env` and `api/.env` when running from source.
load_dotenv(Path(__file__).resolve().parents[2] / ".env")
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _as_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_csv_list(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    items = [p.strip().rstrip("/") for p in str(value).split(",")]
    return tuple(p for p in items if p)


@dataclass(frozen=True)
class Settings:
    db_backend: str = getenv("APP_DB_BACKEND", "sqlite").strip().lower()
    db_path: str = getenv("APP_DB_PATH", "./data/metas.db")
    postgres_url: str = getenv("APP_POSTGRES_URL", "").strip()
    postgres_host: str = getenv("POSTGRES_HOST", "").strip()
    postgres_port: str = getenv("POSTGRES_PORT", "5432").strip()
    postgres_db: str = getenv("POSTGRES_DB", "").strip()
    postgres_user: str = getenv("POSTGRES_USER", "").strip()
    postgres_password: str = getenv("POSTGRES_PASSWORD", "").strip()
    timezone: str = getenv("APP_TIMEZONE", "America/Sao_Paulo")
    agent_token: str = getenv("APP_AGENT_TOKEN", "")
    admin_token: str = getenv("APP_ADMIN_TOKEN", "")
    web_login_user: str = getenv("APP_WEB_LOGIN_USER", "admin").strip()
    web_login_password: str = getenv("APP_WEB_LOGIN_PASSWORD", "").strip()
    web_session_secret: str = getenv("APP_WEB_SESSION_SECRET", "change-me").strip()
    web_cookie_secure: bool = _as_bool(getenv("APP_WEB_COOKIE_SECURE"), True)
    embed_parent_origin: str = getenv(
        "APP_EMBED_PARENT_ORIGIN", "https://institucional.policiamilitar.mg.gov.br"
    ).strip().rstrip("/")
    embed_parent_origins: tuple[str, ...] = _as_csv_list(getenv("APP_EMBED_PARENT_ORIGINS"))
    web_session_max_age_seconds: int = int(getenv("APP_WEB_SESSION_MAX_AGE_SECONDS", "43200"))
    support_whatsapp_number: str = getenv("APP_SUPPORT_WHATSAPP_NUMBER", "").strip()
    support_contact_label: str = getenv("APP_SUPPORT_CONTACT_LABEL", "P3/19 BPM / Sgt Novais").strip()
    auto_daily_enabled: bool = _as_bool(getenv("APP_AUTO_DAILY_ENABLED"), True)
    daily_hour: int = int(getenv("APP_DAILY_HOUR", "6"))
    online_minutes: int = int(getenv("APP_ONLINE_MINUTES", "3"))
    heartbeat_stale_seconds: int = int(getenv("APP_HEARTBEAT_STALE_SECONDS", "45"))
    poll_seconds: int = int(getenv("APP_POLL_SECONDS", "10"))
    mapa_intranet_allowed_referers: tuple[str, ...] = _as_csv_list(
        getenv(
            "APP_MAPA_INTRANET_ALLOWED_REFERERS",
            ",".join(
                [
                    "https://institucional.policiamilitar.mg.gov.br/#/area-institucional/39/subpagina/3200",
                    "https://institucional-administracao.policiamilitar.mg.gov.br/#/edicao/39/sub-paginas/editar-sub-pagina/3200",
                ]
            ),
        )
    )
    minio_access_key: str = getenv("MINIO_ACCESS_KEY", "").strip()
    minio_secret_key: str = getenv("MINIO_SECRET_KEY", "").strip()
    minio_endpoint: str = getenv("MINIO_ENDPOINT", "").strip()
    minio_secure: bool = _as_bool(getenv("MINIO_SECURE"), True)
    minio_bucket_public: str = getenv("MINIO_BUCKET_PUBLIC", "").strip()
    minio_public_url: str = getenv("MINIO_PUBLIC_URL", "").strip().rstrip("/")


settings = Settings()
