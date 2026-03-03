import asyncio
import base64
import hashlib
import hmac
import io
import json
import mimetypes
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote, quote_plus

from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError

from .config import settings
from .db import (
    create_job,
    create_access_request,
    create_web_user,
    ensure_agent,
    get_next_job,
    get_overview,
    get_kpi_units,
    get_kpi_stats,
    get_web_user_by_numero_policia,
    get_web_user_by_username,
    get_web_user_by_id,
    init_db,
    list_access_requests,
    list_web_users,
    mark_stale_agents,
    set_job_result,
    set_job_started,
    get_kpi_summary,
    get_kpi_monthly,
    upsert_kpi_rows,
    upsert_reds_rows,
    query_reds,
    query_reds_cia_presos_summary,
    purge_kpi_rows,
    purge_reds_rows,
    purge_fact_rows,
    set_access_request_status,
    set_web_user_active,
    set_web_user_password,
    update_web_user,
    touch_web_user_login,
    upsert_fact_rows,
    create_mapa_assignment,
    delete_mapa_assignment,
    get_mapa_assignment_by_id,
    get_mapa_commander_by_numero_policia,
    list_mapa_assignments,
    list_mapa_commanders,
    list_mapa_scope_metadata,
    upsert_mapa_scope_metadata,
    update_mapa_assignment_status,
    upsert_mapa_commander,
)
from .scheduler import maybe_create_daily_job
from .schemas import (
    FactIngestIn,
    HeartbeatIn,
    JobResultIn,
    KpiIngestIn,
    ManualJobIn,
    PurgeIn,
    RedsIngestIn,
    WebUserActiveIn,
    WebUserCreateIn,
    WebUserSelfPasswordIn,
    WebUserUpdateIn,
    WebUserPasswordIn,
    AccessRequestIn,
    MapaCommanderEditIn,
    MapaScopeMetadataUpsertIn,
    MapaAssignmentStatusIn,
    MapaAssignmentUpsertIn,
)
from .intranet_mapa_utils import (
    build_xlsx_bytes,
    list_mapa_bairro_rows,
    list_mapa_export_rows,
    list_mapa_features,
    normalize_mapa_text,
)

SESSION_COOKIE_NAME = "meta_ui_session"
PWD_HASH_PREFIX = "pbkdf2_sha256"
PWD_HASH_ITERS = 390000


def _check_agent_token(authorization: str = Header(default="")) -> None:
    expected = f"Bearer {settings.agent_token}" if settings.agent_token else ""
    if expected and authorization != expected:
        raise HTTPException(status_code=401, detail="invalid agent token")


def _check_admin_token(x_admin_token: str = Header(default="")) -> None:
    if settings.admin_token and x_admin_token == settings.admin_token:
        return
    if settings.admin_token:
        raise HTTPException(status_code=401, detail="invalid admin token")


def _password_hash(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        (password or "").encode("utf-8"),
        salt,
        PWD_HASH_ITERS,
    )
    return (
        f"{PWD_HASH_PREFIX}${PWD_HASH_ITERS}$"
        f"{base64.urlsafe_b64encode(salt).decode('ascii').rstrip('=')}$"
        f"{base64.urlsafe_b64encode(digest).decode('ascii').rstrip('=')}"
    )


def _password_verify(password: str, encoded: str) -> bool:
    try:
        prefix, iter_s, salt_b64, digest_b64 = str(encoded or "").split("$", 3)
        if prefix != PWD_HASH_PREFIX:
            return False
        iterations = int(iter_s)
        salt = base64.urlsafe_b64decode(salt_b64 + "=" * (-len(salt_b64) % 4))
        expected = base64.urlsafe_b64decode(digest_b64 + "=" * (-len(digest_b64) % 4))
        got = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, iterations)
        return hmac.compare_digest(got, expected)
    except Exception:
        return False


def _is_trusted_embed(request: Request) -> bool:
    referer = str(request.headers.get("referer") or "").strip()
    sec_fetch_dest = str(request.headers.get("sec-fetch-dest") or "").strip().lower()
    origins = [o for o in (settings.embed_parent_origins or ()) if o]
    if not origins and settings.embed_parent_origin:
        origins = [settings.embed_parent_origin]
    admin_embed_origin = "https://institucional-administracao.policiamilitar.mg.gov.br"
    if admin_embed_origin not in origins:
        origins.append(admin_embed_origin)
    if not origins:
        return False
    return any(referer.startswith(origin) for origin in origins) and sec_fetch_dest == "iframe"


def _is_mapa_intranet_embed(request: Request) -> bool:
    if not _is_trusted_embed(request):
        return False
    referer = str(request.headers.get("referer") or "").strip()
    allowed = [r for r in (settings.mapa_intranet_allowed_referers or ()) if r]
    if not allowed:
        return True
    return any(referer.startswith(prefix) for prefix in allowed)


def _session_token(user: str, exp: int, kind: str = "login") -> str:
    kind_norm = (kind or "login").strip().lower() or "login"
    payload = f"{user}|{exp}|{kind_norm}"
    sig = hmac.new(settings.web_session_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _parse_session_token(token: str) -> dict | None:
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        parts = raw.split("|")
        if len(parts) == 4:
            user, exp_str, kind, sig = parts
            payload = f"{user}|{exp_str}|{kind}"
        elif len(parts) == 3:
            # Compatibilidade com cookies antigos (antes da separacao login/embed).
            user, exp_str, sig = parts
            kind = "login"
            payload = f"{user}|{exp_str}"
        else:
            return None
        expected_sig = hmac.new(
            settings.web_session_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        exp = int(exp_str)
        if exp < int(time.time()):
            return None
        return {"user": user, "exp": exp, "kind": (kind or "login")}
    except Exception:
        return None


def _is_valid_session_token(token: str) -> bool:
    return _parse_session_token(token) is not None


def _has_valid_session_cookie(request: Request) -> bool:
    token = str(request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    return bool(token and _is_valid_session_token(token))


def _session_user_from_request(request: Request) -> str | None:
    token = str(request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    parsed = _parse_session_token(token) if token else None
    user = str((parsed or {}).get("user") or "").strip()
    return user or None


def _session_kind_from_request(request: Request) -> str | None:
    token = str(request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    parsed = _parse_session_token(token) if token else None
    kind = str((parsed or {}).get("kind") or "").strip().lower()
    return kind or None


def _session_db_user(request: Request) -> dict | None:
    username = _session_user_from_request(request)
    if not username:
        return None
    user = get_web_user_by_username(username)
    if user:
        return user
    return get_web_user_by_numero_policia(username)


def _is_ui_admin(request: Request) -> bool:
    if _is_trusted_embed(request):
        return False
    db_user = _session_db_user(request)
    if db_user is not None:
        return bool(db_user.get("ativo")) and str(db_user.get("perfil") or "").lower() == "admin"
    username = (_session_user_from_request(request) or "").strip().lower()
    legacy_user = (settings.web_login_user or "admin").strip().lower()
    return bool(username) and username == legacy_user


def _check_admin_token_or_ui_admin(request: Request, x_admin_token: str = Header(default="")) -> None:
    if settings.admin_token and x_admin_token == settings.admin_token:
        return
    if _is_ui_admin(request):
        return
    raise HTTPException(status_code=403, detail="admin required")


def _set_session_cookie(response, user: str, kind: str = "login") -> None:
    max_age = max(300, int(settings.web_session_max_age_seconds))
    exp = int(time.time()) + max_age
    token = _session_token(user=user or "user", exp=exp, kind=kind)
    secure_cookie = bool(settings.web_cookie_secure)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=max_age,
        httponly=True,
        secure=secure_cookie,
        samesite="none" if secure_cookie else "lax",
        path="/",
    )


def _is_ui_authenticated(request: Request) -> bool:
    if _is_trusted_embed(request):
        return True

    username = _session_user_from_request(request)
    if not username:
        return False

    db_user = _session_db_user(request)
    if db_user is not None:
        return bool(db_user.get("ativo"))

    legacy_user = (settings.web_login_user or "admin").strip().lower()
    return username.lower() == legacy_user


def _has_login_session(request: Request) -> bool:
    if not _has_valid_session_cookie(request):
        return False
    return _session_kind_from_request(request) == "login"


def _check_admin_or_ui(request: Request, x_admin_token: str = Header(default="")) -> None:
    if settings.admin_token and x_admin_token == settings.admin_token:
        return
    if _is_ui_authenticated(request):
        return
    if settings.admin_token:
        raise HTTPException(status_code=401, detail="invalid admin token")


def _mapa_html_path() -> Path:
    return Path(__file__).resolve().parents[2] / "public" / "intranet_mapa_comando.html"


def _mapa_scope_norm(value: str) -> str:
    v = str(value or "").strip().upper()
    return v.replace("SUB_SETOR", "SUBSETOR")


def _mapa_assignment_score(row: dict) -> tuple:
    role = str(row.get("role_kind") or "").lower()
    situ = str(row.get("situacao") or "").lower()
    role_score = 3 if (role == "interino" and situ == "ativo") else 2 if (role == "titular" and situ == "ativo") else 1 if (role == "titular" and situ == "afastado") else 0
    start = str(row.get("data_inicio") or "")
    return (role_score, start, int(row.get("id") or 0))


def _mapa_pick_display_assignment(rows: list[dict]) -> dict | None:
    open_rows = [r for r in rows if not r.get("data_fim")]
    if not open_rows:
        return None
    ranked = sorted(open_rows, key=_mapa_assignment_score, reverse=True)
    return ranked[0] if ranked and _mapa_assignment_score(ranked[0])[0] > 0 else None


def _mapa_placeholder(scope_type: str, scope_code: str) -> dict:
    label = {
        "CIA": "Cmt de Cia não definido",
        "PELOTAO": "Cmt de Pelotão não definido",
        "SETOR": "Cmt de Setor não definido",
        "SUBSETOR": "Cmt de Subsetor não definido",
    }.get(_mapa_scope_norm(scope_type), "Comando não definido")
    return {
        "assignment": None,
        "commander": {
            "numero_policia": "-",
            "posto_graduacao": "A definir",
            "nome_guerra": label,
            "foto_url": "",
            "placeholder": True,
        },
        "scope_type": _mapa_scope_norm(scope_type),
        "scope_code": scope_code,
    }


def _mapa_resolved_card(scope_type: str, scope_code: str, assignments_by_scope: dict[tuple[str, str], list[dict]]) -> dict:
    key = (_mapa_scope_norm(scope_type), str(scope_code or "").strip())
    rows = assignments_by_scope.get(key, [])
    chosen = _mapa_pick_display_assignment(rows)
    if not chosen:
        return _mapa_placeholder(key[0], key[1])
    commander = {
        "numero_policia": chosen.get("commander_numero_policia"),
        "posto_graduacao": chosen.get("commander_posto_graduacao") or "",
        "nome_guerra": chosen.get("commander_nome_guerra") or "",
        "nome_completo": chosen.get("commander_nome_completo") or "",
        "telefone": chosen.get("commander_telefone") or "",
        "email": chosen.get("commander_email") or "",
        "foto_url": chosen.get("commander_foto_url") or "",
        "ativo": bool(chosen.get("commander_ativo")),
        "placeholder": False,
    }
    return {
        "assignment": chosen,
        "commander": commander,
        "scope_type": key[0],
        "scope_code": key[1],
    }


def _mapa_resolved_card_from_rows(rows: list[dict], scope_type: str, scope_code: str, empty_label: str) -> dict:
    open_rows = [r for r in rows if not r.get("data_fim")]
    if open_rows:
        def _rank_any_role(r: dict) -> tuple:
            situ = str(r.get("situacao") or "").strip().lower()
            situ_score = 3 if situ == "ativo" else 2 if situ == "afastado" else 1
            return (situ_score, str(r.get("data_inicio") or ""), int(r.get("id") or 0))
        chosen = sorted(open_rows, key=_rank_any_role, reverse=True)[0]
    else:
        chosen = None
    if not chosen:
        placeholder = _mapa_placeholder(scope_type, scope_code)
        placeholder["commander"]["nome_guerra"] = empty_label
        placeholder["commander"]["posto_graduacao"] = "A definir"
        return placeholder
    commander = {
        "numero_policia": chosen.get("commander_numero_policia"),
        "posto_graduacao": chosen.get("commander_posto_graduacao") or "",
        "nome_guerra": chosen.get("commander_nome_guerra") or "",
        "nome_completo": chosen.get("commander_nome_completo") or "",
        "telefone": chosen.get("commander_telefone") or "",
        "email": chosen.get("commander_email") or "",
        "foto_url": chosen.get("commander_foto_url") or "",
        "ativo": bool(chosen.get("commander_ativo")),
        "placeholder": False,
    }
    return {
        "assignment": chosen,
        "commander": commander,
        "scope_type": _mapa_scope_norm(scope_type),
        "scope_code": str(scope_code or "").strip(),
    }


def _mapa_viewer_info(request: Request, assignments_by_scope: dict[tuple[str, str], list[dict]]) -> dict:
    db_user = _session_db_user(request) or {}
    viewer_np = str(db_user.get("numero_policia") or "").strip()
    is_admin = _is_ui_admin(request)
    editable_cias: list[str] = []
    if viewer_np:
        for (scope_type, scope_code), rows in assignments_by_scope.items():
            if scope_type != "CIA":
                continue
            chosen = _mapa_pick_display_assignment(rows)
            if not chosen:
                continue
            if str(chosen.get("commander_numero_policia") or "").strip() == viewer_np and str(chosen.get("situacao") or "").lower() == "ativo":
                editable_cias.append(scope_code)
    return {
        "is_admin": is_admin,
        "numero_policia": viewer_np,
        "username": str((db_user or {}).get("username") or _session_user_from_request(request) or ""),
        "perfil": str((db_user or {}).get("perfil") or ""),
        "editable_cias": sorted(set(editable_cias)),
    }


def _ensure_mapa_write_permission(request: Request, cia_code: str, scope_type: str) -> None:
    if _is_ui_admin(request):
        return
    db_user = _session_db_user(request)
    if not db_user or not bool(db_user.get("ativo")):
        raise HTTPException(status_code=403, detail="usuario sem permissao")
    if _mapa_scope_norm(scope_type) == "CIA":
        raise HTTPException(status_code=403, detail="apenas admin pode alterar comando de Cia")
    numero_policia = str(db_user.get("numero_policia") or "").strip()
    if not numero_policia:
        raise HTTPException(status_code=403, detail="usuario sem numero de policia cadastrado")
    cia = str(cia_code or "").strip()
    if not cia:
        raise HTTPException(status_code=403, detail="cia de referencia obrigatoria")
    rows = list_mapa_assignments(scope_type="CIA", scope_code=cia, include_closed=False, limit=50)
    chosen = _mapa_pick_display_assignment(rows)
    if not chosen:
        raise HTTPException(status_code=403, detail="comando da Cia nao configurado")
    if str(chosen.get("commander_numero_policia") or "").strip() != numero_policia:
        raise HTTPException(status_code=403, detail="somente o comandante da Cia pode editar este registro")


def _build_mapa_dataset(request: Request) -> dict:
    features = list_mapa_features()
    assignments = list_mapa_assignments(include_closed=True, limit=20000)
    scope_metadata_rows = list_mapa_scope_metadata(limit=10000)
    scope_metadata_by_key = {
        (_mapa_scope_norm(r.get("scope_type") or ""), str(r.get("scope_code") or "").strip()): r
        for r in scope_metadata_rows
    }
    assignments_by_scope: dict[tuple[str, str], list[dict]] = {}
    for row in assignments:
        key = (_mapa_scope_norm(row.get("scope_type") or ""), str(row.get("scope_code") or "").strip())
        assignments_by_scope.setdefault(key, []).append(row)

    feature_out = []
    cias = set()
    pelotoes = set()
    setores = set()
    subsetores = set()
    municipios = set()
    agg_pop: dict[str, dict[str, int]] = {"SUBSETOR": {}, "SETOR": {}, "PELOTAO": {}, "CIA": {}, "BPM": {}}
    agg_efet: dict[str, dict[str, int]] = {"SUBSETOR": {}, "SETOR": {}, "PELOTAO": {}, "CIA": {}, "BPM": {}}

    def _int_or_none(v):
        if v in (None, ""):
            return None
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return None

    for feature in features:
        p = dict(feature.get("properties") or {})
        cia = str(p.get("cia_code") or "").strip()
        pel = str(p.get("pelotao_code") or "").strip()
        setor = str(p.get("setor_code") or "").strip()
        subsetor = str(p.get("subsetor_code") or "").strip()
        mun = str(p.get("municipio_nome") or "").strip()
        if cia:
            cias.add(cia)
        if pel:
            pelotoes.add(pel)
        if setor:
            setores.add(setor)
        if subsetor:
            subsetores.add(subsetor)
        if mun:
            municipios.add(mun)
        subsetor_meta = scope_metadata_by_key.get(("SUBSETOR", subsetor), {}) if subsetor else {}
        pop_v = _int_or_none(subsetor_meta.get("populacao_municipio"))
        efet_v = _int_or_none(subsetor_meta.get("efetivo_fracao"))
        if subsetor and pop_v is not None:
            agg_pop["SUBSETOR"][subsetor] = pop_v
        if subsetor and efet_v is not None:
            agg_efet["SUBSETOR"][subsetor] = efet_v
        if setor and pop_v is not None:
            agg_pop["SETOR"][setor] = int(agg_pop["SETOR"].get(setor, 0)) + pop_v
        if setor and efet_v is not None:
            agg_efet["SETOR"][setor] = int(agg_efet["SETOR"].get(setor, 0)) + efet_v
        if pel and pop_v is not None:
            agg_pop["PELOTAO"][pel] = int(agg_pop["PELOTAO"].get(pel, 0)) + pop_v
        if pel and efet_v is not None:
            agg_efet["PELOTAO"][pel] = int(agg_efet["PELOTAO"].get(pel, 0)) + efet_v
        if cia and pop_v is not None:
            agg_pop["CIA"][cia] = int(agg_pop["CIA"].get(cia, 0)) + pop_v
        if cia and efet_v is not None:
            agg_efet["CIA"][cia] = int(agg_efet["CIA"].get(cia, 0)) + efet_v
        if pop_v is not None:
            agg_pop["BPM"]["19 BPM"] = int(agg_pop["BPM"].get("19 BPM", 0)) + pop_v
        if efet_v is not None:
            agg_efet["BPM"]["19 BPM"] = int(agg_efet["BPM"].get("19 BPM", 0)) + efet_v

    for feature in features:
        p = dict(feature.get("properties") or {})
        cia = str(p.get("cia_code") or "").strip()
        pel = str(p.get("pelotao_code") or "").strip()
        setor = str(p.get("setor_code") or "").strip()
        subsetor = str(p.get("subsetor_code") or "").strip()
        subsetor_meta = scope_metadata_by_key.get(("SUBSETOR", subsetor), {}) if subsetor else {}
        cards = {
            "cia": _mapa_resolved_card("CIA", cia, assignments_by_scope),
            "pelotao": _mapa_resolved_card("PELOTAO", pel, assignments_by_scope),
            "setor": _mapa_resolved_card("SETOR", setor, assignments_by_scope),
            "subsetor": _mapa_resolved_card("SUBSETOR", subsetor, assignments_by_scope),
        }
        tooltip = cards["subsetor"]
        feature_out.append(
            {
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    **p,
                    "populacao_municipio": subsetor_meta.get("populacao_municipio"),
                    "efetivo_fracao": subsetor_meta.get("efetivo_fracao"),
                    "scope_metrics": {
                        "subsetor": {"populacao": agg_pop["SUBSETOR"].get(subsetor), "efetivo": agg_efet["SUBSETOR"].get(subsetor)},
                        "setor": {"populacao": agg_pop["SETOR"].get(setor), "efetivo": agg_efet["SETOR"].get(setor)},
                        "pelotao": {"populacao": agg_pop["PELOTAO"].get(pel), "efetivo": agg_efet["PELOTAO"].get(pel)},
                        "cia": {"populacao": agg_pop["CIA"].get(cia), "efetivo": agg_efet["CIA"].get(cia)},
                        "bpm": {"populacao": agg_pop["BPM"].get("19 BPM"), "efetivo": agg_efet["BPM"].get("19 BPM")},
                    },
                    "cards": cards,
                    "tooltip": tooltip,
                    "can_edit_scope_cia": False,  # preenchido pelo frontend com base no viewer
                },
            }
        )

    bpm_rows = assignments_by_scope.get(("BPM", "19 BPM"), [])
    bpm_cmd_rows = [r for r in bpm_rows if str(r.get("role_kind") or "").strip().lower() in {"titular", "cmt", "comandante"}]
    bpm_sub_rows = [r for r in bpm_rows if str(r.get("role_kind") or "").strip().lower() in {"subcmt", "subcmd", "subcomandante"}]
    overview_19bpm = {
        "scope_type": "BPM",
        "scope_code": "19 BPM",
        "comandante": _mapa_resolved_card_from_rows(bpm_cmd_rows, "BPM", "19 BPM", "Cmt 19 BPM não definido"),
        "subcomandante": _mapa_resolved_card_from_rows(bpm_sub_rows, "BPM", "19 BPM", "SubCmt 19 BPM não definido"),
        "metrics": {
            "populacao": agg_pop["BPM"].get("19 BPM"),
            "efetivo": agg_efet["BPM"].get("19 BPM"),
        },
    }

    viewer = _mapa_viewer_info(request, assignments_by_scope)
    return {
        "geojson": {"type": "FeatureCollection", "features": feature_out},
        "filters": {
            "cia": sorted(cias),
            "pelotao": sorted(pelotoes),
            "setor": sorted(setores),
            "subsetor": sorted(subsetores),
            "municipio": sorted(municipios),
        },
        "overview_19bpm": overview_19bpm,
        "viewer": viewer,
        "commanders": list_mapa_commanders(ativo_only=False, limit=2000),
    }


def _minio_client():
    if not (settings.minio_endpoint and settings.minio_access_key and settings.minio_secret_key and settings.minio_bucket_public):
        raise HTTPException(status_code=500, detail="configuracao MinIO incompleta")
    try:
        from minio import Minio  # type: ignore
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SDK MinIO indisponivel: {exc}") from exc
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=bool(settings.minio_secure),
    )


def _minio_public_object_url(object_name: str) -> str:
    if settings.minio_public_url:
        return f"{settings.minio_public_url}/{quote(object_name, safe='/')}"
    scheme = "https" if settings.minio_secure else "http"
    return f"{scheme}://{settings.minio_endpoint}/{settings.minio_bucket_public}/{quote(object_name, safe='/')}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    stop = False

    async def _loop() -> None:
        while not stop:
            mark_stale_agents(settings.heartbeat_stale_seconds)
            maybe_create_daily_job()
            await asyncio.sleep(settings.poll_seconds)

    task = asyncio.create_task(_loop())
    try:
        yield
    finally:
        stop = True
        task.cancel()


app = FastAPI(title="Metas 19 API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def ui_gate(request: Request, call_next):
    path = request.url.path
    method = request.method.upper()

    api_prefixes = ("/status", "/heartbeat", "/jobs", "/ingest", "/kpi", "/reds", "/health", "/auth")
    if path in {"/login", "/logout"} or any(path.startswith(p) for p in api_prefixes):
        return await call_next(request)

    is_html = method == "GET" and (path == "/" or path.endswith(".html"))
    if path == "/admin_users.html" and not _is_ui_admin(request):
        return RedirectResponse(url="/login?next=/admin_users.html", status_code=302)
    if is_html and not _is_trusted_embed(request) and not _has_login_session(request):
        nxt = path + (f"?{request.url.query}" if request.url.query else "")
        return RedirectResponse(url=f"/login?next={nxt}", status_code=302)

    response = await call_next(request)
    if is_html and _is_trusted_embed(request) and not _has_valid_session_cookie(request):
        _set_session_cookie(response, settings.web_login_user or "embed", kind="embed")
    return response


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/login", response_class=HTMLResponse)
def login_page(
    next: str | None = Query(default="/"),
    error: int | None = Query(default=None),
) -> str:
    next_path = next if (next or "").startswith("/") else "/"
    support_number = "".join(ch for ch in (settings.support_whatsapp_number or "5533999481310") if ch.isdigit())
    support_label = settings.support_contact_label or "P3/19 BPM / Sgt Novais"
    error_block = (
        '<div class="login-alert">Credenciais inválidas. Verifique usuário e senha e tente novamente.</div>'
        if error
        else ""
    )
    support_disabled_attr = 'disabled title="Configure APP_SUPPORT_WHATSAPP_NUMBER"' if not support_number else ""
    return f"""
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Login - Metas 19 BPM</title>
  <link rel="icon" type="image/png" href="/imagens/Icones_logo_19bpm/android-chrome-192x192.png">
  <link rel="stylesheet" href="/css/identidade_visual.css">
</head>
<body class="login-body">
  <div class="login-page">
    <header class="login-topbar">
      <div class="login-topbar-inner">
        <div class="login-brand">
          <img class="login-brand-logo" src="/imagens/Icones_logo_19bpm/android-chrome-192x192.png" alt="Logo 19º BPM">
          <div>
            <p class="login-brand-kicker">Acesso Institucional</p>
            <p class="login-brand-title">Metas 19º BPM</p>
          </div>
        </div>
        <a class="login-topbar-action" href="/">Voltar ao painel</a>
      </div>
    </header>

    <main class="login-main">
      <section class="login-panel">
        <p class="login-panel-kicker">Controle de acesso</p>
        <h1 class="login-panel-title">Entrada no painel operacional</h1>
        <p class="login-panel-subtitle">Use seu número PM e senha para autenticar no ambiente de metas.</p>
        <div class="login-helper">
          <b>Como funciona:</b> quando o painel é aberto pelo <b>Portal Institucional (iframe)</b>, o acesso é liberado automaticamente.
          Ao abrir o link <b>direto no navegador</b>, é necessário login.
        </div>
        <ul class="login-quick-list">
          <li>Conferir dados consolidados por indicador e unidade.</li>
          <li>Acessar análises e evolução mensal com filtros por período.</li>
          <li>Solicitar suporte direto pelo canal oficial do batalhão.</li>
        </ul>
      </section>

      <form class="login-panel login-auth-panel" method="post" action="/login" novalidate>
        <p class="login-panel-kicker">Acesso restrito</p>
        <h2 class="login-panel-title">Entrar no sistema</h2>
        <p class="login-panel-subtitle">Credenciais vinculadas ao usuário institucional.</p>
        {error_block}
        <input type="hidden" name="next" value="{next_path}">

        <div class="login-form">
          <label class="login-label" for="user">NR PM</label>
          <input class="login-input" id="user" name="user" placeholder="Ex.: 1438704" autocomplete="username" required>

          <label class="login-label" for="password">Senha</label>
          <div class="login-pwd-wrap">
            <input class="login-input" id="password" type="password" name="password" placeholder="Informe a senha" autocomplete="current-password" required>
            <button type="button" class="login-icon-btn" id="togglePwd" aria-label="Mostrar ou ocultar senha" title="Mostrar/ocultar senha">
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M2 12s3.5-6 10-6 10 6 10 6-3.5 6-10 6-10-6-10-6Z"></path>
                <circle cx="12" cy="12" r="3.2"></circle>
              </svg>
            </button>
          </div>
        </div>

        <button class="login-btn login-btn-primary" type="submit">Entrar</button>

        <div class="login-support">
          <h3 class="login-support-title">Suporte</h3>
          <p class="login-support-text">Se precisar de auxílio, preencha seus dados e clique para encaminhar mensagem para <b>{support_label}</b> no WhatsApp.</p>

          <div class="login-support-grid">
            <div>
              <label class="login-label" for="suporteNome">Nome</label>
              <input class="login-input" id="suporteNome" type="text" placeholder="Seu nome">
            </div>
            <div>
              <label class="login-label" for="suporteNumero">Nº Polícia</label>
              <input class="login-input" id="suporteNumero" type="text" placeholder="Ex.: 1234567">
            </div>
          </div>

          <div class="login-support-actions">
            <button type="button" class="login-btn login-btn-secondary" id="waBtn" {support_disabled_attr}>Solicitar acesso e abrir WhatsApp</button>
            <span id="waStatus" class="login-small"></span>
          </div>
          <p class="login-small">A solicitação é registrada no sistema e a mensagem é preparada para envio ao WhatsApp do suporte.</p>
        </div>
      </form>
    </main>
  </div>

  <script>
    (function() {{
      const pwd = document.getElementById('password');
      const toggle = document.getElementById('togglePwd');
      if (pwd && toggle) {{
        toggle.addEventListener('click', function() {{
          pwd.type = pwd.type === 'password' ? 'text' : 'password';
        }});
      }}

      const waBtn = document.getElementById('waBtn');
      const nomeEl = document.getElementById('suporteNome');
      const numEl = document.getElementById('suporteNumero');
      const waStatus = document.getElementById('waStatus');
      const waNumber = {json.dumps(support_number)};
      if (waBtn && waNumber) {{
        waBtn.addEventListener('click', async function() {{
          const nome = (nomeEl?.value || '').trim();
          const numeroPolicia = (numEl?.value || '').trim();
          if (!nome || !numeroPolicia) {{
            alert('Preencha Nome e N\u00ba Pol\u00edcia antes de abrir o WhatsApp.');
            return;
          }}
          waBtn.disabled = true;
          if (waStatus) waStatus.textContent = 'Enviando...';
          try {{
            const r = await fetch('/auth/access-request', {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify({{
                nome,
                numero_policia: numeroPolicia,
                unidade_setor: '',
                telefone: '',
                motivo: 'Solicitação de acesso pela tela de login'
              }})
            }});
            const data = await r.json().catch(() => ({{}}));
            if (!r.ok) throw new Error(data.detail || 'Falha ao registrar solicitação');
            if (waStatus) waStatus.textContent = 'Solicitação registrada.';
            if (data.whatsapp_url) {{
              window.open(data.whatsapp_url, '_blank', 'noopener');
            }} else {{
              const msg =
                'Estou precisando de Auxilio no site de metas 19 bpm.\\n' +
                'Nome: ' + nome + '\\n' +
                'Nº Polícia: ' + numeroPolicia;
              const url = 'https://wa.me/' + waNumber + '?text=' + encodeURIComponent(msg);
              window.open(url, '_blank', 'noopener');
            }}
          }} catch (e) {{
            if (waStatus) waStatus.textContent = '';
            alert(e.message || 'Falha ao registrar solicitação');
          }} finally {{
            waBtn.disabled = false;
          }}
        }});
      }}

    }})();
  </script>
</body>
</html>
"""


@app.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    user = str(form.get("user") or "").strip()
    password = str(form.get("password") or "")
    next_path = str(form.get("next") or "/")
    if not next_path.startswith("/"):
        next_path = "/"

    login_user = user.strip()

    # Login prioritario por NR PM; fallback por username para compatibilidade.
    db_user = get_web_user_by_numero_policia(login_user) or get_web_user_by_username(login_user.lower())
    if db_user:
        is_active = bool(db_user.get("ativo"))
        pwd_hash = str(db_user.get("password_hash") or "")
        if is_active and _password_verify(password, pwd_hash):
            touch_web_user_login(int(db_user["id"]))
            target = next_path
            if bool(db_user.get("must_change_password")):
                target = f"/minha_conta.html?force_password=1&next={next_path}"
            response = RedirectResponse(url=target, status_code=302)
            _set_session_cookie(response, str(db_user.get("numero_policia") or db_user.get("username") or login_user))
            return response
        return RedirectResponse(url=f"/login?next={next_path}&error=1", status_code=302)

    # Transitional fallback: env-based single user login.
    expected_user = (settings.web_login_user or "admin").strip()
    expected_password = settings.web_login_password or settings.admin_token
    if not expected_password:
        raise HTTPException(status_code=500, detail="login password not configured")
    if user != expected_user or password != expected_password:
        return RedirectResponse(url=f"/login?next={next_path}&error=1", status_code=302)

    response = RedirectResponse(url=next_path, status_code=302)
    _set_session_cookie(response, expected_user)
    return response


@app.get("/logout")
def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
    return response


@app.get("/auth/me")
def auth_me(request: Request) -> dict:
    username = _session_user_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail="not authenticated")
    db_user = _session_db_user(request)
    if db_user:
        return {
            "authenticated": True,
            "source": "web_users",
            "user": {
                "id": db_user.get("id"),
                "nome": db_user.get("nome"),
                "numero_policia": db_user.get("numero_policia"),
                "posto_graduacao": db_user.get("posto_graduacao"),
                "unidade_setor": db_user.get("unidade_setor"),
                "username": db_user.get("username"),
                "perfil": db_user.get("perfil"),
                "ativo": bool(db_user.get("ativo")),
                "last_login_at": db_user.get("last_login_at"),
                "must_change_password": bool(db_user.get("must_change_password")),
            },
        }
    return {
        "authenticated": True,
        "source": "legacy_env",
        "user": {"username": username, "perfil": "admin", "ativo": True},
    }


@app.get("/auth/users")
def auth_users_list(_: None = Depends(_check_admin_token_or_ui_admin)) -> dict:
    return {"items": list_web_users()}


@app.post("/auth/users")
def auth_users_create(payload: WebUserCreateIn, _: None = Depends(_check_admin_token_or_ui_admin)) -> dict:
    perfil = (payload.perfil or "consulta").strip().lower()
    if perfil not in {"consulta", "gestor", "admin"}:
        raise HTTPException(status_code=400, detail="perfil invalido")
    try:
        user = create_web_user(
            nome=payload.nome,
            numero_policia=payload.numero_policia,
            posto_graduacao=payload.posto_graduacao,
            unidade_setor=payload.unidade_setor or "",
            username=payload.numero_policia,
            password_hash=_password_hash(payload.password),
            perfil=perfil,
            ativo=payload.ativo,
            must_change_password=payload.senha_provisoria,
        )
    except IntegrityError:
        raise HTTPException(status_code=409, detail="usuario ou numero_policia ja cadastrado") from None
    return {"ok": True, "user": user}


@app.put("/auth/users/{user_id}")
def auth_users_update(
    user_id: int,
    payload: WebUserUpdateIn,
    _: None = Depends(_check_admin_token_or_ui_admin),
) -> dict:
    perfil = (payload.perfil or "consulta").strip().lower()
    if perfil not in {"consulta", "gestor", "admin"}:
        raise HTTPException(status_code=400, detail="perfil invalido")
    try:
        user = update_web_user(
            user_id=user_id,
            nome=payload.nome,
            numero_policia=payload.numero_policia,
            posto_graduacao=payload.posto_graduacao,
            unidade_setor=payload.unidade_setor or "",
            username=payload.numero_policia,
            perfil=perfil,
            ativo=payload.ativo,
        )
    except IntegrityError:
        raise HTTPException(status_code=409, detail="usuario ou numero_policia ja cadastrado") from None
    if not user:
        raise HTTPException(status_code=404, detail="usuario nao encontrado")
    return {"ok": True, "user": user}


@app.post("/auth/users/{user_id}/password")
def auth_users_set_password(
    user_id: int,
    payload: WebUserPasswordIn,
    _: None = Depends(_check_admin_token_or_ui_admin),
) -> dict:
    user = set_web_user_password(user_id, _password_hash(payload.password), must_change_password=payload.senha_provisoria)
    if not user:
        raise HTTPException(status_code=404, detail="usuario nao encontrado")
    return {"ok": True}


@app.post("/auth/users/{user_id}/active")
def auth_users_set_active(
    user_id: int,
    payload: WebUserActiveIn,
    _: None = Depends(_check_admin_token_or_ui_admin),
) -> dict:
    user = set_web_user_active(user_id, payload.ativo)
    if not user:
        raise HTTPException(status_code=404, detail="usuario nao encontrado")
    return {"ok": True, "user": user}


@app.post("/auth/me/password")
def auth_me_change_password(request: Request, payload: WebUserSelfPasswordIn) -> dict:
    db_user = _session_db_user(request)
    if not db_user or not bool(db_user.get("ativo")):
        raise HTTPException(status_code=401, detail="not authenticated")
    if not _password_verify(payload.current_password, str(db_user.get("password_hash") or "")):
        raise HTTPException(status_code=400, detail="senha atual invalida")
    set_web_user_password(int(db_user["id"]), _password_hash(payload.new_password), must_change_password=False)
    return {"ok": True}


@app.post("/auth/access-request")
def auth_access_request_create(payload: AccessRequestIn) -> dict:
    req = create_access_request(
        nome=payload.nome,
        numero_policia=payload.numero_policia,
        unidade_setor=payload.unidade_setor or "",
        telefone=payload.telefone or "",
        motivo=payload.motivo or "",
    )
    support_number = ''.join(ch for ch in (settings.support_whatsapp_number or '5533999481310') if ch.isdigit())
    if support_number and len(support_number) == 11:
        support_number = '55' + support_number
    msg = (
        "Estou precisando de Auxilio no site de metas 19 bpm.\n"
        f"Nome: {payload.nome}\n"
        f"N? Pol?cia: {payload.numero_policia}\n"
        f"Unidade/Setor: {payload.unidade_setor or '-'}\n"
        f"Telefone: {payload.telefone or '-'}\n"
        f"Motivo: {payload.motivo or 'Solicita??o de acesso'}"
    )
    wa_url = f"https://wa.me/{support_number}?text={quote_plus(msg)}" if support_number else None
    return {"ok": True, "request": req, "whatsapp_url": wa_url}


@app.get("/auth/access-requests")
def auth_access_requests_list(
    limit: int = Query(default=200, ge=1, le=1000),
    _: None = Depends(_check_admin_token_or_ui_admin),
) -> dict:
    return {"items": list_access_requests(limit=limit)}


@app.post("/auth/access-requests/{request_id}/status")
def auth_access_requests_set_status(
    request_id: int,
    status: str = Query(...),
    _: None = Depends(_check_admin_token_or_ui_admin),
) -> dict:
    if status not in {"pendente", "atendido", "negado"}:
        raise HTTPException(status_code=400, detail="status invalido")
    row = set_access_request_status(request_id, status)
    if not row:
        raise HTTPException(status_code=404, detail="solicitacao nao encontrada")
    return {"ok": True, "request": row}


@app.get("/status")
def status(_: None = Depends(_check_admin_or_ui)) -> dict:
    return get_overview()


@app.post("/heartbeat")
def heartbeat(payload: HeartbeatIn, _: None = Depends(_check_agent_token)) -> dict:
    ensure_agent(payload.agent_id, payload.version)
    return {"ok": True}


@app.get("/jobs/next")
def jobs_next(_: None = Depends(_check_agent_token)) -> dict:
    job = get_next_job()
    if not job:
        return {"job": None}

    job_dict = dict(job)
    if job_dict.get("payload_json"):
        job_dict["payload"] = json.loads(job_dict["payload_json"])
    return {"job": job_dict}


@app.post("/jobs/{job_id}/start")
def job_start(job_id: int, payload: HeartbeatIn, _: None = Depends(_check_agent_token)) -> dict:
    set_job_started(job_id, payload.agent_id)
    return {"ok": True}


@app.post("/jobs/{job_id}/result")
def job_result(job_id: int, payload: JobResultIn, _: None = Depends(_check_agent_token)) -> dict:
    set_job_result(job_id=job_id, success=payload.success, result=payload.result, error=payload.error)
    return {"ok": True}


@app.post("/jobs/manual")
def manual_job(payload: ManualJobIn, _: None = Depends(_check_admin_or_ui)) -> dict:
    job_id = create_job(
        job_type="manual_update",
        requested_by="ui",
        payload={"run_mode": payload.run_mode, "scripts": payload.scripts, "source": "bisp"},
    )
    return {"ok": True, "job_id": job_id}


@app.post("/ingest/kpi")
def ingest_kpi(payload: KpiIngestIn, _: None = Depends(_check_agent_token)) -> dict:
    stats = upsert_kpi_rows([row.model_dump() for row in payload.rows])
    return {"ok": True, **stats}


@app.post("/ingest/reds")
def ingest_reds(payload: RedsIngestIn, _: None = Depends(_check_agent_token)) -> dict:
    stats = upsert_reds_rows([row.model_dump() for row in payload.rows])
    return {"ok": True, **stats}


@app.post("/ingest/fact")
def ingest_fact(payload: FactIngestIn, _: None = Depends(_check_agent_token)) -> dict:
    stats = upsert_fact_rows(payload.indicador, payload.rows)
    return {"ok": True, **stats}


@app.post("/ingest/purge")
def ingest_purge(payload: PurgeIn, _: None = Depends(_check_agent_token)) -> dict:
    kpi_deleted = purge_kpi_rows(payload.indicador, payload.date_from, payload.date_to)
    reds_deleted = purge_reds_rows(payload.indicador, payload.date_from, payload.date_to)
    fact_deleted = purge_fact_rows(payload.indicador, payload.date_from, payload.date_to)
    return {"ok": True, "kpi_deleted": kpi_deleted, "reds_deleted": reds_deleted, "fact_deleted": fact_deleted}


@app.get("/kpi/summary")
def kpi_summary(
    indicador: str = Query(...),
    nivel: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    unidade_id: str | None = Query(default=None),
) -> dict:
    return get_kpi_summary(
        indicador=indicador,
        nivel=nivel,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        unidade_id=unidade_id,
    )


@app.get("/kpi/monthly")
def kpi_monthly(
    indicador: str = Query(...),
    nivel: str = Query(...),
    ano: int = Query(..., ge=2000, le=2100),
    unidade_id: str | None = Query(default=None),
) -> dict:
    return get_kpi_monthly(indicador=indicador, nivel=nivel, ano=ano, unidade_id=unidade_id)


@app.get("/kpi/stats")
def kpi_stats(_: None = Depends(_check_admin_or_ui)) -> dict:
    return get_kpi_stats()


@app.get("/kpi/units")
def kpi_units(
    indicador: str = Query(...),
    nivel: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict:
    return get_kpi_units(
        indicador=indicador,
        nivel=nivel,
        date_from=date_from,
        date_to=date_to,
        search=search,
        limit=limit,
    )


@app.get("/reds/export")
def reds_export(
    request: Request,
    indicador: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    nivel: str | None = Query(default=None),
    unidade_id: str | None = Query(default=None),
    admin_token: str | None = Query(default=None),
    x_admin_token: str = Header(default=""),
    limit: int = Query(default=20000, ge=1, le=200000),
) -> StreamingResponse:
    has_admin_token = settings.admin_token and (admin_token == settings.admin_token or x_admin_token == settings.admin_token)
    if settings.admin_token and not has_admin_token and not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="invalid admin token")

    rows = query_reds(
        indicador=indicador,
        date_from=date_from,
        date_to=date_to,
        nivel=nivel,
        unidade_id=unidade_id,
        limit=limit,
    )

    def _iter_csv():
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output, delimiter=";", lineterminator="\n")
        writer.writerow(
            [
                "numero_ocorrencia",
                "data_hora_fato",
                "natureza_codigo",
                "natureza_descricao",
                "municipio_codigo",
                "municipio_nome",
                "CIA_PM",
                "PELOTAO",
                "SETOR",
                "SUBSETOR",
                "latitude",
                "longitude",
            ]
        )
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for r in rows:
            writer.writerow([r.get(k, "") for k in [
                "numero_ocorrencia",
                "data_hora_fato",
                "natureza_codigo",
                "natureza_descricao",
                "municipio_codigo",
                "municipio_nome",
                "CIA_PM",
                "PELOTAO",
                "SETOR",
                "SUBSETOR",
                "latitude",
                "longitude",
            ]])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    filename = f"REDS_{indicador}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(_iter_csv(), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/reds/points")
def reds_points(
    request: Request,
    indicador: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    nivel: str | None = Query(default=None),
    unidade_id: str | None = Query(default=None),
    admin_token: str | None = Query(default=None),
    x_admin_token: str = Header(default=""),
    limit: int = Query(default=5000, ge=1, le=20000),
) -> dict:
    has_admin_token = settings.admin_token and (admin_token == settings.admin_token or x_admin_token == settings.admin_token)
    if settings.admin_token and not has_admin_token and not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="invalid admin token")

    rows = query_reds(
        indicador=indicador,
        date_from=date_from,
        date_to=date_to,
        nivel=nivel,
        unidade_id=unidade_id,
        limit=limit,
    )
    return {"rows": rows}


@app.get("/reds/cia-presos")
def reds_cia_presos(
    request: Request,
    indicador: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    admin_token: str | None = Query(default=None),
    x_admin_token: str = Header(default=""),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict:
    has_admin_token = settings.admin_token and (admin_token == settings.admin_token or x_admin_token == settings.admin_token)
    if settings.admin_token and not has_admin_token and not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="invalid admin token")

    rows = query_reds_cia_presos_summary(
        indicador=indicador,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )
    return {"rows": rows}


@app.get("/intranet_mapa_comando.html", response_class=HTMLResponse)
def intranet_mapa_comando_page(request: Request) -> str:
    if _is_mapa_intranet_embed(request):
        html_path = _mapa_html_path()
        if not html_path.exists():
            raise HTTPException(status_code=404, detail="pagina nao encontrada")
        return html_path.read_text(encoding="utf-8")
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=403, detail="acesso permitido apenas pela intranet")
    html_path = _mapa_html_path()
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="pagina nao encontrada")
    return html_path.read_text(encoding="utf-8")


@app.get("/intranet/mapa-comando/data")
def intranet_mapa_comando_data(request: Request) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    return _build_mapa_dataset(request)


@app.get("/intranet/mapa-comando/history")
def intranet_mapa_comando_history(
    request: Request,
    scope_type: str = Query(...),
    scope_code: str = Query(...),
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    rows = list_mapa_assignments(
        scope_type=_mapa_scope_norm(scope_type),
        scope_code=scope_code,
        include_closed=True,
        limit=limit,
    )
    return {"items": rows}


@app.post("/intranet/mapa-comando/assignment")
def intranet_mapa_comando_assignment_upsert(request: Request, payload: MapaAssignmentUpsertIn) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    data = payload.model_dump()
    scope_type = _mapa_scope_norm(data.get("scope_type"))
    cia_code = str(data.get("cia_code") or "").strip()
    if scope_type == "CIA" and not cia_code:
        cia_code = str(data.get("scope_code") or "").strip()
    _ensure_mapa_write_permission(request, cia_code=cia_code, scope_type=scope_type)

    cmd = data.get("commander") or {}
    commander = upsert_mapa_commander(
        numero_policia=str(cmd.get("numero_policia") or ""),
        posto_graduacao=str(cmd.get("posto_graduacao") or ""),
        nome_guerra=str(cmd.get("nome_guerra") or ""),
        nome_completo=str(cmd.get("nome_completo") or ""),
        telefone=str(cmd.get("telefone") or ""),
        email=str(cmd.get("email") or ""),
        foto_url=str(cmd.get("foto_url") or ""),
        observacoes=str(cmd.get("observacoes") or ""),
        ativo=bool(cmd.get("ativo", True)),
    )
    actor = str((_session_db_user(request) or {}).get("numero_policia") or _session_user_from_request(request) or "")
    assignment = create_mapa_assignment(
        commander_id=int(commander["id"]),
        scope_type=scope_type,
        scope_code=str(data.get("scope_code") or ""),
        scope_label=str(data.get("scope_label") or "") or str(data.get("scope_code") or ""),
        cia_code=cia_code,
        pelotao_code=str(data.get("pelotao_code") or ""),
        setor_code=str(data.get("setor_code") or ""),
        subsetor_code=str(data.get("subsetor_code") or ""),
        municipio_nome=str(data.get("municipio_nome") or ""),
        role_kind=str(data.get("role_kind") or "titular"),
        situacao=str(data.get("situacao") or "ativo"),
        motivo=str(data.get("motivo") or ""),
        data_inicio=data.get("data_inicio"),
        cadastrado_por=actor,
        atualizado_por=actor,
        replace_existing_same_role=bool(data.get("replace_existing_same_role", True)),
    )
    return {"ok": True, "commander": commander, "assignment": assignment}


@app.post("/intranet/mapa-comando/commander/upsert")
def intranet_mapa_comando_commander_upsert(request: Request, payload: MapaCommanderEditIn) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    scope_type = _mapa_scope_norm(payload.scope_type)
    cia_code = str(payload.cia_code or "").strip()
    if scope_type == "BPM":
        if not _is_ui_admin(request):
            raise HTTPException(status_code=403, detail="apenas admin pode alterar comando do BPM")
    else:
        _ensure_mapa_write_permission(request, cia_code=cia_code, scope_type=scope_type)

    cmd = payload.commander.model_dump()
    commander = upsert_mapa_commander(
        numero_policia=str(cmd.get("numero_policia") or ""),
        posto_graduacao=str(cmd.get("posto_graduacao") or ""),
        nome_guerra=str(cmd.get("nome_guerra") or ""),
        nome_completo=str(cmd.get("nome_completo") or ""),
        telefone=str(cmd.get("telefone") or ""),
        email=str(cmd.get("email") or ""),
        foto_url=str(cmd.get("foto_url") or ""),
        observacoes=str(cmd.get("observacoes") or ""),
        ativo=bool(cmd.get("ativo", True)),
    )
    return {"ok": True, "commander": commander}


@app.patch("/intranet/mapa-comando/assignments/{assignment_id}/status")
def intranet_mapa_comando_assignment_status(
    assignment_id: int,
    request: Request,
    payload: MapaAssignmentStatusIn,
) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    existing = get_mapa_assignment_by_id(assignment_id)
    if not existing:
        raise HTTPException(status_code=404, detail="registro nao encontrado")
    _ensure_mapa_write_permission(
        request,
        cia_code=str(existing.get("cia_code") or ""),
        scope_type=str(existing.get("scope_type") or ""),
    )
    actor = str((_session_db_user(request) or {}).get("numero_policia") or _session_user_from_request(request) or "")
    row = update_mapa_assignment_status(
        assignment_id=assignment_id,
        situacao=payload.situacao,
        motivo=payload.motivo or "",
        data_fim=payload.data_fim,
        encerrar_periodo=bool(payload.encerrar_periodo),
        atualizado_por=actor,
    )
    if not row:
        raise HTTPException(status_code=404, detail="registro nao encontrado")
    return {"ok": True, "assignment": row}


@app.delete("/intranet/mapa-comando/assignments/{assignment_id}")
def intranet_mapa_comando_assignment_delete(assignment_id: int, request: Request) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    existing = get_mapa_assignment_by_id(assignment_id)
    if not existing:
        raise HTTPException(status_code=404, detail="registro nao encontrado")
    _ensure_mapa_write_permission(
        request,
        cia_code=str(existing.get("cia_code") or ""),
        scope_type=str(existing.get("scope_type") or ""),
    )
    ok = delete_mapa_assignment(int(assignment_id))
    if not ok:
        raise HTTPException(status_code=404, detail="registro nao encontrado")
    return {"ok": True}


@app.post("/intranet/mapa-comando/scope-metadata")
def intranet_mapa_comando_scope_metadata_upsert(request: Request, payload: MapaScopeMetadataUpsertIn) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    scope_type = _mapa_scope_norm(payload.scope_type)
    scope_code = str(payload.scope_code or "").strip()
    if not scope_code:
        raise HTTPException(status_code=400, detail="scope_code obrigatorio")
    cia_code = str(payload.cia_code or "").strip()
    if scope_type == "BPM":
        if not _is_ui_admin(request):
            raise HTTPException(status_code=403, detail="apenas admin pode alterar metadata do BPM")
    else:
        _ensure_mapa_write_permission(request, cia_code=cia_code, scope_type=scope_type)
    actor = str((_session_db_user(request) or {}).get("numero_policia") or _session_user_from_request(request) or "")
    row = upsert_mapa_scope_metadata(
        scope_type=scope_type,
        scope_code=scope_code,
        cia_code=cia_code,
        municipio_nome=str(payload.municipio_nome or ""),
        populacao_municipio=payload.populacao_municipio,
        efetivo_fracao=payload.efetivo_fracao,
        atualizado_por=actor,
    )
    return {"ok": True, "metadata": row}


@app.post("/intranet/mapa-comando/upload-photo")
async def intranet_mapa_comando_upload_photo(
    request: Request,
    file: UploadFile = File(...),
    numero_policia: str = Query(default=""),
) -> dict:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    content_type = str(file.content_type or "").strip().lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="arquivo deve ser imagem")
    blob = await file.read()
    if not blob:
        raise HTTPException(status_code=400, detail="arquivo vazio")
    if len(blob) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="imagem maior que 8MB")
    ext = mimetypes.guess_extension(content_type) or Path(str(file.filename or "foto.jpg")).suffix or ".jpg"
    ext = ext if ext.startswith(".") else f".{ext}"
    np_safe = "".join(ch for ch in str(numero_policia or "").strip() if ch.isalnum()) or "sem-numero"
    stamp = str(int(time.time()))
    rand = secrets.token_hex(4)
    object_name = f"mapa-comando/{np_safe}/{stamp}_{rand}{ext.lower()}"
    try:
        client = _minio_client()
        client.put_object(
            settings.minio_bucket_public,
            object_name,
            io.BytesIO(blob),
            len(blob),
            content_type=content_type,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"falha no MinIO upload: {exc}") from exc
    public_url = _minio_public_object_url(object_name)

    persisted_commander = False
    commander_row = None
    numero_raw = str(numero_policia or "").strip()
    if numero_raw:
        existing = get_mapa_commander_by_numero_policia(numero_raw)
        if existing:
            commander_row = upsert_mapa_commander(
                numero_policia=str(existing.get("numero_policia") or numero_raw),
                posto_graduacao=str(existing.get("posto_graduacao") or ""),
                nome_guerra=str(existing.get("nome_guerra") or ""),
                nome_completo=str(existing.get("nome_completo") or ""),
                telefone=str(existing.get("telefone") or ""),
                email=str(existing.get("email") or ""),
                foto_url=public_url,
                observacoes=str(existing.get("observacoes") or ""),
                ativo=bool(existing.get("ativo", True)),
            )
            persisted_commander = True

    return {
        "ok": True,
        "url": public_url,
        "object_name": object_name,
        "persisted_commander": persisted_commander,
        "commander": commander_row,
    }


@app.get("/intranet/mapa-comando/export.xlsx")
def intranet_mapa_comando_export_xlsx(request: Request) -> StreamingResponse:
    if not _is_ui_authenticated(request):
        raise HTTPException(status_code=401, detail="not authenticated")
    dataset = _build_mapa_dataset(request)
    q = request.query_params
    f_cia = str(q.get("cia") or "").strip()
    f_pel = str(q.get("pelotao") or "").strip()
    f_setor = str(q.get("setor") or "").strip()
    f_subsetor = str(q.get("subsetor") or "").strip()
    f_municipio = str(q.get("municipio") or "").strip()
    all_columns = [
        ("municipio", "Municipio"),
        ("bairro", "Bairro"),
        ("codigo_municipio", "Codigo Municipio"),
        ("cia_pm", "CIA PM"),
        ("pelotao", "Pelotao"),
        ("setor", "Setor"),
        ("subsetor", "SubSetor"),
        ("area_aprox", "Area (aprox.)"),
        ("populacao", "Populacao"),
        ("efetivo_fracao", "Efetivo Fracao"),
        ("cmt_cia", "Cmt Cia"),
        ("nr_pm_cia", "Nr PM Cia"),
        ("cmt_pelotao", "Cmt Pelotao"),
        ("nr_pm_pelotao", "Nr PM Pelotao"),
        ("cmt_setor", "Cmt Setor"),
        ("nr_pm_setor", "Nr PM Setor"),
        ("cmt_subsetor", "Cmt SubSetor"),
        ("nr_pm_subsetor", "Nr PM SubSetor"),
    ]
    allowed_cols = {k: (k, label) for k, label in all_columns}
    cols_param = [str(x).strip() for x in str(q.get("columns") or "").split(",") if str(x).strip()]
    columns = [allowed_cols[k] for k in cols_param if k in allowed_cols] or all_columns
    selected_col_keys = {k for k, _ in columns}

    def _match_export_filters(props: dict) -> bool:
        return (
            (not f_cia or str(props.get("cia_code") or "") == f_cia)
            and (not f_pel or str(props.get("pelotao_code") or "") == f_pel)
            and (not f_setor or str(props.get("setor_code") or "") == f_setor)
            and (not f_subsetor or str(props.get("subsetor_code") or "") == f_subsetor)
            and (not f_municipio or str(props.get("municipio_nome") or "") == f_municipio)
        )

    def _match_row_filters(row: dict) -> bool:
        return (
            (not f_cia or str(row.get("cia_pm") or "") == f_cia)
            and (not f_pel or str(row.get("pelotao") or "") == f_pel)
            and (not f_setor or str(row.get("setor") or "") == f_setor)
            and (not f_subsetor or str(row.get("subsetor") or "") == f_subsetor)
            and (not f_municipio or str(row.get("municipio") or "") == f_municipio)
        )

    rows = []
    for feature in list(dataset.get("geojson", {}).get("features", [])):
        p = feature.get("properties") or {}
        if not _match_export_filters(p):
            continue
        cards = p.get("cards") or {}
        subsetor_card = (cards.get("subsetor") or {})
        setor_card = (cards.get("setor") or {})
        pel_card = (cards.get("pelotao") or {})
        cia_card = (cards.get("cia") or {})
        bairro_val = p.get("bairro") or p.get("bairro_nome") or p.get("bairros") or ""
        if isinstance(bairro_val, (list, tuple)):
            bairro_val = " | ".join(str(x) for x in bairro_val if x not in (None, ""))
        rows.append(
            {
                "municipio": p.get("municipio_nome") or "",
                "bairro": bairro_val,
                "cia_pm": p.get("cia_code") or "",
                "pelotao": p.get("pelotao_code") or "",
                "setor": p.get("setor_code") or "",
                "subsetor": p.get("subsetor_code") or "",
                "codigo_municipio": p.get("cd_municipio") or "",
                "area_aprox": p.get("area") or "",
                "populacao": p.get("populacao_municipio") or "",
                "efetivo_fracao": p.get("efetivo_fracao") or "",
                "cmt_cia": ((cia_card.get("commander") or {}).get("nome_guerra") or ""),
                "nr_pm_cia": ((cia_card.get("commander") or {}).get("numero_policia") or ""),
                "cmt_pelotao": ((pel_card.get("commander") or {}).get("nome_guerra") or ""),
                "nr_pm_pelotao": ((pel_card.get("commander") or {}).get("numero_policia") or ""),
                "cmt_setor": ((setor_card.get("commander") or {}).get("nome_guerra") or ""),
                "nr_pm_setor": ((setor_card.get("commander") or {}).get("numero_policia") or ""),
                "cmt_subsetor": ((subsetor_card.get("commander") or {}).get("nome_guerra") or ""),
                "nr_pm_subsetor": ((subsetor_card.get("commander") or {}).get("numero_policia") or ""),
            }
        )
    if not rows:
        rows = [r for r in list_mapa_export_rows() if _match_row_filters(r)]

    if "bairro" in selected_col_keys:
        bairros_idx: dict[str, list[dict]] = {}
        for b_row in list_mapa_bairro_rows():
            if not _match_row_filters(b_row):
                continue
            subsetor_key = str(b_row.get("subsetor") or "").strip()
            if not subsetor_key:
                continue
            bairros_idx.setdefault(subsetor_key, []).append(b_row)
        expanded_rows: list[dict] = []
        for row in rows:
            subsetor_key = str(row.get("subsetor") or "").strip()
            bairro_matches = bairros_idx.get(subsetor_key) or []
            if not bairro_matches:
                expanded_rows.append(dict(row))
                continue
            for b_row in bairro_matches:
                out = dict(row)
                for key in ("municipio", "bairro", "cia_pm", "pelotao", "setor", "subsetor", "codigo_municipio"):
                    val = b_row.get(key)
                    if val not in (None, ""):
                        out[key] = val
                expanded_rows.append(out)
        rows = expanded_rows

    normalized_rows = []
    for row in rows:
        out = {}
        for key, value in (row or {}).items():
            if isinstance(value, str):
                out[key] = normalize_mapa_text(value)
            else:
                out[key] = value
        normalized_rows.append(out)
    rows = normalized_rows
    rows.sort(
        key=lambda r: (
            str(r.get("cia_pm") or ""),
            str(r.get("setor") or ""),
            str(r.get("subsetor") or ""),
            str(r.get("bairro") or ""),
        )
    )
    data = build_xlsx_bytes(rows, columns, sheet_name="Mapa 19BPM")
    headers = {"Content-Disposition": 'attachment; filename="mapa_comando_19bpm.xlsx"'}
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# Serve frontend from /public (fallback for unmatched routes).
PUBLIC_DIR = Path(__file__).resolve().parents[2] / "public"
if PUBLIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(PUBLIC_DIR), html=True), name="public")
