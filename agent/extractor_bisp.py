import os
import re
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
try:
    from shapely.geometry import Point
except Exception:  # pragma: no cover - optional at runtime
    Point = None

try:
    import geopandas as gpd
except Exception:  # pragma: no cover - optional at runtime
    gpd = None

try:
    from mmpg_netroute import ensure_host_route
except Exception:  # pragma: no cover - optional at runtime
    ensure_host_route = None




def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[extractor] {ts} {msg}", flush=True)

LEGACY_GROUP_ALIAS = {
    "indicadores_resultado_gdo": "GDO",
    "indicador_plano_acao": "PL",
    "indicador_interacao_comunitaria": "GDI",
    "indicador_proximidade": "PPAG",
}

FACT_TABLE_BY_INDICADOR = {
    "BD_IMV": "fact_imv",
    "BD_ICVPE": "fact_icvpe",
    "BD_ICVPA": "fact_icvpa",
}

FACT_COLUMNS = {
    "fact_imv": [
        "numero_ocorrencia", "envolvimento_codigo", "envolvimento_descricao", "numero_envolvido", "chave_envolvido",
        "nome_completo_envolvido", "nome_mae", "data_nascimento", "letalidade", "condicao_fisica_descricao",
        "natureza_ocorrencia_codigo", "natureza_ocorrencia_descricao", "ind_consumado", "rpm_2024", "ueop_2024",
        "unidade_area_militar_codigo", "unidade_area_militar_nome", "unidade_responsavel_registro_codigo",
        "unidade_responsavel_registro_nome", "latitude_sirgas2000", "longitude_sirgas2000", "situacao_zona",
        "tipo_descricao", "codigo_municipio", "nome_municipio", "tipo_logradouro_descricao", "logradouro_nome",
        "numero_endereco", "nome_bairro", "ocorrencia_uf", "numero_latitude", "numero_longitude", "data_hora_fato",
        "ano", "mes", "nome_tipo_relatorio", "digitador_sigla_orgao", "udi", "ueop", "cia", "codigo_espacial_pm",
        "cia_pel_final", "geo_name", "geo_ueop", "geo_cia_pm", "geo_pelotao", "geo_cd_municipio",
        "geo_nm_municipio", "geo_setor", "geo_municipio", "geo_area", "geo_subsetor", "created_at", "updated_at",
    ],
    "fact_icvpe": [
        "numero_ocorrencia", "envolvimento_codigo", "envolvimento_descricao", "numero_envolvido",
        "nome_completo_envolvido", "nome_mae", "data_nascimento", "ind_militar_policial_servico",
        "condicao_fisica_descricao", "natureza_ocorrencia_codigo", "natureza_ocorrencia_descricao", "ind_consumado",
        "rpm_2024", "ueop_2024", "unidade_area_militar_codigo", "unidade_area_militar_nome",
        "unidade_responsavel_registro_codigo", "unidade_responsavel_registro_nome", "latitude_sirgas2000",
        "longitude_sirgas2000", "numero_latitude", "numero_longitude", "situacao_zona", "tipo_descricao",
        "codigo_municipio", "nome_municipio", "tipo_logradouro_descricao", "logradouro_nome", "numero_endereco",
        "nome_bairro", "ocorrencia_uf", "data_hora_fato", "ano", "mes", "nome_tipo_relatorio",
        "digitador_sigla_orgao", "udi", "ueop", "cia", "codigo_espacial_pm", "cia_pel_final", "geo_name",
        "geo_ueop", "geo_cia_pm", "geo_pelotao", "geo_setor", "geo_subsetor", "geo_cd_municipio",
        "geo_nm_municipio", "geo_municipio", "geo_area", "created_at", "updated_at",
    ],
    "fact_icvpa": [
        "numero_ocorrencia", "envolvimento_codigo", "envolvimento_descricao", "numero_envolvido",
        "nome_completo_envolvido", "nome_mae", "data_nascimento", "condicao_fisica_descricao",
        "natureza_ocorrencia_codigo", "natureza_ocorrencia_descricao", "ind_consumado", "rpm_2024", "ueop_2024",
        "unidade_area_militar_codigo", "unidade_area_militar_nome", "unidade_responsavel_registro_codigo",
        "unidade_responsavel_registro_nome", "latitude_sirgas2000", "longitude_sirgas2000", "numero_latitude",
        "numero_longitude", "situacao_zona", "tipo_descricao", "codigo_municipio", "nome_municipio",
        "tipo_logradouro_descricao", "logradouro_nome", "numero_endereco", "nome_bairro", "ocorrencia_uf",
        "data_hora_fato", "ano", "mes", "nome_tipo_relatorio", "digitador_sigla_orgao", "udi", "ueop", "cia",
        "codigo_espacial_pm", "cia_pel_final", "geo_name", "geo_ueop", "geo_cia_pm", "geo_pelotao", "geo_setor",
        "geo_subsetor", "geo_cd_municipio", "geo_nm_municipio", "geo_municipio", "geo_area", "created_at",
        "updated_at",
    ],
}


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _load_agent_env() -> None:
    # Allows execution from project root or inside `agent/`.
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(env_path)


def _normalize_group(folder_name: str) -> str:
    upper = (folder_name or "").strip().upper()
    return LEGACY_GROUP_ALIAS.get(folder_name, upper)


def discover_sql_scripts(sql_root: Path) -> dict[str, Path]:
    scripts: dict[str, Path] = {}
    if not sql_root.exists():
        return scripts

    for sql_file in sorted(sql_root.rglob("*.sql")):
        rel_parts = sql_file.relative_to(sql_root).parts
        if len(rel_parts) == 1:
            rel_key = rel_parts[0]
        else:
            rel_key = f"{_normalize_group(rel_parts[0])}/{rel_parts[-1]}"
        scripts[rel_key] = sql_file
    return scripts


def _read_sql(sql_path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return sql_path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    # Last attempt with replacement.
    return sql_path.read_text(encoding="utf-8", errors="replace")


def _ensure_route(hostname: str, next_hop: str) -> None:
    if not ensure_host_route:
        _log("rota BISP: helper indisponivel; seguindo sem ajuste")
        return
    try:
        _log(f"rota BISP: garantindo rota para {hostname} via {next_hop}")
        ensure_host_route(hostname, next_hop)
        _log("rota BISP: ok")
    except Exception as exc:
        # Non-fatal for extraction flow.
        _log(f"rota BISP: falha ao garantir rota ({exc}); seguindo assim mesmo")
        pass

def fetch_data_from_impala(sql_query: str, user: str, pwd: str, cert_path: Path, hostname: str, next_hop: str) -> pd.DataFrame:
    import pyodbc

    _ensure_route(hostname, next_hop)
    connection_string = (
        "Driver={Cloudera ODBC Driver for Impala};"
        f"Host={hostname};"
        "Port=21051;"
        "AuthMech=3;"
        f"UID={user};"
        f"PWD={pwd};"
        "TransportMode=sasl;"
        "KrbServiceName=impala;"
        "SSL=1;"
        "AllowSelfSignedServerCert=1;"
        f"TrustedCerts={str(cert_path)};"
        "AutoReconnect=1;"
        "UseSQLUnicode=1;"
    )

    try:
        _log("conexao BISP: conectando ao Impala...")
        conn = pyodbc.connect(connection_string, autocommit=True)
        _log("conexao BISP: ok")
    except Exception as exc:
        _log(f"conexao BISP: falhou ({exc})")
        raise

    try:
        _log("consulta: executando SQL...")
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        _log(f"consulta: concluida com {len(rows)} linhas")
        return pd.DataFrame.from_records(rows, columns=columns)
    finally:
        conn.close()

def process_dataframe_for_spatial_join(df: pd.DataFrame, geojson_path: Path) -> pd.DataFrame:
    _log("processamento espacial: inicio")
    # Keep extraction running even without geospatial dependencies.
    if gpd is None or Point is None:
        _log("processamento espacial: geopandas/shapely indisponivel; pulando join")
        out = df.copy()
        for col in ("Setor", "SubSetor", "Pelotao", "CIA_PM", "geometry"):
            out[col] = None
        return out

    out = df.copy()
    required = ("numero_latitude", "numero_longitude")
    if not all(col in out.columns for col in required):
        _log("processamento espacial: colunas de coordenadas ausentes; pulando join")
        for col in ("Setor", "SubSetor", "Pelotao", "CIA_PM", "geometry"):
            out[col] = None
        return out

    coords_df = out.copy()
    coords_df["numero_latitude"] = pd.to_numeric(coords_df["numero_latitude"], errors="coerce")
    coords_df["numero_longitude"] = pd.to_numeric(coords_df["numero_longitude"], errors="coerce")
    coords_df.dropna(subset=list(required), inplace=True)
    if coords_df.empty:
        _log("processamento espacial: nenhuma coordenada valida; pulando join")
        for col in ("Setor", "SubSetor", "Pelotao", "CIA_PM", "geometry"):
            out[col] = None
        return out

    _log(f"processamento espacial: lendo geojson {geojson_path}")
    points = [Point(xy) for xy in zip(coords_df["numero_longitude"], coords_df["numero_latitude"])]
    points_gdf = gpd.GeoDataFrame(coords_df, geometry=points, crs="EPSG:4326")
    polygons = gpd.read_file(geojson_path)
    if polygons.crs != points_gdf.crs:
        polygons = polygons.to_crs(points_gdf.crs)

    joined = gpd.sjoin(points_gdf, polygons, how="left", predicate="within")
    joined["Setor"] = joined.get("SETOR")
    joined["SubSetor"] = joined.get("SUB_SETOR")
    if "SubSetor" not in joined or joined["SubSetor"].isna().all():
        joined["SubSetor"] = joined.get("name")
    joined["Pelotao"] = joined.get("PELOTAO")
    joined["CIA_PM"] = joined.get("CIA_PM")

    if "geometry" in joined.columns:
        joined["geometry_wkt"] = joined["geometry"].apply(lambda g: g.wkt if g is not None else None)

    merge_cols = []
    if "geometry_wkt" in joined.columns:
        merge_cols.append("geometry_wkt")
    merge_cols.extend([c for c in ("Setor", "SubSetor", "Pelotao", "CIA_PM") if c in joined.columns])
    merged = out.merge(
        joined[merge_cols],
        left_index=True,
        right_index=True,
        how="left",
    )
    if "geometry_wkt" in merged.columns:
        merged.rename(columns={"geometry_wkt": "geometry"}, inplace=True)
    _log("processamento espacial: join concluido")
    return merged

def _guess_date_range(sql_query: str) -> tuple[str, str] | None:
    q = sql_query or ''
    # YEAR(...) >= 2026 or YEAR(...) = 2026
    m = re.search(r"YEAR\([^\)]*\)\s*>=\s*(\d{4})", q, re.IGNORECASE)
    if not m:
        m = re.search(r"YEAR\([^\)]*\)\s*=\s*(\d{4})", q, re.IGNORECASE)
    if not m:
        return None
    year = int(m.group(1))
    m_start = 1
    m_end = 12
    m2 = re.search(r"MONTH\([^\)]*\)\s*>=\s*(\d{1,2})", q, re.IGNORECASE)
    if m2:
        m_start = max(1, min(12, int(m2.group(1))))
    m3 = re.search(r"MONTH\([^\)]*\)\s*<=\s*(\d{1,2})", q, re.IGNORECASE)
    if m3:
        m_end = max(1, min(12, int(m3.group(1))))
    # compute last day of end month
    import calendar
    last_day = calendar.monthrange(year, m_end)[1]
    date_from = f"{year}-{m_start:02d}-01"
    date_to = f"{year}-{m_end:02d}-{last_day:02d}"
    return date_from, date_to


def _select_scripts(run_mode: str, requested: list[str], catalog: dict[str, Path]) -> dict[str, Path]:
    if run_mode == "all" or not requested:
        return dict(catalog)

    selected: dict[str, Path] = {}
    by_basename = {path.name: key for key, path in catalog.items()}
    for item in requested:
        normalized = item.strip().replace("\\", "/")
        if not normalized:
            continue
        # Supports both new aliases (GDO/FILE.sql) and old folders in payload.
        if normalized in catalog:
            selected[normalized] = catalog[normalized]
            continue

        parts = normalized.split("/")
        if len(parts) >= 2:
            alias_key = f"{_normalize_group(parts[0])}/{parts[-1]}"
            if alias_key in catalog:
                selected[alias_key] = catalog[alias_key]
                continue

        basename = parts[-1]
        if basename in by_basename:
            key = by_basename[basename]
            selected[key] = catalog[key]

    return selected


def _build_kpi_rows_from_dataframe(indicador: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if df is None or df.empty:
        return rows

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    if "data_hora_fato" in df.columns:
        ref_series = pd.to_datetime(df["data_hora_fato"], errors="coerce").dt.date
    elif "data_fato" in df.columns:
        ref_series = pd.to_datetime(df["data_fato"], errors="coerce").dt.date
    else:
        ref_series = pd.Series([datetime.utcnow().date()] * len(df), index=df.index)

    work = df.copy()
    # IRTD is mostly area-driven, but records explicitly tagged as 74 CIA TM
    # must be counted under this CIA (registro-based ownership).
    if str(indicador or "").upper() == "BD_IRTD" and "CIA_PM" in work.columns:
        reg_col = next((c for c in work.columns if str(c).strip().lower() == "cia_pm_registro"), None)
        if reg_col:
            reg_vals = work[reg_col].astype(str).str.upper()
            mask_74 = reg_vals.str.contains("74 CIA TM", na=False)
            if mask_74.any():
                work.loc[mask_74, "CIA_PM"] = "74 CIA TM"
    work["__ref_date__"] = ref_series.fillna(datetime.utcnow().date())

    def _append_level(level_id_col: str, nivel: str, level_name_col: str | None = None) -> None:
        if level_id_col not in work.columns:
            return
        cols = ["__ref_date__", level_id_col]
        if level_name_col and level_name_col in work.columns:
            cols.append(level_name_col)
        tmp = work[cols].copy()
        tmp[level_id_col] = tmp[level_id_col].astype(str).str.strip()
        if level_name_col and level_name_col in tmp.columns:
            tmp[level_name_col] = tmp[level_name_col].astype(str).str.strip()
        tmp = tmp[(tmp[level_id_col] != "") & (tmp[level_id_col].str.lower() != "nan")]
        if tmp.empty:
            return

        group_cols = ["__ref_date__", level_id_col]
        if level_name_col and level_name_col in tmp.columns:
            group_cols.append(level_name_col)
        grouped = tmp.groupby(group_cols).size().reset_index(name="valor_realizado")
        for _, row in grouped.iterrows():
            unidade_id = str(row[level_id_col]).strip()
            unidade_nome = (
                str(row[level_name_col]).strip()
                if level_name_col and level_name_col in row and str(row[level_name_col]).strip()
                else unidade_id
            )
            rows.append(
                {
                    "referencia_data": str(row["__ref_date__"]),
                    "indicador": indicador,
                    "nivel": nivel,
                    "unidade_id": unidade_id,
                    "unidade_nome": unidade_nome,
                    "valor_realizado": float(row["valor_realizado"]),
                    "valor_meta": None,
                    "valor_plr": None,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

    _append_level("CIA_PM", "CIA_PM")
    _append_level("Pelotao", "PELOTAO")
    _append_level("Setor", "SETOR")
    _append_level("SubSetor", "SUBSETOR")

    municipio_id_col = next(
        (c for c in work.columns if "codigo_municipio" in c.lower() or c.lower() in {"municipio_codigo", "cod_municipio"}),
        None,
    )
    municipio_name_col = next(
        (c for c in work.columns if c.lower() in {"nome_municipio", "municipio", "munic"}),
        None,
    )
    if municipio_id_col:
        _append_level(municipio_id_col, "MUNICIPIO", municipio_name_col)

    return rows


def _build_reds_rows_from_dataframe(indicador: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    """Build event-level rows for export/auditing.

    We keep just the columns needed for CSV export and filtering in the API.
    """
    if df is None or df.empty:
        return []

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    def _pick(col_names: list[str]) -> str | None:
        lower = {c.lower(): c for c in df.columns}
        for name in col_names:
            if name.lower() in lower:
                return lower[name.lower()]
        return None

    col_num = _pick(["numero_ocorrencia", "num_ocorrencia", "reds"])
    if not col_num:
        return []

    col_dt = _pick(["data_hora_fato", "data_fato"])
    col_nat_cod = _pick(["natureza_ocorrencia_codigo", "natureza_codigo"])
    col_nat_desc = _pick(["natureza_ocorrencia_descricao", "natureza_descricao"])
    col_mun_nome = _pick(["nome_municipio", "municipio", "nm_mun"])

    # Sometimes comes as "cast(oco.codigo_municipio as int)"
    col_mun_cod = next((c for c in df.columns if "codigo_municipio" in c.lower()), None) or _pick(
        ["municipio_codigo", "cod_municipio", "cd_municip"]
    )

    col_lat = _pick(["numero_latitude", "latitude"])
    col_lon = _pick(["numero_longitude", "longitude"])
    col_qtd_presos = _pick(["QTD_PRESOS", "qtd_presos"])

    col_env = _pick(["numero_envolvido"])
    col_chave = _pick(["chave_envolvido"])

    def _safe_str(v) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return None
        return s

    def _safe_float(v) -> float | None:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            # suporta valores com virgula decimal (pt-BR)
            s = s.replace(",", ".")
            v = s
        try:
            f = float(v)
        except Exception:
            return None
        if math.isnan(f) or math.isinf(f):
            return None
        return f

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        num = _safe_str(r.get(col_num))
        if not num:
            continue

        dt_raw = r.get(col_dt) if col_dt else None
        ref_date = None
        if dt_raw is not None:
            try:
                ref_date = str(pd.to_datetime(dt_raw, errors="coerce").date())
            except Exception:
                ref_date = None

        cia_pm_value = _safe_str(r.get("CIA_PM"))
        if str(indicador or "").upper() == "BD_IRTD":
            registro_cia = _safe_str(r.get("cia_pm_registro")) or _safe_str(r.get("CIA_PM_REGISTRO"))
            if registro_cia and "74 CIA TM" in registro_cia.upper():
                cia_pm_value = "74 CIA TM"

        rows.append(
            {
                "indicador": indicador,
                "numero_ocorrencia": num,
                "numero_envolvido": _safe_str(r.get(col_env)) if col_env else None,
                "chave_envolvido": _safe_str(r.get(col_chave)) if col_chave else None,
                "data_hora_fato": _safe_str(dt_raw),
                "referencia_data": ref_date,
                "natureza_codigo": _safe_str(r.get(col_nat_cod)) if col_nat_cod else None,
                "natureza_descricao": _safe_str(r.get(col_nat_desc)) if col_nat_desc else None,
                "municipio_codigo": _safe_str(r.get(col_mun_cod)) if col_mun_cod else None,
                "municipio_nome": _safe_str(r.get(col_mun_nome)) if col_mun_nome else None,
                "CIA_PM": cia_pm_value,
                "PELOTAO": _safe_str(r.get("Pelotao")),
                "SETOR": _safe_str(r.get("Setor")),
                "SUBSETOR": _safe_str(r.get("SubSetor")),
                "latitude": _safe_float(r.get(col_lat)) if col_lat else None,
                "longitude": _safe_float(r.get(col_lon)) if col_lon else None,
                "qtd_presos": int(_safe_float(r.get(col_qtd_presos)) or 0) if col_qtd_presos else None,
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )

    return rows


def _fact_table_for(indicador: str) -> str | None:
    return FACT_TABLE_BY_INDICADOR.get(str(indicador or "").upper())


def _build_fact_rows_from_dataframe(indicador: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    table = _fact_table_for(indicador)
    if not table or df is None or df.empty:
        return []

    columns = FACT_COLUMNS.get(table, [])
    if not columns:
        return []

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    lower_map = {str(c).strip().lower(): c for c in df.columns}

    # aliases from spatial join / query naming to fact naming
    source_alias = {
        "setor": "geo_setor",
        "subsetor": "geo_subsetor",
        "pelotao": "geo_pelotao",
        "cia_pm": "geo_cia_pm",
        "municipio_codigo": "codigo_municipio",
        "natureza_codigo": "natureza_ocorrencia_codigo",
        "natureza_descricao": "natureza_ocorrencia_descricao",
        "rmp_2024": "rpm_2024",
    }

    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rec: dict[str, Any] = {}
        for col in columns:
            src = lower_map.get(col.lower())
            if not src:
                # reverse lookup from source alias
                src_name = next((k for k, v in source_alias.items() if v == col), None)
                if src_name:
                    src = lower_map.get(src_name)
            if src:
                rec[col] = row.get(src)
            else:
                rec[col] = None

        # enrich from spatial join when present
        if not rec.get("cia"):
            rec["cia"] = row.get(lower_map.get("cia_pm")) if lower_map.get("cia_pm") else None
        if not rec.get("geo_setor"):
            rec["geo_setor"] = row.get(lower_map.get("setor")) if lower_map.get("setor") else None
        if not rec.get("geo_subsetor"):
            rec["geo_subsetor"] = row.get(lower_map.get("subsetor")) if lower_map.get("subsetor") else None
        if not rec.get("geo_pelotao"):
            rec["geo_pelotao"] = row.get(lower_map.get("pelotao")) if lower_map.get("pelotao") else None
        if not rec.get("geo_cia_pm"):
            rec["geo_cia_pm"] = row.get(lower_map.get("cia_pm")) if lower_map.get("cia_pm") else None

        rec["created_at"] = rec.get("created_at") or now_iso
        rec["updated_at"] = rec.get("updated_at") or now_iso
        out.append(rec)
    return out


def run_extraction(run_mode: str = "all", scripts: list[str] | None = None) -> dict[str, Any]:
    _load_agent_env()

    db_user = _env("DB_USERNAME")
    db_pass = _env("DB_PASSWORD")
    sql_root = Path(_env("AGENT_SQL_ROOT", "")).expanduser()
    geojson_path = Path(_env("AGENT_GEOJSON_PATH", "")).expanduser()
    cert_path = Path(_env("AGENT_CERT_PATH", "")).expanduser()
    bisp_host = _env("BISP_HOSTNAME", "dlmg.prodemge.gov.br")
    bisp_next_hop = _env("BISP_NEXT_HOP", "10.14.56.1")
    output_root = Path(_env("AGENT_OUTPUT_DIR", str(Path(__file__).resolve().parent / "output"))).expanduser()

    _log("inicio extracao")
    _log(f"config: sql_root={sql_root}")
    _log(f"config: geojson={geojson_path}")
    _log(f"config: cert={cert_path}")
    _log(f"config: bisp_host={bisp_host}")

    if not db_user or not db_pass:
        raise RuntimeError("DB_USERNAME/DB_PASSWORD nao definidos no agent/.env")
    if not sql_root.exists():
        raise RuntimeError(f"Pasta de SQL nao encontrada: {sql_root}")
    if not geojson_path.exists():
        raise RuntimeError(f"GeoJSON nao encontrado: {geojson_path}")
    if not cert_path.exists():
        raise RuntimeError(f"Certificado nao encontrado: {cert_path}")

    catalog = discover_sql_scripts(sql_root)
    _log(f"scripts catalogados: {len(catalog)}")
    if not catalog:
        raise RuntimeError(f"Nenhum arquivo .sql encontrado em {sql_root}")

    selected = _select_scripts(run_mode, scripts or [], catalog)
    _log(f"scripts selecionados: {list(selected.keys())}")
    if not selected:
        raise RuntimeError("Nenhum script valido selecionado para execucao")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = output_root / ts
    raw_dir = out_dir / "raw"
    proc_dir = out_dir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    proc_dir.mkdir(parents=True, exist_ok=True)

    files_summary = []
    total_rows = 0
    kpi_rows: list[dict[str, Any]] = []
    reds_rows: list[dict[str, Any]] = []
    fact_rows_by_indicador: dict[str, list[dict[str, Any]]] = {}
    purge_ranges: dict[str, dict[str, str]] = {}

    for rel_key, sql_path in selected.items():
        _log(f"script: iniciando {rel_key}")
        sql_query = _read_sql(sql_path)
        dr = _guess_date_range(sql_query)
        raw_df = fetch_data_from_impala(
            sql_query=sql_query,
            user=db_user,
            pwd=db_pass,
            cert_path=cert_path,
            hostname=bisp_host,
            next_hop=bisp_next_hop,
        )
        _log(f"script: {rel_key} retornou {int(raw_df.shape[0])} linhas")

        processed_df = process_dataframe_for_spatial_join(raw_df, geojson_path=geojson_path)

        safe_name = rel_key.replace("/", "__").replace(".sql", "")
        raw_csv = raw_dir / f"{safe_name}.csv"
        proc_csv = proc_dir / f"{safe_name}.csv"
        raw_df.to_csv(raw_csv, index=False, encoding="utf-8-sig")
        processed_df.to_csv(proc_csv, index=False, encoding="utf-8-sig")
        _log(f"csv bruto salvo: {raw_csv}")
        _log(f"csv processado salvo: {proc_csv}")

        rows = int(processed_df.shape[0])
        total_rows += rows
        indicador = Path(rel_key).name.replace(".sql", "")
        if dr:
            purge_ranges[indicador] = {"date_from": dr[0], "date_to": dr[1]}
        kpi_part = _build_kpi_rows_from_dataframe(indicador=indicador, df=processed_df)
        reds_part = _build_reds_rows_from_dataframe(indicador=indicador, df=processed_df)
        fact_part = _build_fact_rows_from_dataframe(indicador=indicador, df=processed_df)
        _log(f"kpi gerado: {indicador} -> {len(kpi_part)} linhas")
        _log(f"reds gerado: {indicador} -> {len(reds_part)} linhas")
        _log(f"fact gerado: {indicador} -> {len(fact_part)} linhas")
        kpi_rows.extend(kpi_part)
        reds_rows.extend(reds_part)
        if fact_part:
            fact_rows_by_indicador.setdefault(indicador, []).extend(fact_part)
        files_summary.append(
            {
                "script": rel_key,
                "sql_file": str(sql_path),
                "rows": rows,
                "raw_csv": str(raw_csv),
                "processed_csv": str(proc_csv),
            }
        )

    _log(f"extracao finalizada: scripts={len(files_summary)} rows_total={total_rows}")
    _log(
        f"kpi_total={len(kpi_rows)} reds_total={len(reds_rows)} "
        f"fact_total={sum(len(v) for v in fact_rows_by_indicador.values())}"
    )

    return {
        "started_at": ts,
        "run_mode": run_mode,
        "scripts_requested": scripts or [],
        "scripts_executed": len(files_summary),
        "rows_total": total_rows,
        "kpi_rows_total": len(kpi_rows),
        "kpi_rows": kpi_rows,
        "reds_rows_total": len(reds_rows),
        "reds_rows": reds_rows,
        "fact_rows_by_indicador": fact_rows_by_indicador,
        "purge_ranges": purge_ranges,
        "output_dir": str(out_dir),
        "files": files_summary,
    }

