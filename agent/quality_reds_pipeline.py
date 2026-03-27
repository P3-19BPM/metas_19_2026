from datetime import datetime
from pathlib import Path
from typing import Any
import re

import pandas as pd

from extractor_bisp import (
    _env,
    _load_agent_env,
    _log,
    _read_sql,
    fetch_data_from_impala,
)


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in names:
        got = lower.get(name.strip().lower())
        if got:
            return got
    return None


def _normalize_quality_df(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["numero_ocorrencia"] = df.get(_pick_col(df, ["numero_ocorrencia", "num_ocorrencia"]), None)
    out["cia_pm"] = df.get(_pick_col(df, ["cia_pm"]), None)
    out["pelotao_pm"] = df.get(_pick_col(df, ["pelotao_pm", "pelotao"]), None)
    out["data_hora_fato"] = pd.to_datetime(df.get(_pick_col(df, ["data_hora_fato"])), errors="coerce")
    out["data_hora_inclusao"] = pd.to_datetime(df.get(_pick_col(df, ["data_hora_inclusao"])), errors="coerce")
    out["data_hora_final"] = pd.to_datetime(df.get(_pick_col(df, ["data_hora_final"])), errors="coerce")
    out["data_fato"] = pd.to_datetime(df.get(_pick_col(df, ["data_fato"])), errors="coerce")
    out["mes"] = pd.to_numeric(df.get(_pick_col(df, ["mes"])), errors="coerce")
    out["ano"] = pd.to_numeric(df.get(_pick_col(df, ["ano"])), errors="coerce")
    out["data_hora_fechamento"] = pd.to_datetime(
        df.get(_pick_col(df, ["data_hora_fechamento"])), errors="coerce"
    )
    out["ind_estado"] = df.get(_pick_col(df, ["ind_estado"]), None)
    out["descricao_estado"] = df.get(_pick_col(df, ["descricao_estado"]), None)
    out["data_hora_alteracao"] = pd.to_datetime(
        df.get(_pick_col(df, ["data_hora_alteracao"])), errors="coerce"
    )
    out["data_hora_primeiro_fim_preenchimento"] = pd.to_datetime(
        df.get(_pick_col(df, ["data_hora_primeiro_fim_preenchimento"])), errors="coerce"
    )
    out["nome_tipo_relatorio"] = df.get(_pick_col(df, ["nome_tipo_relatorio"]), None)
    out["digitador_cargo_efetivo"] = df.get(_pick_col(df, ["digitador_cargo_efetivo"]), None)
    out["digitador_matricula"] = df.get(_pick_col(df, ["digitador_matricula"]), None)
    out["digitador_nome"] = df.get(_pick_col(df, ["digitador_nome"]), None)
    out["digitador_nome_unidade"] = df.get(_pick_col(df, ["digitador_nome_unidade"]), None)
    out["natureza_codigo"] = df.get(_pick_col(df, ["natureza_codigo"]), None)
    out["natureza_descricao_longa"] = df.get(
        _pick_col(df, ["natureza_descricao_longa", "natureza_descricao"]), None
    )
    out["natureza_secundaria1_codigo"] = df.get(_pick_col(df, ["natureza_secundaria1_codigo"]), None)
    out["natureza_secundaria1_descricao"] = df.get(_pick_col(df, ["natureza_secundaria1_descricao"]), None)
    out["natureza_secundaria2_codigo"] = df.get(_pick_col(df, ["natureza_secundaria2_codigo"]), None)
    out["natureza_secundaria2_descricao"] = df.get(_pick_col(df, ["natureza_secundaria2_descricao"]), None)
    out["natureza_secundaria3_codigo"] = df.get(_pick_col(df, ["natureza_secundaria3_codigo"]), None)
    out["natureza_secundaria3_descricao"] = df.get(_pick_col(df, ["natureza_secundaria3_descricao"]), None)
    out["historico_ocorrencia"] = df.get(_pick_col(df, ["historico_ocorrencia"]), None)
    out["flag_envolvido_sem_cpf"] = pd.to_numeric(
        df.get(_pick_col(df, ["flag_envolvido_sem_cpf"])), errors="coerce"
    ).fillna(0).astype(int)
    out["flag_envolvido_sem_rg"] = pd.to_numeric(
        df.get(_pick_col(df, ["flag_envolvido_sem_rg"])), errors="coerce"
    ).fillna(0).astype(int)
    out["flag_envolvido_sem_email"] = pd.to_numeric(
        df.get(_pick_col(df, ["flag_envolvido_sem_email"])), errors="coerce"
    ).fillna(0).astype(int)
    out["flag_envolvido_sem_telefone"] = pd.to_numeric(
        df.get(_pick_col(df, ["flag_envolvido_sem_telefone"])), errors="coerce"
    ).fillna(0).astype(int)

    if out["data_fato"].isna().all():
        out["data_fato"] = out["data_hora_fato"].dt.normalize()
    if out["mes"].isna().all():
        out["mes"] = out["data_hora_fato"].dt.month
    if out["ano"].isna().all():
        out["ano"] = out["data_hora_fato"].dt.year

    for col in (
        "numero_ocorrencia",
        "cia_pm",
        "pelotao_pm",
        "ind_estado",
        "descricao_estado",
        "natureza_codigo",
        "natureza_secundaria1_codigo",
        "natureza_secundaria2_codigo",
        "natureza_secundaria3_codigo",
    ):
        out[col] = out[col].astype("string")
    return out


def _rows_from_quality_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dt_cols = {
        "data_hora_fato",
        "data_hora_inclusao",
        "data_hora_final",
        "data_hora_fechamento",
        "data_hora_alteracao",
        "data_hora_primeiro_fim_preenchimento",
    }
    for _, row in df.iterrows():
        num = str(row.get("numero_ocorrencia") or "").strip()
        if not num:
            continue
        rec: dict[str, Any] = {}
        for col in df.columns:
            v = row.get(col)
            if pd.isna(v):
                rec[col] = None
                continue
            if col in dt_cols:
                rec[col] = pd.to_datetime(v).isoformat()
                continue
            if col == "data_fato":
                rec[col] = str(pd.to_datetime(v).date())
                continue
            if col in {"mes", "ano"}:
                rec[col] = int(v)
                continue
            rec[col] = str(v).strip() if isinstance(v, str) else v
        rows.append(rec)
    return rows


def _pick_quality_sql(sql_root: Path, scripts: list[str] | None) -> Path:
    env_sql = _env("AGENT_QUALITY_SQL_PATH", "")
    if env_sql:
        p = Path(env_sql).expanduser()
        if p.exists():
            return p
    if scripts:
        for s in scripts:
            p = Path(s)
            if not p.is_absolute():
                p = sql_root / s
            if p.exists():
                return p
    default = sql_root / "qualidade_reds" / "qualidade_reds.sql"
    if default.exists():
        return default
    raise RuntimeError(f"SQL de qualidade REDS nao encontrado. Caminho esperado: {default}")


def _quality_date_filter_sql(extract_mode: str) -> str:
    mode = str(extract_mode or "").strip().lower()
    # Full de normalizacao: inicio do ano corrente operacional (2026) ate hoje.
    if mode == "full_2026":
        return (
            "OCO.data_hora_fato >= CAST('2026-01-01' AS TIMESTAMP) "
            "AND OCO.data_hora_fato < CAST(date_add(current_date(), 1) AS TIMESTAMP)"
        )
    # Padrao: incremental dos ultimos 12 dias por referencia de atualizacao.
    return (
        "COALESCE("
        "OCO.data_hora_primeiro_fim_preenchimento, "
        "OCO.data_hora_alteracao, "
        "OCO.data_hora_fechamento, "
        "OCO.data_hora_inclusao, "
        "OCO.data_hora_fato"
        ") >= CAST(date_sub(current_date(), 12) AS TIMESTAMP)"
    )


def _inject_quality_date_filter(query: str, extract_mode: str) -> str:
    marker = "{{QUALITY_DATE_FILTER}}"
    filter_sql = _quality_date_filter_sql(extract_mode=extract_mode)
    if marker in query:
        return query.replace(marker, filter_sql)

    # Backward-compatible fallback for legacy SQL without marker.
    legacy = re.compile(
        r"AND\s+COALESCE\(\s*OCO\.data_hora_primeiro_fim_preenchimento[\s\S]*?\)\s*>=\s*CAST\(date_sub\(current_date\(\),\s*12\)\s+AS\s+TIMESTAMP\)",
        re.IGNORECASE,
    )
    if legacy.search(query):
        return legacy.sub(f"AND {filter_sql}", query)

    q = re.sub(
        r"(AND\s+OCO\.digitador_sigla_orgao\s*=\s*'PM'\s*;)",
        f"AND {filter_sql}\n\t\t\\1",
        query,
        flags=re.IGNORECASE,
    )
    return q


def run_quality_reds_pipeline(
    scripts: list[str] | None = None,
    extract_mode: str = "incremental_12d",
) -> dict[str, Any]:
    _load_agent_env()
    db_user = _env("DB_USERNAME")
    db_pass = _env("DB_PASSWORD")
    sql_root = Path(_env("AGENT_SQL_ROOT", "")).expanduser()
    cert_path = Path(_env("AGENT_CERT_PATH", "")).expanduser()
    bisp_host = _env("BISP_HOSTNAME", "dlmg.prodemge.gov.br")
    bisp_next_hop = _env("BISP_NEXT_HOP", "10.14.56.1")
    output_root = Path(_env("AGENT_OUTPUT_DIR", str(Path(__file__).resolve().parent / "output"))).expanduser()

    if not db_user or not db_pass:
        raise RuntimeError("DB_USERNAME/DB_PASSWORD nao definidos no agent/.env")
    if not sql_root.exists():
        raise RuntimeError(f"Pasta de SQL nao encontrada: {sql_root}")
    if not cert_path.exists():
        raise RuntimeError(f"Certificado nao encontrado: {cert_path}")

    sql_path = _pick_quality_sql(sql_root=sql_root, scripts=scripts)
    _log(f"quality reds: sql={sql_path}")
    query = _inject_quality_date_filter(_read_sql(sql_path), extract_mode=extract_mode)
    _log(f"quality reds: extract_mode={extract_mode}")

    raw_df = fetch_data_from_impala(
        sql_query=query,
        user=db_user,
        pwd=db_pass,
        cert_path=cert_path,
        hostname=bisp_host,
        next_hop=bisp_next_hop,
    )
    _log(f"quality reds: linhas extraidas={int(raw_df.shape[0])}")

    norm_df = _normalize_quality_df(raw_df)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = output_root / ts / "quality_reds"
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_csv = out_dir / "qualidade_reds_raw.csv"
    norm_csv = out_dir / "qualidade_reds_normalized.csv"
    raw_df.to_csv(raw_csv, index=False, encoding="utf-8-sig")
    norm_df.to_csv(norm_csv, index=False, encoding="utf-8-sig")
    _log(f"quality reds: csv bruto={raw_csv}")
    _log(f"quality reds: csv normalizado={norm_csv}")

    rows = _rows_from_quality_df(norm_df)
    return {
        "started_at": ts,
        "run_mode": "quality_reds",
        "extract_mode": extract_mode,
        "sql_file": str(sql_path),
        "rows_total": len(rows),
        "raw_csv": str(raw_csv),
        "normalized_csv": str(norm_csv),
        "quality_rows": rows,
    }
