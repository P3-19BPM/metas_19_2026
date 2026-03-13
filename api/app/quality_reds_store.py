from datetime import datetime, timezone
from math import ceil
import hashlib
import re
from pathlib import Path
from typing import Any
import json

from sqlalchemy import inspect, text

from .db import get_engine


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    v = str(value or "").strip().lower()
    return 1 if v in {"1", "true", "yes", "y", "on"} else 0


_QR_RAW_TRACKED_FIELDS = (
    "cia_pm",
    "pelotao_pm",
    "data_hora_fato",
    "data_hora_inclusao",
    "data_hora_final",
    "data_fato",
    "mes",
    "ano",
    "data_hora_fechamento",
    "descricao_estado",
    "data_hora_alteracao",
    "data_hora_primeiro_fim_preenchimento",
    "nome_tipo_relatorio",
    "digitador_cargo_efetivo",
    "digitador_matricula",
    "digitador_nome",
    "digitador_nome_unidade",
    "natureza_codigo",
    "natureza_descricao_longa",
    "natureza_secundaria1_codigo",
    "natureza_secundaria1_descricao",
    "natureza_secundaria2_codigo",
    "natureza_secundaria2_descricao",
    "natureza_secundaria3_codigo",
    "natureza_secundaria3_descricao",
    "historico_ocorrencia",
)


def _canon_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return value
    return str(value).strip()


def _raw_payload_for_hash(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _canon_value(row.get(k)) for k in _QR_RAW_TRACKED_FIELDS}


def _hash_payload(payload: dict[str, Any]) -> str:
    base = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _chunks(items: list[str], size: int = 900) -> list[list[str]]:
    if size <= 0:
        size = 900
    return [items[i:i + size] for i in range(0, len(items), size)]


def ensure_quality_tables() -> None:
    engine = get_engine()
    is_postgres = str(engine.dialect.name).startswith("postgres")
    raw_pk = "BIGSERIAL PRIMARY KEY" if is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
    quality_pk = "BIGSERIAL PRIMARY KEY" if is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"

    sql_raw = f"""
    CREATE TABLE IF NOT EXISTS qr_raw_reds (
      id {raw_pk},
      numero_ocorrencia TEXT NOT NULL,
      cia_pm TEXT,
      pelotao_pm TEXT,
      data_hora_fato TEXT,
      data_hora_inclusao TEXT,
      data_hora_final TEXT,
      data_fato TEXT,
      mes INTEGER,
      ano INTEGER,
      data_hora_fechamento TEXT,
      descricao_estado TEXT,
      data_hora_alteracao TEXT,
      data_hora_primeiro_fim_preenchimento TEXT,
      nome_tipo_relatorio TEXT,
      digitador_cargo_efetivo TEXT,
      digitador_matricula TEXT,
      digitador_nome TEXT,
      digitador_nome_unidade TEXT,
      natureza_codigo TEXT,
      natureza_descricao_longa TEXT,
      natureza_secundaria1_codigo TEXT,
      natureza_secundaria1_descricao TEXT,
      natureza_secundaria2_codigo TEXT,
      natureza_secundaria2_descricao TEXT,
      natureza_secundaria3_codigo TEXT,
      natureza_secundaria3_descricao TEXT,
      historico_ocorrencia TEXT,
      change_signature TEXT,
      alteracoes_count INTEGER NOT NULL DEFAULT 0,
      versao_atual INTEGER NOT NULL DEFAULT 1,
      primeira_captura_em TEXT,
      ultima_alteracao_detectada_em TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(numero_ocorrencia)
    )
    """
    sql_raw_history = f"""
    CREATE TABLE IF NOT EXISTS qr_raw_reds_history (
      id {"BIGSERIAL PRIMARY KEY" if is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"},
      numero_ocorrencia TEXT NOT NULL,
      versao INTEGER NOT NULL,
      change_type TEXT NOT NULL,
      change_signature TEXT NOT NULL,
      source_data_hora_alteracao TEXT,
      source_data_hora_fechamento TEXT,
      source_data_hora_primeiro_fim_preenchimento TEXT,
      payload_json TEXT NOT NULL,
      detected_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
    """
    sql_quality = f"""
    CREATE TABLE IF NOT EXISTS qr_quality_analysis (
      id {quality_pk},
      numero_ocorrencia TEXT NOT NULL,
      cia_pm TEXT,
      pelotao_pm TEXT,
      data_hora_fato TEXT,
      data_fato TEXT,
      natureza_codigo TEXT,
      natureza_descricao_longa TEXT,
      historico_ocorrencia TEXT,
      score_textual INTEGER NOT NULL DEFAULT 0,
      alerta_texto_curto INTEGER NOT NULL DEFAULT 0,
      erro_ortografico INTEGER NOT NULL DEFAULT 0,
      ausencia_pontuacao INTEGER NOT NULL DEFAULT 0,
      historico_incompleto INTEGER NOT NULL DEFAULT 0,
      alerta_convergencia INTEGER NOT NULL DEFAULT 0,
      alerta_padrao_operacional INTEGER NOT NULL DEFAULT 0,
      alerta_classificacao INTEGER NOT NULL DEFAULT 0,
      alerta_c99 INTEGER NOT NULL DEFAULT 0,
      alerta_w01 INTEGER NOT NULL DEFAULT 0,
      alerta_w02 INTEGER NOT NULL DEFAULT 0,
      alerta_w03 INTEGER NOT NULL DEFAULT 0,
      alerta_w04 INTEGER NOT NULL DEFAULT 0,
      possivel_copia_historico INTEGER NOT NULL DEFAULT 0,
      repeticao_por_guarnicao INTEGER NOT NULL DEFAULT 0,
      tempo_empenho_suspeito INTEGER NOT NULL DEFAULT 0,
      possivel_erro_natureza_violenta INTEGER NOT NULL DEFAULT 0,
      natureza_sugerida TEXT,
      recomendacoes_json TEXT,
      data_analise TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(numero_ocorrencia)
    )
    """
    sql_eval = f"""
    CREATE TABLE IF NOT EXISTS qr_evaluation (
      id {"BIGSERIAL PRIMARY KEY" if is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"},
      numero_ocorrencia TEXT NOT NULL,
      avaliado INTEGER NOT NULL DEFAULT 0,
      ajustes_json TEXT,
      observacao TEXT,
      avaliado_por TEXT,
      avaliado_em TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(numero_ocorrencia)
    )
    """
    with engine.begin() as conn:
        conn.execute(text(sql_raw))
        # Backward-compatible migrations for existing environments.
        raw_cols = {str(c.get("name") or "").strip().lower() for c in inspect(conn).get_columns("qr_raw_reds")}
        for col_name, ddl in (
            ("data_hora_inclusao", "ALTER TABLE qr_raw_reds ADD COLUMN data_hora_inclusao TEXT"),
            ("data_hora_final", "ALTER TABLE qr_raw_reds ADD COLUMN data_hora_final TEXT"),
            ("descricao_estado", "ALTER TABLE qr_raw_reds ADD COLUMN descricao_estado TEXT"),
            ("change_signature", "ALTER TABLE qr_raw_reds ADD COLUMN change_signature TEXT"),
            ("alteracoes_count", "ALTER TABLE qr_raw_reds ADD COLUMN alteracoes_count INTEGER NOT NULL DEFAULT 0"),
            ("versao_atual", "ALTER TABLE qr_raw_reds ADD COLUMN versao_atual INTEGER NOT NULL DEFAULT 1"),
            ("primeira_captura_em", "ALTER TABLE qr_raw_reds ADD COLUMN primeira_captura_em TEXT"),
            ("ultima_alteracao_detectada_em", "ALTER TABLE qr_raw_reds ADD COLUMN ultima_alteracao_detectada_em TEXT"),
            ("natureza_secundaria1_codigo", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria1_codigo TEXT"),
            ("natureza_secundaria1_descricao", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria1_descricao TEXT"),
            ("natureza_secundaria2_codigo", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria2_codigo TEXT"),
            ("natureza_secundaria2_descricao", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria2_descricao TEXT"),
            ("natureza_secundaria3_codigo", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria3_codigo TEXT"),
            ("natureza_secundaria3_descricao", "ALTER TABLE qr_raw_reds ADD COLUMN natureza_secundaria3_descricao TEXT"),
        ):
            if col_name in raw_cols:
                continue
            conn.execute(text(ddl))
            raw_cols.add(col_name)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_data ON qr_raw_reds(data_fato)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_cia ON qr_raw_reds(cia_pm)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_pelotao ON qr_raw_reds(pelotao_pm)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_alteracoes ON qr_raw_reds(alteracoes_count)"))

        conn.execute(text(sql_raw_history))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_hist_numero ON qr_raw_reds_history(numero_ocorrencia)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_hist_detected ON qr_raw_reds_history(detected_at)"))

        conn.execute(text(sql_quality))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_data ON qr_quality_analysis(data_fato)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_cia ON qr_quality_analysis(cia_pm)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_pelotao ON qr_quality_analysis(pelotao_pm)"))
        conn.execute(text(sql_eval))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_eval_avaliado ON qr_evaluation(avaliado)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_eval_updated ON qr_evaluation(updated_at)"))


def _load_qr_raw_existing_map(conn: Any, numeros: list[str]) -> dict[str, dict[str, Any]]:
    if not numeros:
        return {}
    out: dict[str, dict[str, Any]] = {}
    unique_nums = sorted(set(numeros))
    for chunk in _chunks(unique_nums, size=900):
        params: dict[str, Any] = {}
        marks: list[str] = []
        for idx, numero in enumerate(chunk):
            key = f"n{idx}"
            marks.append(f":{key}")
            params[key] = numero
        q = text(
            f"""
            SELECT
              numero_ocorrencia,
              change_signature,
              alteracoes_count,
              versao_atual,
              primeira_captura_em,
              ultima_alteracao_detectada_em
            FROM qr_raw_reds
            WHERE numero_ocorrencia IN ({",".join(marks)})
            """
        )
        for row in conn.execute(q, params):
            m = dict(row._mapping)
            out[str(m.get("numero_ocorrencia") or "")] = m
    return out


def upsert_qr_raw_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {"received": 0, "upserted": 0}
    ensure_quality_tables()
    now = _utc_now_iso()
    values: list[dict[str, Any]] = []
    history_values: list[dict[str, Any]] = []
    inserted = 0
    changed = 0

    incoming_numeros: list[str] = []
    for row in rows:
        num = str(row.get("numero_ocorrencia") or "").strip()
        if num:
            incoming_numeros.append(num)

    existing_map: dict[str, dict[str, Any]] = {}
    with get_engine().begin() as conn:
        existing_map = _load_qr_raw_existing_map(conn, incoming_numeros)

    for row in rows:
        num = str(row.get("numero_ocorrencia") or "").strip()
        if not num:
            continue
        payload_row = {
            "numero_ocorrencia": num,
            "cia_pm": row.get("cia_pm"),
            "pelotao_pm": row.get("pelotao_pm"),
            "data_hora_fato": row.get("data_hora_fato"),
            "data_hora_inclusao": row.get("data_hora_inclusao"),
            "data_hora_final": row.get("data_hora_final"),
            "data_fato": row.get("data_fato"),
            "mes": row.get("mes"),
            "ano": row.get("ano"),
            "data_hora_fechamento": row.get("data_hora_fechamento"),
            "descricao_estado": row.get("descricao_estado"),
            "data_hora_alteracao": row.get("data_hora_alteracao"),
            "data_hora_primeiro_fim_preenchimento": row.get("data_hora_primeiro_fim_preenchimento"),
            "nome_tipo_relatorio": row.get("nome_tipo_relatorio"),
            "digitador_cargo_efetivo": row.get("digitador_cargo_efetivo"),
            "digitador_matricula": row.get("digitador_matricula"),
            "digitador_nome": row.get("digitador_nome"),
            "digitador_nome_unidade": row.get("digitador_nome_unidade"),
            "natureza_codigo": row.get("natureza_codigo"),
            "natureza_descricao_longa": row.get("natureza_descricao_longa"),
            "natureza_secundaria1_codigo": row.get("natureza_secundaria1_codigo"),
            "natureza_secundaria1_descricao": row.get("natureza_secundaria1_descricao"),
            "natureza_secundaria2_codigo": row.get("natureza_secundaria2_codigo"),
            "natureza_secundaria2_descricao": row.get("natureza_secundaria2_descricao"),
            "natureza_secundaria3_codigo": row.get("natureza_secundaria3_codigo"),
            "natureza_secundaria3_descricao": row.get("natureza_secundaria3_descricao"),
            "historico_ocorrencia": row.get("historico_ocorrencia"),
        }
        hashed_payload = _raw_payload_for_hash(payload_row)
        signature = _hash_payload(hashed_payload)
        existing = existing_map.get(num)
        if existing is None:
            inserted += 1
            versao_atual = 1
            alteracoes_count = 0
            primeira_captura_em = now
            ultima_alteracao_detectada_em = now
            event_type = "insert"
            is_changed = True
        else:
            prev_sig = str(existing.get("change_signature") or "")
            prev_count = int(existing.get("alteracoes_count") or 0)
            prev_ver = int(existing.get("versao_atual") or 1)
            primeira_captura_em = existing.get("primeira_captura_em") or now
            if prev_sig != signature:
                changed += 1
                versao_atual = prev_ver + 1
                alteracoes_count = prev_count + 1
                ultima_alteracao_detectada_em = now
                event_type = "update"
                is_changed = True
            else:
                versao_atual = prev_ver
                alteracoes_count = prev_count
                ultima_alteracao_detectada_em = existing.get("ultima_alteracao_detectada_em")
                event_type = "noop"
                is_changed = False

        values.append(
            {
                **payload_row,
                "change_signature": signature,
                "alteracoes_count": alteracoes_count,
                "versao_atual": versao_atual,
                "primeira_captura_em": primeira_captura_em,
                "ultima_alteracao_detectada_em": ultima_alteracao_detectada_em,
                "created_at": row.get("created_at") or now,
                "updated_at": row.get("updated_at") or now,
            }
        )
        if is_changed:
            history_values.append(
                {
                    "numero_ocorrencia": num,
                    "versao": versao_atual,
                    "change_type": event_type,
                    "change_signature": signature,
                    "source_data_hora_alteracao": row.get("data_hora_alteracao"),
                    "source_data_hora_fechamento": row.get("data_hora_fechamento"),
                    "source_data_hora_primeiro_fim_preenchimento": row.get("data_hora_primeiro_fim_preenchimento"),
                    "payload_json": json.dumps(hashed_payload, ensure_ascii=False, sort_keys=True),
                    "detected_at": now,
                    "created_at": now,
                }
            )
        # Keep in-memory state in sync to handle duplicate numero_ocorrencia in the same batch.
        existing_map[num] = {
            "change_signature": signature,
            "alteracoes_count": alteracoes_count,
            "versao_atual": versao_atual,
            "primeira_captura_em": primeira_captura_em,
            "ultima_alteracao_detectada_em": ultima_alteracao_detectada_em,
        }

    if not values:
        return {"received": len(rows), "upserted": 0}

    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO qr_raw_reds(
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_hora_inclusao, data_hora_final, data_fato, mes, ano,
                  data_hora_fechamento, descricao_estado, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa,
                  natureza_secundaria1_codigo, natureza_secundaria1_descricao,
                  natureza_secundaria2_codigo, natureza_secundaria2_descricao,
                  natureza_secundaria3_codigo, natureza_secundaria3_descricao,
                  historico_ocorrencia,
                  change_signature, alteracoes_count, versao_atual, primeira_captura_em, ultima_alteracao_detectada_em,
                  created_at, updated_at
                )
                VALUES(
                  :numero_ocorrencia, :cia_pm, :pelotao_pm, :data_hora_fato, :data_hora_inclusao, :data_hora_final, :data_fato, :mes, :ano,
                  :data_hora_fechamento, :descricao_estado, :data_hora_alteracao, :data_hora_primeiro_fim_preenchimento,
                  :nome_tipo_relatorio, :digitador_cargo_efetivo, :digitador_matricula, :digitador_nome, :digitador_nome_unidade,
                  :natureza_codigo, :natureza_descricao_longa,
                  :natureza_secundaria1_codigo, :natureza_secundaria1_descricao,
                  :natureza_secundaria2_codigo, :natureza_secundaria2_descricao,
                  :natureza_secundaria3_codigo, :natureza_secundaria3_descricao,
                  :historico_ocorrencia,
                  :change_signature, :alteracoes_count, :versao_atual, :primeira_captura_em, :ultima_alteracao_detectada_em,
                  :created_at, :updated_at
                )
                ON CONFLICT(numero_ocorrencia)
                DO UPDATE SET
                  cia_pm=excluded.cia_pm,
                  pelotao_pm=excluded.pelotao_pm,
                  data_hora_fato=excluded.data_hora_fato,
                  data_hora_inclusao=excluded.data_hora_inclusao,
                  data_hora_final=excluded.data_hora_final,
                  data_fato=excluded.data_fato,
                  mes=excluded.mes,
                  ano=excluded.ano,
                  data_hora_fechamento=excluded.data_hora_fechamento,
                  descricao_estado=excluded.descricao_estado,
                  data_hora_alteracao=excluded.data_hora_alteracao,
                  data_hora_primeiro_fim_preenchimento=excluded.data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio=excluded.nome_tipo_relatorio,
                  digitador_cargo_efetivo=excluded.digitador_cargo_efetivo,
                  digitador_matricula=excluded.digitador_matricula,
                  digitador_nome=excluded.digitador_nome,
                  digitador_nome_unidade=excluded.digitador_nome_unidade,
                  natureza_codigo=excluded.natureza_codigo,
                  natureza_descricao_longa=excluded.natureza_descricao_longa,
                  natureza_secundaria1_codigo=excluded.natureza_secundaria1_codigo,
                  natureza_secundaria1_descricao=excluded.natureza_secundaria1_descricao,
                  natureza_secundaria2_codigo=excluded.natureza_secundaria2_codigo,
                  natureza_secundaria2_descricao=excluded.natureza_secundaria2_descricao,
                  natureza_secundaria3_codigo=excluded.natureza_secundaria3_codigo,
                  natureza_secundaria3_descricao=excluded.natureza_secundaria3_descricao,
                  historico_ocorrencia=excluded.historico_ocorrencia,
                  change_signature=excluded.change_signature,
                  alteracoes_count=excluded.alteracoes_count,
                  versao_atual=excluded.versao_atual,
                  primeira_captura_em=excluded.primeira_captura_em,
                  ultima_alteracao_detectada_em=excluded.ultima_alteracao_detectada_em,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )
        if history_values:
            conn.execute(
                text(
                    """
                    INSERT INTO qr_raw_reds_history(
                      numero_ocorrencia, versao, change_type, change_signature,
                      source_data_hora_alteracao, source_data_hora_fechamento, source_data_hora_primeiro_fim_preenchimento,
                      payload_json, detected_at, created_at
                    )
                    VALUES(
                      :numero_ocorrencia, :versao, :change_type, :change_signature,
                      :source_data_hora_alteracao, :source_data_hora_fechamento, :source_data_hora_primeiro_fim_preenchimento,
                      :payload_json, :detected_at, :created_at
                    )
                    """
                ),
                history_values,
            )
    return {
        "received": len(rows),
        "upserted": len(values),
        "inserted": inserted,
        "changed": changed,
        "history_events": len(history_values),
    }


def list_qr_raw_rows(
    date_from: str | None = None,
    date_to: str | None = None,
    cia_pm: str | None = None,
    pelotao_pm: str | None = None,
    natureza_codigo: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    ensure_quality_tables()
    where = ["1=1"]
    params: dict[str, Any] = {"limit": int(limit)}
    if date_from:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) <= :date_to")
        params["date_to"] = date_to
    if cia_pm:
        where.append("(cia_pm = :cia_pm OR cia_pm LIKE :cia_pm_like)")
        params["cia_pm"] = cia_pm
        params["cia_pm_like"] = f"%{cia_pm}%"
    if pelotao_pm:
        where.append("pelotao_pm = :pelotao_pm")
        params["pelotao_pm"] = pelotao_pm
    if natureza_codigo:
        where.append("natureza_codigo = :natureza_codigo")
        params["natureza_codigo"] = natureza_codigo
    where_sql = " AND ".join(where)
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_hora_inclusao, data_hora_final, data_fato, mes, ano,
                  data_hora_fechamento, descricao_estado, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa,
                  natureza_secundaria1_codigo, natureza_secundaria1_descricao,
                  natureza_secundaria2_codigo, natureza_secundaria2_descricao,
                  natureza_secundaria3_codigo, natureza_secundaria3_descricao,
                  historico_ocorrencia,
                  alteracoes_count, versao_atual, ultima_alteracao_detectada_em
                FROM qr_raw_reds
                WHERE {where_sql}
                ORDER BY COALESCE(data_hora_fato, data_fato) ASC, numero_ocorrencia ASC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(r._mapping) for r in rows]


def upsert_qr_quality_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {"received": 0, "upserted": 0}
    ensure_quality_tables()
    now = _utc_now_iso()
    values = []
    for row in rows:
        num = str(row.get("numero_ocorrencia") or "").strip()
        if not num:
            continue
        values.append(
            {
                "numero_ocorrencia": num,
                "cia_pm": row.get("cia_pm"),
                "pelotao_pm": row.get("pelotao_pm"),
                "data_hora_fato": row.get("data_hora_fato"),
                "data_fato": row.get("data_fato"),
                "natureza_codigo": row.get("natureza_codigo"),
                "natureza_descricao_longa": row.get("natureza_descricao_longa"),
                "historico_ocorrencia": row.get("historico_ocorrencia"),
                "score_textual": int(row.get("score_textual") or 0),
                "alerta_texto_curto": _to_bool_int(row.get("alerta_texto_curto")),
                "erro_ortografico": _to_bool_int(row.get("erro_ortografico")),
                "ausencia_pontuacao": _to_bool_int(row.get("ausencia_pontuacao")),
                "historico_incompleto": _to_bool_int(row.get("historico_incompleto")),
                "alerta_convergencia": _to_bool_int(row.get("alerta_convergencia")),
                "alerta_padrao_operacional": _to_bool_int(row.get("alerta_padrao_operacional")),
                "alerta_classificacao": _to_bool_int(row.get("alerta_classificacao")),
                "alerta_c99": _to_bool_int(row.get("alerta_c99")),
                "alerta_w01": _to_bool_int(row.get("alerta_w01")),
                "alerta_w02": _to_bool_int(row.get("alerta_w02")),
                "alerta_w03": _to_bool_int(row.get("alerta_w03")),
                "alerta_w04": _to_bool_int(row.get("alerta_w04")),
                "possivel_copia_historico": _to_bool_int(row.get("possivel_copia_historico")),
                "repeticao_por_guarnicao": _to_bool_int(row.get("repeticao_por_guarnicao")),
                "tempo_empenho_suspeito": _to_bool_int(row.get("tempo_empenho_suspeito")),
                "possivel_erro_natureza_violenta": _to_bool_int(row.get("possivel_erro_natureza_violenta")),
                "natureza_sugerida": row.get("natureza_sugerida"),
                "recomendacoes_json": row.get("recomendacoes_json"),
                "data_analise": row.get("data_analise") or now,
                "created_at": row.get("created_at") or now,
                "updated_at": row.get("updated_at") or now,
            }
        )
    if not values:
        return {"received": len(rows), "upserted": 0}

    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO qr_quality_analysis(
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_fato,
                  natureza_codigo, natureza_descricao_longa, historico_ocorrencia,
                  score_textual,
                  alerta_texto_curto, erro_ortografico, ausencia_pontuacao, historico_incompleto,
                  alerta_convergencia, alerta_padrao_operacional, alerta_classificacao,
                  alerta_c99, alerta_w01, alerta_w02, alerta_w03, alerta_w04,
                  possivel_copia_historico, repeticao_por_guarnicao, tempo_empenho_suspeito,
                  possivel_erro_natureza_violenta,
                  natureza_sugerida, recomendacoes_json, data_analise, created_at, updated_at
                )
                VALUES(
                  :numero_ocorrencia, :cia_pm, :pelotao_pm, :data_hora_fato, :data_fato,
                  :natureza_codigo, :natureza_descricao_longa, :historico_ocorrencia,
                  :score_textual,
                  :alerta_texto_curto, :erro_ortografico, :ausencia_pontuacao, :historico_incompleto,
                  :alerta_convergencia, :alerta_padrao_operacional, :alerta_classificacao,
                  :alerta_c99, :alerta_w01, :alerta_w02, :alerta_w03, :alerta_w04,
                  :possivel_copia_historico, :repeticao_por_guarnicao, :tempo_empenho_suspeito,
                  :possivel_erro_natureza_violenta,
                  :natureza_sugerida, :recomendacoes_json, :data_analise, :created_at, :updated_at
                )
                ON CONFLICT(numero_ocorrencia)
                DO UPDATE SET
                  cia_pm=excluded.cia_pm,
                  pelotao_pm=excluded.pelotao_pm,
                  data_hora_fato=excluded.data_hora_fato,
                  data_fato=excluded.data_fato,
                  natureza_codigo=excluded.natureza_codigo,
                  natureza_descricao_longa=excluded.natureza_descricao_longa,
                  historico_ocorrencia=excluded.historico_ocorrencia,
                  score_textual=excluded.score_textual,
                  alerta_texto_curto=excluded.alerta_texto_curto,
                  erro_ortografico=excluded.erro_ortografico,
                  ausencia_pontuacao=excluded.ausencia_pontuacao,
                  historico_incompleto=excluded.historico_incompleto,
                  alerta_convergencia=excluded.alerta_convergencia,
                  alerta_padrao_operacional=excluded.alerta_padrao_operacional,
                  alerta_classificacao=excluded.alerta_classificacao,
                  alerta_c99=excluded.alerta_c99,
                  alerta_w01=excluded.alerta_w01,
                  alerta_w02=excluded.alerta_w02,
                  alerta_w03=excluded.alerta_w03,
                  alerta_w04=excluded.alerta_w04,
                  possivel_copia_historico=excluded.possivel_copia_historico,
                  repeticao_por_guarnicao=excluded.repeticao_por_guarnicao,
                  tempo_empenho_suspeito=excluded.tempo_empenho_suspeito,
                  possivel_erro_natureza_violenta=excluded.possivel_erro_natureza_violenta,
                  natureza_sugerida=excluded.natureza_sugerida,
                  recomendacoes_json=excluded.recomendacoes_json,
                  data_analise=excluded.data_analise,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )
    return {"received": len(rows), "upserted": len(values)}


def get_qr_dashboard(
    date_from: str | None = None, date_to: str | None = None, cia_pm: str | None = None
) -> dict[str, Any]:
    ensure_quality_tables()
    where = ["1=1"]
    params: dict[str, Any] = {}
    if date_from:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) <= :date_to")
        params["date_to"] = date_to
    if cia_pm:
        where.append("(cia_pm = :cia_pm OR cia_pm LIKE :cia_pm_like)")
        params["cia_pm"] = cia_pm
        params["cia_pm_like"] = f"%{cia_pm}%"
    where_sql = " AND ".join(where)
    with get_engine().begin() as conn:
        s = conn.execute(
            text(
                f"""
                SELECT
                  COUNT(*) AS total_analisados,
                  COALESCE(SUM(
                    CASE WHEN
                      alerta_texto_curto=1 OR erro_ortografico=1 OR ausencia_pontuacao=1 OR historico_incompleto=1 OR
                      alerta_convergencia=1 OR alerta_padrao_operacional=1 OR alerta_classificacao=1
                    THEN 1 ELSE 0 END
                  ),0) AS inconformidades,
                  COALESCE(SUM(CASE WHEN alerta_texto_curto=1 THEN 1 ELSE 0 END),0) AS historicos_curtos,
                  COALESCE(SUM(CASE WHEN alerta_classificacao=1 THEN 1 ELSE 0 END),0) AS erros_natureza,
                  COALESCE(AVG(score_textual),0) AS score_medio
                FROM qr_quality_analysis
                WHERE {where_sql}
                """
            ),
            params,
        ).first()
        rk = conn.execute(
            text(
                f"""
                SELECT
                  COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA') AS cia_pm,
                  COUNT(*) AS total_analisados,
                  COALESCE(SUM(
                    CASE WHEN
                      alerta_texto_curto=1 OR erro_ortografico=1 OR ausencia_pontuacao=1 OR historico_incompleto=1 OR
                      alerta_convergencia=1 OR alerta_padrao_operacional=1 OR alerta_classificacao=1
                    THEN 1 ELSE 0 END
                  ),0) AS inconformidades
                FROM qr_quality_analysis
                WHERE {where_sql}
                GROUP BY COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA')
                ORDER BY inconformidades DESC, total_analisados DESC, cia_pm ASC
                """
            ),
            params,
        )
        ranking = [dict(r._mapping) for r in rk]
    m = dict(s._mapping) if s else {}
    return {
        "total_analisados": int(m.get("total_analisados") or 0),
        "inconformidades": int(m.get("inconformidades") or 0),
        "historicos_curtos": int(m.get("historicos_curtos") or 0),
        "erros_natureza": int(m.get("erros_natureza") or 0),
        "score_medio": round(float(m.get("score_medio") or 0), 2),
        "ranking_cias": [
            {
                "cia_pm": str(r.get("cia_pm") or "SEM_CIA"),
                "total_analisados": int(r.get("total_analisados") or 0),
                "inconformidades": int(r.get("inconformidades") or 0),
            }
            for r in ranking
        ],
    }


def get_qr_company_summary(cia_pm: str, date_from: str | None = None, date_to: str | None = None) -> dict[str, Any]:
    ensure_quality_tables()
    cia = str(cia_pm or "").strip() or "SEM_CIA"
    where = ["(COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA') = :cia_pm OR COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA') LIKE :cia_pm_like)"]
    params: dict[str, Any] = {"cia_pm": cia, "cia_pm_like": f"%{cia}%"}
    if date_from:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) <= :date_to")
        params["date_to"] = date_to
    where_sql = " AND ".join(where)

    with get_engine().begin() as conn:
        s = conn.execute(
            text(
                f"""
                SELECT
                  COUNT(*) AS total_analisados,
                  COALESCE(SUM(CASE WHEN alerta_texto_curto=1 THEN 1 ELSE 0 END),0) AS historicos_curtos,
                  COALESCE(SUM(CASE WHEN alerta_classificacao=1 THEN 1 ELSE 0 END),0) AS erros_natureza,
                  COALESCE(AVG(score_textual),0) AS score_medio
                FROM qr_quality_analysis
                WHERE {where_sql}
                """
            ),
            params,
        ).first()
        pel_rows = conn.execute(
            text(
                f"""
                SELECT
                  COALESCE(NULLIF(TRIM(pelotao_pm),''), 'SEM_PELOTAO') AS pelotao_pm,
                  COUNT(*) AS total_analisados,
                  COALESCE(AVG(score_textual),0) AS score_medio
                FROM qr_quality_analysis
                WHERE {where_sql}
                GROUP BY COALESCE(NULLIF(TRIM(pelotao_pm),''), 'SEM_PELOTAO')
                ORDER BY total_analisados DESC, pelotao_pm ASC
                """
            ),
            params,
        )
        pel = [dict(r._mapping) for r in pel_rows]
    m = dict(s._mapping) if s else {}
    return {
        "cia_pm": cia,
        "total_analisados": int(m.get("total_analisados") or 0),
        "historicos_curtos": int(m.get("historicos_curtos") or 0),
        "erros_natureza": int(m.get("erros_natureza") or 0),
        "score_medio": round(float(m.get("score_medio") or 0), 2),
        "por_pelotao": [
            {
                "pelotao_pm": str(p.get("pelotao_pm") or "SEM_PELOTAO"),
                "total_analisados": int(p.get("total_analisados") or 0),
                "score_medio": round(float(p.get("score_medio") or 0), 2),
            }
            for p in pel
        ],
    }


def get_qr_company_report(
    cia_pm: str, date_from: str | None = None, date_to: str | None = None, limit: int = 2000
) -> list[dict[str, Any]]:
    ensure_quality_tables()
    cia = str(cia_pm or "").strip() or "SEM_CIA"
    where = ["(COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA') = :cia_pm OR COALESCE(NULLIF(TRIM(cia_pm),''), 'SEM_CIA') LIKE :cia_pm_like)"]
    params: dict[str, Any] = {"cia_pm": cia, "cia_pm_like": f"%{cia}%", "limit": int(limit)}
    if date_from:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("COALESCE(data_fato, SUBSTR(data_hora_fato,1,10)) <= :date_to")
        params["date_to"] = date_to
    where_sql = " AND ".join(where)
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_fato,
                  natureza_codigo, natureza_descricao_longa,
                  score_textual, alerta_texto_curto, erro_ortografico, ausencia_pontuacao,
                  historico_incompleto, alerta_convergencia, alerta_padrao_operacional, alerta_classificacao,
                  alerta_c99, alerta_w01, alerta_w02, alerta_w03, alerta_w04,
                  possivel_copia_historico, repeticao_por_guarnicao, tempo_empenho_suspeito,
                  possivel_erro_natureza_violenta, natureza_sugerida, recomendacoes_json, data_analise
                FROM qr_quality_analysis
                WHERE {where_sql}
                ORDER BY data_analise DESC, numero_ocorrencia ASC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(r._mapping) for r in rows]


_CV_CODES = {"B01121", "C01157", "C01158", "C01159", "D01213", "D01217", "B01148"}
_ARMA_CODES = {"E03012", "E03014", "E03016"}
_GRUPO_W_CODES = {"W01000", "W02000", "W03000", "W04000"}
_C99_CODES = {"C99000"}
_ALL_CLASSIFIED_CODES = _CV_CODES | _ARMA_CODES | _GRUPO_W_CODES | _C99_CODES


def _norm_natureza(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _natureza_tipo(value: Any) -> str:
    nat = _norm_natureza(value)
    if nat in _CV_CODES:
        return "CV"
    if nat in _ARMA_CODES:
        return "ARMA"
    if nat in _GRUPO_W_CODES:
        return "GRUPO_W"
    if nat in _C99_CODES:
        return "C99000"
    return "OUTROS"


def _norm_natureza_grupo(value: Any) -> str:
    v = str(value or "").strip().upper()
    aliases = {
        "W": "GRUPO_W",
        "GRUPOW": "GRUPO_W",
        "W01-W04": "GRUPO_W",
        "C99": "C99000",
    }
    return aliases.get(v, v)


def _format_date_br(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return f"{raw[8:10]}/{raw[5:7]}/{raw[0:4]}"
    return raw


def _periodo_label_pt(date_from: str | None, date_to: str | None, periodo_label: str | None = None) -> str:
    custom = str(periodo_label or "").strip()
    if custom:
        return custom.upper()
    month_names = {
        "01": "JANEIRO",
        "02": "FEVEREIRO",
        "03": "MARCO",
        "04": "ABRIL",
        "05": "MAIO",
        "06": "JUNHO",
        "07": "JULHO",
        "08": "AGOSTO",
        "09": "SETEMBRO",
        "10": "OUTUBRO",
        "11": "NOVEMBRO",
        "12": "DEZEMBRO",
    }
    if date_from and len(str(date_from)) >= 7:
        m = str(date_from)[5:7]
        return month_names.get(m, str(date_from))
    if date_to and len(str(date_to)) >= 7:
        m = str(date_to)[5:7]
        return month_names.get(m, str(date_to))
    return "PERIODO NAO INFORMADO"


def _extract_codes_from_marker(text_value: Any, marker: str) -> list[str]:
    txt = str(text_value or "")
    m = re.search(rf"\[{re.escape(marker)}\]\s*([^\r\n]+)", txt, flags=re.IGNORECASE)
    if not m:
        return []
    raw = m.group(1)
    out: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n;]+", raw):
        code = str(part or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _strip_markers_from_obs(text_value: Any) -> str:
    txt = str(text_value or "")
    txt = re.sub(r"\n?\[ERROS_COD\][^\r\n]*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\n?\[MEDIDAS_COD\][^\r\n]*", "", txt, flags=re.IGNORECASE)
    return txt.strip()


def _load_medidas_catalog() -> dict[str, str]:
    base = Path(__file__).resolve().parents[2] / "public" / "data" / "medidas_adotadas_qualidad.json"
    if not base.exists():
        return {
            "01": "Recomendacao para emissao de novo recibo com destinacao correta",
            "02": "Orientacao ao relator quanto ao correto preenchimento",
            "03": "Indicacao para treinamento especifico sobre REDS",
            "04": "Advertencia do comando por reincidencia",
            "05": "Comunicacao disciplinar",
            "99": "Outras medidas",
        }
    try:
        payload = json.loads(base.read_text(encoding="utf-8"))
    except Exception:
        try:
            payload = json.loads(base.read_text(encoding="latin-1"))
        except Exception:
            return {}
    mapping: dict[str, str] = {}
    for row in payload.get("medidas_adotadas") or []:
        code = str(row.get("codigo") or "").strip()
        desc = str(row.get("descricao") or "").strip()
        if code:
            mapping[code] = desc or code
    return mapping


def _aggregate_criteria(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    medidas_dict = _load_medidas_catalog()
    defs = [
        (
            "correcoes_ortograficas",
            "Correcao ortografica, gramatical, morfologica e semantica",
            lambda r: bool(r.get("erro_ortografico")),
            ["02"],
        ),
        (
            "qualidade_texto",
            "Qualidade do texto produzido sobre os fatos",
            lambda r: bool(r.get("alerta_texto_curto")) or bool(r.get("historico_incompleto")) or bool(r.get("ausencia_pontuacao")),
            ["02", "03"],
        ),
        (
            "qualificacao_envolvidos",
            "Qualificacao dos envolvidos (testemunhas, vitimas e agentes)",
            lambda r: "14.01" in _extract_codes_from_marker(r.get("observacao"), "ERROS_COD"),
            ["02"],
        ),
        (
            "tempo_empenho",
            "Tempo de empenho da guarnicao policial",
            lambda r: bool(r.get("tempo_empenho_suspeito")) or "15.01" in _extract_codes_from_marker(r.get("observacao"), "ERROS_COD"),
            ["02"],
        ),
        (
            "convergencia_dados",
            "Convergencia entre dados reais do fato e os lancados no REDS",
            lambda r: bool(r.get("alerta_convergencia")) or bool(r.get("alerta_classificacao")),
            ["01", "02"],
        ),
    ]
    out: list[dict[str, Any]] = []
    for key, label, fn, default_measures in defs:
        selected = [r for r in rows if fn(r)]
        qtd = len(selected)
        obs_lines: list[str] = []
        measures_count: dict[str, int] = {}
        for row in selected[:10]:
            nr = str(row.get("numero_ocorrencia") or "").strip()
            obs = _strip_markers_from_obs(row.get("observacao"))
            if obs:
                obs_lines.append(f"{nr}: {obs}")
            else:
                nat = str(row.get("natureza_codigo") or "").strip()
                obs_lines.append(f"{nr}: Inconformidade detectada em {nat or 'natureza nao informada'}.")
            codes = _extract_codes_from_marker(row.get("observacao"), "MEDIDAS_COD")
            if not codes:
                codes = list(default_measures)
            for c in codes:
                measures_count[c] = measures_count.get(c, 0) + 1
        measures_text = []
        for code, cnt in sorted(measures_count.items(), key=lambda kv: (-kv[1], kv[0])):
            desc = medidas_dict.get(code, code)
            measures_text.append(f"{code} - {desc} ({cnt})")
        out.append(
            {
                "key": key,
                "criterio": label,
                "quantidade_inconformidades": qtd,
                "observacao_inconformidades": "\n".join(obs_lines) if obs_lines else "Sem inconformidades registradas.",
                "medidas_adotadas": "; ".join(measures_text) if measures_text else "Sem medidas registradas.",
            }
        )
    return out


def _cia_label(cia_pm: str) -> str:
    c = str(cia_pm or "").strip()
    if c == "42":
        return "42ª Cia PM/19º BPM"
    if c == "47":
        return "47ª Cia PM/19º BPM"
    if c == "155":
        return "155ª Cia PM/19º BPM"
    if c == "232":
        return "232ª Cia PM/19º BPM"
    if c == "74":
        return "74ª Cia TM/19º BPM"
    return f"{c} Cia PM/19º BPM"


def upsert_qr_evaluation_rows(rows: list[dict[str, Any]], avaliado_por: str = "") -> dict[str, int]:
    if not rows:
        return {"received": 0, "upserted": 0}
    ensure_quality_tables()
    now = _utc_now_iso()
    values: list[dict[str, Any]] = []
    for row in rows:
        num = str(row.get("numero_ocorrencia") or "").strip()
        if not num:
            continue
        avaliado = _to_bool_int(row.get("avaliado"))
        ajustes = row.get("ajustes") or []
        if not isinstance(ajustes, list):
            ajustes = []
        ajustes = [str(x).strip() for x in ajustes if str(x).strip()]
        values.append(
            {
                "numero_ocorrencia": num,
                "avaliado": avaliado,
                "ajustes_json": json.dumps(ajustes, ensure_ascii=False),
                "observacao": str(row.get("observacao") or "").strip(),
                "avaliado_por": str(avaliado_por or "").strip(),
                "avaliado_em": now if avaliado else None,
                "created_at": now,
                "updated_at": now,
            }
        )
    if not values:
        return {"received": len(rows), "upserted": 0}
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO qr_evaluation(
                  numero_ocorrencia, avaliado, ajustes_json, observacao, avaliado_por, avaliado_em, created_at, updated_at
                )
                VALUES(
                  :numero_ocorrencia, :avaliado, :ajustes_json, :observacao, :avaliado_por, :avaliado_em, :created_at, :updated_at
                )
                ON CONFLICT(numero_ocorrencia)
                DO UPDATE SET
                  avaliado=excluded.avaliado,
                  ajustes_json=excluded.ajustes_json,
                  observacao=excluded.observacao,
                  avaliado_por=excluded.avaliado_por,
                  avaliado_em=excluded.avaliado_em,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )
    return {"received": len(rows), "upserted": len(values)}


def list_qr_review_rows(
    cia_pm: str,
    date_from: str | None = None,
    date_to: str | None = None,
    natureza_grupo: str | None = None,
    nome_tipo_relatorio: str | None = None,
    limit: int = 2000,
    only_pending: bool = False,
) -> list[dict[str, Any]]:
    ensure_quality_tables()
    cia = str(cia_pm or "").strip()
    where = ["(COALESCE(NULLIF(TRIM(r.cia_pm),''), 'SEM_CIA') = :cia_pm OR COALESCE(NULLIF(TRIM(r.cia_pm),''), 'SEM_CIA') LIKE :cia_pm_like)"]
    params: dict[str, Any] = {"cia_pm": cia, "cia_pm_like": f"%{cia}%", "limit": int(limit)}
    if date_from:
        where.append("COALESCE(r.data_fato, SUBSTR(r.data_hora_fato,1,10)) >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("COALESCE(r.data_fato, SUBSTR(r.data_hora_fato,1,10)) <= :date_to")
        params["date_to"] = date_to
    nat_norm_expr = "UPPER(REPLACE(REPLACE(REPLACE(COALESCE(r.natureza_codigo,''),'.',''),'-',''),' ',''))"
    nat_group = _norm_natureza_grupo(natureza_grupo)
    if nat_group == "CV":
        where.append(f"{nat_norm_expr} IN ('B01121','C01157','C01158','C01159','D01213','D01217','B01148')")
    elif nat_group == "ARMA":
        where.append(f"{nat_norm_expr} IN ('E03012','E03014','E03016')")
    elif nat_group == "GRUPO_W":
        where.append(f"{nat_norm_expr} IN ('W01000','W02000','W03000','W04000')")
    elif nat_group == "C99000":
        where.append(f"{nat_norm_expr} IN ('C99000')")
    elif nat_group == "OUTROS":
        where.append(
            f"{nat_norm_expr} NOT IN ("
            f"'B01121','C01157','C01158','C01159','D01213','D01217','B01148',"
            f"'E03012','E03014','E03016',"
            f"'W01000','W02000','W03000','W04000',"
            f"'C99000'"
            f")"
        )
    tipo_rel = str(nome_tipo_relatorio or "").strip()
    if tipo_rel:
        where.append("UPPER(COALESCE(r.nome_tipo_relatorio,'')) LIKE :tipo_relatorio_like")
        params["tipo_relatorio_like"] = f"%{tipo_rel.upper()}%"
    if only_pending:
        where.append("COALESCE(e.avaliado, 0) = 0")
    where_sql = " AND ".join(where)
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  r.numero_ocorrencia,
                  r.cia_pm,
                  r.pelotao_pm,
                  r.data_hora_fato,
                  r.data_hora_inclusao,
                  r.data_hora_final,
                  r.data_fato,
                  r.descricao_estado,
                  r.nome_tipo_relatorio,
                  r.natureza_codigo,
                  r.natureza_descricao_longa,
                  r.natureza_secundaria1_codigo,
                  r.natureza_secundaria1_descricao,
                  r.natureza_secundaria2_codigo,
                  r.natureza_secundaria2_descricao,
                  r.natureza_secundaria3_codigo,
                  r.natureza_secundaria3_descricao,
                  r.historico_ocorrencia,
                  r.alteracoes_count,
                  r.versao_atual,
                  r.ultima_alteracao_detectada_em,
                  qa.score_textual,
                  qa.alerta_texto_curto,
                  qa.erro_ortografico,
                  qa.ausencia_pontuacao,
                  qa.historico_incompleto,
                  qa.alerta_convergencia,
                  qa.alerta_padrao_operacional,
                  qa.alerta_classificacao,
                  qa.alerta_c99,
                  qa.alerta_w01,
                  qa.alerta_w02,
                  qa.alerta_w03,
                  qa.alerta_w04,
                  qa.possivel_copia_historico,
                  qa.repeticao_por_guarnicao,
                  qa.tempo_empenho_suspeito,
                  qa.possivel_erro_natureza_violenta,
                  qa.natureza_sugerida,
                  qa.recomendacoes_json,
                  qa.data_analise,
                  COALESCE(e.avaliado, 0) AS avaliado,
                  e.ajustes_json,
                  e.observacao,
                  e.avaliado_por,
                  e.avaliado_em,
                  e.updated_at AS avaliacao_updated_at
                FROM qr_raw_reds r
                LEFT JOIN qr_quality_analysis qa
                  ON qa.numero_ocorrencia = r.numero_ocorrencia
                LEFT JOIN qr_evaluation e
                  ON e.numero_ocorrencia = r.numero_ocorrencia
                WHERE {where_sql}
                ORDER BY COALESCE(r.data_hora_fato, r.data_fato) DESC, r.numero_ocorrencia ASC
                LIMIT :limit
                """
            ),
            params,
        )
        out = []
        for r in rows:
            m = dict(r._mapping)
            ajustes: list[str] = []
            raw_ajustes = m.get("ajustes_json")
            if raw_ajustes:
                try:
                    parsed = json.loads(str(raw_ajustes))
                    if isinstance(parsed, list):
                        ajustes = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    ajustes = []
            m["ajustes"] = ajustes
            m["avaliado"] = bool(int(m.get("avaliado") or 0))
            m["tipo_natureza"] = _natureza_tipo(m.get("natureza_codigo"))
            for f in (
                "alerta_texto_curto",
                "erro_ortografico",
                "ausencia_pontuacao",
                "historico_incompleto",
                "alerta_convergencia",
                "alerta_padrao_operacional",
                "alerta_classificacao",
                "alerta_c99",
                "alerta_w01",
                "alerta_w02",
                "alerta_w03",
                "alerta_w04",
                "possivel_copia_historico",
                "repeticao_por_guarnicao",
                "tempo_empenho_suspeito",
                "possivel_erro_natureza_violenta",
            ):
                m[f] = bool(int(m.get(f) or 0)) if m.get(f) is not None else False
            recs: list[str] = []
            raw_recs = m.get("recomendacoes_json")
            if raw_recs:
                try:
                    parsed = json.loads(str(raw_recs))
                    if isinstance(parsed, list):
                        recs = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    recs = []
            m["recomendacoes"] = recs
            out.append(m)
        return out


def get_qr_monthly_kpis(
    cia_pm: str,
    date_from: str | None = None,
    date_to: str | None = None,
    natureza_grupo: str | None = None,
    nome_tipo_relatorio: str | None = None,
) -> dict[str, Any]:
    rows = list_qr_review_rows(
        cia_pm=cia_pm,
        date_from=date_from,
        date_to=date_to,
        natureza_grupo=natureza_grupo,
        nome_tipo_relatorio=nome_tipo_relatorio,
        limit=50000,
        only_pending=False,
    )
    total = len(rows)
    avaliados = sum(1 for r in rows if r.get("avaliado"))
    pct = round((avaliados * 100.0 / total), 2) if total else 0.0

    def is_tipo(r: dict[str, Any], tipo: str) -> bool:
        return _natureza_tipo(r.get("natureza_codigo")) == tipo

    cv_total = sum(1 for r in rows if is_tipo(r, "CV"))
    cv_av = sum(1 for r in rows if is_tipo(r, "CV") and r.get("avaliado"))
    arma_total = sum(1 for r in rows if is_tipo(r, "ARMA"))
    arma_av = sum(1 for r in rows if is_tipo(r, "ARMA") and r.get("avaliado"))
    w_total = sum(1 for r in rows if is_tipo(r, "GRUPO_W"))
    w_av = sum(1 for r in rows if is_tipo(r, "GRUPO_W") and r.get("avaliado"))
    c99_total = sum(1 for r in rows if is_tipo(r, "C99000"))
    c99_av = sum(1 for r in rows if is_tipo(r, "C99000") and r.get("avaliado"))

    sinalizados_total = 0
    sinalizados_av = 0
    for r in rows:
        has_alert = any(
            [
                bool(r.get("alerta_texto_curto")),
                bool(r.get("erro_ortografico")),
                bool(r.get("ausencia_pontuacao")),
                bool(r.get("historico_incompleto")),
                bool(r.get("alerta_convergencia")),
                bool(r.get("alerta_padrao_operacional")),
                bool(r.get("alerta_classificacao")),
            ]
        )
        if has_alert:
            sinalizados_total += 1
            if r.get("avaliado"):
                sinalizados_av += 1

    return {
        "cia_pm": str(cia_pm or "").strip(),
        "total_registros": total,
        "total_avaliados": avaliados,
        "percentual_avaliado": pct,
        "cv_total": cv_total,
        "cv_avaliados": cv_av,
        "arma_total": arma_total,
        "arma_avaliados": arma_av,
        "w_total": w_total,
        "w_avaliados": w_av,
        "c99_total": c99_total,
        "c99_avaliados": c99_av,
        "sinalizados_total": sinalizados_total,
        "sinalizados_avaliados": sinalizados_av,
    }


def build_qr_monthly_report(
    cia_pm: str,
    date_from: str | None = None,
    date_to: str | None = None,
    natureza_grupo: str | None = None,
    nome_tipo_relatorio: str | None = None,
    data_reuniao: str | None = None,
    comissao: str | None = None,
    periodo_label: str | None = None,
) -> dict[str, Any]:
    k = get_qr_monthly_kpis(
        cia_pm=cia_pm,
        date_from=date_from,
        date_to=date_to,
        natureza_grupo=natureza_grupo,
        nome_tipo_relatorio=nome_tipo_relatorio,
    )
    rows = list_qr_review_rows(
        cia_pm=cia_pm,
        date_from=date_from,
        date_to=date_to,
        natureza_grupo=natureza_grupo,
        nome_tipo_relatorio=nome_tipo_relatorio,
        limit=50000,
        only_pending=False,
    )
    avaliados_rows = [r for r in rows if bool(r.get("avaliado"))]
    cv_base_rows = [r for r in rows if str(r.get("tipo_natureza") or "") == "CV"]
    nao_cv_base_rows = [r for r in rows if str(r.get("tipo_natureza") or "") != "CV"]
    cv_rows = [r for r in avaliados_rows if str(r.get("tipo_natureza") or "") == "CV"]
    nao_cv_rows = [r for r in avaliados_rows if str(r.get("tipo_natureza") or "") != "CV"]

    total_base = len(rows)
    alvo_2pct = int(ceil(total_base * 0.02)) if total_base else 0
    amostra_ref = min(alvo_2pct, len(nao_cv_base_rows)) if nao_cv_base_rows else 0
    pct_amostra = round((amostra_ref * 100.0 / total_base), 2) if total_base else 0.0
    arma_qtd = sum(1 for r in nao_cv_base_rows if str(r.get("tipo_natureza") or "") == "ARMA")
    w_qtd = sum(1 for r in nao_cv_base_rows if str(r.get("tipo_natureza") or "") == "GRUPO_W")
    c99_qtd = sum(1 for r in nao_cv_base_rows if str(r.get("tipo_natureza") or "") == "C99000")

    cia_label = _cia_label(str(cia_pm or ""))
    periodo = f"{date_from or 'inicio'} a {date_to or 'fim'}"
    periodo_nome = _periodo_label_pt(date_from, date_to, periodo_label=periodo_label)
    t1 = (
        f"A Comissão de avaliação de REDS da {cia_label} procedeu a análise de "
        f"{len(cv_base_rows)} registro(s) de crimes violentos ocorridos na área da Cia "
        f"no período avaliado ({periodo}). A seguir serão descritas as inconformidades detectadas."
    )
    t2 = (
        f"A Comissão de avaliação de REDS da {cia_label} procedeu a análise de {amostra_ref} "
        f"ocorrências referente a {pct_amostra}% de todos os registros confeccionados na área da Cia no período avaliado; "
        f"de {arma_qtd} REDS de natureza E03012, E03014 e E03016; "
        f"de {w_qtd} BOS de natureza W01.000, W02.000, W03.000, W04.000 e; "
        f"de {c99_qtd} REDS de natureza C99.000. A seguir serão descritas inconformidades detectadas, "
        f"separando-se por naturezas."
    )
    preview = {
        "meta": {
            "cia_pm": str(cia_pm or "").strip(),
            "cia_label": cia_label,
            "date_from": date_from,
            "date_to": date_to,
            "periodo_intervalo": periodo,
            "periodo_label": periodo_nome,
            "data_reuniao": _format_date_br(data_reuniao or date_to or date_from),
            "comissao": str(comissao or "").strip() or "Nao informada",
            "total_registros_periodo": total_base,
            "total_alvo_2pct": alvo_2pct,
            "total_avaliados_periodo": len(avaliados_rows),
        },
        "blocos": [
            {
                "id": "cv",
                "titulo": "Bloco 1 - Crimes Violentos",
                "naturezas": "Crimes Violentos",
                "total_registros": len(cv_base_rows),
                "total_avaliados": len(cv_rows),
                "texto_abertura": t1,
                "criterios": _aggregate_criteria(cv_rows),
            },
            {
                "id": "nao_violentos",
                "titulo": "Bloco 2 - Crimes Nao Violentos",
                "naturezas": "Crimes Nao Violentos",
                "total_registros": len(nao_cv_base_rows),
                "total_avaliados": len(nao_cv_rows),
                "percentual_amostra": pct_amostra,
                "alvo_2pct": alvo_2pct,
                "amostra_referencia": amostra_ref,
                "qtd_arma": arma_qtd,
                "qtd_grupo_w": w_qtd,
                "qtd_c99": c99_qtd,
                "texto_abertura": t2,
                "criterios": _aggregate_criteria(nao_cv_rows),
            },
        ],
    }
    return {"kpis": k, "texto_modelo_1": t1, "texto_modelo_2": t2, "preview": preview}
