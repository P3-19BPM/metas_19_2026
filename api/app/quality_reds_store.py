from datetime import datetime, timezone
from math import ceil
import hashlib
import re
import textwrap
from pathlib import Path
from typing import Any
import json
from io import BytesIO
from xml.sax.saxutils import escape as _xml_escape

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
    "ind_estado",
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
    "flag_envolvido_sem_cpf",
    "flag_envolvido_sem_rg",
    "flag_envolvido_sem_email",
    "flag_envolvido_sem_telefone",
    "total_envolvidos",
    "qtd_envolvidos_sem_cpf",
    "qtd_envolvidos_sem_rg",
    "qtd_envolvidos_sem_email",
    "qtd_envolvidos_sem_telefone",
    "flag_todos_envolvidos_sem_cpf",
    "flag_todos_envolvidos_sem_rg",
    "flag_todos_envolvidos_sem_email",
    "flag_todos_envolvidos_sem_telefone",
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
      ind_estado TEXT,
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
      flag_envolvido_sem_cpf INTEGER NOT NULL DEFAULT 0,
      flag_envolvido_sem_rg INTEGER NOT NULL DEFAULT 0,
      flag_envolvido_sem_email INTEGER NOT NULL DEFAULT 0,
      flag_envolvido_sem_telefone INTEGER NOT NULL DEFAULT 0,
      total_envolvidos INTEGER NOT NULL DEFAULT 0,
      qtd_envolvidos_sem_cpf INTEGER NOT NULL DEFAULT 0,
      qtd_envolvidos_sem_rg INTEGER NOT NULL DEFAULT 0,
      qtd_envolvidos_sem_email INTEGER NOT NULL DEFAULT 0,
      qtd_envolvidos_sem_telefone INTEGER NOT NULL DEFAULT 0,
      flag_todos_envolvidos_sem_cpf INTEGER NOT NULL DEFAULT 0,
      flag_todos_envolvidos_sem_rg INTEGER NOT NULL DEFAULT 0,
      flag_todos_envolvidos_sem_email INTEGER NOT NULL DEFAULT 0,
      flag_todos_envolvidos_sem_telefone INTEGER NOT NULL DEFAULT 0,
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
      alerta_envolvido_sem_cpf INTEGER NOT NULL DEFAULT 0,
      alerta_envolvido_sem_rg INTEGER NOT NULL DEFAULT 0,
      alerta_envolvido_sem_email INTEGER NOT NULL DEFAULT 0,
      alerta_envolvido_sem_telefone INTEGER NOT NULL DEFAULT 0,
      alerta_campos_envolvido_incompletos INTEGER NOT NULL DEFAULT 0,
      alerta_procedimento_relatorio_policial INTEGER NOT NULL DEFAULT 0,
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
            ("ind_estado", "ALTER TABLE qr_raw_reds ADD COLUMN ind_estado TEXT"),
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
            ("flag_envolvido_sem_cpf", "ALTER TABLE qr_raw_reds ADD COLUMN flag_envolvido_sem_cpf INTEGER NOT NULL DEFAULT 0"),
            ("flag_envolvido_sem_rg", "ALTER TABLE qr_raw_reds ADD COLUMN flag_envolvido_sem_rg INTEGER NOT NULL DEFAULT 0"),
            ("flag_envolvido_sem_email", "ALTER TABLE qr_raw_reds ADD COLUMN flag_envolvido_sem_email INTEGER NOT NULL DEFAULT 0"),
            ("flag_envolvido_sem_telefone", "ALTER TABLE qr_raw_reds ADD COLUMN flag_envolvido_sem_telefone INTEGER NOT NULL DEFAULT 0"),
            ("total_envolvidos", "ALTER TABLE qr_raw_reds ADD COLUMN total_envolvidos INTEGER NOT NULL DEFAULT 0"),
            ("qtd_envolvidos_sem_cpf", "ALTER TABLE qr_raw_reds ADD COLUMN qtd_envolvidos_sem_cpf INTEGER NOT NULL DEFAULT 0"),
            ("qtd_envolvidos_sem_rg", "ALTER TABLE qr_raw_reds ADD COLUMN qtd_envolvidos_sem_rg INTEGER NOT NULL DEFAULT 0"),
            ("qtd_envolvidos_sem_email", "ALTER TABLE qr_raw_reds ADD COLUMN qtd_envolvidos_sem_email INTEGER NOT NULL DEFAULT 0"),
            ("qtd_envolvidos_sem_telefone", "ALTER TABLE qr_raw_reds ADD COLUMN qtd_envolvidos_sem_telefone INTEGER NOT NULL DEFAULT 0"),
            ("flag_todos_envolvidos_sem_cpf", "ALTER TABLE qr_raw_reds ADD COLUMN flag_todos_envolvidos_sem_cpf INTEGER NOT NULL DEFAULT 0"),
            ("flag_todos_envolvidos_sem_rg", "ALTER TABLE qr_raw_reds ADD COLUMN flag_todos_envolvidos_sem_rg INTEGER NOT NULL DEFAULT 0"),
            ("flag_todos_envolvidos_sem_email", "ALTER TABLE qr_raw_reds ADD COLUMN flag_todos_envolvidos_sem_email INTEGER NOT NULL DEFAULT 0"),
            ("flag_todos_envolvidos_sem_telefone", "ALTER TABLE qr_raw_reds ADD COLUMN flag_todos_envolvidos_sem_telefone INTEGER NOT NULL DEFAULT 0"),
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
        qa_cols = {str(c.get("name") or "").strip().lower() for c in inspect(conn).get_columns("qr_quality_analysis")}
        for col_name, ddl in (
            ("alerta_envolvido_sem_cpf", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_envolvido_sem_cpf INTEGER NOT NULL DEFAULT 0"),
            ("alerta_envolvido_sem_rg", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_envolvido_sem_rg INTEGER NOT NULL DEFAULT 0"),
            ("alerta_envolvido_sem_email", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_envolvido_sem_email INTEGER NOT NULL DEFAULT 0"),
            ("alerta_envolvido_sem_telefone", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_envolvido_sem_telefone INTEGER NOT NULL DEFAULT 0"),
            ("alerta_campos_envolvido_incompletos", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_campos_envolvido_incompletos INTEGER NOT NULL DEFAULT 0"),
            ("alerta_procedimento_relatorio_policial", "ALTER TABLE qr_quality_analysis ADD COLUMN alerta_procedimento_relatorio_policial INTEGER NOT NULL DEFAULT 0"),
        ):
            if col_name in qa_cols:
                continue
            conn.execute(text(ddl))
            qa_cols.add(col_name)
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
            "ind_estado": row.get("ind_estado"),
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
            "flag_envolvido_sem_cpf": _to_bool_int(row.get("flag_envolvido_sem_cpf")),
            "flag_envolvido_sem_rg": _to_bool_int(row.get("flag_envolvido_sem_rg")),
            "flag_envolvido_sem_email": _to_bool_int(row.get("flag_envolvido_sem_email")),
            "flag_envolvido_sem_telefone": _to_bool_int(row.get("flag_envolvido_sem_telefone")),
            "total_envolvidos": int(row.get("total_envolvidos") or 0),
            "qtd_envolvidos_sem_cpf": int(row.get("qtd_envolvidos_sem_cpf") or 0),
            "qtd_envolvidos_sem_rg": int(row.get("qtd_envolvidos_sem_rg") or 0),
            "qtd_envolvidos_sem_email": int(row.get("qtd_envolvidos_sem_email") or 0),
            "qtd_envolvidos_sem_telefone": int(row.get("qtd_envolvidos_sem_telefone") or 0),
            "flag_todos_envolvidos_sem_cpf": _to_bool_int(row.get("flag_todos_envolvidos_sem_cpf")),
            "flag_todos_envolvidos_sem_rg": _to_bool_int(row.get("flag_todos_envolvidos_sem_rg")),
            "flag_todos_envolvidos_sem_email": _to_bool_int(row.get("flag_todos_envolvidos_sem_email")),
            "flag_todos_envolvidos_sem_telefone": _to_bool_int(row.get("flag_todos_envolvidos_sem_telefone")),
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
                  data_hora_fechamento, ind_estado, descricao_estado, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa,
                  natureza_secundaria1_codigo, natureza_secundaria1_descricao,
                  natureza_secundaria2_codigo, natureza_secundaria2_descricao,
                  natureza_secundaria3_codigo, natureza_secundaria3_descricao,
                  historico_ocorrencia,
                  flag_envolvido_sem_cpf, flag_envolvido_sem_rg, flag_envolvido_sem_email, flag_envolvido_sem_telefone,
                  total_envolvidos, qtd_envolvidos_sem_cpf, qtd_envolvidos_sem_rg, qtd_envolvidos_sem_email, qtd_envolvidos_sem_telefone,
                  flag_todos_envolvidos_sem_cpf, flag_todos_envolvidos_sem_rg, flag_todos_envolvidos_sem_email, flag_todos_envolvidos_sem_telefone,
                  change_signature, alteracoes_count, versao_atual, primeira_captura_em, ultima_alteracao_detectada_em,
                  created_at, updated_at
                )
                VALUES(
                  :numero_ocorrencia, :cia_pm, :pelotao_pm, :data_hora_fato, :data_hora_inclusao, :data_hora_final, :data_fato, :mes, :ano,
                  :data_hora_fechamento, :ind_estado, :descricao_estado, :data_hora_alteracao, :data_hora_primeiro_fim_preenchimento,
                  :nome_tipo_relatorio, :digitador_cargo_efetivo, :digitador_matricula, :digitador_nome, :digitador_nome_unidade,
                  :natureza_codigo, :natureza_descricao_longa,
                  :natureza_secundaria1_codigo, :natureza_secundaria1_descricao,
                  :natureza_secundaria2_codigo, :natureza_secundaria2_descricao,
                  :natureza_secundaria3_codigo, :natureza_secundaria3_descricao,
                  :historico_ocorrencia,
                  :flag_envolvido_sem_cpf, :flag_envolvido_sem_rg, :flag_envolvido_sem_email, :flag_envolvido_sem_telefone,
                  :total_envolvidos, :qtd_envolvidos_sem_cpf, :qtd_envolvidos_sem_rg, :qtd_envolvidos_sem_email, :qtd_envolvidos_sem_telefone,
                  :flag_todos_envolvidos_sem_cpf, :flag_todos_envolvidos_sem_rg, :flag_todos_envolvidos_sem_email, :flag_todos_envolvidos_sem_telefone,
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
                  ind_estado=excluded.ind_estado,
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
                  flag_envolvido_sem_cpf=excluded.flag_envolvido_sem_cpf,
                  flag_envolvido_sem_rg=excluded.flag_envolvido_sem_rg,
                  flag_envolvido_sem_email=excluded.flag_envolvido_sem_email,
                  flag_envolvido_sem_telefone=excluded.flag_envolvido_sem_telefone,
                  total_envolvidos=excluded.total_envolvidos,
                  qtd_envolvidos_sem_cpf=excluded.qtd_envolvidos_sem_cpf,
                  qtd_envolvidos_sem_rg=excluded.qtd_envolvidos_sem_rg,
                  qtd_envolvidos_sem_email=excluded.qtd_envolvidos_sem_email,
                  qtd_envolvidos_sem_telefone=excluded.qtd_envolvidos_sem_telefone,
                  flag_todos_envolvidos_sem_cpf=excluded.flag_todos_envolvidos_sem_cpf,
                  flag_todos_envolvidos_sem_rg=excluded.flag_todos_envolvidos_sem_rg,
                  flag_todos_envolvidos_sem_email=excluded.flag_todos_envolvidos_sem_email,
                  flag_todos_envolvidos_sem_telefone=excluded.flag_todos_envolvidos_sem_telefone,
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
                  data_hora_fechamento, ind_estado, descricao_estado, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa,
                  natureza_secundaria1_codigo, natureza_secundaria1_descricao,
                  natureza_secundaria2_codigo, natureza_secundaria2_descricao,
                  natureza_secundaria3_codigo, natureza_secundaria3_descricao,
                  historico_ocorrencia,
                  flag_envolvido_sem_cpf, flag_envolvido_sem_rg, flag_envolvido_sem_email, flag_envolvido_sem_telefone,
                  total_envolvidos, qtd_envolvidos_sem_cpf, qtd_envolvidos_sem_rg, qtd_envolvidos_sem_email, qtd_envolvidos_sem_telefone,
                  flag_todos_envolvidos_sem_cpf, flag_todos_envolvidos_sem_rg, flag_todos_envolvidos_sem_email, flag_todos_envolvidos_sem_telefone,
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
                "alerta_envolvido_sem_cpf": _to_bool_int(row.get("alerta_envolvido_sem_cpf")),
                "alerta_envolvido_sem_rg": _to_bool_int(row.get("alerta_envolvido_sem_rg")),
                "alerta_envolvido_sem_email": _to_bool_int(row.get("alerta_envolvido_sem_email")),
                "alerta_envolvido_sem_telefone": _to_bool_int(row.get("alerta_envolvido_sem_telefone")),
                "alerta_campos_envolvido_incompletos": _to_bool_int(row.get("alerta_campos_envolvido_incompletos")),
                "alerta_procedimento_relatorio_policial": _to_bool_int(row.get("alerta_procedimento_relatorio_policial")),
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
                  alerta_envolvido_sem_cpf, alerta_envolvido_sem_rg, alerta_envolvido_sem_email, alerta_envolvido_sem_telefone,
                  alerta_campos_envolvido_incompletos, alerta_procedimento_relatorio_policial,
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
                  :alerta_envolvido_sem_cpf, :alerta_envolvido_sem_rg, :alerta_envolvido_sem_email, :alerta_envolvido_sem_telefone,
                  :alerta_campos_envolvido_incompletos, :alerta_procedimento_relatorio_policial,
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
                  alerta_envolvido_sem_cpf=excluded.alerta_envolvido_sem_cpf,
                  alerta_envolvido_sem_rg=excluded.alerta_envolvido_sem_rg,
                  alerta_envolvido_sem_email=excluded.alerta_envolvido_sem_email,
                  alerta_envolvido_sem_telefone=excluded.alerta_envolvido_sem_telefone,
                  alerta_campos_envolvido_incompletos=excluded.alerta_campos_envolvido_incompletos,
                  alerta_procedimento_relatorio_policial=excluded.alerta_procedimento_relatorio_policial,
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
                      alerta_convergencia=1 OR alerta_padrao_operacional=1 OR alerta_classificacao=1 OR
                      alerta_campos_envolvido_incompletos=1 OR alerta_procedimento_relatorio_policial=1
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
                      alerta_convergencia=1 OR alerta_padrao_operacional=1 OR alerta_classificacao=1 OR
                      alerta_campos_envolvido_incompletos=1 OR alerta_procedimento_relatorio_policial=1
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
                  possivel_erro_natureza_violenta,
                  alerta_envolvido_sem_cpf, alerta_envolvido_sem_rg, alerta_envolvido_sem_email, alerta_envolvido_sem_telefone,
                  alerta_campos_envolvido_incompletos, alerta_procedimento_relatorio_policial,
                  natureza_sugerida, recomendacoes_json, data_analise
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


def _clean_comissao_member_name(value: Any) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    txt = re.sub(r"^(?:Presidente|Auxiliar|[1-4][º°o]?\s*Membro|Membro)\s*-\s*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"^(?:MAJ|CAP|[12]\s*TEN|SUB\s*TEN|[12]\s*SGT)\s*PM\s+", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s*\(NR\s*\d+\)\s*$", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s+", " ", txt).strip(" -")
    return txt


def _parse_comissao_member_names(raw_value: Any) -> list[str]:
    txt = str(raw_value or "")
    if not txt.strip():
        return []
    chunks = re.split(r"[\r\n;,]+", txt)
    out: list[str] = []
    seen: set[str] = set()
    for part in chunks:
        name = _clean_comissao_member_name(part)
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _normalize_comissao_funcao(value: Any) -> str:
    raw = str(value or "").strip()
    norm = (
        raw
        .lower()
        .replace("º", "o")
        .replace("°", "o")
        .replace("ª", "a")
        .replace("âº", "o")
        .replace("â°", "o")
    )
    if norm.startswith("pres"):
        return "Presidente"
    if norm.startswith("aux"):
        return "Auxiliar"
    if norm.startswith("1"):
        return "1º Membro"
    if norm.startswith("2"):
        return "2º Membro"
    if norm.startswith("3"):
        return "3º Membro"
    if norm.startswith("4"):
        return "4º Membro"
    if "membro" in norm:
        return "Membro"
    if raw in {"Presidente", "1º Membro", "2º Membro", "3º Membro", "4º Membro", "Membro", "Auxiliar"}:
        return raw
    return "Membro"


def _comissao_funcao_rank(value: Any) -> int:
    funcao = _normalize_comissao_funcao(value)
    order = {
        "Presidente": 0,
        "1º Membro": 1,
        "2º Membro": 2,
        "3º Membro": 3,
        "4º Membro": 4,
        "Membro": 5,
        "Auxiliar": 90,
    }
    return order.get(funcao, 99)


def _comissao_signature_rank(value: Any) -> int:
    funcao = _normalize_comissao_funcao(value)
    order = {
        "4º Membro": 1,
        "3º Membro": 2,
        "2º Membro": 3,
        "1º Membro": 4,
        "Membro": 5,
        "Presidente": 99,
    }
    return order.get(funcao, 90)


def _normalize_member_id(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _normalize_comissao_member_dict(row: dict[str, Any], idx: int = 0) -> dict[str, Any]:
    nome = str(row.get("nome_completo") or row.get("nome") or "").strip()
    member_id = _normalize_member_id(row.get("id") or row.get("numero_policia") or row.get("nr") or row.get("matricula"))
    posto = str(row.get("posto_graduacao") or row.get("posto") or "").strip().upper()
    funcao = _normalize_comissao_funcao(row.get("funcao"))
    ordem_raw = row.get("ordem")
    try:
        ordem = int(ordem_raw) if ordem_raw is not None and str(ordem_raw).strip() != "" else idx + 1
    except Exception:
        ordem = idx + 1
    if ordem < 1:
        ordem = idx + 1
    vig_ini = str(row.get("vigencia_inicio") or row.get("inicio_vigencia") or "").strip()
    vig_fim = str(row.get("vigencia_fim") or row.get("fim_vigencia") or "").strip()
    if len(vig_ini) >= 10 and vig_ini[2] == "/" and vig_ini[5] == "/":
        vig_ini = f"{vig_ini[6:10]}-{vig_ini[3:5]}-{vig_ini[0:2]}"
    if len(vig_fim) >= 10 and vig_fim[2] == "/" and vig_fim[5] == "/":
        vig_fim = f"{vig_fim[6:10]}-{vig_fim[3:5]}-{vig_fim[0:2]}"
    if not (len(vig_ini) >= 10 and vig_ini[4] == "-" and vig_ini[7] == "-"):
        vig_ini = ""
    else:
        vig_ini = vig_ini[:10]
    if not (len(vig_fim) >= 10 and vig_fim[4] == "-" and vig_fim[7] == "-"):
        vig_fim = ""
    else:
        vig_fim = vig_fim[:10]
    return {
        "id": member_id,
        "nome_completo": nome,
        "posto_graduacao": posto,
        "funcao": funcao,
        "ordem": ordem,
        "vigencia_inicio": vig_ini,
        "vigencia_fim": vig_fim,
    }


def _format_comissao_display_line(member: dict[str, Any]) -> str:
    posto = str(member.get("posto_graduacao") or "").strip()
    nome = str(member.get("nome_completo") or "").strip() or "Sem nome"
    member_id = str(member.get("id") or "").strip()
    base = f"{posto} {nome}".strip() if posto else nome
    if member_id:
        base = f"{base} - NR PM {member_id}"
    return f"{_normalize_comissao_funcao(member.get('funcao'))} - {base}"


def _format_comissao_signature_line(member: dict[str, Any]) -> str:
    posto = str(member.get("posto_graduacao") or "").strip()
    nome = str(member.get("nome_completo") or "").strip() or "Sem nome"
    member_id = str(member.get("id") or "").strip()
    base = f"{posto} {nome}".strip() if posto else nome
    return f"{base} - NR PM {member_id}" if member_id else base


def _parse_comissao_members(comissao_json: Any, comissao_text: Any) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    if str(comissao_json or "").strip():
        try:
            parsed = json.loads(str(comissao_json))
            if isinstance(parsed, list):
                members = [
                    _normalize_comissao_member_dict(item if isinstance(item, dict) else {}, idx=i)
                    for i, item in enumerate(parsed)
                ]
        except Exception:
            members = []
    if members:
        return members

    # Fallback legacy: comma/newline separated text.
    names = _parse_comissao_member_names(comissao_text)
    return [
        {
            "id": "",
            "nome_completo": name,
            "posto_graduacao": "",
            "funcao": "Membro",
            "ordem": idx + 1,
            "vigencia_inicio": "",
            "vigencia_fim": "",
        }
        for idx, name in enumerate(names)
    ]


def _parse_iso_date_flex(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d")
        except Exception:
            return None
    if len(raw) >= 10 and raw[2] == "/" and raw[5] == "/":
        try:
            return datetime.strptime(raw[:10], "%d/%m/%Y")
        except Exception:
            return None
    return None


def _resolve_comissao_members_from_config(
    comissao_cfg: dict[str, Any] | None,
    cia_pm: str | None,
    report_scope: str | None,
    date_from: str | None,
    date_to: str | None,
) -> list[dict[str, Any]]:
    cfg = comissao_cfg if isinstance(comissao_cfg, dict) else {}
    ref_dt = _parse_iso_date_flex(date_to) or _parse_iso_date_flex(date_from) or datetime.now()
    scope = str(report_scope or "").strip().lower()
    is_geral = scope == "comissao_geral"
    cia_key = "".join(ch for ch in str(cia_pm or "") if ch.isdigit())
    base_members: list[Any]
    if is_geral:
        base_members = list(cfg.get("comissao") or [])
    else:
        sub = cfg.get("subcomissoes") or {}
        base_members = list((sub.get(cia_key) or [])) if cia_key else []
    norm_members = [
        _normalize_comissao_member_dict(item if isinstance(item, dict) else {}, idx=i)
        for i, item in enumerate(base_members)
    ]
    out: list[dict[str, Any]] = []
    for m in norm_members:
        ini = _parse_iso_date_flex(m.get("vigencia_inicio"))
        fim = _parse_iso_date_flex(m.get("vigencia_fim"))
        if ini and ref_dt < ini:
            continue
        if fim and ref_dt > fim:
            continue
        out.append(m)
    return out


def _strip_markers_from_obs(text_value: Any) -> str:
    txt = str(text_value or "")
    txt = re.sub(r"\n?\[ERROS_COD\][^\r\n]*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\n?\[MEDIDAS_COD\][^\r\n]*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\n?\[AJUSTES_OBS_JSON\][^\r\n]*", "", txt, flags=re.IGNORECASE)
    return txt.strip()


def _extract_ajustes_obs_map(text_value: Any) -> dict[str, str]:
    txt = str(text_value or "")
    m = re.search(r"\[AJUSTES_OBS_JSON\]\s*([^\r\n]+)", txt, flags=re.IGNORECASE)
    if not m:
        return {}
    try:
        parsed = json.loads(str(m.group(1) or ""))
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in parsed.items():
        key = str(k or "").strip()
        val = str(v or "").strip()
        if key and val:
            out[key] = val
    return out


def _normalize_ajustes_set(row: dict[str, Any]) -> set[str]:
    ajustes = row.get("ajustes")
    if isinstance(ajustes, list):
        base = ajustes
    else:
        raw = row.get("ajustes_json")
        base = []
        if raw:
            try:
                parsed = json.loads(str(raw))
                if isinstance(parsed, list):
                    base = parsed
            except Exception:
                base = []
    return {str(x or "").strip().lower() for x in base if str(x or "").strip()}


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
    ajustes_por_row = {
        str(r.get("numero_ocorrencia") or "").strip(): _normalize_ajustes_set(r)
        for r in rows
        if str(r.get("numero_ocorrencia") or "").strip()
    }

    def _has_ajuste(row: dict[str, Any], *labels: str) -> bool:
        nr = str(row.get("numero_ocorrencia") or "").strip()
        got = ajustes_por_row.get(nr, set())
        wanted = {str(x or "").strip().lower() for x in labels if str(x or "").strip()}
        return bool(got.intersection(wanted))

    defs = [
        (
            "correcoes_ortograficas",
            "Correcao ortografica, gramatical, morfologica e semantica",
            lambda r: bool(r.get("erro_ortografico")) or _has_ajuste(r, "Ajuste textual"),
            ["02"],
        ),
        (
            "qualidade_texto",
            "Qualidade do texto produzido sobre os fatos",
            lambda r: (
                bool(r.get("alerta_texto_curto"))
                or bool(r.get("historico_incompleto"))
                or bool(r.get("ausencia_pontuacao"))
                or _has_ajuste(r, "Ajuste textual")
            ),
            ["02", "03"],
        ),
        (
            "qualificacao_envolvidos",
            "Qualificacao dos envolvidos (testemunhas, vitimas e agentes)",
            lambda r: (
                bool(int(r.get("qtd_envolvidos_sem_cpf") or 0) > 0)
                or bool(int(r.get("qtd_envolvidos_sem_rg") or 0) > 0)
                or bool(int(r.get("qtd_envolvidos_sem_email") or 0) > 0)
                or bool(int(r.get("qtd_envolvidos_sem_telefone") or 0) > 0)
                or (int(r.get("total_envolvidos") or 0) > 0 and bool(r.get("flag_envolvido_sem_cpf")))
                or (int(r.get("total_envolvidos") or 0) > 0 and bool(r.get("flag_envolvido_sem_rg")))
                or (int(r.get("total_envolvidos") or 0) > 0 and bool(r.get("flag_envolvido_sem_email")))
                or (int(r.get("total_envolvidos") or 0) > 0 and bool(r.get("flag_envolvido_sem_telefone")))
                or (int(r.get("total_envolvidos") or 0) > 0 and bool(r.get("alerta_campos_envolvido_incompletos")))
                or ("14.01" in _extract_codes_from_marker(r.get("observacao"), "ERROS_COD"))
            ),
            ["02"],
        ),
        (
            "tempo_empenho",
            "Tempo de empenho da guarnicao policial",
            lambda r: (
                bool(r.get("tempo_empenho_suspeito"))
                or "15.01" in _extract_codes_from_marker(r.get("observacao"), "ERROS_COD")
                or _has_ajuste(r, "Ajuste procedimento")
            ),
            ["02"],
        ),
        (
            "convergencia_dados",
            "Convergencia entre dados reais do fato e os lancados no REDS",
            lambda r: (
                bool(r.get("alerta_convergencia"))
                or bool(r.get("alerta_classificacao"))
                or _has_ajuste(r, "Ajuste natureza", "Ajuste classificacao")
            ),
            ["01", "02"],
        ),
    ]
    ajuste_obs_map_by_key = {
        "correcoes_ortograficas": ("Ajuste textual",),
        "qualidade_texto": ("Ajuste textual",),
        "qualificacao_envolvidos": ("Ajuste procedimento",),
        "tempo_empenho": ("Ajuste procedimento",),
        "convergencia_dados": ("Ajuste natureza", "Ajuste classificacao"),
    }
    out: list[dict[str, Any]] = []
    for key, label, fn, default_measures in defs:
        selected = [r for r in rows if fn(r)]
        qtd = len(selected)
        obs_lines: list[str] = []
        measures_count: dict[str, int] = {}
        if key == "qualificacao_envolvidos":
            cad_rows: list[dict[str, Any]] = []
            detail_lines: list[str] = []
            for row in selected:
                nr = str(row.get("numero_ocorrencia") or "").strip() or "-"
                problemas: list[tuple[str, int]] = []
                qtd_cpf = int(row.get("qtd_envolvidos_sem_cpf") or 0)
                qtd_rg = int(row.get("qtd_envolvidos_sem_rg") or 0)
                qtd_email = int(row.get("qtd_envolvidos_sem_email") or 0)
                qtd_tel = int(row.get("qtd_envolvidos_sem_telefone") or 0)
                qtd_end = int(row.get("qtd_envolvidos_sem_endereco") or 0)
                if qtd_cpf > 0:
                    problemas.append(("CPF invalido/nao informado", qtd_cpf))
                if qtd_email > 0:
                    problemas.append(("Email vazio/invalido", qtd_email))
                if qtd_tel > 0:
                    problemas.append(("Telefone vazio/invalido", qtd_tel))
                if qtd_end > 0:
                    problemas.append(("Endereco nao informado", qtd_end))
                if qtd_rg > 0:
                    problemas.append(("Documento nao informado", qtd_rg))
                if not problemas:
                    if "14.01" in _extract_codes_from_marker(row.get("observacao"), "ERROS_COD"):
                        cad_rows.append(row)
                        detail_lines.append(f"REDS {nr} | Inconformidade de qualificacao registrada pela comissao | 0 envolvido(s)")
                    continue
                cad_rows.append(row)
                for campo, qtd_env in problemas:
                    detail_lines.append(f"REDS {nr} | {campo} | {qtd_env} envolvido(s)")
            qtd = len(cad_rows)
            if detail_lines:
                obs_lines.append("REDS com inconsistência nos cadastros:")
                obs_lines.extend(detail_lines[:80])
            selected = cad_rows
        for row in selected[:10]:
            nr = str(row.get("numero_ocorrencia") or "").strip()
            if key != "qualificacao_envolvidos":
                obs = _strip_markers_from_obs(row.get("observacao"))
                if obs:
                    obs_lines.append(f"{nr}: {obs}")
                else:
                    ajustes_obs = _extract_ajustes_obs_map(row.get("observacao"))
                    mapped_labels = ajuste_obs_map_by_key.get(key, tuple())
                    mapped_obs = [str(ajustes_obs.get(lbl) or "").strip() for lbl in mapped_labels]
                    mapped_obs = [txt for txt in mapped_obs if txt]
                    if mapped_obs:
                        obs_lines.append(f"{nr}: {' | '.join(mapped_obs)}")
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


_REPORT_ALL_SCOPE_TOKENS = {"", "ALL", "TODAS", "TODOS", "19", "19BPM", "19_BPM", "BPM19"}
_DEFAULT_19BPM_CIAS = ("42", "47", "74", "155", "232")


def _is_report_all_scope(cia_pm: str | None) -> bool:
    raw = str(cia_pm or "").strip().upper()
    return raw in _REPORT_ALL_SCOPE_TOKENS


def _cia_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _row_iso_date(row: dict[str, Any]) -> str:
    date_fato = str(row.get("data_fato") or "").strip()
    if len(date_fato) >= 10 and date_fato[4] == "-" and date_fato[7] == "-":
        return date_fato[:10]
    dt_fato = str(row.get("data_hora_fato") or "").strip()
    if len(dt_fato) >= 10 and dt_fato[4] == "-" and dt_fato[7] == "-":
        return dt_fato[:10]
    return ""


def _month_key_from_iso(value: str) -> str:
    txt = str(value or "").strip()
    if len(txt) >= 7 and txt[4] == "-":
        return txt[:7]
    return ""


def _month_label_pt(month_key: str) -> str:
    txt = str(month_key or "").strip()
    if len(txt) != 7 or txt[4] != "-":
        return txt
    month_names = {
        "01": "JAN",
        "02": "FEV",
        "03": "MAR",
        "04": "ABR",
        "05": "MAI",
        "06": "JUN",
        "07": "JUL",
        "08": "AGO",
        "09": "SET",
        "10": "OUT",
        "11": "NOV",
        "12": "DEZ",
    }
    mm = txt[5:7]
    yy = txt[0:4]
    return f"{month_names.get(mm, mm)}/{yy}"


def _iter_month_keys(date_from: str | None, date_to: str | None) -> list[str]:
    ini = str(date_from or "").strip()
    fim = str(date_to or "").strip()
    if not (len(ini) >= 10 and len(fim) >= 10 and ini[4] == "-" and ini[7] == "-" and fim[4] == "-" and fim[7] == "-"):
        return []
    try:
        start = datetime.strptime(ini[:10], "%Y-%m-%d")
        end = datetime.strptime(fim[:10], "%Y-%m-%d")
    except Exception:
        return []
    if end < start:
        start, end = end, start
    cur_y, cur_m = start.year, start.month
    end_y, end_m = end.year, end.month
    out: list[str] = []
    while (cur_y < end_y) or (cur_y == end_y and cur_m <= end_m):
        out.append(f"{cur_y:04d}-{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    return out


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
    where: list[str] = []
    params: dict[str, Any] = {"limit": int(limit)}
    if not _is_report_all_scope(cia):
        where.append("(COALESCE(NULLIF(TRIM(r.cia_pm),''), 'SEM_CIA') = :cia_pm OR COALESCE(NULLIF(TRIM(r.cia_pm),''), 'SEM_CIA') LIKE :cia_pm_like)")
        params["cia_pm"] = cia
        params["cia_pm_like"] = f"%{cia}%"
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
    where_sql = " AND ".join(where) if where else "1=1"
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
                  r.ind_estado,
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
                  r.flag_envolvido_sem_cpf,
                  r.flag_envolvido_sem_rg,
                  r.flag_envolvido_sem_email,
                  r.flag_envolvido_sem_telefone,
                  r.total_envolvidos,
                  r.qtd_envolvidos_sem_cpf,
                  r.qtd_envolvidos_sem_rg,
                  r.qtd_envolvidos_sem_email,
                  r.qtd_envolvidos_sem_telefone,
                  r.flag_todos_envolvidos_sem_cpf,
                  r.flag_todos_envolvidos_sem_rg,
                  r.flag_todos_envolvidos_sem_email,
                  r.flag_todos_envolvidos_sem_telefone,
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
                  qa.alerta_envolvido_sem_cpf,
                  qa.alerta_envolvido_sem_rg,
                  qa.alerta_envolvido_sem_email,
                  qa.alerta_envolvido_sem_telefone,
                  qa.alerta_campos_envolvido_incompletos,
                  qa.alerta_procedimento_relatorio_policial,
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
                "alerta_envolvido_sem_cpf",
                "alerta_envolvido_sem_rg",
                "alerta_envolvido_sem_email",
                "alerta_envolvido_sem_telefone",
                "alerta_campos_envolvido_incompletos",
                "alerta_procedimento_relatorio_policial",
            ):
                m[f] = bool(int(m.get(f) or 0)) if m.get(f) is not None else False
            for f in (
                "flag_envolvido_sem_cpf",
                "flag_envolvido_sem_rg",
                "flag_envolvido_sem_email",
                "flag_envolvido_sem_telefone",
                "flag_todos_envolvidos_sem_cpf",
                "flag_todos_envolvidos_sem_rg",
                "flag_todos_envolvidos_sem_email",
                "flag_todos_envolvidos_sem_telefone",
            ):
                m[f] = bool(int(m.get(f) or 0)) if m.get(f) is not None else False
            for f in (
                "total_envolvidos",
                "qtd_envolvidos_sem_cpf",
                "qtd_envolvidos_sem_rg",
                "qtd_envolvidos_sem_email",
                "qtd_envolvidos_sem_telefone",
            ):
                m[f] = int(m.get(f) or 0)
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


def _compute_qr_kpis_from_rows(rows: list[dict[str, Any]], cia_pm: str) -> dict[str, Any]:
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
    obrigatorios_total = cv_total + arma_total + w_total + c99_total
    outros_total = max(0, total - obrigatorios_total)
    outros_av = sum(1 for r in rows if is_tipo(r, "OUTROS") and r.get("avaliado"))
    outros_meta_2pct = int(ceil(outros_total * 0.02)) if outros_total else 0
    outros_pend_meta = max(0, outros_meta_2pct - outros_av)
    outros_pct_av = round((outros_av * 100.0 / outros_total), 2) if outros_total else 0.0

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
                bool(r.get("alerta_campos_envolvido_incompletos")),
                bool(r.get("alerta_procedimento_relatorio_policial")),
            ]
        )
        if has_alert:
            sinalizados_total += 1
            if r.get("avaliado"):
                sinalizados_av += 1

    def _count_reds_env(qtd_key: str, flag_key: str) -> tuple[int, int]:
        reds = 0
        envs = 0
        for r in rows:
            total_env = int(r.get("total_envolvidos") or 0)
            if total_env <= 0:
                continue
            qtd_env = int(r.get(qtd_key) or 0)
            has_flag = bool(r.get(flag_key)) if flag_key else False
            if qtd_env > 0 or has_flag:
                reds += 1
            if qtd_env > 0:
                envs += qtd_env
        return reds, envs

    reds_sem_cpf, env_sem_cpf = _count_reds_env("qtd_envolvidos_sem_cpf", "flag_envolvido_sem_cpf")
    reds_sem_email, env_sem_email = _count_reds_env("qtd_envolvidos_sem_email", "flag_envolvido_sem_email")
    reds_sem_tel, env_sem_tel = _count_reds_env("qtd_envolvidos_sem_telefone", "flag_envolvido_sem_telefone")
    reds_sem_doc, env_sem_doc = _count_reds_env("qtd_envolvidos_sem_rg", "flag_envolvido_sem_rg")
    reds_sem_endereco = 0
    env_sem_endereco = 0

    def _line(reds: int, label: str, envs: int) -> str:
        return f"{reds:02d} REDS sem {label} | {envs} Envolvidos"

    resumo_cadastro_linhas = [
        _line(reds_sem_cpf, "CPF", env_sem_cpf),
        _line(reds_sem_endereco, "endereco", env_sem_endereco),
        _line(reds_sem_email, "email", env_sem_email),
        _line(reds_sem_tel, "telefone", env_sem_tel),
        f"{reds_sem_doc:02d} REDS faltando documento | {env_sem_doc} Envolvidos",
    ]

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
        "obrigatorios_total": obrigatorios_total,
        "outros_total": outros_total,
        "outros_avaliados": outros_av,
        "outros_meta_2pct": outros_meta_2pct,
        "outros_pendentes_meta_2pct": outros_pend_meta,
        "outros_percentual_avaliado_2pct_base": outros_pct_av,
        "outros_pendentes": outros_pend_meta,
        "sinalizados_total": sinalizados_total,
        "sinalizados_avaliados": sinalizados_av,
        "reds_sem_cpf": reds_sem_cpf,
        "envolvidos_sem_cpf": env_sem_cpf,
        "reds_sem_endereco": reds_sem_endereco,
        "envolvidos_sem_endereco": env_sem_endereco,
        "reds_sem_email": reds_sem_email,
        "envolvidos_sem_email": env_sem_email,
        "reds_sem_telefone": reds_sem_tel,
        "envolvidos_sem_telefone": env_sem_tel,
        "reds_faltando_documento": reds_sem_doc,
        "envolvidos_faltando_documento": env_sem_doc,
        "resumo_flags_cadastro_linhas": resumo_cadastro_linhas,
    }


def _build_comissao_geral_blocks(rows: list[dict[str, Any]], date_from: str | None, date_to: str | None) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        cia = _cia_key(row.get("cia_pm")) or "SEM_CIA"
        grouped.setdefault(cia, []).append(row)

    cias = list(_DEFAULT_19BPM_CIAS)
    extras = [c for c in grouped.keys() if c not in set(cias) and c != "SEM_CIA"]
    extras_sorted = sorted(extras, key=lambda x: int(x) if str(x).isdigit() else 9999)
    cias.extend(extras_sorted)
    if "SEM_CIA" in grouped:
        cias.append("SEM_CIA")

    kpis_por_cia: list[dict[str, Any]] = []
    for cia in cias:
        rows_cia = grouped.get(cia, [])
        kpi = _compute_qr_kpis_from_rows(rows_cia, cia)
        kpi["cia_label"] = "Sem CIA informada" if cia == "SEM_CIA" else _cia_label(cia)
        kpis_por_cia.append(kpi)

    month_keys = _iter_month_keys(date_from, date_to)
    if not month_keys:
        month_keys = sorted({mk for mk in (_month_key_from_iso(_row_iso_date(r)) for r in rows) if mk})

    tabela_mensal_linhas: list[list[Any]] = []
    for cia in cias:
        rows_cia = grouped.get(cia, [])
        cia_label = "Sem CIA informada" if cia == "SEM_CIA" else _cia_label(cia)
        for month_key in month_keys:
            rows_mes = [r for r in rows_cia if _month_key_from_iso(_row_iso_date(r)) == month_key]
            k_mes = _compute_qr_kpis_from_rows(rows_mes, cia)
            tabela_mensal_linhas.append(
                [
                    cia_label,
                    _month_label_pt(month_key),
                    int(k_mes.get("total_registros") or 0),
                    int(k_mes.get("total_avaliados") or 0),
                    f"{float(k_mes.get('percentual_avaliado') or 0):.2f}%",
                    int(k_mes.get("outros_meta_2pct") or 0),
                    int(k_mes.get("outros_avaliados") or 0),
                ]
            )

    tabela_ajustes_linhas: list[list[Any]] = []
    for cia in cias:
        rows_cia = grouped.get(cia, [])
        avaliados = [r for r in rows_cia if bool(r.get("avaliado"))]
        registros_com_ajuste = 0
        marcacoes_total = 0
        ajuste_textual = 0
        ajuste_natureza = 0
        ajuste_classificacao = 0
        ajuste_procedimento = 0
        for row in avaliados:
            ajustes = [str(x or "").strip().lower() for x in (row.get("ajustes") or []) if str(x or "").strip()]
            if not ajustes:
                continue
            registros_com_ajuste += 1
            marcacoes_total += len(ajustes)
            if "ajuste textual" in ajustes:
                ajuste_textual += 1
            if "ajuste natureza" in ajustes:
                ajuste_natureza += 1
            if "ajuste classificacao" in ajustes:
                ajuste_classificacao += 1
            if "ajuste procedimento" in ajustes:
                ajuste_procedimento += 1
        pct_com_ajuste = (registros_com_ajuste * 100.0 / len(avaliados)) if avaliados else 0.0
        cia_label = "Sem CIA informada" if cia == "SEM_CIA" else _cia_label(cia)
        tabela_ajustes_linhas.append(
            [
                cia_label,
                len(avaliados),
                registros_com_ajuste,
                f"{pct_com_ajuste:.2f}%",
                marcacoes_total,
                ajuste_textual,
                ajuste_natureza,
                ajuste_classificacao,
                ajuste_procedimento,
            ]
        )

    bloco_kpis = {
        "id": "comparativo_percentual_cias",
        "titulo": "Bloco 3 - Indicadores Percentuais por CIA",
        "naturezas": "Todas as naturezas",
        "total_registros": len(rows),
        "total_avaliados": sum(1 for r in rows if r.get("avaliado")),
        "texto_abertura": "Comparativo consolidado com os mesmos indicadores percentuais aplicados nas CIAs.",
        "tabela": {
            "colunas": [
                "CIA",
                "Total",
                "Avaliados",
                "% Avaliado",
                "CV (av/total)",
                "ARMA (av/total)",
                "W (av/total)",
                "C99 (av/total)",
                "Outros meta 2%",
                "Outros avaliados",
            ],
            "linhas": [
                [
                    str(k.get("cia_label") or k.get("cia_pm") or "-"),
                    int(k.get("total_registros") or 0),
                    int(k.get("total_avaliados") or 0),
                    f"{float(k.get('percentual_avaliado') or 0):.2f}%",
                    f"{int(k.get('cv_avaliados') or 0)}/{int(k.get('cv_total') or 0)}",
                    f"{int(k.get('arma_avaliados') or 0)}/{int(k.get('arma_total') or 0)}",
                    f"{int(k.get('w_avaliados') or 0)}/{int(k.get('w_total') or 0)}",
                    f"{int(k.get('c99_avaliados') or 0)}/{int(k.get('c99_total') or 0)}",
                    int(k.get("outros_meta_2pct") or 0),
                    int(k.get("outros_avaliados") or 0),
                ]
                for k in kpis_por_cia
            ],
        },
    }

    bloco_mensal = {
        "id": "evolucao_mensal_cias",
        "titulo": "Bloco 4 - Evolucao Mensal por CIA",
        "naturezas": "Consolidado mensal",
        "total_registros": len(rows),
        "total_avaliados": sum(1 for r in rows if r.get("avaliado")),
        "texto_abertura": "Distribuicao mensal da analise em cada CIA no periodo do relatorio da Comissao.",
        "tabela": {
            "colunas": ["CIA", "Mes", "Total", "Avaliados", "% Avaliado", "Meta 2% (Outros)", "Outros avaliados"],
            "linhas": tabela_mensal_linhas,
        },
    }

    bloco_ajustes = {
        "id": "ajustes_por_cia",
        "titulo": "Bloco 5 - Ajustes Registrados por CIA",
        "naturezas": "Ajustes de avaliacao",
        "total_registros": len(rows),
        "total_avaliados": sum(1 for r in rows if r.get("avaliado")),
        "texto_abertura": "Apanhado de todos os ajustes marcados pelos avaliadores em cada CIA.",
        "tabela": {
            "colunas": [
                "CIA",
                "Avaliados",
                "Registros com ajuste",
                "% avaliados com ajuste",
                "Marcacoes de ajustes",
                "Ajuste textual",
                "Ajuste natureza",
                "Ajuste classificacao",
                "Ajuste procedimento",
            ],
            "linhas": tabela_ajustes_linhas,
        },
    }

    return {
        "kpis_por_cia": kpis_por_cia,
        "meses_referencia": [_month_label_pt(m) for m in month_keys],
        "blocos_extra": [bloco_kpis, bloco_mensal, bloco_ajustes],
    }


def get_qr_monthly_kpis(
    cia_pm: str,
    date_from: str | None = None,
    date_to: str | None = None,
    natureza_grupo: str | None = None,
    nome_tipo_relatorio: str | None = None,
) -> dict[str, Any]:
    cia_query = "" if _is_report_all_scope(cia_pm) else cia_pm
    rows = list_qr_review_rows(
        cia_pm=cia_query,
        date_from=date_from,
        date_to=date_to,
        natureza_grupo=natureza_grupo,
        nome_tipo_relatorio=nome_tipo_relatorio,
        limit=50000,
        only_pending=False,
    )
    scope_cia = "19BPM" if _is_report_all_scope(cia_pm) else str(cia_pm or "").strip()
    return _compute_qr_kpis_from_rows(rows, scope_cia)


def build_qr_monthly_report(
    cia_pm: str,
    date_from: str | None = None,
    date_to: str | None = None,
    natureza_grupo: str | None = None,
    nome_tipo_relatorio: str | None = None,
    data_reuniao: str | None = None,
    comissao: str | None = None,
    comissao_json: str | None = None,
    comissao_cfg: dict[str, Any] | None = None,
    periodo_label: str | None = None,
    report_scope: str | None = None,
) -> dict[str, Any]:
    scope = str(report_scope or "").strip().lower()
    comissao_geral_scope = scope == "comissao_geral"
    cia_query = "" if comissao_geral_scope else cia_pm
    rows = list_qr_review_rows(
        cia_pm=cia_query,
        date_from=date_from,
        date_to=date_to,
        natureza_grupo=natureza_grupo,
        nome_tipo_relatorio=nome_tipo_relatorio,
        limit=50000,
        only_pending=False,
    )
    k = _compute_qr_kpis_from_rows(rows, "19BPM" if comissao_geral_scope else str(cia_pm or "").strip())
    avaliados_rows = [r for r in rows if bool(r.get("avaliado"))]
    cv_base_rows = [r for r in rows if str(r.get("tipo_natureza") or "") == "CV"]
    nao_cv_base_rows = [r for r in rows if str(r.get("tipo_natureza") or "") != "CV"]
    cv_rows = [r for r in avaliados_rows if str(r.get("tipo_natureza") or "") == "CV"]
    nao_cv_rows = [r for r in avaliados_rows if str(r.get("tipo_natureza") or "") != "CV"]

    total_base = len(rows)
    alvo_2pct = int(ceil(total_base * 0.02)) if total_base else 0
    amostra_ref = min(alvo_2pct, len(nao_cv_base_rows)) if nao_cv_base_rows else 0
    pct_amostra = round((amostra_ref * 100.0 / total_base), 2) if total_base else 0.0
    total_avaliados_nao_cv = len(nao_cv_rows)
    pct_avaliados_nao_cv = round((total_avaliados_nao_cv * 100.0 / total_base), 2) if total_base else 0.0
    pct_avaliados_nao_cv_txt = f"{pct_avaliados_nao_cv:.2f}"
    arma_qtd = sum(1 for r in nao_cv_rows if str(r.get("tipo_natureza") or "") == "ARMA")
    w_qtd = sum(1 for r in nao_cv_rows if str(r.get("tipo_natureza") or "") == "GRUPO_W")
    c99_qtd = sum(1 for r in nao_cv_rows if str(r.get("tipo_natureza") or "") == "C99000")
    outros_qtd = int(k.get("outros_avaliados") or 0)
    comissao_raw_members = _resolve_comissao_members_from_config(
        comissao_cfg=comissao_cfg,
        cia_pm=cia_pm,
        report_scope=report_scope,
        date_from=date_from,
        date_to=date_to,
    )
    if not comissao_raw_members:
        comissao_raw_members = _parse_comissao_members(comissao_json, comissao)
    comissao_validos = [
        m for m in comissao_raw_members
        if _normalize_comissao_funcao(m.get("funcao")) != "Auxiliar" and str(m.get("nome_completo") or "").strip()
    ]
    comissao_ordenada = sorted(
        comissao_validos,
        key=lambda m: (
            _comissao_funcao_rank(m.get("funcao")),
            int(m.get("ordem") or 9999),
            str(m.get("nome_completo") or "").casefold(),
        ),
    )
    comissao_assinaturas = sorted(
        comissao_validos,
        key=lambda m: (
            _comissao_signature_rank(m.get("funcao")),
            -int(m.get("ordem") or 0),
            str(m.get("nome_completo") or "").casefold(),
        ),
    )
    comissao_membros = [_format_comissao_display_line(m) for m in comissao_ordenada]
    comissao_assinaturas_meta = [
        {
            "linha": _format_comissao_signature_line(m),
            "funcao": _normalize_comissao_funcao(m.get("funcao")),
        }
        for m in comissao_assinaturas
    ]
    comissao_texto = "\n".join(comissao_membros) if comissao_membros else "Nao informada"

    cia_label = "19 BPM (Consolidado)" if comissao_geral_scope else _cia_label(str(cia_pm or ""))
    periodo = f"{date_from or 'inicio'} a {date_to or 'fim'}"
    periodo_nome = _periodo_label_pt(date_from, date_to, periodo_label=periodo_label)
    if comissao_geral_scope:
        t1 = (
            f"A Comissao Geral de Qualidade REDS do {cia_label} analisou {len(cv_base_rows)} registro(s) de crimes violentos "
            f"no periodo {periodo}, consolidando dados de todas as CIAs do batalhao."
        )
        t2 = (
            f"No bloco de nao violentos, foram avaliadas {total_avaliados_nao_cv} ocorrencias ({pct_avaliados_nao_cv_txt}% do total), "
            f"sendo {arma_qtd} de natureza ARMA, {w_qtd} do Grupo W, {c99_qtd} C99 e {outros_qtd} de outras naturezas."
        )
        report_title = "RELATORIO TRIMESTRAL CONSOLIDADO DE QUALIDADE REDS"
    else:
        t1 = (
            f"A Comissao de avaliacao de REDS da {cia_label} procedeu a analise de "
            f"{len(cv_base_rows)} registro(s) de crimes violentos ocorridos na area da Cia "
            f"no periodo avaliado ({periodo}). A seguir serao descritas as inconformidades detectadas."
        )
        t2 = (
            f"A Comissao de avaliacao de REDS da {cia_label} procedeu a analise de {total_avaliados_nao_cv} "
            f"ocorrencias referente a {pct_avaliados_nao_cv_txt}% de todos os registros confeccionados na area da Cia no periodo avaliado; "
            f"de {arma_qtd} REDS de natureza E03012, E03014 e E03016; "
            f"de {w_qtd} BOS de natureza W01.000, W02.000, W03.000, W04.000 e; "
            f"de {c99_qtd} REDS de natureza C99.000; e de {outros_qtd} REDS de outras naturezas. "
            f"A seguir serao descritas inconformidades detectadas, "
            f"separando-se por naturezas."
        )
        report_title = "RELATORIO MENSAL DE QUALIDADE REDS"

    blocos = [
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
            "total_avaliados": total_avaliados_nao_cv,
            "percentual_amostra": pct_amostra,
            "alvo_2pct": alvo_2pct,
            "amostra_referencia": amostra_ref,
            "qtd_arma": arma_qtd,
            "qtd_grupo_w": w_qtd,
            "qtd_c99": c99_qtd,
            "texto_abertura": t2,
            "criterios": _aggregate_criteria(nao_cv_rows),
        },
    ]

    meta_extra: dict[str, Any] = {}
    if comissao_geral_scope:
        extra = _build_comissao_geral_blocks(rows, date_from, date_to)
        blocos.extend(extra.get("blocos_extra") or [])
        meta_extra = {
            "kpis_por_cia": extra.get("kpis_por_cia") or [],
            "meses_referencia": extra.get("meses_referencia") or [],
        }

    preview = {
        "meta": {
            "cia_pm": "19BPM" if comissao_geral_scope else str(cia_pm or "").strip(),
            "cia_label": cia_label,
            "date_from": date_from,
            "date_to": date_to,
            "periodo_intervalo": periodo,
            "periodo_label": periodo_nome,
            "data_reuniao": _format_date_br(data_reuniao or date_to or date_from),
            "comissao": comissao_texto,
            "comissao_membros": comissao_membros,
            "comissao_assinaturas": comissao_assinaturas_meta,
            "total_registros_periodo": total_base,
            "total_alvo_2pct": alvo_2pct,
            "total_avaliados_periodo": len(avaliados_rows),
            "report_scope": "comissao_geral" if comissao_geral_scope else "cia",
            "report_title": report_title,
            **meta_extra,
        },
        "blocos": blocos,
    }
    return {"kpis": k, "texto_modelo_1": t1, "texto_modelo_2": t2, "preview": preview}


def _pdf_escape_text(value: Any) -> str:
    txt = str(value or "")
    txt = txt.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    txt = txt.replace("\r", " ").replace("\n", " ")
    return txt


def _report_to_plain_lines(report: dict[str, Any]) -> list[str]:
    preview = dict((report or {}).get("preview") or {})
    meta = dict(preview.get("meta") or {})
    blocos = list(preview.get("blocos") or [])
    lines: list[str] = []

    lines.append(str(meta.get("report_title") or "RELATORIO DE QUALIDADE REDS"))
    lines.append("DECIMO NONO BATALHAO DE POLICIA MILITAR - SECAO P3")
    lines.append("")
    lines.append(f"Unidade: {meta.get('cia_label') or meta.get('cia_pm') or '-'}")
    lines.append(f"Periodo: {meta.get('periodo_label') or meta.get('periodo_intervalo') or '-'}")
    lines.append(f"Data da reuniao: {meta.get('data_reuniao') or '-'}")
    lines.append(f"Total no periodo: {meta.get('total_registros_periodo') or 0}")
    lines.append(f"Amostra alvo (2%): {meta.get('total_alvo_2pct') or 0}")
    lines.append(f"Total avaliados: {meta.get('total_avaliados_periodo') or 0}")

    comissao = meta.get("comissao_membros")
    if isinstance(comissao, list) and comissao:
        lines.append("Comissao:")
        for membro in comissao:
            lines.append(f"- {membro}")
    else:
        lines.append(f"Comissao: {meta.get('comissao') or 'Nao informada'}")

    lines.append("")
    for idx, bloco in enumerate(blocos, start=1):
        lines.append(f"{idx}. {bloco.get('titulo') or f'Bloco {idx}'}")
        lines.append(f"Naturezas: {bloco.get('naturezas') or '-'}")
        lines.append(f"Total no periodo: {bloco.get('total_registros') or 0}")
        lines.append(f"Total avaliados: {bloco.get('total_avaliados') or 0}")
        lines.append(f"Texto: {bloco.get('texto_abertura') or '-'}")

        tabela = bloco.get("tabela") if isinstance(bloco.get("tabela"), dict) else {}
        colunas = list(tabela.get("colunas") or [])
        linhas = list(tabela.get("linhas") or [])
        if colunas:
            lines.append("Tabela:")
            lines.append(" | ".join(str(c or "-") for c in colunas))
            for linha in linhas:
                vals = linha if isinstance(linha, list) else [linha]
                lines.append(" | ".join(str(v if v is not None else "-") for v in vals))
        else:
            criterios = list(bloco.get("criterios") or [])
            if criterios:
                lines.append("Criterios:")
                for c in criterios:
                    lines.append(f"- Criterio: {c.get('criterio') or '-'}")
                    lines.append(f"  Observacoes: {c.get('observacao_inconformidades') or '-'}")
                    lines.append(f"  Medidas: {c.get('medidas_adotadas') or '-'}")
            else:
                lines.append("Sem criterios para este bloco.")
        lines.append("")

    assinatura_items_raw = meta.get("comissao_assinaturas")
    assinatura_items = [
        dict(item or {})
        for item in assinatura_items_raw
        if isinstance(item, dict) and str((item or {}).get("linha") or "").strip()
    ] if isinstance(assinatura_items_raw, list) else []
    if assinatura_items:
        lines.append("Assinaturas dos Responsaveis:")
        for item in assinatura_items:
            linha = str(item.get("linha") or "").strip()
            funcao = str(item.get("funcao") or "").strip()
            lines.append(f"- {linha} ({funcao})")

    return lines


def _build_basic_pdf_bytes(lines: list[str], title: str = "Relatorio Qualidade REDS") -> bytes:
    page_width = 595
    page_height = 842
    margin_left = 36
    start_y = 806
    line_height = 12
    max_chars = 112
    lines_per_page = 58

    normalized: list[str] = []
    normalized.append(str(title or "Relatorio Qualidade REDS").strip())
    normalized.append("")
    for raw_line in lines:
        txt = str(raw_line or "")
        wrapped = textwrap.wrap(txt, width=max_chars, replace_whitespace=False) or [""]
        normalized.extend(wrapped)

    pages: list[list[str]] = [normalized[i:i + lines_per_page] for i in range(0, len(normalized), lines_per_page)] or [[]]
    total_pages = len(pages)

    objects: dict[int, bytes] = {}
    objects[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects[3] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

    page_obj_nums: list[int] = []
    next_obj = 4
    for _ in pages:
        page_obj_nums.append(next_obj)
        next_obj += 2

    kids_refs = " ".join(f"{n} 0 R" for n in page_obj_nums)
    objects[2] = f"<< /Type /Pages /Kids [{kids_refs}] /Count {len(page_obj_nums)} >>".encode("latin-1")

    for page_idx, page_lines in enumerate(pages, start=1):
        page_obj_num = page_obj_nums[page_idx - 1]
        content_obj_num = page_obj_num + 1

        stream_cmds = [
            "BT",
            "/F1 9 Tf",
            f"{margin_left} {start_y} Td",
            f"{line_height} TL",
        ]
        for line in page_lines:
            stream_cmds.append(f"({_pdf_escape_text(line)}) Tj")
            stream_cmds.append("T*")
        stream_cmds.append(f"(Pagina {page_idx}/{total_pages}) Tj")
        stream_cmds.append("ET")
        stream = "\n".join(stream_cmds).encode("latin-1", errors="replace")
        objects[content_obj_num] = b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        objects[page_obj_num] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_obj_num} 0 R >>"
        ).encode("latin-1")

    max_obj = max(objects.keys())
    out = bytearray()
    out.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0] * (max_obj + 1)
    for n in range(1, max_obj + 1):
        body = objects.get(n, b"<<>>")
        offsets[n] = len(out)
        out.extend(f"{n} 0 obj\n".encode("ascii"))
        out.extend(body)
        out.extend(b"\nendobj\n")

    xref_offset = len(out)
    out.extend(f"xref\n0 {max_obj + 1}\n".encode("ascii"))
    out.extend(b"0000000000 65535 f \n")
    for n in range(1, max_obj + 1):
        out.extend(f"{offsets[n]:010d} 00000 n \n".encode("ascii"))
    out.extend(
        (
            f"trailer\n<< /Size {max_obj + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(out)


def _build_qr_monthly_report_pdf_fallback(report: dict[str, Any]) -> bytes:
    preview = dict((report or {}).get("preview") or {})
    meta = dict(preview.get("meta") or {})
    title = str(meta.get("report_title") or "Relatorio de Qualidade REDS")
    lines = _report_to_plain_lines(report)
    return _build_basic_pdf_bytes(lines, title=title)


def build_qr_monthly_report_pdf(report: dict[str, Any]) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception:  # pragma: no cover
        return _build_qr_monthly_report_pdf_fallback(report)

    preview = dict((report or {}).get("preview") or {})
    meta = dict(preview.get("meta") or {})
    blocos = list(preview.get("blocos") or [])

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
        title=str(meta.get("report_title") or "Relatorio Mensal de Qualidade REDS"),
    )

    styles = getSampleStyleSheet()
    st_h2 = ParagraphStyle(
        "qr_h2",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=15,
        spaceBefore=8,
        spaceAfter=6,
    )
    st_small = ParagraphStyle(
        "qr_small",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11,
    )
    st_head_1 = ParagraphStyle(
        "qr_head_1",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=13.5,
        spaceAfter=1,
    )
    st_head_2 = ParagraphStyle(
        "qr_head_2",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=10.5,
        leading=12,
        spaceAfter=2,
    )
    st_head_3 = ParagraphStyle(
        "qr_head_3",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=16,
    )

    def _p(text: Any) -> Any:
        safe = _xml_escape(str(text if text is not None else "-")).replace("\n", "<br/>")
        return Paragraph(safe, st_small)

    story: list[Any] = []
    logo_path = Path(__file__).resolve().parents[2] / "public" / "imagens" / "Icones_logo_19bpm" / "android-chrome-192x192.png"
    logo_cell: Any = Spacer(20 * mm, 20 * mm)
    if logo_path.exists():
        try:
            logo_cell = Image(str(logo_path), width=20 * mm, height=20 * mm)
        except Exception:
            logo_cell = Spacer(20 * mm, 20 * mm)

    header_text = [
        Paragraph("D\u00c9CIMO NONO BATALH\u00c3O DE POL\u00cdCIA MILITAR", st_head_1),
        Paragraph("SE\u00c7\u00c3O DE PLANEJAMENTO OPERACIONAL - P3", st_head_2),
        Paragraph(_xml_escape(str(meta.get("report_title") or "RELAT\u00d3RIO MENSAL DE QUALIDADE REDS")), st_head_3),
    ]
    head_table = Table([[logo_cell, header_text]], colWidths=[20 * mm, 160 * mm], repeatRows=0)
    head_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ("LEFTPADDING", (1, 0), (1, 0), 10 * mm),
            ]
        )
    )
    story.append(head_table)
    story.append(Spacer(1, 4))

    comissao_items_raw = meta.get("comissao_membros")
    if isinstance(comissao_items_raw, list):
        comissao_items = [str(x or "").strip() for x in comissao_items_raw if str(x or "").strip()]
    else:
        comissao_items = _parse_comissao_member_names(meta.get("comissao"))
    assinatura_items_raw = meta.get("comissao_assinaturas")
    if isinstance(assinatura_items_raw, list):
        assinatura_items = [
            {
                "linha": str((item or {}).get("linha") or "").strip(),
                "funcao": _normalize_comissao_funcao((item or {}).get("funcao")),
            }
            for item in assinatura_items_raw
            if str((item or {}).get("linha") or "").strip()
        ]
    else:
        assinatura_items = []
    if comissao_items:
        comissao_cell: Any = Paragraph("<br/>".join(_xml_escape(item) for item in comissao_items), st_small)
    else:
        comissao_cell = "Nao informada"

    meta_rows = [
        ["Unidade", str(meta.get("cia_label") or meta.get("cia_pm") or "-")],
        ["Periodo", str(meta.get("periodo_label") or meta.get("periodo_intervalo") or "-")],
        ["Data da reuniao", str(meta.get("data_reuniao") or "-")],
        ["Total no periodo", str(meta.get("total_registros_periodo") or 0)],
        ["Amostra alvo (2%)", str(meta.get("total_alvo_2pct") or 0)],
        ["Total avaliados", str(meta.get("total_avaliados_periodo") or 0)],
        ["Comissao", comissao_cell],
    ]
    meta_table = Table(meta_rows, colWidths=[42 * mm, 138 * mm], repeatRows=0)
    meta_table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cdbf9e")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f5efdf")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(meta_table)
    story.append(Spacer(1, 8))

    for idx, bloco in enumerate(blocos, start=1):
        story.append(Paragraph(str(bloco.get("titulo") or f"Bloco {idx}"), st_h2))
        resumo = [
            ["Naturezas", str(bloco.get("naturezas") or "-")],
            ["Total no periodo", str(bloco.get("total_registros") or 0)],
            ["Total avaliados", str(bloco.get("total_avaliados") or 0)],
        ]
        resumo_table = Table(resumo, colWidths=[42 * mm, 138 * mm], repeatRows=0)
        resumo_table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d8cfbc")),
                    ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f9f3e5")),
                    ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(resumo_table)
        story.append(Spacer(1, 4))
        story.append(_p(bloco.get("texto_abertura") or "-"))
        story.append(Spacer(1, 5))

        tabela = bloco.get("tabela") if isinstance(bloco.get("tabela"), dict) else {}
        colunas = list(tabela.get("colunas") or [])
        linhas = list(tabela.get("linhas") or [])
        if colunas and linhas:
            tab_rows: list[list[Any]] = [[_p(c or "-") for c in colunas]]
            for linha in linhas:
                vals = linha if isinstance(linha, list) else [linha]
                tab_rows.append([_p(v if v is not None else "-") for v in vals])
            col_count = max(1, len(colunas))
            col_w = (180 * mm) / col_count
            full_table = Table(tab_rows, colWidths=[col_w] * col_count, repeatRows=1)
            full_table.setStyle(
                TableStyle(
                    [
                        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d8cfbc")),
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eee4cf")),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 4),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                        ("TOPPADDING", (0, 0), (-1, -1), 3),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ]
                )
            )
            story.append(full_table)
            story.append(Spacer(1, 8))
            continue

        criterios = list(bloco.get("criterios") or [])
        if not criterios:
            story.append(Paragraph("Sem criterios para este bloco.", st_small))
            story.append(Spacer(1, 8))
            continue

        header = [
            Paragraph("Criterios", st_small),
            Paragraph("Observacao e Inconformidades", st_small),
            Paragraph("Medidas Adotadas", st_small),
        ]
        rows = [header]
        for c in criterios:
            rows.append(
                [
                    _p(c.get("criterio") or "-"),
                    _p(c.get("observacao_inconformidades") or "-"),
                    _p(c.get("medidas_adotadas") or "-"),
                ]
            )

        crit_table = Table(
            rows,
            colWidths=[48 * mm, 72 * mm, 60 * mm],
            repeatRows=1,
        )
        crit_table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d8cfbc")),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eee4cf")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        story.append(crit_table)
        story.append(Spacer(1, 10))

    if assinatura_items:
        story.append(Paragraph("Assinaturas dos Responsaveis", st_h2))
        assinatura_cells: list[Any] = []
        for item in assinatura_items:
            linha = _xml_escape(str(item.get("linha") or "").strip())
            funcao = _xml_escape(str(item.get("funcao") or "").strip())
            assinatura_cells.append(
                Paragraph(
                    f"_______________________________<br/>{linha}<br/><font size='8'>{funcao}</font>",
                    st_small,
                )
            )
        assinatura_rows: list[list[Any]] = []
        for i in range(0, len(assinatura_cells), 2):
            left = assinatura_cells[i]
            right = assinatura_cells[i + 1] if (i + 1) < len(assinatura_cells) else Paragraph("", st_small)
            assinatura_rows.append([left, right])
        assinatura_table = Table(assinatura_rows, colWidths=[90 * mm, 90 * mm], repeatRows=0)
        assinatura_table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                    ("TOPPADDING", (0, 0), (-1, -1), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ]
            )
        )
        story.append(assinatura_table)

    def _draw_page_number(canvas_obj: Any, doc_obj: Any) -> None:
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(colors.HexColor("#5a4f39"))
        canvas_obj.drawRightString(195 * mm, 8 * mm, f"Pagina {canvas_obj.getPageNumber()}")

    try:
        doc.build(story, onFirstPage=_draw_page_number, onLaterPages=_draw_page_number)
        return buf.getvalue()
    except Exception:
        return _build_qr_monthly_report_pdf_fallback(report)
