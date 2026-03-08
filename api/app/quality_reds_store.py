from datetime import datetime, timezone
from typing import Any
import json

from sqlalchemy import text

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
      data_fato TEXT,
      mes INTEGER,
      ano INTEGER,
      data_hora_fechamento TEXT,
      data_hora_alteracao TEXT,
      data_hora_primeiro_fim_preenchimento TEXT,
      nome_tipo_relatorio TEXT,
      digitador_cargo_efetivo TEXT,
      digitador_matricula TEXT,
      digitador_nome TEXT,
      digitador_nome_unidade TEXT,
      natureza_codigo TEXT,
      natureza_descricao_longa TEXT,
      historico_ocorrencia TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(numero_ocorrencia)
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
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_data ON qr_raw_reds(data_fato)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_cia ON qr_raw_reds(cia_pm)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_raw_pelotao ON qr_raw_reds(pelotao_pm)"))

        conn.execute(text(sql_quality))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_data ON qr_quality_analysis(data_fato)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_cia ON qr_quality_analysis(cia_pm)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_quality_pelotao ON qr_quality_analysis(pelotao_pm)"))
        conn.execute(text(sql_eval))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_eval_avaliado ON qr_evaluation(avaliado)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qr_eval_updated ON qr_evaluation(updated_at)"))


def upsert_qr_raw_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
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
                "mes": row.get("mes"),
                "ano": row.get("ano"),
                "data_hora_fechamento": row.get("data_hora_fechamento"),
                "data_hora_alteracao": row.get("data_hora_alteracao"),
                "data_hora_primeiro_fim_preenchimento": row.get("data_hora_primeiro_fim_preenchimento"),
                "nome_tipo_relatorio": row.get("nome_tipo_relatorio"),
                "digitador_cargo_efetivo": row.get("digitador_cargo_efetivo"),
                "digitador_matricula": row.get("digitador_matricula"),
                "digitador_nome": row.get("digitador_nome"),
                "digitador_nome_unidade": row.get("digitador_nome_unidade"),
                "natureza_codigo": row.get("natureza_codigo"),
                "natureza_descricao_longa": row.get("natureza_descricao_longa"),
                "historico_ocorrencia": row.get("historico_ocorrencia"),
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
                INSERT INTO qr_raw_reds(
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_fato, mes, ano,
                  data_hora_fechamento, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa, historico_ocorrencia,
                  created_at, updated_at
                )
                VALUES(
                  :numero_ocorrencia, :cia_pm, :pelotao_pm, :data_hora_fato, :data_fato, :mes, :ano,
                  :data_hora_fechamento, :data_hora_alteracao, :data_hora_primeiro_fim_preenchimento,
                  :nome_tipo_relatorio, :digitador_cargo_efetivo, :digitador_matricula, :digitador_nome, :digitador_nome_unidade,
                  :natureza_codigo, :natureza_descricao_longa, :historico_ocorrencia,
                  :created_at, :updated_at
                )
                ON CONFLICT(numero_ocorrencia)
                DO UPDATE SET
                  cia_pm=excluded.cia_pm,
                  pelotao_pm=excluded.pelotao_pm,
                  data_hora_fato=excluded.data_hora_fato,
                  data_fato=excluded.data_fato,
                  mes=excluded.mes,
                  ano=excluded.ano,
                  data_hora_fechamento=excluded.data_hora_fechamento,
                  data_hora_alteracao=excluded.data_hora_alteracao,
                  data_hora_primeiro_fim_preenchimento=excluded.data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio=excluded.nome_tipo_relatorio,
                  digitador_cargo_efetivo=excluded.digitador_cargo_efetivo,
                  digitador_matricula=excluded.digitador_matricula,
                  digitador_nome=excluded.digitador_nome,
                  digitador_nome_unidade=excluded.digitador_nome_unidade,
                  natureza_codigo=excluded.natureza_codigo,
                  natureza_descricao_longa=excluded.natureza_descricao_longa,
                  historico_ocorrencia=excluded.historico_ocorrencia,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )
    return {"received": len(rows), "upserted": len(values)}


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
                  numero_ocorrencia, cia_pm, pelotao_pm, data_hora_fato, data_fato, mes, ano,
                  data_hora_fechamento, data_hora_alteracao, data_hora_primeiro_fim_preenchimento,
                  nome_tipo_relatorio, digitador_cargo_efetivo, digitador_matricula, digitador_nome, digitador_nome_unidade,
                  natureza_codigo, natureza_descricao_longa, historico_ocorrencia
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


_CV_CODES = {"E03012", "E03014"}
_W_PREFIXES = ("W01", "W02", "W03", "W04")


def _norm_natureza(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


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
                  r.data_fato,
                  r.natureza_codigo,
                  r.natureza_descricao_longa,
                  r.historico_ocorrencia,
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
            ):
                m[f] = bool(int(m.get(f) or 0)) if m.get(f) is not None else False
            out.append(m)
        return out


def get_qr_monthly_kpis(cia_pm: str, date_from: str | None = None, date_to: str | None = None) -> dict[str, Any]:
    rows = list_qr_review_rows(cia_pm=cia_pm, date_from=date_from, date_to=date_to, limit=50000, only_pending=False)
    total = len(rows)
    avaliados = sum(1 for r in rows if r.get("avaliado"))
    pct = round((avaliados * 100.0 / total), 2) if total else 0.0

    def is_cv(r: dict[str, Any]) -> bool:
        return _norm_natureza(r.get("natureza_codigo")) in _CV_CODES

    def is_w(r: dict[str, Any]) -> bool:
        nat = _norm_natureza(r.get("natureza_codigo"))
        return nat.startswith(_W_PREFIXES)

    def is_c99(r: dict[str, Any]) -> bool:
        nat = _norm_natureza(r.get("natureza_codigo"))
        return nat.startswith("C99")

    cv_total = sum(1 for r in rows if is_cv(r))
    cv_av = sum(1 for r in rows if is_cv(r) and r.get("avaliado"))
    w_total = sum(1 for r in rows if is_w(r))
    w_av = sum(1 for r in rows if is_w(r) and r.get("avaliado"))
    c99_total = sum(1 for r in rows if is_c99(r))
    c99_av = sum(1 for r in rows if is_c99(r) and r.get("avaliado"))

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
        "w_total": w_total,
        "w_avaliados": w_av,
        "c99_total": c99_total,
        "c99_avaliados": c99_av,
        "sinalizados_total": sinalizados_total,
        "sinalizados_avaliados": sinalizados_av,
    }


def build_qr_monthly_report(cia_pm: str, date_from: str | None = None, date_to: str | None = None) -> dict[str, Any]:
    k = get_qr_monthly_kpis(cia_pm=cia_pm, date_from=date_from, date_to=date_to)
    cia_label = _cia_label(str(cia_pm or ""))
    periodo = f"{date_from or 'inicio'} a {date_to or 'fim'}"
    t1 = (
        f"Comissão de avaliação de REDS da {cia_label} procedeu a análise de "
        f"{k['total_avaliados']} registro(s) de crimes violentos ocorridos na área da Cia "
        f"no período avaliado ({periodo}). A seguir serão descritas as inconformidades detectadas."
    )
    t2 = (
        f"A Comissão de avaliação de REDS da {cia_label} procedeu a análise de {k['total_avaliados']} "
        f"ocorrências referente a {k['percentual_avaliado']}% de todos os registros confeccionados na área "
        f"da Cia no período avaliado; de {k['cv_total']} REDS de natureza E03012, E03014; "
        f"{k['w_total']} BOS de natureza W01.000, W02.000, W03.000, W04.000 e; "
        f"{k['c99_total']} REDS de natureza C99.000. A seguir serão descritas inconformidades detectadas."
    )
    return {"kpis": k, "texto_modelo_1": t1, "texto_modelo_2": t2}
