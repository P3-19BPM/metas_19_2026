import json
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import settings

FACT_TABLE_BY_INDICADOR = {
    'BD_IMV': 'fact_imv',
    'BD_ICVPE': 'fact_icvpe',
    'BD_ICVPA': 'fact_icvpa',
}

FACT_COLUMNS = {
    'fact_imv': [
        'numero_ocorrencia', 'envolvimento_codigo', 'envolvimento_descricao', 'numero_envolvido', 'chave_envolvido',
        'nome_completo_envolvido', 'nome_mae', 'data_nascimento', 'letalidade', 'condicao_fisica_descricao',
        'natureza_ocorrencia_codigo', 'natureza_ocorrencia_descricao', 'ind_consumado', 'rpm_2024', 'ueop_2024',
        'unidade_area_militar_codigo', 'unidade_area_militar_nome', 'unidade_responsavel_registro_codigo',
        'unidade_responsavel_registro_nome', 'latitude_sirgas2000', 'longitude_sirgas2000', 'situacao_zona',
        'tipo_descricao', 'codigo_municipio', 'nome_municipio', 'tipo_logradouro_descricao', 'logradouro_nome',
        'numero_endereco', 'nome_bairro', 'ocorrencia_uf', 'numero_latitude', 'numero_longitude', 'data_hora_fato',
        'ano', 'mes', 'nome_tipo_relatorio', 'digitador_sigla_orgao', 'udi', 'ueop', 'cia', 'codigo_espacial_pm',
        'cia_pel_final', 'geo_name', 'geo_ueop', 'geo_cia_pm', 'geo_pelotao', 'geo_cd_municipio',
        'geo_nm_municipio', 'geo_setor', 'geo_municipio', 'geo_area', 'geo_subsetor', 'created_at', 'updated_at',
    ],
    'fact_icvpe': [
        'numero_ocorrencia', 'envolvimento_codigo', 'envolvimento_descricao', 'numero_envolvido',
        'nome_completo_envolvido', 'nome_mae', 'data_nascimento', 'ind_militar_policial_servico',
        'condicao_fisica_descricao', 'natureza_ocorrencia_codigo', 'natureza_ocorrencia_descricao', 'ind_consumado',
        'rpm_2024', 'ueop_2024', 'unidade_area_militar_codigo', 'unidade_area_militar_nome',
        'unidade_responsavel_registro_codigo', 'unidade_responsavel_registro_nome', 'latitude_sirgas2000',
        'longitude_sirgas2000', 'numero_latitude', 'numero_longitude', 'situacao_zona', 'tipo_descricao',
        'codigo_municipio', 'nome_municipio', 'tipo_logradouro_descricao', 'logradouro_nome', 'numero_endereco',
        'nome_bairro', 'ocorrencia_uf', 'data_hora_fato', 'ano', 'mes', 'nome_tipo_relatorio',
        'digitador_sigla_orgao', 'udi', 'ueop', 'cia', 'codigo_espacial_pm', 'cia_pel_final', 'geo_name',
        'geo_ueop', 'geo_cia_pm', 'geo_pelotao', 'geo_setor', 'geo_subsetor', 'geo_cd_municipio',
        'geo_nm_municipio', 'geo_municipio', 'geo_area', 'created_at', 'updated_at',
    ],
    'fact_icvpa': [
        'numero_ocorrencia', 'envolvimento_codigo', 'envolvimento_descricao', 'numero_envolvido',
        'nome_completo_envolvido', 'nome_mae', 'data_nascimento', 'condicao_fisica_descricao',
        'natureza_ocorrencia_codigo', 'natureza_ocorrencia_descricao', 'ind_consumado', 'rpm_2024', 'ueop_2024',
        'unidade_area_militar_codigo', 'unidade_area_militar_nome', 'unidade_responsavel_registro_codigo',
        'unidade_responsavel_registro_nome', 'latitude_sirgas2000', 'longitude_sirgas2000', 'numero_latitude',
        'numero_longitude', 'situacao_zona', 'tipo_descricao', 'codigo_municipio', 'nome_municipio',
        'tipo_logradouro_descricao', 'logradouro_nome', 'numero_endereco', 'nome_bairro', 'ocorrencia_uf',
        'data_hora_fato', 'ano', 'mes', 'nome_tipo_relatorio', 'digitador_sigla_orgao', 'udi', 'ueop', 'cia',
        'codigo_espacial_pm', 'cia_pel_final', 'geo_name', 'geo_ueop', 'geo_cia_pm', 'geo_pelotao', 'geo_setor',
        'geo_subsetor', 'geo_cd_municipio', 'geo_nm_municipio', 'geo_municipio', 'geo_area', 'created_at',
        'updated_at',
    ],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _is_postgres() -> bool:
    return settings.db_backend == 'postgres' or bool(settings.postgres_url) or bool(settings.postgres_host)


def _postgres_url() -> str:
    if settings.postgres_url:
        return settings.postgres_url

    required = {
        'POSTGRES_HOST': settings.postgres_host,
        'POSTGRES_DB': settings.postgres_db,
        'POSTGRES_USER': settings.postgres_user,
        'POSTGRES_PASSWORD': settings.postgres_password,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Configuracao Postgres incompleta. Faltando: {', '.join(missing)}")

    user = quote_plus(settings.postgres_user)
    pwd = quote_plus(settings.postgres_password)
    host = settings.postgres_host
    port = settings.postgres_port or '5432'
    db = settings.postgres_db
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    if _is_postgres():
        return create_engine(_postgres_url(), pool_pre_ping=True)

    db_path = Path(settings.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f'sqlite:///{db_path}', connect_args={'check_same_thread': False}, pool_pre_ping=True)


def _rows_to_dicts(result) -> list[dict[str, Any]]:
    return [dict(r._mapping) for r in result]


def _row_to_dict(row) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    return dict(row._mapping)


def init_db() -> None:
    sql_name = 'init_postgres.sql' if _is_postgres() else 'init.sql'
    sql_path = Path(__file__).resolve().parents[1] / 'sql' / sql_name
    script = sql_path.read_text(encoding='utf-8')

    engine = get_engine()
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        if _is_postgres():
            cur.execute(script)
        else:
            cur.executescript(script)
        raw.commit()
    finally:
        raw.close()

    _apply_runtime_migrations()


def ensure_agent(agent_id: str, version: Optional[str] = None) -> None:
    now = utc_now_iso()
    q = text(
        """
        INSERT INTO agent_status(agent_id, version, status, online_since, last_seen, updated_at)
        VALUES(:agent_id, :version, 'online', :online_since, :last_seen, :updated_at)
        ON CONFLICT(agent_id) DO UPDATE SET
            version=excluded.version,
            status='online',
            last_seen=excluded.last_seen,
            updated_at=excluded.updated_at,
            online_since=CASE
                WHEN agent_status.status='online' THEN agent_status.online_since
                ELSE excluded.online_since
            END
        """
    )
    with get_engine().begin() as conn:
        row = conn.execute(
            text('SELECT agent_id, online_since FROM agent_status WHERE agent_id = :agent_id'),
            {'agent_id': agent_id},
        ).first()
        online_since = row._mapping.get('online_since') if row else now
        conn.execute(
            q,
            {
                'agent_id': agent_id,
                'version': version,
                'online_since': online_since or now,
                'last_seen': now,
                'updated_at': now,
            },
        )


def mark_stale_agents(stale_seconds: int) -> None:
    now_dt = datetime.now(timezone.utc)
    stale_ids: list[str] = []
    with get_engine().begin() as conn:
        rows = conn.execute(text("SELECT agent_id, last_seen FROM agent_status WHERE status='online'"))
        for row in rows:
            last_seen = str(row._mapping.get('last_seen') or '').strip()
            if not last_seen:
                continue
            try:
                dt = datetime.fromisoformat(last_seen.replace('Z', '+00:00'))
            except ValueError:
                continue
            delta = (now_dt - dt).total_seconds()
            if delta > stale_seconds:
                stale_ids.append(str(row._mapping['agent_id']))

        for agent_id in stale_ids:
            conn.execute(
                text("UPDATE agent_status SET status='offline', updated_at=:updated_at WHERE agent_id=:agent_id"),
                {'updated_at': utc_now_iso(), 'agent_id': agent_id},
            )


def create_job(job_type: str, requested_by: str, payload: dict[str, Any], available_after: Optional[str] = None) -> int:
    now = utc_now_iso()
    available_after = available_after or now
    payload_json = json.dumps(payload, ensure_ascii=False)

    with get_engine().begin() as conn:
        if _is_postgres():
            row = conn.execute(
                text(
                    """
                    INSERT INTO jobs(job_type, status, requested_by, requested_at, available_after, payload_json)
                    VALUES(:job_type, 'pending', :requested_by, :requested_at, :available_after, :payload_json)
                    RETURNING id
                    """
                ),
                {
                    'job_type': job_type,
                    'requested_by': requested_by,
                    'requested_at': now,
                    'available_after': available_after,
                    'payload_json': payload_json,
                },
            ).first()
            return int(row._mapping['id'])

        conn.execute(
            text(
                """
                INSERT INTO jobs(job_type, status, requested_by, requested_at, available_after, payload_json)
                VALUES(:job_type, 'pending', :requested_by, :requested_at, :available_after, :payload_json)
                """
            ),
            {
                'job_type': job_type,
                'requested_by': requested_by,
                'requested_at': now,
                'available_after': available_after,
                'payload_json': payload_json,
            },
        )
        row = conn.execute(text('SELECT last_insert_rowid() AS id')).first()
        return int(row._mapping['id'])


def get_next_job() -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT * FROM jobs
                WHERE status='pending'
                  AND (available_after IS NULL OR available_after <= :now)
                ORDER BY id ASC
                LIMIT 1
                """
            ),
            {'now': utc_now_iso()},
        ).first()
    return _row_to_dict(row)


def set_job_started(job_id: int, agent_id: str) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE jobs
                SET status='running', started_at=:started_at, agent_id=:agent_id
                WHERE id=:job_id AND status='pending'
                """
            ),
            {'started_at': utc_now_iso(), 'agent_id': agent_id, 'job_id': job_id},
        )


def set_job_result(job_id: int, success: bool, result: dict[str, Any], error: Optional[str] = None) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE jobs
                SET status=:status, finished_at=:finished_at, result_json=:result_json, error_text=:error_text
                WHERE id=:job_id
                """
            ),
            {
                'status': 'success' if success else 'failed',
                'finished_at': utc_now_iso(),
                'result_json': json.dumps(result, ensure_ascii=False),
                'error_text': error,
                'job_id': job_id,
            },
        )


def purge_kpi_rows(indicador: str, date_from: str, date_to: str) -> int:
    if not indicador or not date_from or not date_to:
        return 0
    with get_engine().begin() as conn:
        cur = conn.execute(
            text(
                'DELETE FROM kpi_daily WHERE indicador = :indicador AND referencia_data >= :date_from AND referencia_data <= :date_to'
            ),
            {'indicador': indicador, 'date_from': date_from, 'date_to': date_to},
        )
        return int(cur.rowcount or 0)


def purge_reds_rows(indicador: str, date_from: str, date_to: str) -> int:
    if not indicador or not date_from or not date_to:
        return 0
    with get_engine().begin() as conn:
        cur = conn.execute(
            text(
                'DELETE FROM reds_events WHERE indicador = :indicador AND referencia_data >= :date_from AND referencia_data <= :date_to'
            ),
            {'indicador': indicador, 'date_from': date_from, 'date_to': date_to},
        )
        return int(cur.rowcount or 0)


def _fact_table_for_indicador(indicador: str) -> str | None:
    key = str(indicador or '').strip().upper()
    return FACT_TABLE_BY_INDICADOR.get(key)


def _table_exists(table: str) -> bool:
    if not table:
        return False
    with get_engine().begin() as conn:
        if _is_postgres():
            row = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = :table
                    LIMIT 1
                    """
                ),
                {'table': table},
            ).first()
            return row is not None

        row = conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:table LIMIT 1"),
            {'table': table},
        ).first()
        return row is not None


@lru_cache(maxsize=32)
def _get_table_columns(table: str) -> list[str]:
    if not table:
        return []
    with get_engine().begin() as conn:
        if _is_postgres():
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table
                    ORDER BY ordinal_position
                    """
                ),
                {'table': table},
            )
            return [str(r._mapping['column_name']) for r in rows]

        rows = conn.execute(text(f'PRAGMA table_info({table})'))
        return [str(r._mapping['name']) for r in rows]


def _ensure_column(table: str, column: str, ddl_sqlite: str, ddl_postgres: str) -> None:
    if not _table_exists(table):
        return
    cols = set(_get_table_columns(table))
    if column in cols:
        return
    with get_engine().begin() as conn:
        conn.execute(text(ddl_postgres if _is_postgres() else ddl_sqlite))
    _get_table_columns.cache_clear()


def _apply_runtime_migrations() -> None:
    # Existing installations need ALTERs because init.sql only runs CREATE IF NOT EXISTS.
    _ensure_column(
        'reds_events',
        'qtd_presos',
        "ALTER TABLE reds_events ADD COLUMN qtd_presos INTEGER",
        "ALTER TABLE reds_events ADD COLUMN IF NOT EXISTS qtd_presos INTEGER",
    )

    _ensure_column(
        'web_users',
        'posto_graduacao',
        "ALTER TABLE web_users ADD COLUMN posto_graduacao TEXT",
        "ALTER TABLE web_users ADD COLUMN IF NOT EXISTS posto_graduacao TEXT",
    )

    _ensure_column(
        'web_users',
        'must_change_password',
        "ALTER TABLE web_users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE web_users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT TRUE",
    )

    sql_access_requests = """
    CREATE TABLE IF NOT EXISTS web_access_requests (
      id {id_type} PRIMARY KEY{identity},
      nome TEXT NOT NULL,
      numero_policia TEXT NOT NULL,
      unidade_setor TEXT,
      telefone TEXT,
      motivo TEXT,
      status TEXT NOT NULL DEFAULT 'pendente',
      created_at TEXT NOT NULL,
      processed_at TEXT
    )
    """
    if _is_postgres():
        create_sql = sql_access_requests.format(id_type='BIGSERIAL', identity='')
    else:
        create_sql = sql_access_requests.format(id_type='INTEGER', identity=' AUTOINCREMENT')
    with get_engine().begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_web_access_requests_status ON web_access_requests(status)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_web_access_requests_created_at ON web_access_requests(created_at)'))

    sql_mapa_commanders = """
    CREATE TABLE IF NOT EXISTS mapa_commanders (
      id {id_type} PRIMARY KEY{identity},
      numero_policia TEXT NOT NULL,
      posto_graduacao TEXT,
      nome_guerra TEXT NOT NULL,
      nome_completo TEXT,
      telefone TEXT,
      email TEXT,
      foto_url TEXT,
      observacoes TEXT,
      ativo {bool_type} NOT NULL DEFAULT {bool_true},
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """
    sql_mapa_assignments = """
    CREATE TABLE IF NOT EXISTS mapa_command_assignments (
      id {id_type} PRIMARY KEY{identity},
      commander_id {ref_id_type} NOT NULL,
      scope_type TEXT NOT NULL,
      scope_code TEXT NOT NULL,
      scope_label TEXT NOT NULL,
      cia_code TEXT,
      pelotao_code TEXT,
      setor_code TEXT,
      subsetor_code TEXT,
      municipio_nome TEXT,
      role_kind TEXT NOT NULL DEFAULT 'titular',
      situacao TEXT NOT NULL DEFAULT 'ativo',
      motivo TEXT,
      data_inicio TEXT NOT NULL,
      data_fim TEXT,
      data_cadastro TEXT NOT NULL,
      cadastrado_por TEXT,
      updated_at TEXT NOT NULL,
      atualizado_por TEXT
    )
    """
    sql_mapa_scope_metadata = """
    CREATE TABLE IF NOT EXISTS mapa_scope_metadata (
      id {id_type} PRIMARY KEY{identity},
      scope_type TEXT NOT NULL,
      scope_code TEXT NOT NULL,
      cia_code TEXT,
      municipio_nome TEXT,
      populacao_municipio BIGINT,
      efetivo_fracao INTEGER,
      updated_at TEXT NOT NULL,
      atualizado_por TEXT,
      UNIQUE(scope_type, scope_code)
    )
    """
    if _is_postgres():
        mapa_cmd_sql = sql_mapa_commanders.format(
            id_type='BIGSERIAL', identity='', bool_type='BOOLEAN', bool_true='TRUE'
        )
        mapa_asg_sql = sql_mapa_assignments.format(id_type='BIGSERIAL', identity='', ref_id_type='BIGINT')
        mapa_meta_sql = sql_mapa_scope_metadata.format(id_type='BIGSERIAL', identity='')
    else:
        mapa_cmd_sql = sql_mapa_commanders.format(
            id_type='INTEGER', identity=' AUTOINCREMENT', bool_type='INTEGER', bool_true='1'
        )
        mapa_asg_sql = sql_mapa_assignments.format(
            id_type='INTEGER', identity=' AUTOINCREMENT', ref_id_type='INTEGER'
        )
        mapa_meta_sql = sql_mapa_scope_metadata.format(id_type='INTEGER', identity=' AUTOINCREMENT')
    with get_engine().begin() as conn:
        conn.execute(text(mapa_cmd_sql))
        conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS idx_mapa_commanders_numero_policia ON mapa_commanders(numero_policia)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_commanders_ativo ON mapa_commanders(ativo)'))
        conn.execute(text(mapa_asg_sql))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_assign_scope ON mapa_command_assignments(scope_type, scope_code)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_assign_scope_open ON mapa_command_assignments(scope_type, scope_code, data_fim)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_assign_cia ON mapa_command_assignments(cia_code)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_assign_role ON mapa_command_assignments(role_kind, situacao)'))
        conn.execute(text(mapa_meta_sql))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_mapa_meta_scope ON mapa_scope_metadata(scope_type, scope_code)'))


def purge_fact_rows(indicador: str, date_from: str, date_to: str) -> int:
    table = _fact_table_for_indicador(indicador)
    if not table or not date_from or not date_to:
        return 0
    if not _table_exists(table):
        return 0
    with get_engine().begin() as conn:
        cur = conn.execute(
            text(
                f"""
                DELETE FROM {table}
                WHERE data_hora_fato IS NOT NULL
                  AND CAST(data_hora_fato AS DATE) >= :date_from
                  AND CAST(data_hora_fato AS DATE) <= :date_to
                """
            ),
            {'date_from': date_from, 'date_to': date_to},
        )
        return int(cur.rowcount or 0)


def upsert_kpi_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {'received': 0, 'upserted': 0}

    now = utc_now_iso()
    values = [
        {
            'referencia_data': row.get('referencia_data'),
            'indicador': row.get('indicador'),
            'nivel': row.get('nivel'),
            'unidade_id': row.get('unidade_id'),
            'unidade_nome': row.get('unidade_nome'),
            'valor_realizado': float(row.get('valor_realizado', 0) or 0),
            'valor_meta': row.get('valor_meta'),
            'valor_plr': row.get('valor_plr'),
            'created_at': row.get('created_at') or now,
            'updated_at': row.get('updated_at') or now,
        }
        for row in rows
    ]

    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO kpi_daily(
                  referencia_data, indicador, nivel, unidade_id, unidade_nome,
                  valor_realizado, valor_meta, valor_plr, created_at, updated_at
                )
                VALUES(
                  :referencia_data, :indicador, :nivel, :unidade_id, :unidade_nome,
                  :valor_realizado, :valor_meta, :valor_plr, :created_at, :updated_at
                )
                ON CONFLICT(referencia_data, indicador, nivel, unidade_id)
                DO UPDATE SET
                  unidade_nome=excluded.unidade_nome,
                  valor_realizado=excluded.valor_realizado,
                  valor_meta=excluded.valor_meta,
                  valor_plr=excluded.valor_plr,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )

    return {'received': len(rows), 'upserted': len(values)}


def get_kpi_summary(
    indicador: str,
    nivel: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 20,
    unidade_id: str | None = None,
) -> dict[str, Any]:
    where = ['indicador = :indicador', 'nivel = :nivel']
    params: dict[str, Any] = {'indicador': indicador, 'nivel': nivel, 'limit': limit}
    if date_from:
        where.append('referencia_data >= :date_from')
        params['date_from'] = date_from
    if date_to:
        where.append('referencia_data <= :date_to')
        params['date_to'] = date_to
    if unidade_id:
        where.append('unidade_id = :unidade_id')
        params['unidade_id'] = unidade_id
    where_sql = ' AND '.join(where)

    with get_engine().begin() as conn:
        total = conn.execute(
            text(f'SELECT COALESCE(SUM(valor_realizado),0) AS total_realizado FROM kpi_daily WHERE {where_sql}'),
            params,
        ).first()

        by_unit = conn.execute(
            text(
                f"""
                SELECT
                  unidade_id,
                  unidade_nome,
                  COALESCE(SUM(valor_realizado),0) AS total_realizado,
                  MAX(updated_at) AS last_update
                FROM kpi_daily
                WHERE {where_sql}
                GROUP BY unidade_id, unidade_nome
                ORDER BY total_realizado DESC
                LIMIT :limit
                """
            ),
            params,
        )

        last_update = conn.execute(
            text(f'SELECT MAX(updated_at) AS last_update FROM kpi_daily WHERE {where_sql}'),
            params,
        ).first()

    total_realizado = float(total._mapping['total_realizado'] if total else 0)
    return {
        'indicador': indicador,
        'nivel': nivel,
        'unidade_id': unidade_id,
        'total_realizado': total_realizado,
        'last_update': (last_update._mapping['last_update'] if last_update else None),
        'unidades': _rows_to_dicts(by_unit),
    }


def get_kpi_monthly(indicador: str, nivel: str, ano: int, unidade_id: str | None = None) -> dict[str, Any]:
    if _is_postgres():
        year_clause = 'EXTRACT(YEAR FROM TO_DATE(referencia_data, \'YYYY-MM-DD\')) = :ano'
        month_expr = 'EXTRACT(MONTH FROM TO_DATE(referencia_data, \'YYYY-MM-DD\'))'
    else:
        year_clause = 'substr(referencia_data, 1, 4) = :ano_txt'
        month_expr = 'CAST(substr(referencia_data, 6, 2) AS INTEGER)'

    where = ['indicador = :indicador', 'nivel = :nivel', year_clause]
    params: dict[str, Any] = {'indicador': indicador, 'nivel': nivel, 'ano': ano, 'ano_txt': str(ano)}
    if unidade_id:
        where.append('unidade_id = :unidade_id')
        params['unidade_id'] = unidade_id
    where_sql = ' AND '.join(where)

    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  {month_expr} AS mes,
                  COALESCE(SUM(valor_realizado),0) AS valor_realizado
                FROM kpi_daily
                WHERE {where_sql}
                GROUP BY {month_expr}
                ORDER BY mes
                """
            ),
            params,
        )
        rows_dict = _rows_to_dicts(rows)

    values = {int(r['mes']): float(r['valor_realizado']) for r in rows_dict}
    series = [{'mes': m, 'valor_realizado': values.get(m, 0.0)} for m in range(1, 13)]
    return {'indicador': indicador, 'nivel': nivel, 'ano': ano, 'unidade_id': unidade_id, 'series': series}


def get_kpi_units(
    indicador: str,
    nivel: str,
    date_from: str | None = None,
    date_to: str | None = None,
    search: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    where = ['indicador = :indicador', 'nivel = :nivel']
    params: dict[str, Any] = {'indicador': indicador, 'nivel': nivel, 'limit': limit}
    if date_from:
        where.append('referencia_data >= :date_from')
        params['date_from'] = date_from
    if date_to:
        where.append('referencia_data <= :date_to')
        params['date_to'] = date_to
    if search:
        where.append('(unidade_nome LIKE :search OR unidade_id LIKE :search)')
        params['search'] = f'%{search}%'
    where_sql = ' AND '.join(where)

    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT unidade_id, unidade_nome, COALESCE(SUM(valor_realizado),0) AS total_realizado
                FROM kpi_daily
                WHERE {where_sql}
                GROUP BY unidade_id, unidade_nome
                ORDER BY unidade_nome
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)
    return {'indicador': indicador, 'nivel': nivel, 'total': len(out), 'unidades': out}


def get_kpi_stats() -> dict[str, Any]:
    with get_engine().begin() as conn:
        total = conn.execute(text('SELECT COUNT(*) AS c FROM kpi_daily')).first()
        por_indicador = conn.execute(
            text(
                """
                SELECT indicador, nivel, COUNT(*) AS total
                FROM kpi_daily
                GROUP BY indicador, nivel
                ORDER BY indicador, nivel
                """
            )
        )
        ref_range = conn.execute(
            text('SELECT MIN(referencia_data) AS min_ref, MAX(referencia_data) AS max_ref FROM kpi_daily')
        ).first()
        last_update = conn.execute(text('SELECT MAX(updated_at) AS last_update FROM kpi_daily')).first()

    return {
        'total': int(total._mapping['c'] if total else 0),
        'por_indicador': [
            {'indicador': row['indicador'], 'nivel': row['nivel'], 'total': int(row['total'])}
            for row in _rows_to_dicts(por_indicador)
        ],
        'min_referencia_data': ref_range._mapping['min_ref'] if ref_range else None,
        'max_referencia_data': ref_range._mapping['max_ref'] if ref_range else None,
        'last_update': last_update._mapping['last_update'] if last_update else None,
    }


def upsert_reds_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    if not rows:
        return {'received': 0, 'upserted': 0}

    now = utc_now_iso()
    values = []
    for row in rows:
        envolvido_key = f"{row.get('numero_envolvido') or ''}::{row.get('chave_envolvido') or ''}"
        values.append(
            {
                'indicador': row.get('indicador'),
                'numero_ocorrencia': row.get('numero_ocorrencia'),
                'numero_envolvido': row.get('numero_envolvido'),
                'chave_envolvido': row.get('chave_envolvido'),
                'envolvido_key': envolvido_key,
                'data_hora_fato': row.get('data_hora_fato'),
                'referencia_data': row.get('referencia_data'),
                'natureza_codigo': row.get('natureza_codigo'),
                'natureza_descricao': row.get('natureza_descricao'),
                'municipio_codigo': row.get('municipio_codigo'),
                'municipio_nome': row.get('municipio_nome'),
                'CIA_PM': row.get('CIA_PM'),
                'PELOTAO': row.get('PELOTAO'),
                'SETOR': row.get('SETOR'),
                'SUBSETOR': row.get('SUBSETOR'),
                'latitude': row.get('latitude'),
                'longitude': row.get('longitude'),
                'qtd_presos': row.get('qtd_presos'),
                'created_at': row.get('created_at') or now,
                'updated_at': row.get('updated_at') or now,
            }
        )

    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO reds_events(
                  indicador, numero_ocorrencia, numero_envolvido, chave_envolvido,
                  envolvido_key,
                  data_hora_fato, referencia_data, natureza_codigo, natureza_descricao,
                  municipio_codigo, municipio_nome,
                  CIA_PM, PELOTAO, SETOR, SUBSETOR,
                  latitude, longitude, qtd_presos,
                  created_at, updated_at
                )
                VALUES(
                  :indicador, :numero_ocorrencia, :numero_envolvido, :chave_envolvido,
                  :envolvido_key,
                  :data_hora_fato, :referencia_data, :natureza_codigo, :natureza_descricao,
                  :municipio_codigo, :municipio_nome,
                  :CIA_PM, :PELOTAO, :SETOR, :SUBSETOR,
                  :latitude, :longitude, :qtd_presos,
                  :created_at, :updated_at
                )
                ON CONFLICT(indicador, numero_ocorrencia, envolvido_key)
                DO UPDATE SET
                  data_hora_fato=excluded.data_hora_fato,
                  referencia_data=excluded.referencia_data,
                  natureza_codigo=excluded.natureza_codigo,
                  natureza_descricao=excluded.natureza_descricao,
                  municipio_codigo=excluded.municipio_codigo,
                  municipio_nome=excluded.municipio_nome,
                  CIA_PM=excluded.CIA_PM,
                  PELOTAO=excluded.PELOTAO,
                  SETOR=excluded.SETOR,
                  SUBSETOR=excluded.SUBSETOR,
                  latitude=excluded.latitude,
                  longitude=excluded.longitude,
                  qtd_presos=excluded.qtd_presos,
                  updated_at=excluded.updated_at
                """
            ),
            values,
        )

    return {'received': len(rows), 'upserted': len(values)}


def upsert_fact_rows(indicador: str, rows: list[dict[str, Any]]) -> dict[str, int]:
    table = _fact_table_for_indicador(indicador)
    if not table:
        return {'received': len(rows or []), 'upserted': 0}
    if not _table_exists(table):
        return {'received': len(rows or []), 'upserted': 0, 'table': table, 'warning': 'table_not_found'}
    if not rows:
        return {'received': 0, 'upserted': 0}

    table_columns = _get_table_columns(table)
    if not table_columns:
        return {'received': len(rows), 'upserted': 0}

    now = utc_now_iso()
    values: list[dict[str, Any]] = []
    for row in rows:
        rec: dict[str, Any] = {}
        for col in table_columns:
            src_col = col
            # Compatibilidade com schemas antigos (rpm/ueop 2025).
            if col in {'rpm_2025', 'ueop_2025'}:
                src_col = col.replace('_2025', '_2024')
            elif col in {'rpm_2024', 'ueop_2024'} and row.get(col) in (None, ''):
                alt = col.replace('_2024', '_2025')
                if row.get(alt) not in (None, ''):
                    src_col = alt

            val = row.get(src_col)
            if isinstance(val, str):
                sval = val.strip()
                val = None if sval == '' or sval.lower() in {'null', 'none', 'nan'} else sval
            rec[col] = val

        if 'created_at' in rec and not rec['created_at']:
            rec['created_at'] = now
        if 'updated_at' in rec and not rec['updated_at']:
            rec['updated_at'] = now
        values.append(rec)

    col_sql = ', '.join(table_columns)
    val_sql = ', '.join([f':{c}' for c in table_columns])
    with get_engine().begin() as conn:
        conn.execute(
            text(f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql})"),
            values,
        )
    return {'received': len(rows), 'upserted': len(values), 'table': table}


def _first_existing_column(available: set[str], candidates: list[str]) -> str | None:
    for col in candidates:
        if col in available:
            return col
    return None


def _query_reds_from_fact(
    indicador: str,
    date_from: str | None = None,
    date_to: str | None = None,
    nivel: str | None = None,
    unidade_id: str | None = None,
    limit: int = 20000,
) -> list[dict[str, Any]]:
    table = _fact_table_for_indicador(indicador)
    if not table or not _table_exists(table):
        return []

    available = set(_get_table_columns(table))
    if not available or 'numero_ocorrencia' not in available:
        return []

    # Maps normalized response fields used by the frontend to fact_* column names.
    date_col = _first_existing_column(available, ['data_hora_fato'])
    nat_code_col = _first_existing_column(available, ['natureza_ocorrencia_codigo', 'natureza_codigo'])
    nat_desc_col = _first_existing_column(available, ['natureza_ocorrencia_descricao', 'natureza_descricao'])
    mun_code_col = _first_existing_column(available, ['codigo_municipio', 'municipio_codigo', 'geo_cd_municipio'])
    mun_name_col = _first_existing_column(available, ['nome_municipio', 'municipio_nome', 'geo_nm_municipio'])
    cia_col = _first_existing_column(available, ['geo_cia_pm', 'cia'])
    pelotao_col = _first_existing_column(available, ['geo_pelotao', 'pelotao', 'cia_pel_final'])
    setor_col = _first_existing_column(available, ['geo_setor', 'setor'])
    subsetor_col = _first_existing_column(available, ['geo_subsetor', 'subsetor'])
    lat_col = _first_existing_column(available, ['numero_latitude', 'latitude_sirgas2000', 'latitude'])
    lon_col = _first_existing_column(available, ['numero_longitude', 'longitude_sirgas2000', 'longitude'])
    qtd_presos_col = _first_existing_column(available, ['qtd_presos'])

    def agg_expr(col: str | None, alias: str) -> str:
        if not col:
            return f'NULL AS {alias}'
        return f'MAX({col}) AS {alias}'

    where = ['numero_ocorrencia IS NOT NULL']
    params: dict[str, Any] = {'limit': limit}
    if date_col and date_from:
        where.append(f'CAST({date_col} AS DATE) >= :date_from')
        params['date_from'] = date_from
    if date_col and date_to:
        where.append(f'CAST({date_col} AS DATE) <= :date_to')
        params['date_to'] = date_to

    if nivel and unidade_id:
        unit_col_map = {
            'CIA_PM': [cia_col],
            'PELOTAO': [pelotao_col],
            'SETOR': [setor_col],
            'SUBSETOR': [subsetor_col],
            'MUNICIPIO': [mun_code_col],
        }
        candidates = [c for c in (unit_col_map.get(str(nivel).upper()) or []) if c]
        if candidates:
            where.append(f'{candidates[0]} = :unidade_id')
            params['unidade_id'] = unidade_id

    where_sql = ' AND '.join(where)
    order_expr = f'MAX({date_col}) ASC' if date_col else 'numero_ocorrencia ASC'
    sql = f"""
        SELECT
          numero_ocorrencia,
          {agg_expr(date_col, 'data_hora_fato')},
          {agg_expr(nat_code_col, 'natureza_codigo')},
          {agg_expr(nat_desc_col, 'natureza_descricao')},
          {agg_expr(mun_code_col, 'municipio_codigo')},
          {agg_expr(mun_name_col, 'municipio_nome')},
          {agg_expr(cia_col, 'CIA_PM')},
          {agg_expr(pelotao_col, 'PELOTAO')},
          {agg_expr(setor_col, 'SETOR')},
          {agg_expr(subsetor_col, 'SUBSETOR')},
          {agg_expr(lat_col, 'latitude')},
          {agg_expr(lon_col, 'longitude')},
          {agg_expr(qtd_presos_col, 'qtd_presos')}
        FROM {table}
        WHERE {where_sql}
        GROUP BY numero_ocorrencia
        ORDER BY {order_expr}
        LIMIT :limit
    """
    with get_engine().begin() as conn:
        rows = conn.execute(text(sql), params)
        return _rows_to_dicts(rows)


def query_reds(
    indicador: str,
    date_from: str | None = None,
    date_to: str | None = None,
    nivel: str | None = None,
    unidade_id: str | None = None,
    limit: int = 20000,
) -> list[dict[str, Any]]:
    where = ['indicador = :indicador']
    params: dict[str, Any] = {'indicador': indicador, 'limit': limit}
    if date_from:
        where.append('referencia_data >= :date_from')
        params['date_from'] = date_from
    if date_to:
        where.append('referencia_data <= :date_to')
        params['date_to'] = date_to

    if nivel and unidade_id:
        col_map = {
            'CIA_PM': 'CIA_PM',
            'PELOTAO': 'PELOTAO',
            'SETOR': 'SETOR',
            'SUBSETOR': 'SUBSETOR',
            'MUNICIPIO': 'municipio_codigo',
        }
        col = col_map.get(str(nivel).upper())
        if col:
            where.append(f'{col} = :unidade_id')
            params['unidade_id'] = unidade_id

    where_sql = ' AND '.join(where)
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  numero_ocorrencia,
                  MAX(data_hora_fato) AS data_hora_fato,
                  MAX(natureza_codigo) AS natureza_codigo,
                  MAX(natureza_descricao) AS natureza_descricao,
                  MAX(municipio_codigo) AS municipio_codigo,
                  MAX(municipio_nome) AS municipio_nome,
                  MAX(CIA_PM) AS CIA_PM,
                  MAX(PELOTAO) AS PELOTAO,
                  MAX(SETOR) AS SETOR,
                  MAX(SUBSETOR) AS SUBSETOR,
                  MAX(latitude) AS latitude,
                  MAX(longitude) AS longitude,
                  MAX(qtd_presos) AS qtd_presos
                FROM reds_events
                WHERE {where_sql}
                GROUP BY numero_ocorrencia
                ORDER BY MAX(data_hora_fato) ASC
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)
        if out:
            return out

    # Fallback: if reds_events is empty, return grouped occurrences from fact_* table.
    return _query_reds_from_fact(
        indicador=indicador,
        date_from=date_from,
        date_to=date_to,
        nivel=nivel,
        unidade_id=unidade_id,
        limit=limit,
    )


def query_reds_cia_presos_summary(
    indicador: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = ['indicador = :indicador']
    params: dict[str, Any] = {'indicador': indicador, 'limit': int(limit)}
    if date_from:
        where.append('referencia_data >= :date_from')
        params['date_from'] = date_from
    if date_to:
        where.append('referencia_data <= :date_to')
        params['date_to'] = date_to
    where_sql = ' AND '.join(where)

    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  COALESCE(NULLIF(TRIM(CIA_PM), ''), 'SEM_CIA') AS cia_pm,
                  COUNT(DISTINCT numero_ocorrencia) AS total_registros,
                  COUNT(DISTINCT CASE WHEN COALESCE(qtd_presos, 0) > 0 THEN numero_ocorrencia END) AS com_preso,
                  COUNT(DISTINCT CASE WHEN COALESCE(qtd_presos, 0) <= 0 OR qtd_presos IS NULL THEN numero_ocorrencia END) AS sem_preso
                FROM reds_events
                WHERE {where_sql}
                GROUP BY COALESCE(NULLIF(TRIM(CIA_PM), ''), 'SEM_CIA')
                ORDER BY total_registros DESC, cia_pm ASC
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)

    for row in out:
        row['cia_pm'] = str(row.get('cia_pm') or 'SEM_CIA')
        row['total_registros'] = int(row.get('total_registros') or 0)
        row['com_preso'] = int(row.get('com_preso') or 0)
        row['sem_preso'] = int(row.get('sem_preso') or 0)
    return out


def get_overview() -> dict[str, Any]:
    with get_engine().begin() as conn:
        agent = conn.execute(text('SELECT * FROM agent_status ORDER BY updated_at DESC LIMIT 1')).first()
        last_job = conn.execute(text('SELECT * FROM jobs ORDER BY id DESC LIMIT 1')).first()
        return {'agent': _row_to_dict(agent), 'last_job': _row_to_dict(last_job)}


def get_web_user_by_username(username: str) -> Optional[dict[str, Any]]:
    uname = str(username or '').strip().lower()
    if not uname:
        return None
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT * FROM web_users WHERE username = :username LIMIT 1"),
            {'username': uname},
        ).first()
    out = _row_to_dict(row)
    if out and 'ativo' in out:
        out['ativo'] = bool(out.get('ativo'))
    if out and 'must_change_password' in out:
        out['must_change_password'] = bool(out.get('must_change_password'))
    return out


def get_web_user_by_numero_policia(numero_policia: str) -> Optional[dict[str, Any]]:
    n = str(numero_policia or '').strip()
    if not n:
        return None
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT * FROM web_users WHERE numero_policia = :numero_policia LIMIT 1"),
            {'numero_policia': n},
        ).first()
    out = _row_to_dict(row)
    if out and 'ativo' in out:
        out['ativo'] = bool(out.get('ativo'))
    if out and 'must_change_password' in out:
        out['must_change_password'] = bool(out.get('must_change_password'))
    return out


def get_web_user_by_id(user_id: int) -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT * FROM web_users WHERE id = :id LIMIT 1"),
            {'id': int(user_id)},
        ).first()
    out = _row_to_dict(row)
    if out and 'ativo' in out:
        out['ativo'] = bool(out.get('ativo'))
    if out and 'must_change_password' in out:
        out['must_change_password'] = bool(out.get('must_change_password'))
    return out


def list_web_users() -> list[dict[str, Any]]:
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  id, nome, numero_policia, posto_graduacao, unidade_setor, username, perfil, ativo,
                  must_change_password, created_at, updated_at, last_login_at
                FROM web_users
                ORDER BY ativo DESC, nome ASC, id ASC
                """
            )
        )
        out = _rows_to_dicts(rows)
    for r in out:
        # Normalize SQLite integer booleans to bool in API responses.
        if 'ativo' in r:
            r['ativo'] = bool(r.get('ativo'))
        if 'must_change_password' in r:
            r['must_change_password'] = bool(r.get('must_change_password'))
    return out


def create_web_user(
    nome: str,
    numero_policia: str,
    posto_graduacao: str,
    unidade_setor: str,
    username: str,
    password_hash: str,
    perfil: str = 'consulta',
    ativo: bool = True,
    must_change_password: bool = True,
) -> dict[str, Any]:
    now = utc_now_iso()
    payload = {
        'nome': str(nome or '').strip(),
        'numero_policia': str(numero_policia or '').strip(),
        'posto_graduacao': str(posto_graduacao or '').strip() or None,
        'unidade_setor': str(unidade_setor or '').strip() or None,
        'username': str(username or '').strip().lower(),
        'password_hash': str(password_hash or '').strip(),
        'perfil': str(perfil or 'consulta').strip().lower() or 'consulta',
        'ativo': bool(ativo),
        'must_change_password': bool(must_change_password),
        'created_at': now,
        'updated_at': now,
    }
    if _is_postgres():
        with get_engine().begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO web_users(
                      nome, numero_policia, posto_graduacao, unidade_setor, username, password_hash,
                      perfil, ativo, must_change_password, created_at, updated_at
                    )
                    VALUES(
                      :nome, :numero_policia, :posto_graduacao, :unidade_setor, :username, :password_hash,
                      :perfil, :ativo, :must_change_password, :created_at, :updated_at
                    )
                    RETURNING id, nome, numero_policia, posto_graduacao, unidade_setor, username, perfil, ativo, must_change_password, created_at, updated_at, last_login_at
                    """
                ),
                payload,
            ).first()
            out = _row_to_dict(row) or {}
            if 'ativo' in out:
                out['ativo'] = bool(out.get('ativo'))
            return out

    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO web_users(
                  nome, numero_policia, posto_graduacao, unidade_setor, username, password_hash,
                  perfil, ativo, must_change_password, created_at, updated_at
                )
                VALUES(
                  :nome, :numero_policia, :posto_graduacao, :unidade_setor, :username, :password_hash,
                  :perfil, :ativo, :must_change_password, :created_at, :updated_at
                )
                """
            ),
            payload,
        )
        row = conn.execute(text("SELECT last_insert_rowid() AS id")).first()
    return get_web_user_by_id(int((row or {})._mapping['id'])) or {}


def create_or_update_web_user(
    nome: str,
    numero_policia: str,
    posto_graduacao: str,
    unidade_setor: str,
    username: str,
    password_hash: str,
    perfil: str = 'admin',
    ativo: bool = True,
    must_change_password: bool = True,
) -> dict[str, Any]:
    existing = get_web_user_by_username(username)
    now = utc_now_iso()
    if existing:
        with get_engine().begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE web_users
                    SET nome=:nome,
                        numero_policia=:numero_policia,
                        posto_graduacao=:posto_graduacao,
                        unidade_setor=:unidade_setor,
                        password_hash=:password_hash,
                        perfil=:perfil,
                        ativo=:ativo,
                        must_change_password=:must_change_password,
                        updated_at=:updated_at
                    WHERE username=:username
                    """
                ),
                {
                    'nome': str(nome or '').strip(),
                    'numero_policia': str(numero_policia or '').strip(),
                    'posto_graduacao': str(posto_graduacao or '').strip() or None,
                    'unidade_setor': str(unidade_setor or '').strip() or None,
                    'password_hash': str(password_hash or '').strip(),
                    'perfil': str(perfil or 'admin').strip().lower() or 'admin',
                    'ativo': bool(ativo),
                    'must_change_password': bool(must_change_password),
                    'updated_at': now,
                    'username': str(username or '').strip().lower(),
                },
            )
        return get_web_user_by_username(username) or {}

    return create_web_user(
        nome=nome,
        numero_policia=numero_policia,
        posto_graduacao=posto_graduacao,
        unidade_setor=unidade_setor,
        username=username,
        password_hash=password_hash,
        perfil=perfil,
        ativo=ativo,
        must_change_password=must_change_password,
    )


def set_web_user_password(
    user_id: int,
    password_hash: str,
    must_change_password: Optional[bool] = None,
) -> Optional[dict[str, Any]]:
    sql = "UPDATE web_users SET password_hash=:password_hash, updated_at=:updated_at"
    params = {'password_hash': str(password_hash or '').strip(), 'updated_at': utc_now_iso(), 'id': int(user_id)}
    if must_change_password is not None and 'must_change_password' in set(_get_table_columns('web_users')):
        sql += ", must_change_password=:must_change_password"
        params['must_change_password'] = bool(must_change_password)
    sql += " WHERE id=:id"
    with get_engine().begin() as conn:
        conn.execute(text(sql), params)
    return get_web_user_by_id(user_id)


def set_web_user_active(user_id: int, ativo: bool) -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        conn.execute(
            text("UPDATE web_users SET ativo=:ativo, updated_at=:updated_at WHERE id=:id"),
            {'ativo': bool(ativo), 'updated_at': utc_now_iso(), 'id': int(user_id)},
        )
    return get_web_user_by_id(user_id)


def update_web_user(
    user_id: int,
    nome: str,
    numero_policia: str,
    posto_graduacao: str,
    unidade_setor: str,
    username: str,
    perfil: str,
    ativo: bool,
) -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE web_users
                SET nome=:nome,
                    numero_policia=:numero_policia,
                    posto_graduacao=:posto_graduacao,
                    unidade_setor=:unidade_setor,
                    username=:username,
                    perfil=:perfil,
                    ativo=:ativo,
                    updated_at=:updated_at
                WHERE id=:id
                """
            ),
            {
                'id': int(user_id),
                'nome': str(nome or '').strip(),
                'numero_policia': str(numero_policia or '').strip(),
                'posto_graduacao': str(posto_graduacao or '').strip() or None,
                'unidade_setor': str(unidade_setor or '').strip() or None,
                'username': str(username or '').strip().lower(),
                'perfil': str(perfil or 'consulta').strip().lower(),
                'ativo': bool(ativo),
                'updated_at': utc_now_iso(),
            },
        )
    return get_web_user_by_id(user_id)


def create_access_request(
    nome: str,
    numero_policia: str,
    unidade_setor: str = '',
    telefone: str = '',
    motivo: str = '',
) -> dict[str, Any]:
    now = utc_now_iso()
    payload = {
        'nome': str(nome or '').strip(),
        'numero_policia': str(numero_policia or '').strip(),
        'unidade_setor': str(unidade_setor or '').strip() or None,
        'telefone': str(telefone or '').strip() or None,
        'motivo': str(motivo or '').strip() or None,
        'status': 'pendente',
        'created_at': now,
    }
    with get_engine().begin() as conn:
        if _is_postgres():
            row = conn.execute(
                text(
                    """
                    INSERT INTO web_access_requests(nome, numero_policia, unidade_setor, telefone, motivo, status, created_at)
                    VALUES(:nome, :numero_policia, :unidade_setor, :telefone, :motivo, :status, :created_at)
                    RETURNING id, nome, numero_policia, unidade_setor, telefone, motivo, status, created_at, processed_at
                    """
                ),
                payload,
            ).first()
            return _row_to_dict(row) or {}

        conn.execute(
            text(
                """
                INSERT INTO web_access_requests(nome, numero_policia, unidade_setor, telefone, motivo, status, created_at)
                VALUES(:nome, :numero_policia, :unidade_setor, :telefone, :motivo, :status, :created_at)
                """
            ),
            payload,
        )
        row = conn.execute(text('SELECT last_insert_rowid() AS id')).first()
    with get_engine().begin() as conn:
        row2 = conn.execute(text('SELECT * FROM web_access_requests WHERE id=:id'), {'id': int(row._mapping['id'])}).first()
    return _row_to_dict(row2) or {}


def list_access_requests(limit: int = 200) -> list[dict[str, Any]]:
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, nome, numero_policia, unidade_setor, telefone, motivo, status, created_at, processed_at
                FROM web_access_requests
                ORDER BY id DESC
                LIMIT :limit
                """
            ),
            {'limit': int(limit)},
        )
        return _rows_to_dicts(rows)


def set_access_request_status(request_id: int, status: str) -> Optional[dict[str, Any]]:
    st = str(status or '').strip().lower() or 'pendente'
    processed = utc_now_iso() if st in {'atendido', 'negado'} else None
    with get_engine().begin() as conn:
        conn.execute(
            text('UPDATE web_access_requests SET status=:status, processed_at=:processed_at WHERE id=:id'),
            {'status': st, 'processed_at': processed, 'id': int(request_id)},
        )
        row = conn.execute(text('SELECT * FROM web_access_requests WHERE id=:id'), {'id': int(request_id)}).first()
    return _row_to_dict(row)


def touch_web_user_login(user_id: int) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            text("UPDATE web_users SET last_login_at=:last_login_at, updated_at=:updated_at WHERE id=:id"),
            {'last_login_at': utc_now_iso(), 'updated_at': utc_now_iso(), 'id': int(user_id)},
        )


def _norm_mapa_commander(row: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not row:
        return row
    out = dict(row)
    if 'ativo' in out:
        out['ativo'] = bool(out.get('ativo'))
    return out


def _norm_mapa_assignment(row: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not row:
        return row
    out = dict(row)
    if 'commander_ativo' in out:
        out['commander_ativo'] = bool(out.get('commander_ativo'))
    return out


def get_mapa_commander_by_numero_policia(numero_policia: str) -> Optional[dict[str, Any]]:
    n = str(numero_policia or '').strip()
    if not n:
        return None
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT * FROM mapa_commanders WHERE numero_policia=:numero_policia LIMIT 1"),
            {'numero_policia': n},
        ).first()
    return _norm_mapa_commander(_row_to_dict(row))


def get_mapa_commander_by_id(commander_id: int) -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT * FROM mapa_commanders WHERE id=:id LIMIT 1"),
            {'id': int(commander_id)},
        ).first()
    return _norm_mapa_commander(_row_to_dict(row))


def upsert_mapa_commander(
    numero_policia: str,
    posto_graduacao: str = '',
    nome_guerra: str = '',
    nome_completo: str = '',
    telefone: str = '',
    email: str = '',
    foto_url: str = '',
    observacoes: str = '',
    ativo: bool = True,
) -> dict[str, Any]:
    now = utc_now_iso()
    payload = {
        'numero_policia': str(numero_policia or '').strip(),
        'posto_graduacao': str(posto_graduacao or '').strip() or None,
        'nome_guerra': str(nome_guerra or '').strip(),
        'nome_completo': str(nome_completo or '').strip() or None,
        'telefone': str(telefone or '').strip() or None,
        'email': str(email or '').strip() or None,
        'foto_url': str(foto_url or '').strip() or None,
        'observacoes': str(observacoes or '').strip() or None,
        'ativo': bool(ativo),
        'created_at': now,
        'updated_at': now,
    }
    existing = get_mapa_commander_by_numero_policia(payload['numero_policia'])
    with get_engine().begin() as conn:
        if existing:
            conn.execute(
                text(
                    """
                    UPDATE mapa_commanders
                    SET posto_graduacao=:posto_graduacao,
                        nome_guerra=:nome_guerra,
                        nome_completo=:nome_completo,
                        telefone=:telefone,
                        email=:email,
                        foto_url=:foto_url,
                        observacoes=:observacoes,
                        ativo=:ativo,
                        updated_at=:updated_at
                    WHERE numero_policia=:numero_policia
                    """
                ),
                payload,
            )
            return get_mapa_commander_by_numero_policia(payload['numero_policia']) or {}

        if _is_postgres():
            row = conn.execute(
                text(
                    """
                    INSERT INTO mapa_commanders(
                      numero_policia, posto_graduacao, nome_guerra, nome_completo,
                      telefone, email, foto_url, observacoes, ativo, created_at, updated_at
                    )
                    VALUES(
                      :numero_policia, :posto_graduacao, :nome_guerra, :nome_completo,
                      :telefone, :email, :foto_url, :observacoes, :ativo, :created_at, :updated_at
                    )
                    RETURNING *
                    """
                ),
                payload,
            ).first()
            return _norm_mapa_commander(_row_to_dict(row)) or {}

        conn.execute(
            text(
                """
                INSERT INTO mapa_commanders(
                  numero_policia, posto_graduacao, nome_guerra, nome_completo,
                  telefone, email, foto_url, observacoes, ativo, created_at, updated_at
                )
                VALUES(
                  :numero_policia, :posto_graduacao, :nome_guerra, :nome_completo,
                  :telefone, :email, :foto_url, :observacoes, :ativo, :created_at, :updated_at
                )
                """
            ),
            payload,
        )
        row = conn.execute(text('SELECT last_insert_rowid() AS id')).first()
    return get_mapa_commander_by_id(int(row._mapping['id'])) or {}


def list_mapa_commanders(ativo_only: bool = False, limit: int = 1000) -> list[dict[str, Any]]:
    where = []
    params: dict[str, Any] = {'limit': int(limit)}
    if ativo_only:
        where.append('ativo = :ativo')
        params['ativo'] = True if _is_postgres() else 1
    where_sql = f"WHERE {' AND '.join(where)}" if where else ''
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM mapa_commanders
                {where_sql}
                ORDER BY ativo DESC, nome_guerra ASC, numero_policia ASC
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)
    return [_norm_mapa_commander(r) or {} for r in out]


def get_mapa_assignment_by_id(assignment_id: int) -> Optional[dict[str, Any]]:
    with get_engine().begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                  a.*,
                  c.numero_policia AS commander_numero_policia,
                  c.posto_graduacao AS commander_posto_graduacao,
                  c.nome_guerra AS commander_nome_guerra,
                  c.nome_completo AS commander_nome_completo,
                  c.telefone AS commander_telefone,
                  c.email AS commander_email,
                  c.foto_url AS commander_foto_url,
                  c.ativo AS commander_ativo
                FROM mapa_command_assignments a
                JOIN mapa_commanders c ON c.id = a.commander_id
                WHERE a.id = :id
                LIMIT 1
                """
            ),
            {'id': int(assignment_id)},
        ).first()
    return _norm_mapa_assignment(_row_to_dict(row))


def list_mapa_assignments(
    scope_type: Optional[str] = None,
    scope_code: Optional[str] = None,
    cia_code: Optional[str] = None,
    include_closed: bool = True,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    where = []
    params: dict[str, Any] = {'limit': int(limit)}
    if scope_type:
        where.append('a.scope_type = :scope_type')
        params['scope_type'] = str(scope_type).strip().upper()
    if scope_code:
        where.append('a.scope_code = :scope_code')
        params['scope_code'] = str(scope_code).strip()
    if cia_code:
        where.append('a.cia_code = :cia_code')
        params['cia_code'] = str(cia_code).strip()
    if not include_closed:
        where.append('a.data_fim IS NULL')
    where_sql = f"WHERE {' AND '.join(where)}" if where else ''
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT
                  a.*,
                  c.numero_policia AS commander_numero_policia,
                  c.posto_graduacao AS commander_posto_graduacao,
                  c.nome_guerra AS commander_nome_guerra,
                  c.nome_completo AS commander_nome_completo,
                  c.telefone AS commander_telefone,
                  c.email AS commander_email,
                  c.foto_url AS commander_foto_url,
                  c.observacoes AS commander_observacoes,
                  c.ativo AS commander_ativo
                FROM mapa_command_assignments a
                JOIN mapa_commanders c ON c.id = a.commander_id
                {where_sql}
                ORDER BY
                  COALESCE(a.data_fim, '9999-12-31') DESC,
                  a.data_inicio DESC,
                  a.id DESC
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)
    return [(_norm_mapa_assignment(r) or {}) for r in out]


def create_mapa_assignment(
    commander_id: int,
    scope_type: str,
    scope_code: str,
    scope_label: str,
    cia_code: str = '',
    pelotao_code: str = '',
    setor_code: str = '',
    subsetor_code: str = '',
    municipio_nome: str = '',
    role_kind: str = 'titular',
    situacao: str = 'ativo',
    motivo: str = '',
    data_inicio: Optional[str] = None,
    cadastrado_por: str = '',
    atualizado_por: str = '',
    replace_existing_same_role: bool = True,
) -> dict[str, Any]:
    now = utc_now_iso()
    start_date = (str(data_inicio or '').strip() or now[:10])
    payload = {
        'commander_id': int(commander_id),
        'scope_type': str(scope_type or '').strip().upper(),
        'scope_code': str(scope_code or '').strip(),
        'scope_label': str(scope_label or '').strip() or str(scope_code or '').strip(),
        'cia_code': str(cia_code or '').strip() or None,
        'pelotao_code': str(pelotao_code or '').strip() or None,
        'setor_code': str(setor_code or '').strip() or None,
        'subsetor_code': str(subsetor_code or '').strip() or None,
        'municipio_nome': str(municipio_nome or '').strip() or None,
        'role_kind': str(role_kind or 'titular').strip().lower() or 'titular',
        'situacao': str(situacao or 'ativo').strip().lower() or 'ativo',
        'motivo': str(motivo or '').strip() or None,
        'data_inicio': start_date,
        'data_fim': None,
        'data_cadastro': now,
        'cadastrado_por': str(cadastrado_por or '').strip() or None,
        'updated_at': now,
        'atualizado_por': str(atualizado_por or '').strip() or None,
    }
    with get_engine().begin() as conn:
        if replace_existing_same_role:
            conn.execute(
                text(
                    """
                    UPDATE mapa_command_assignments
                    SET data_fim = :data_fim,
                        situacao = CASE
                          WHEN situacao IN ('inativo', 'substituido') THEN situacao
                          ELSE 'substituido'
                        END,
                        updated_at = :updated_at,
                        atualizado_por = :atualizado_por
                    WHERE scope_type = :scope_type
                      AND scope_code = :scope_code
                      AND role_kind = :role_kind
                      AND data_fim IS NULL
                    """
                ),
                {
                    'data_fim': start_date,
                    'updated_at': now,
                    'atualizado_por': payload['atualizado_por'],
                    'scope_type': payload['scope_type'],
                    'scope_code': payload['scope_code'],
                    'role_kind': payload['role_kind'],
                },
            )

        if _is_postgres():
            row = conn.execute(
                text(
                    """
                    INSERT INTO mapa_command_assignments(
                      commander_id, scope_type, scope_code, scope_label, cia_code, pelotao_code, setor_code,
                      subsetor_code, municipio_nome, role_kind, situacao, motivo, data_inicio, data_fim,
                      data_cadastro, cadastrado_por, updated_at, atualizado_por
                    )
                    VALUES(
                      :commander_id, :scope_type, :scope_code, :scope_label, :cia_code, :pelotao_code, :setor_code,
                      :subsetor_code, :municipio_nome, :role_kind, :situacao, :motivo, :data_inicio, :data_fim,
                      :data_cadastro, :cadastrado_por, :updated_at, :atualizado_por
                    )
                    RETURNING id
                    """
                ),
                payload,
            ).first()
            assignment_id = int(row._mapping['id'])
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO mapa_command_assignments(
                      commander_id, scope_type, scope_code, scope_label, cia_code, pelotao_code, setor_code,
                      subsetor_code, municipio_nome, role_kind, situacao, motivo, data_inicio, data_fim,
                      data_cadastro, cadastrado_por, updated_at, atualizado_por
                    )
                    VALUES(
                      :commander_id, :scope_type, :scope_code, :scope_label, :cia_code, :pelotao_code, :setor_code,
                      :subsetor_code, :municipio_nome, :role_kind, :situacao, :motivo, :data_inicio, :data_fim,
                      :data_cadastro, :cadastrado_por, :updated_at, :atualizado_por
                    )
                    """
                ),
                payload,
            )
            row = conn.execute(text('SELECT last_insert_rowid() AS id')).first()
            assignment_id = int(row._mapping['id'])
    return get_mapa_assignment_by_id(assignment_id) or {}


def update_mapa_assignment_status(
    assignment_id: int,
    situacao: str,
    motivo: str = '',
    data_fim: Optional[str] = None,
    encerrar_periodo: bool = False,
    atualizado_por: str = '',
) -> Optional[dict[str, Any]]:
    existing = get_mapa_assignment_by_id(assignment_id)
    if not existing:
        return None
    now = utc_now_iso()
    final_date = str(data_fim or '').strip() or None
    if encerrar_periodo and not final_date:
        final_date = now[:10]
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE mapa_command_assignments
                SET situacao=:situacao,
                    motivo=:motivo,
                    data_fim=:data_fim,
                    updated_at=:updated_at,
                    atualizado_por=:atualizado_por
                WHERE id=:id
                """
            ),
            {
                'id': int(assignment_id),
                'situacao': str(situacao or '').strip().lower() or existing.get('situacao') or 'ativo',
                'motivo': str(motivo or '').strip() or None,
                'data_fim': final_date if encerrar_periodo else existing.get('data_fim'),
                'updated_at': now,
                'atualizado_por': str(atualizado_por or '').strip() or None,
            },
        )
    return get_mapa_assignment_by_id(assignment_id)


def delete_mapa_assignment(assignment_id: int) -> bool:
    with get_engine().begin() as conn:
        cur = conn.execute(
            text("DELETE FROM mapa_command_assignments WHERE id = :id"),
            {'id': int(assignment_id)},
        )
    return bool(cur.rowcount)


def _norm_mapa_scope_metadata(row: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not row:
        return None
    out = dict(row)
    out['scope_type'] = str(out.get('scope_type') or '').strip().upper()
    out['scope_code'] = str(out.get('scope_code') or '').strip()
    for k in ('cia_code', 'municipio_nome', 'updated_at', 'atualizado_por'):
        if out.get(k) is None:
            out[k] = ''
    for k in ('populacao_municipio', 'efetivo_fracao'):
        v = out.get(k)
        if v in (None, ''):
            out[k] = None
            continue
        try:
            out[k] = int(v)
        except Exception:
            try:
                out[k] = int(float(v))
            except Exception:
                out[k] = None
    return out


def list_mapa_scope_metadata(scope_type: Optional[str] = None, limit: int = 5000) -> list[dict[str, Any]]:
    where = []
    params: dict[str, Any] = {'limit': int(limit)}
    if scope_type:
        where.append('scope_type = :scope_type')
        params['scope_type'] = str(scope_type).strip().upper()
    where_sql = f"WHERE {' AND '.join(where)}" if where else ''
    with get_engine().begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM mapa_scope_metadata
                {where_sql}
                ORDER BY scope_type, scope_code
                LIMIT :limit
                """
            ),
            params,
        )
        out = _rows_to_dicts(rows)
    return [(_norm_mapa_scope_metadata(r) or {}) for r in out]


def upsert_mapa_scope_metadata(
    scope_type: str,
    scope_code: str,
    cia_code: str = '',
    municipio_nome: str = '',
    populacao_municipio: Optional[int] = None,
    efetivo_fracao: Optional[int] = None,
    atualizado_por: str = '',
) -> dict[str, Any]:
    now = utc_now_iso()
    payload = {
        'scope_type': str(scope_type or '').strip().upper(),
        'scope_code': str(scope_code or '').strip(),
        'cia_code': str(cia_code or '').strip() or None,
        'municipio_nome': str(municipio_nome or '').strip() or None,
        'populacao_municipio': int(populacao_municipio) if populacao_municipio not in (None, '') else None,
        'efetivo_fracao': int(efetivo_fracao) if efetivo_fracao not in (None, '') else None,
        'updated_at': now,
        'atualizado_por': str(atualizado_por or '').strip() or None,
    }
    with get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO mapa_scope_metadata(
                  scope_type, scope_code, cia_code, municipio_nome, populacao_municipio, efetivo_fracao, updated_at, atualizado_por
                )
                VALUES(
                  :scope_type, :scope_code, :cia_code, :municipio_nome, :populacao_municipio, :efetivo_fracao, :updated_at, :atualizado_por
                )
                ON CONFLICT(scope_type, scope_code) DO UPDATE SET
                  cia_code=excluded.cia_code,
                  municipio_nome=excluded.municipio_nome,
                  populacao_municipio=excluded.populacao_municipio,
                  efetivo_fracao=excluded.efetivo_fracao,
                  updated_at=excluded.updated_at,
                  atualizado_por=excluded.atualizado_por
                """
            ),
            payload,
        )
        row = conn.execute(
            text(
                """
                SELECT * FROM mapa_scope_metadata
                WHERE scope_type = :scope_type AND scope_code = :scope_code
                LIMIT 1
                """
            ),
            {'scope_type': payload['scope_type'], 'scope_code': payload['scope_code']},
        ).first()
    return _norm_mapa_scope_metadata(_row_to_dict(row)) or {}
