-- ======================================================
-- GDO 2026 - Schema compativel com API + pipeline atual
-- Banco alvo: PostgreSQL
-- ======================================================

BEGIN;

-- ------------------------------
-- Tabelas operacionais da API
-- ------------------------------
CREATE TABLE IF NOT EXISTS agent_status (
  agent_id TEXT PRIMARY KEY,
  version TEXT,
  status TEXT NOT NULL DEFAULT 'offline',
  online_since TEXT,
  last_seen TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
  id BIGSERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  requested_by TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  available_after TEXT,
  started_at TEXT,
  finished_at TEXT,
  agent_id TEXT,
  payload_json TEXT,
  result_json TEXT,
  error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_available ON jobs(status, available_after);
CREATE INDEX IF NOT EXISTS idx_jobs_requested_at ON jobs(requested_at);

CREATE TABLE IF NOT EXISTS kpi_daily (
  id BIGSERIAL PRIMARY KEY,
  referencia_data TEXT NOT NULL,
  indicador TEXT NOT NULL,
  nivel TEXT NOT NULL,
  unidade_id TEXT NOT NULL,
  unidade_nome TEXT NOT NULL,
  valor_realizado DOUBLE PRECISION NOT NULL,
  valor_meta DOUBLE PRECISION,
  valor_plr DOUBLE PRECISION,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(referencia_data, indicador, nivel, unidade_id)
);

CREATE TABLE IF NOT EXISTS reds_events (
  id BIGSERIAL PRIMARY KEY,
  indicador TEXT NOT NULL,
  numero_ocorrencia TEXT NOT NULL,
  numero_envolvido TEXT,
  chave_envolvido TEXT,
  envolvido_key TEXT NOT NULL DEFAULT '',
  data_hora_fato TEXT,
  referencia_data TEXT,
  natureza_codigo TEXT,
  natureza_descricao TEXT,
  municipio_codigo TEXT,
  municipio_nome TEXT,
  CIA_PM TEXT,
  PELOTAO TEXT,
  SETOR TEXT,
  SUBSETOR TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(indicador, numero_ocorrencia, envolvido_key)
);

CREATE INDEX IF NOT EXISTS idx_reds_ref ON reds_events(indicador, referencia_data);
CREATE INDEX IF NOT EXISTS idx_reds_cia ON reds_events(indicador, CIA_PM);
CREATE INDEX IF NOT EXISTS idx_reds_pel ON reds_events(indicador, PELOTAO);
CREATE INDEX IF NOT EXISTS idx_reds_setor ON reds_events(indicador, SETOR);
CREATE INDEX IF NOT EXISTS idx_reds_subsetor ON reds_events(indicador, SUBSETOR);
CREATE INDEX IF NOT EXISTS idx_reds_mun ON reds_events(indicador, municipio_codigo);

-- ------------------------------
-- Tabelas analiticas (GDO)
-- ------------------------------
CREATE TABLE IF NOT EXISTS indicators (
  id BIGSERIAL PRIMARY KEY,
  domain TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  unit_type TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_indicators_code ON indicators(code);

CREATE TABLE IF NOT EXISTS periods (
  id BIGSERIAL PRIMARY KEY,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  label TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_periods_year_month ON periods(year, month);

CREATE TABLE IF NOT EXISTS geo_units (
  id BIGSERIAL PRIMARY KEY,
  unit_type TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  parent_code TEXT
);

CREATE INDEX IF NOT EXISTS idx_geo_units_type_code ON geo_units(unit_type, code);

CREATE TABLE IF NOT EXISTS results (
  id BIGSERIAL PRIMARY KEY,
  indicator_id BIGINT NOT NULL,
  period_id BIGINT NOT NULL,
  geo_unit_id BIGINT NOT NULL,
  value NUMERIC NOT NULL,
  metric_type TEXT NOT NULL,
  payload JSONB,
  source TEXT NOT NULL,
  loaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_results_indicator ON results(indicator_id);
CREATE INDEX IF NOT EXISTS idx_results_period ON results(period_id);
CREATE INDEX IF NOT EXISTS idx_results_geo ON results(geo_unit_id);

CREATE TABLE IF NOT EXISTS targets (
  id BIGSERIAL PRIMARY KEY,
  indicator_id BIGINT NOT NULL,
  period_id BIGINT NOT NULL,
  geo_unit_id BIGINT NOT NULL,
  target_value NUMERIC NOT NULL,
  metric_type TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_targets_indicator ON targets(indicator_id);
CREATE INDEX IF NOT EXISTS idx_targets_period ON targets(period_id);
CREATE INDEX IF NOT EXISTS idx_targets_geo ON targets(geo_unit_id);

CREATE TABLE IF NOT EXISTS whitelist_pm (
  nr_pm TEXT PRIMARY KEY,
  autorizado_por TEXT,
  autorizado_em TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS usuarios_acesso (
  nr_pm TEXT PRIMARY KEY,
  nome_completo TEXT NOT NULL,
  graduacao TEXT,
  unidade TEXT,
  senha_hash TEXT,
  senha_criada_em TIMESTAMPTZ,
  status TEXT NOT NULL,
  foto_url TEXT,
  criado_em TIMESTAMPTZ,
  ultimo_acesso TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS auditoria_acessos (
  id BIGSERIAL PRIMARY KEY,
  nr_pm TEXT,
  origem TEXT,
  acao TEXT,
  detalhes TEXT,
  ip TEXT,
  user_agent TEXT,
  criado_em TIMESTAMPTZ DEFAULT NOW()
);

-- ------------------------------
-- Fact tables (compativeis com scripts SQL BISP)
-- ------------------------------
CREATE TABLE IF NOT EXISTS fact_imv (
  numero_ocorrencia TEXT,
  envolvimento_codigo TEXT,
  envolvimento_descricao TEXT,
  numero_envolvido TEXT,
  chave_envolvido TEXT,
  nome_completo_envolvido TEXT,
  nome_mae TEXT,
  data_nascimento TEXT,
  letalidade TEXT,
  condicao_fisica_descricao TEXT,
  natureza_ocorrencia_codigo TEXT,
  natureza_ocorrencia_descricao TEXT,
  ind_consumado TEXT,
  rpm_2024 TEXT,
  ueop_2024 TEXT,
  unidade_area_militar_codigo TEXT,
  unidade_area_militar_nome TEXT,
  unidade_responsavel_registro_codigo TEXT,
  unidade_responsavel_registro_nome TEXT,
  latitude_sirgas2000 NUMERIC,
  longitude_sirgas2000 NUMERIC,
  situacao_zona TEXT,
  tipo_descricao TEXT,
  codigo_municipio TEXT,
  nome_municipio TEXT,
  tipo_logradouro_descricao TEXT,
  logradouro_nome TEXT,
  numero_endereco TEXT,
  nome_bairro TEXT,
  ocorrencia_uf TEXT,
  numero_latitude NUMERIC,
  numero_longitude NUMERIC,
  data_hora_fato TIMESTAMPTZ,
  ano INTEGER,
  mes INTEGER,
  nome_tipo_relatorio TEXT,
  digitador_sigla_orgao TEXT,
  udi TEXT,
  ueop TEXT,
  cia TEXT,
  codigo_espacial_pm TEXT,
  cia_pel_final TEXT,
  geo_name TEXT,
  geo_ueop TEXT,
  geo_cia_pm TEXT,
  geo_pelotao TEXT,
  geo_cd_municipio TEXT,
  geo_nm_municipio TEXT,
  geo_setor TEXT,
  geo_municipio TEXT,
  geo_area TEXT,
  geo_subsetor TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_imv_data ON fact_imv(data_hora_fato);
CREATE INDEX IF NOT EXISTS idx_fact_imv_ano_mes ON fact_imv(ano, mes);
CREATE INDEX IF NOT EXISTS idx_fact_imv_mun ON fact_imv(codigo_municipio);
CREATE INDEX IF NOT EXISTS idx_fact_imv_cia ON fact_imv(cia);

CREATE TABLE IF NOT EXISTS fact_icvpe (
  numero_ocorrencia TEXT,
  envolvimento_codigo TEXT,
  envolvimento_descricao TEXT,
  numero_envolvido TEXT,
  nome_completo_envolvido TEXT,
  nome_mae TEXT,
  data_nascimento TEXT,
  ind_militar_policial_servico TEXT,
  condicao_fisica_descricao TEXT,
  natureza_ocorrencia_codigo TEXT,
  natureza_ocorrencia_descricao TEXT,
  ind_consumado TEXT,
  rpm_2024 TEXT,
  ueop_2024 TEXT,
  unidade_area_militar_codigo TEXT,
  unidade_area_militar_nome TEXT,
  unidade_responsavel_registro_codigo TEXT,
  unidade_responsavel_registro_nome TEXT,
  latitude_sirgas2000 NUMERIC,
  longitude_sirgas2000 NUMERIC,
  numero_latitude NUMERIC,
  numero_longitude NUMERIC,
  situacao_zona TEXT,
  tipo_descricao TEXT,
  codigo_municipio TEXT,
  nome_municipio TEXT,
  tipo_logradouro_descricao TEXT,
  logradouro_nome TEXT,
  numero_endereco TEXT,
  nome_bairro TEXT,
  ocorrencia_uf TEXT,
  data_hora_fato TIMESTAMPTZ,
  ano INTEGER,
  mes INTEGER,
  nome_tipo_relatorio TEXT,
  digitador_sigla_orgao TEXT,
  udi TEXT,
  ueop TEXT,
  cia TEXT,
  codigo_espacial_pm TEXT,
  cia_pel_final TEXT,
  geo_name TEXT,
  geo_ueop TEXT,
  geo_cia_pm TEXT,
  geo_pelotao TEXT,
  geo_setor TEXT,
  geo_subsetor TEXT,
  geo_cd_municipio TEXT,
  geo_nm_municipio TEXT,
  geo_municipio TEXT,
  geo_area TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_icvpe_data ON fact_icvpe(data_hora_fato);
CREATE INDEX IF NOT EXISTS idx_fact_icvpe_ano_mes ON fact_icvpe(ano, mes);
CREATE INDEX IF NOT EXISTS idx_fact_icvpe_mun ON fact_icvpe(codigo_municipio);
CREATE INDEX IF NOT EXISTS idx_fact_icvpe_cia ON fact_icvpe(cia);

CREATE TABLE IF NOT EXISTS fact_icvpa (
  numero_ocorrencia TEXT,
  envolvimento_codigo TEXT,
  envolvimento_descricao TEXT,
  numero_envolvido TEXT,
  nome_completo_envolvido TEXT,
  nome_mae TEXT,
  data_nascimento TEXT,
  condicao_fisica_descricao TEXT,
  natureza_ocorrencia_codigo TEXT,
  natureza_ocorrencia_descricao TEXT,
  ind_consumado TEXT,
  rpm_2024 TEXT,
  ueop_2024 TEXT,
  unidade_area_militar_codigo TEXT,
  unidade_area_militar_nome TEXT,
  unidade_responsavel_registro_codigo TEXT,
  unidade_responsavel_registro_nome TEXT,
  latitude_sirgas2000 NUMERIC,
  longitude_sirgas2000 NUMERIC,
  numero_latitude NUMERIC,
  numero_longitude NUMERIC,
  situacao_zona TEXT,
  tipo_descricao TEXT,
  codigo_municipio TEXT,
  nome_municipio TEXT,
  tipo_logradouro_descricao TEXT,
  logradouro_nome TEXT,
  numero_endereco TEXT,
  nome_bairro TEXT,
  ocorrencia_uf TEXT,
  data_hora_fato TIMESTAMPTZ,
  ano INTEGER,
  mes INTEGER,
  nome_tipo_relatorio TEXT,
  digitador_sigla_orgao TEXT,
  udi TEXT,
  ueop TEXT,
  cia TEXT,
  codigo_espacial_pm TEXT,
  cia_pel_final TEXT,
  geo_name TEXT,
  geo_ueop TEXT,
  geo_cia_pm TEXT,
  geo_pelotao TEXT,
  geo_setor TEXT,
  geo_subsetor TEXT,
  geo_cd_municipio TEXT,
  geo_nm_municipio TEXT,
  geo_municipio TEXT,
  geo_area TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_icvpa_data ON fact_icvpa(data_hora_fato);
CREATE INDEX IF NOT EXISTS idx_fact_icvpa_ano_mes ON fact_icvpa(ano, mes);
CREATE INDEX IF NOT EXISTS idx_fact_icvpa_mun ON fact_icvpa(codigo_municipio);
CREATE INDEX IF NOT EXISTS idx_fact_icvpa_cia ON fact_icvpa(cia);

COMMIT;
