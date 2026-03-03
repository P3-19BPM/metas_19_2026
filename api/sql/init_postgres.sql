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
  qtd_presos INTEGER,
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

CREATE TABLE IF NOT EXISTS web_users (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  numero_policia TEXT NOT NULL,
  posto_graduacao TEXT,
  unidade_setor TEXT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  perfil TEXT NOT NULL DEFAULT 'consulta',
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  must_change_password BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_login_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_web_users_numero_policia ON web_users(numero_policia);
CREATE INDEX IF NOT EXISTS idx_web_users_username ON web_users(username);
CREATE INDEX IF NOT EXISTS idx_web_users_ativo ON web_users(ativo);

CREATE TABLE IF NOT EXISTS web_access_requests (
  id BIGSERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  numero_policia TEXT NOT NULL,
  unidade_setor TEXT,
  telefone TEXT,
  motivo TEXT,
  status TEXT NOT NULL DEFAULT 'pendente',
  created_at TEXT NOT NULL,
  processed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_web_access_requests_status ON web_access_requests(status);
CREATE INDEX IF NOT EXISTS idx_web_access_requests_created_at ON web_access_requests(created_at);

CREATE TABLE IF NOT EXISTS mapa_commanders (
  id BIGSERIAL PRIMARY KEY,
  numero_policia TEXT NOT NULL,
  posto_graduacao TEXT,
  nome_guerra TEXT NOT NULL,
  nome_completo TEXT,
  telefone TEXT,
  email TEXT,
  foto_url TEXT,
  observacoes TEXT,
  ativo BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mapa_commanders_numero_policia ON mapa_commanders(numero_policia);
CREATE INDEX IF NOT EXISTS idx_mapa_commanders_ativo ON mapa_commanders(ativo);

CREATE TABLE IF NOT EXISTS mapa_command_assignments (
  id BIGSERIAL PRIMARY KEY,
  commander_id BIGINT NOT NULL,
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
);

CREATE INDEX IF NOT EXISTS idx_mapa_assign_scope ON mapa_command_assignments(scope_type, scope_code);
CREATE INDEX IF NOT EXISTS idx_mapa_assign_scope_open ON mapa_command_assignments(scope_type, scope_code, data_fim);
CREATE INDEX IF NOT EXISTS idx_mapa_assign_cia ON mapa_command_assignments(cia_code);
CREATE INDEX IF NOT EXISTS idx_mapa_assign_role ON mapa_command_assignments(role_kind, situacao);
