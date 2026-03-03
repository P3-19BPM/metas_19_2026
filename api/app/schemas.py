from typing import Any, Optional

from pydantic import BaseModel, Field


class HeartbeatIn(BaseModel):
    agent_id: str = Field(min_length=2)
    version: Optional[str] = None


class ManualJobIn(BaseModel):
    run_mode: str = Field(default="all")
    scripts: list[str] = Field(default_factory=list)


class JobResultIn(BaseModel):
    success: bool
    result: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class KpiRowIn(BaseModel):
    referencia_data: str
    indicador: str
    nivel: str
    unidade_id: str
    unidade_nome: str
    valor_realizado: float
    valor_meta: Optional[float] = None
    valor_plr: Optional[float] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class KpiIngestIn(BaseModel):
    rows: list[KpiRowIn] = Field(default_factory=list)


class PurgeIn(BaseModel):
    indicador: str
    date_from: str
    date_to: str


class RedsRowIn(BaseModel):
    indicador: str
    numero_ocorrencia: str
    numero_envolvido: Optional[str] = None
    chave_envolvido: Optional[str] = None
    data_hora_fato: Optional[str] = None
    referencia_data: Optional[str] = None
    natureza_codigo: Optional[str] = None
    natureza_descricao: Optional[str] = None
    municipio_codigo: Optional[str] = None
    municipio_nome: Optional[str] = None
    CIA_PM: Optional[str] = None
    PELOTAO: Optional[str] = None
    SETOR: Optional[str] = None
    SUBSETOR: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    qtd_presos: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class RedsIngestIn(BaseModel):
    rows: list[RedsRowIn] = Field(default_factory=list)


class FactIngestIn(BaseModel):
    indicador: str
    rows: list[dict[str, Any]] = Field(default_factory=list)


class WebUserCreateIn(BaseModel):
    nome: str = Field(min_length=3, max_length=120)
    numero_policia: str = Field(min_length=3, max_length=40)
    posto_graduacao: str = Field(min_length=2, max_length=20)
    unidade_setor: Optional[str] = Field(default="")
    password: str = Field(min_length=6, max_length=200)
    perfil: str = Field(default="consulta")
    ativo: bool = Field(default=True)
    senha_provisoria: bool = Field(default=True)


class WebUserUpdateIn(BaseModel):
    nome: str = Field(min_length=3, max_length=120)
    numero_policia: str = Field(min_length=3, max_length=40)
    posto_graduacao: str = Field(min_length=2, max_length=20)
    unidade_setor: Optional[str] = Field(default="")
    perfil: str = Field(default="consulta")
    ativo: bool = Field(default=True)


class WebUserPasswordIn(BaseModel):
    password: str = Field(min_length=6, max_length=200)
    senha_provisoria: bool = Field(default=True)


class WebUserActiveIn(BaseModel):
    ativo: bool


class WebUserSelfPasswordIn(BaseModel):
    current_password: str = Field(min_length=1, max_length=200)
    new_password: str = Field(min_length=6, max_length=200)


class AccessRequestIn(BaseModel):
    nome: str = Field(min_length=3, max_length=120)
    numero_policia: str = Field(min_length=3, max_length=40)
    unidade_setor: Optional[str] = Field(default="")
    telefone: Optional[str] = Field(default="")
    motivo: Optional[str] = Field(default="")


class MapaCommanderIn(BaseModel):
    numero_policia: str = Field(min_length=3, max_length=40)
    posto_graduacao: str = Field(default="", max_length=40)
    nome_guerra: str = Field(min_length=2, max_length=80)
    nome_completo: Optional[str] = Field(default="", max_length=160)
    telefone: Optional[str] = Field(default="", max_length=40)
    email: Optional[str] = Field(default="", max_length=120)
    foto_url: Optional[str] = Field(default="", max_length=500)
    observacoes: Optional[str] = Field(default="", max_length=500)
    ativo: bool = Field(default=True)


class MapaAssignmentUpsertIn(BaseModel):
    commander: MapaCommanderIn
    scope_type: str = Field(min_length=3, max_length=20)
    scope_code: str = Field(min_length=1, max_length=120)
    scope_label: str = Field(default="", max_length=180)
    cia_code: Optional[str] = Field(default="", max_length=80)
    pelotao_code: Optional[str] = Field(default="", max_length=120)
    setor_code: Optional[str] = Field(default="", max_length=120)
    subsetor_code: Optional[str] = Field(default="", max_length=120)
    municipio_nome: Optional[str] = Field(default="", max_length=120)
    role_kind: str = Field(default="titular", max_length=20)
    situacao: str = Field(default="ativo", max_length=20)
    motivo: Optional[str] = Field(default="", max_length=300)
    data_inicio: Optional[str] = Field(default=None, max_length=30)
    replace_existing_same_role: bool = Field(default=True)


class MapaAssignmentStatusIn(BaseModel):
    situacao: str = Field(min_length=3, max_length=20)
    motivo: Optional[str] = Field(default="", max_length=300)
    data_fim: Optional[str] = Field(default=None, max_length=30)
    encerrar_periodo: bool = Field(default=False)


class MapaCommanderEditIn(BaseModel):
    commander: MapaCommanderIn
    scope_type: str = Field(min_length=3, max_length=20)
    cia_code: Optional[str] = Field(default="", max_length=80)


class MapaScopeMetadataUpsertIn(BaseModel):
    scope_type: str = Field(min_length=3, max_length=20)
    scope_code: str = Field(min_length=1, max_length=120)
    cia_code: Optional[str] = Field(default="", max_length=80)
    municipio_nome: Optional[str] = Field(default="", max_length=120)
    populacao_municipio: Optional[int] = Field(default=None, ge=0)
    efetivo_fracao: Optional[int] = Field(default=None, ge=0)
