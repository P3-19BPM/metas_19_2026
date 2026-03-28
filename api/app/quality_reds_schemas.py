from typing import Optional

from pydantic import BaseModel, Field


class QualityRedsRawRowIn(BaseModel):
    numero_ocorrencia: str = Field(min_length=1, max_length=80)
    cia_pm: Optional[str] = Field(default="", max_length=120)
    pelotao_pm: Optional[str] = Field(default="", max_length=180)
    data_hora_fato: Optional[str] = Field(default=None, max_length=40)
    data_hora_inclusao: Optional[str] = Field(default=None, max_length=40)
    data_hora_final: Optional[str] = Field(default=None, max_length=40)
    data_fato: Optional[str] = Field(default=None, max_length=30)
    mes: Optional[int] = Field(default=None, ge=1, le=12)
    ano: Optional[int] = Field(default=None, ge=2000, le=2100)
    data_hora_fechamento: Optional[str] = Field(default=None, max_length=40)
    ind_estado: Optional[str] = Field(default="", max_length=10)
    descricao_estado: Optional[str] = Field(default="", max_length=80)
    data_hora_alteracao: Optional[str] = Field(default=None, max_length=40)
    data_hora_primeiro_fim_preenchimento: Optional[str] = Field(default=None, max_length=40)
    nome_tipo_relatorio: Optional[str] = Field(default="", max_length=80)
    digitador_cargo_efetivo: Optional[str] = Field(default="", max_length=120)
    digitador_matricula: Optional[str] = Field(default="", max_length=80)
    digitador_nome: Optional[str] = Field(default="", max_length=180)
    digitador_nome_unidade: Optional[str] = Field(default="", max_length=180)
    natureza_codigo: Optional[str] = Field(default="", max_length=80)
    natureza_descricao_longa: Optional[str] = Field(default="", max_length=400)
    natureza_secundaria1_codigo: Optional[str] = Field(default="", max_length=80)
    natureza_secundaria1_descricao: Optional[str] = Field(default="", max_length=400)
    natureza_secundaria2_codigo: Optional[str] = Field(default="", max_length=80)
    natureza_secundaria2_descricao: Optional[str] = Field(default="", max_length=400)
    natureza_secundaria3_codigo: Optional[str] = Field(default="", max_length=80)
    natureza_secundaria3_descricao: Optional[str] = Field(default="", max_length=400)
    historico_ocorrencia: Optional[str] = Field(default="", max_length=12000)
    flag_envolvido_sem_cpf: Optional[int] = Field(default=0, ge=0, le=1)
    flag_envolvido_sem_rg: Optional[int] = Field(default=0, ge=0, le=1)
    flag_envolvido_sem_email: Optional[int] = Field(default=0, ge=0, le=1)
    flag_envolvido_sem_telefone: Optional[int] = Field(default=0, ge=0, le=1)
    total_envolvidos: Optional[int] = Field(default=0, ge=0)
    qtd_envolvidos_sem_cpf: Optional[int] = Field(default=0, ge=0)
    qtd_envolvidos_sem_rg: Optional[int] = Field(default=0, ge=0)
    qtd_envolvidos_sem_email: Optional[int] = Field(default=0, ge=0)
    qtd_envolvidos_sem_telefone: Optional[int] = Field(default=0, ge=0)
    flag_todos_envolvidos_sem_cpf: Optional[int] = Field(default=0, ge=0, le=1)
    flag_todos_envolvidos_sem_rg: Optional[int] = Field(default=0, ge=0, le=1)
    flag_todos_envolvidos_sem_email: Optional[int] = Field(default=0, ge=0, le=1)
    flag_todos_envolvidos_sem_telefone: Optional[int] = Field(default=0, ge=0, le=1)


class QualityRedsIngestIn(BaseModel):
    rows: list[QualityRedsRawRowIn] = Field(default_factory=list)


class QualityRedsAnalyzeIn(BaseModel):
    date_from: Optional[str] = Field(default=None, max_length=30)
    date_to: Optional[str] = Field(default=None, max_length=30)
    cia_pm: Optional[str] = Field(default=None, max_length=120)
    pelotao_pm: Optional[str] = Field(default=None, max_length=180)
    natureza_codigo: Optional[str] = Field(default=None, max_length=80)
    limit: int = Field(default=5000, ge=1, le=50000)
    persist: bool = Field(default=True)


class QualityRedsRunNowIn(BaseModel):
    scripts: list[str] = Field(default_factory=list)
    extract_mode: Optional[str] = Field(default="incremental_12d", max_length=40)


class QualityRedsEvaluationRowIn(BaseModel):
    numero_ocorrencia: str = Field(min_length=1, max_length=80)
    avaliado: bool = Field(default=False)
    ajustes: list[str] = Field(default_factory=list)
    observacao: Optional[str] = Field(default="", max_length=4000)


class QualityRedsEvaluationUpsertIn(BaseModel):
    rows: list[QualityRedsEvaluationRowIn] = Field(default_factory=list)


class QualityRedsComissaoMemberIn(BaseModel):
    id: Optional[str] = Field(default="", max_length=40)
    numero_policia: Optional[str] = Field(default="", max_length=40)
    nr: Optional[str] = Field(default="", max_length=40)
    nome: Optional[str] = Field(default="", max_length=200)
    nome_completo: Optional[str] = Field(default="", max_length=200)
    nome_guerra: Optional[str] = Field(default="", max_length=120)
    posto_graduacao: Optional[str] = Field(default="", max_length=80)
    posto: Optional[str] = Field(default="", max_length=80)
    cia: Optional[str] = Field(default="", max_length=40)
    funcao: Optional[str] = Field(default="", max_length=80)
    ordem: Optional[int] = Field(default=None, ge=1, le=999)


class QualityRedsComissaoConfigIn(BaseModel):
    dev: list[str] = Field(default_factory=list)
    comissao: list[QualityRedsComissaoMemberIn] = Field(default_factory=list)
    subcomissoes: dict[str, list[QualityRedsComissaoMemberIn]] = Field(default_factory=dict)
