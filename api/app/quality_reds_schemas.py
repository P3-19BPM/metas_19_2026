from typing import Optional

from pydantic import BaseModel, Field


class QualityRedsRawRowIn(BaseModel):
    numero_ocorrencia: str = Field(min_length=1, max_length=80)
    cia_pm: Optional[str] = Field(default="", max_length=120)
    pelotao_pm: Optional[str] = Field(default="", max_length=180)
    data_hora_fato: Optional[str] = Field(default=None, max_length=40)
    data_fato: Optional[str] = Field(default=None, max_length=30)
    mes: Optional[int] = Field(default=None, ge=1, le=12)
    ano: Optional[int] = Field(default=None, ge=2000, le=2100)
    data_hora_fechamento: Optional[str] = Field(default=None, max_length=40)
    data_hora_alteracao: Optional[str] = Field(default=None, max_length=40)
    data_hora_primeiro_fim_preenchimento: Optional[str] = Field(default=None, max_length=40)
    nome_tipo_relatorio: Optional[str] = Field(default="", max_length=80)
    digitador_cargo_efetivo: Optional[str] = Field(default="", max_length=120)
    digitador_matricula: Optional[str] = Field(default="", max_length=80)
    digitador_nome: Optional[str] = Field(default="", max_length=180)
    digitador_nome_unidade: Optional[str] = Field(default="", max_length=180)
    natureza_codigo: Optional[str] = Field(default="", max_length=80)
    natureza_descricao_longa: Optional[str] = Field(default="", max_length=400)
    historico_ocorrencia: Optional[str] = Field(default="", max_length=12000)


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


class QualityRedsEvaluationRowIn(BaseModel):
    numero_ocorrencia: str = Field(min_length=1, max_length=80)
    avaliado: bool = Field(default=False)
    ajustes: list[str] = Field(default_factory=list)
    observacao: Optional[str] = Field(default="", max_length=4000)


class QualityRedsEvaluationUpsertIn(BaseModel):
    rows: list[QualityRedsEvaluationRowIn] = Field(default_factory=list)
