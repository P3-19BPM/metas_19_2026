Você é um engenheiro de software especialista em:

Python
FastAPI
ETL de dados
análise de indicadores criminais
auditoria de registros policiais

Você está trabalhando em um projeto real chamado:

metas_19_2026

Estrutura atual do projeto:

metas_19_2026/
    api/
        main.py
        requirements.txt
    agent/
        agent.py
        requirements.txt

O projeto já possui:

API FastAPI
PostgreSQL
SQLAlchemy
Agent ETL que executa scripts SQL no BISP
envio de dados para endpoints da API

O Agent já envia:

/ingest/kpi
/ingest/reds
/ingest/fact

Agora precisamos criar um módulo de:

ANÁLISE DE QUALIDADE DE REDS

Esse módulo será integrado ao projeto atual.

--------------------------------------------------

OBJETIVO

Avaliar automaticamente a qualidade dos registros REDS e auxiliar as Comissões de Avaliação da Qualidade.

O sistema deve analisar:

qualidade textual do histórico
coerência entre natureza e histórico
padrões operacionais
classificação correta da natureza

--------------------------------------------------

ESTRUTURA DO NOVO MÓDULO

Criar dentro da API:

api/services/quality_reds/

arquivos:

text_quality_engine.py
data_convergence_engine.py
pattern_engine.py
nature_classifier_engine.py
quality_service.py

--------------------------------------------------

MOTOR 1 — TEXT QUALITY ENGINE

Responsável por analisar a qualidade textual do histórico.

Entrada:

historico_ocorrencia

Avaliar:

tamanho do histórico
erros ortográficos
pontuação
uso de linguagem genérica
históricos muito curtos

Bibliotecas sugeridas:

language_tool_python
spacy
regex

Retornar score de qualidade textual:

0 a 100

--------------------------------------------------

MOTOR 2 — DATA CONVERGENCE ENGINE

Responsável por verificar coerência entre campos.

Entrada:

natureza_codigo
historico_ocorrencia
logradouro
bairro
pelotao

Verificar:

se o histórico menciona elementos incompatíveis com a natureza
se o local é coerente com a área do pelotão
se campos estão inconsistentes

Exemplo:

natureza = ameaça
histórico menciona "subtraiu celular"

alerta:

possível erro de classificação

--------------------------------------------------

MOTOR 3 — OPERATIONAL PATTERN ENGINE

Detecta padrões operacionais suspeitos.

Analisar:

uso excessivo de naturezas administrativas

C99
W01
W02
W03
W04

Detectar:

históricos repetidos
históricos padrão
copy/paste de registros

Utilizar:

similaridade textual
TF-IDF
cosine similarity

--------------------------------------------------

MOTOR 4 — NATURE CLASSIFIER ENGINE

Classificador de natureza baseado no histórico.

Entrada:

historico_ocorrencia

Utilizar:

NLP
regras baseadas em palavras-chave

Exemplo:

histórico contém:

subtraiu
ameaça
arma

sugerir:

ROUBO

Se natureza registrada for diferente, gerar alerta.

--------------------------------------------------

SERVIÇO CENTRAL

Criar:

quality_service.py

Responsável por:

receber registros REDS
executar os quatro motores
consolidar resultados
salvar no banco

--------------------------------------------------

BANCO DE DADOS

Criar tabela:

reds_quality_analysis

campos:

numero_ocorrencia
score_textual
alerta_texto_curto
erro_ortografico
alerta_convergencia
alerta_padrao_operacional
alerta_classificacao
natureza_sugerida
data_analise

--------------------------------------------------

ENDPOINTS DA API

Criar endpoints:

POST /quality/reds/analyze

GET /quality/reds/dashboard

GET /quality/reds/company/{cia}

GET /quality/reds/report/{cia}

--------------------------------------------------

DASHBOARD

Criar métricas:

REDS analisados
inconformidades
históricos curtos
erros de natureza
ranking de companhias

--------------------------------------------------

IMPORTANTE

O sistema deve ser preparado para evolução futura.

Deve permitir análise de indicadores:

IMV
ICVPe
ICVPa

E também:

furtos
estupro
estupro de vulnerável