# API + Agent (estrutura inicial)

Esta estrutura inicia o projeto com:
- `api/`: FastAPI + SQLite + scheduler para job diario apos 06:00
- `agent/`: agente local (PC fisico) que envia heartbeat e executa jobs

## Fluxo atual
1. Agent envia `/heartbeat` a cada 30s
2. API marca agente online
3. Scheduler cria job diario apos 06h quando online >= 3 min
4. Agent busca `/jobs/next`, executa e publica `/jobs/{id}/result`

## Proximos passos imediatos
- Integrar `agent/agent.py` ao seu extrator real (`exemplo_projeto_extracao_bisp.py`)
- Criar endpoint de ingestao de KPIs no `api/` para popular `kpi_daily`
- Conectar `public/index.html` no endpoint `/status` e KPIs

## Seguranca
- Credenciais BISP ficam somente no `.env` do PC fisico.
- VPS recebe somente resultado consolidado, nunca usuario/senha.
