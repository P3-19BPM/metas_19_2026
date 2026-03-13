---
title: "Painel de Gestão Operacional - 19º BPM (PMMG)"
output: html_document
---

Painel interativo de Business Intelligence (BI) desenvolvido para a Polícia Militar de Minas Gerais (19º BPM). O sistema monitora indicadores criminais (IMV, ICVPe, ICVPa, etc.) e de produtividade, permitindo a análise hierárquica desde a Unidade (Cia) até o nível de Subsetor, com suporte a cálculo de metas e PLR.

## 🚀 Funcionalidades Principais

*   **Visão Hierárquica:** Navegação detalhada Unidade -> Pelotão -> Setor.
*   **Filtros Avançados:**
    *   Multisseleção de meses (Acumulado dinâmico).
    *   Filtro por Unidade (Cia).
*   **Cálculo de Indicadores:**
    *   Valores Absolutos.
    *   Taxas por 100 mil habitantes (cálculo automático).
    *   Integração com metas de PLR (Prêmio de Produtividade).
*   **Resumo Estratégico:** Card de totais do Batalhão vs Meta PLR no topo.
*   **Segurança de Dados:** Arquitetura desacoplada onde o código (Frontend) é separado dos dados sensíveis (JSON), garantindo que informações estratégicas não fiquem expostas na imagem Docker.
*   **Design Responsivo:** Interface otimizada para Desktop e Mobile seguindo a identidade visual da PMMG (Cores heráldicas).

## 📂 Estrutura do Projeto

A arquitetura foi desenhada para facilitar o deploy seguro via Docker.

```
metas_19_2026/
├── docker-compose.yml       # Orquestração para VPS (Define volumes e portas)
├── Dockerfile               # Receita da imagem Nginx (Apenas código, sem dados)
├── nginx.conf               # Configuração do servidor web otimizada
└── public/                  # Código Fonte do Painel
    ├── index.html           # Aplicação Single Page (Alpine.js + Tailwind)
    └── data/                # [IMPORTANTE] Pasta de dados
        ├── metas_plr.json   # Dados unificados de PLR (Meta)
        └── *.json           # Arquivos de dados hierárquicos (IMV, ICVPa, etc.)
```

**Nota de Segurança:** No repositório git e na imagem Docker, a pasta `public/data` deve conter apenas arquivos de exemplo ou estar vazia. Os dados reais são injetados na VPS via Docker Volumes.

## 🛠️ Tecnologias Utilizadas

*   **Frontend:** HTML5, Alpine.js (Lógica Reativa), Tailwind CSS (Estilização).
*   **Infraestrutura:** Docker, Nginx.
*   **Dados:** JSON estruturado.
*   **Backend/Agent:** Python 3.11+ com dependências do arquivo `agent/requirements.txt`.

## ⚙️ Configuração do Ambiente Virtual (Windows)

### Ativar o Ambiente Virtual

No PowerShell, execute na pasta raiz do projeto:
```powershell
# Para PowerShell
.\venvMETAS\Scripts\Activate.ps1

# Se receber erro de execução, execute primeiro:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

No Command Prompt (CMD), execute:
```cmd
venvMETAS\Scripts\activate.bat
```

Após ativar, você verá `(venvMETAS)` no início da linha do terminal.

### Instalar Dependências

Com o ambiente ativado, execute:
```powershell
pip install -r agent/requirements.txt
```

## 🤖 Como Executar o Agent

Com o ambiente virtual ativado, execute na pasta raiz:
```powershell
python agent/agent.py

cd E:\GitHub\metas_19_2026 
.\venvMETAS\Scripts\python.exe -u .\agent\agent.py                                                            
   
```

**Nota:** O agent requer um arquivo `.env` na pasta `agent/` com as seguintes variáveis:
```
AGENT_API_URL=http://localhost:8000
AGENT_TOKEN=seu_token_aqui
AGENT_ID=pc-local
AGENT_VERSION=0.1.0
AGENT_HEARTBEAT_SECONDS=30
AGENT_POLL_SECONDS=20
```

## 🐳 Como Rodar (Desenvolvimento Local)

1.  Clone o repositório.
2.  Coloque os arquivos JSON reais na pasta `public/data/` da sua máquina.
3.  Abra o arquivo `public/index.html` usando uma extensão de servidor local (ex: Live Server do VS Code) para evitar erros de CORS (bloqueio de leitura de arquivos locais pelo navegador).

## 🚢 Como Rodar (Produção / VPS)

Para colocar o projeto no ar utilizando DockerHub e EasyPanel, consulte o arquivo `DEPLOY_GUIDE.md` anexo a este projeto. Ele contém o passo a passo para configurar volumes e manter os dados seguros.

## 📝 Autoria

Desenvolvido para o Setor de Inteligência do 19º BPM - Teófilo Otoni/MG.
