# Engenharia - Ambiente Python (venvMETAS)

## 1) Criar o ambiente virtual
No diretório raiz do projeto, execute:

```powershell
python -m venv venvMETAS
```

## 2) Ativar o ambiente virtual
No PowerShell:

```powershell
.\venvMETAS\Scripts\Activate.ps1
```

No Prompt de Comando (cmd):

```bat
venvMETAS\Scripts\activate.bat
```

## 3) Instalar dependências (caminhos corretos)
Este projeto tem requirements separados por serviço.

Para API (FastAPI + Uvicorn):

```powershell
pip install -r api\requirements.txt
```

Para Agent (executor local):

```powershell
pip install -r agent\requirements.txt
```

Se quiser instalar ambos no mesmo venv:

```powershell
pip install -r api\requirements.txt
pip install -r agent\requirements.txt
```

## 4) Rodar a API local na porta 8181
Use este comando na raiz do projeto:

```powershell
python -m uvicorn api.app.main:app --reload --port 8181
```

Abrir no navegador:

- Health: `http://127.0.0.1:8181/health`
- Docs Swagger: `http://127.0.0.1:8181/docs`

## 5) Erro comum: "uvicorn não é reconhecido"
Se aparecer esse erro:

- provavelmente você instalou apenas `agent\requirements.txt` (não instala uvicorn), ou
- o executável `uvicorn` não entrou no PATH do venv.

Solução recomendada:

1. Instalar os pacotes da API:

```powershell
pip install -r api\requirements.txt
```

2. Executar com módulo Python (mais confiável):

```powershell
python -m uvicorn api.app.main:app --reload --port 8181
```

## 6) Atualizar/gerar requirements
Para salvar as versões atuais do ambiente:

```powershell
pip freeze > requirements.txt
```

Observação: como temos dois serviços, o ideal é manter `api\requirements.txt` e `agent\requirements.txt` separados.

## 7) (Opcional) Desativar ambiente virtual

```powershell
deactivate
```
