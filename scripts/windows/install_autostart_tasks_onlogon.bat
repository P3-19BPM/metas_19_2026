@echo off
setlocal

set "ROOT=%~dp0..\.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

set "API_VBS=%ROOT%\scripts\windows\run_api_28100_hidden.vbs"
set "AGENT_VBS=%ROOT%\scripts\windows\run_agent_hidden.vbs"
set "WSCRIPT=%SystemRoot%\System32\wscript.exe"

if not exist "%API_VBS%" (
  echo [ERROR] Arquivo nao encontrado: "%API_VBS%"
  exit /b 1
)

if not exist "%AGENT_VBS%" (
  echo [ERROR] Arquivo nao encontrado: "%AGENT_VBS%"
  exit /b 1
)

echo Criando tarefas no logon do usuario atual...
schtasks /Create /TN "Metas19_API_28100" /SC ONLOGON /RL LIMITED /TR "\"%WSCRIPT%\" \"\"%API_VBS%\"\"" /F
if errorlevel 1 (
  echo [ERROR] Falha ao criar tarefa Metas19_API_28100
  exit /b 1
)

schtasks /Create /TN "Metas19_Agent" /SC ONLOGON /RL LIMITED /TR "\"%WSCRIPT%\" \"\"%AGENT_VBS%\"\"" /F
if errorlevel 1 (
  echo [ERROR] Falha ao criar tarefa Metas19_Agent
  exit /b 1
)

echo Tarefas ONLOGON criadas com sucesso.
echo Logs:
echo   %ROOT%\logs\api_28100.log
echo   %ROOT%\logs\agent.log
