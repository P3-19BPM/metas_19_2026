@echo off
setlocal

set "ROOT=%~dp0..\.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

set "PYTHON_EXE=%ROOT%\venvMETAS\Scripts\python.exe"
set "AGENT_DIR=%ROOT%\agent"
set "LOG_DIR=%ROOT%\logs"
set "LOG_FILE=%LOG_DIR%\agent.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python do venv nao encontrado: "%PYTHON_EXE%"
  exit /b 1
)

:loop
echo [%date% %time%] Iniciando agent.py >> "%LOG_FILE%"
pushd "%AGENT_DIR%"
"%PYTHON_EXE%" -u agent.py >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
popd
echo [%date% %time%] Agent encerrou com codigo %EXIT_CODE%. Reiniciando em 5s... >> "%LOG_FILE%"
timeout /t 5 /nobreak >nul
goto loop

