@echo off
setlocal

schtasks /Delete /TN "Metas19_API_28100" /F
schtasks /Delete /TN "Metas19_API_28000" /F
schtasks /Delete /TN "Metas19_Agent" /F

echo Remocao concluida.
