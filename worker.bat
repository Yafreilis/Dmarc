@echo off
setlocal

cd /d "C:\Users\Lenovo\Downloads\dmarc-ingesta"
set "LOG_FILE=%~dp0worker_execution.log"

echo %DATE% %TIME% - Iniciando worker.bat >> "%LOG_FILE%"
echo CWD actual: %CD% >> "%LOG_FILE%"

call .venv\Scripts\activate >nul 2>&1
echo Resultado de activate: %ERRORLEVEL% >> "%LOG_FILE%"

where python >> "%LOG_FILE%" 2>&1

python worker.py >> "%LOG_FILE%" 2>&1

endlocal