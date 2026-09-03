@echo off
title Inventario Casa ⚡
echo ========================================================
echo   INVENTARIO CASA - INICIANDO SISTEMA
echo ========================================================
cd /d "%~dp0"
if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe iniciar.py
) else (
    python iniciar.py
)
pause
