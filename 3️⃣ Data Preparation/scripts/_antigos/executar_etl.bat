@echo off
REM ============================================================
REM Executar ETL EDUCACAO
REM ============================================================

cd /d "%~dp0"
python ETL_EDUCACAO_HIBRIDO.py
pause
