@echo off
chcp 65001 > nul
echo ========================================================================
echo   PIPELINE ETL COMPLETO - EXECUCAO AUTOMATICA
echo   Educacao + Laboral + AIMA (40 tabelas)
echo ========================================================================
echo.
echo [INFO] Este script executa TUDO automaticamente:
echo   - Instala dependencias (pandas, numpy)
echo   - ETL Educacao (15 tabelas)
echo   - ETL Laboral (11 tabelas)
echo   - ETL AIMA (14 tabelas)
echo   - Validacao automatica
echo.
echo [TEMPO ESTIMADO] 5-10 minutos
echo.
pause

REM Ir para a pasta do script
cd /d "%~dp0"

echo.
echo ========================================================================
echo   FASE 1: VERIFICACAO E INSTALACAO DE DEPENDENCIAS
echo ========================================================================
echo.

REM Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado!
    echo [INFO] Instale Python 3.8+: https://www.python.org/downloads/
    pause
    exit /b 1
)
echo [OK] Python encontrado
python --version

REM Instalar/verificar dependencias
echo.
echo [INFO] Verificando dependencias...
python -c "import pandas; import numpy" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Instalando pandas e numpy...
    python -m pip install --quiet pandas numpy
    if errorlevel 1 (
        echo [ERRO] Falha na instalacao
        pause
        exit /b 1
    )
)
echo [OK] Dependencias instaladas

REM ========================================================================
REM FASE 2: ETL EDUCACAO
REM ========================================================================
echo.
echo ========================================================================
echo   FASE 2: ETL EDUCACAO (15 tabelas)
echo ========================================================================
echo.
python ETL_EDUCACAO_CONSOLIDADO_v3.py
if errorlevel 1 (
    echo [ERRO] Falha no ETL Educacao!
    pause
    exit /b 1
)
echo.
echo [OK] ETL Educacao concluido!

REM ========================================================================
REM FASE 3: ETL LABORAL
REM ========================================================================
echo.
echo ========================================================================
echo   FASE 3: ETL LABORAL (11 tabelas)
echo ========================================================================
echo.
python ETL_LABORAL_CONSOLIDADO.py
if errorlevel 1 (
    echo [ERRO] Falha no ETL Laboral!
    pause
    exit /b 1
)
echo.
echo [OK] ETL Laboral concluido!

REM ========================================================================
REM FASE 4: ETL AIMA
REM ========================================================================
echo.
echo ========================================================================
echo   FASE 4: ETL AIMA (14 tabelas)
echo ========================================================================
echo.
python ETL_AIMA_CONSOLIDADO.py
if errorlevel 1 (
    echo [AVISO] Falha no ETL AIMA (opcional)
    goto validacao
)
echo.
echo [OK] ETL AIMA concluido!

REM ========================================================================
REM FASE 5: VALIDACAO AUTOMATICA
REM ========================================================================
:validacao
echo.
echo ========================================================================
echo   FASE 5: VALIDACAO AUTOMATICA
echo ========================================================================
echo.
python validar_tabelas.py

REM ========================================================================
REM RESUMO FINAL
REM ========================================================================
echo.
echo ========================================================================
echo   PIPELINE COMPLETO FINALIZADO!
echo ========================================================================
echo.
echo [OK] Resultados gerados em: %CD%\output\
echo.
echo [INFO] 3 arquivos ZIP criados:
dir /b output\*.zip 2>nul
echo.
echo [INFO] Total de ~40 tabelas geradas
echo   - ETL_EDUCACAO: 15 tabelas (Base + Educacao)
echo   - ETL_LABORAL: 11 tabelas (Laborais)
echo   - ETL_AIMA: 14 tabelas (AIMA 2020-2024)
echo.

REM Abrir pasta output
if exist "output" (
    echo [INFO] Abrindo pasta output...
    explorer output
)

echo.
echo ========================================================================
echo   PROXIMOS PASSOS
echo ========================================================================
echo.
echo 1. Extraia os 3 arquivos ZIP
echo 2. Importe CSVs para banco de dados
echo 3. Crie dashboards e analises
echo.
echo Obrigado por usar o Pipeline ETL!
echo.
pause
