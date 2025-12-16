# 🚀 Pipeline ETL Completo - Guia Rápido

## ⚡ Execução Simplificada

### Opção 1: Um Clique (RECOMENDADO)

Execute o arquivo master que faz TUDO automaticamente:

```batch
EXECUTAR_TUDO.bat
```

**O que este arquivo faz:**
1. ✅ Verifica e instala dependências (pandas, numpy)
2. ✅ Executa ETL Educação (15 tabelas)
3. ✅ Executa ETL Laboral (11 tabelas)
4. ✅ Executa ETL AIMA (14 tabelas)
5. ✅ Valida todas as tabelas geradas
6. ✅ Abre pasta com resultados

**Tempo estimado:** 5-10 minutos

---

## 📦 Resultados Gerados

Após execução, você terá **3 arquivos ZIP** em `output/`:

```
output/
├── ETL_EDUCACAO_CONSOLIDADO_2011_2021_[timestamp].zip (15 tabelas)
├── ETL_LABORAL_CONSOLIDADO_2021_[timestamp].zip (11 tabelas)
└── ETL_AIMA_CONSOLIDADO_2020-2024_[timestamp].zip (14 tabelas)
```

**Total:** ~40 tabelas Star Schema

---

## 📊 Estrutura de Arquivos

```
scripts/
├── EXECUTAR_TUDO.bat                    ← ARQUIVO MASTER (execute este!)
├── ETL_EDUCACAO_CONSOLIDADO_v3.py       ← ETL Educação
├── ETL_LABORAL_CONSOLIDADO.py           ← ETL Laboral
├── ETL_AIMA_CONSOLIDADO.py              ← ETL AIMA
├── validar_tabelas.py                   ← Validador
├── README.md                            ← Este arquivo
├── README_PIPELINE_COMPLETO.md          ← Documentação detalhada
├── input/                               ← Dados de 2011 (12 CSVs)
└── output/                              ← Resultados (ZIPs gerados)
```

---

## 🔧 Execução Manual (Opcional)

Se preferir executar cada ETL separadamente:

```batch
# ETL 1: Educação (Base + Educação)
python ETL_EDUCACAO_CONSOLIDADO_v3.py

# ETL 2: Laboral (Mercado de Trabalho)
python ETL_LABORAL_CONSOLIDADO.py

# ETL 3: AIMA (Residência e Concessões 2020-2024)
python ETL_AIMA_CONSOLIDADO.py

# Validar resultados
python validar_tabelas.py
```

---

## ✅ Pré-requisitos

- **Python 3.8+** instalado
- **Pandas e Numpy** (instalados automaticamente pelo EXECUTAR_TUDO.bat)
- **Dados fonte:**
  - ✅ input/ com 12 CSVs de 2011
  - ✅ ../data/processed/DP-01-A/ (dados 2021)
  - ✅ ../data/processed/DP-01-B/DP-01-B1/ (dados laborais)
  - ✅ ../data/processed/DP-02-A/DP-02-A2/ (dados AIMA)

---

## 📈 Validação

O script `validar_tabelas.py` verifica:
- ✅ 44 tabelas esperadas conforme diagrama ER
- ✅ Cobertura por domínio (BASE, EDUCACAO, LABORAL, AIMA)
- ✅ Completude e integridade dos dados

---

## 🆘 Problemas?

### Python não encontrado
```batch
# Instale Python 3.8+: https://www.python.org/downloads/
# Marque "Add Python to PATH" durante instalação
```

### Erros de dependências
```batch
# Instale manualmente:
python -m pip install pandas numpy
```

### Dados não encontrados
```batch
# Verifique se as pastas de dados existem:
# - input/ (12 CSVs de 2011)
# - ../data/processed/DP-01-A/
# - ../data/processed/DP-01-B/DP-01-B1/
# - ../data/processed/DP-02-A/DP-02-A2/
```

---

## 📚 Documentação Completa

Para detalhes técnicos completos, consulte:
- `README_PIPELINE_COMPLETO.md` - Documentação detalhada
- Cada arquivo .py tem documentação inline

---

## 🎯 Próximos Passos

1. Execute `EXECUTAR_TUDO.bat`
2. Extraia os 3 arquivos ZIP
3. Importe CSVs para banco de dados (PostgreSQL/MySQL/SQL Server)
4. Crie dashboards e análises (Power BI/Tableau)

---

**Versão:** 1.0  
**Projeto:** Concurso de Pesquisa Prepara Portugal 2025  
**Autor:** Germano Silva
