# 🚀 Pipeline ETL Completo - Star Schema Unificado

**Versão:** 1.0  
**Data:** Dezembro 2025  
**Autor:** Germano Silva

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Estrutura do Pipeline](#estrutura-do-pipeline)
3. [Pré-requisitos](#pré-requisitos)
4. [Guia Rápido de Execução](#guia-rápido-de-execução)
5. [Scripts Disponíveis](#scripts-disponíveis)
6. [Resultados Esperados](#resultados-esperados)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

Este pipeline ETL integrado processa dados de **Educação**, **Mercado Laboral** e **AIMA/SEF** (2020-2024) para gerar um **Data Warehouse Star Schema Unificado** com **44 tabelas dimensionais e fato**.

### Fontes de Dados
- **Educação:** INE Censos 2011 + 2021
- **Laboral:** INE Censos 2021 - População Estrangeira
- **AIMA:** RIFA 2020-2022 (SEF) + RMA 2023-2024 (AIMA)

### Arquitetura
```
┌─────────────────────────────────────────┐
│     Pipeline ETL Consolidado            │
├─────────────────────────────────────────┤
│  FASE 1: ETL Educação (DP-01-A)        │
│  ├─ 13 Dimensões Base                   │
│  └─ 4 Fatos Educacionais                │
├─────────────────────────────────────────┤
│  FASE 2: ETL Laboral (DP-01-B)         │
│  ├─ 4 Dimensões Laborais                │
│  └─ 4 Fatos Laborais                    │
├─────────────────────────────────────────┤
│  FASE 3: ETL AIMA (DP-02-A)            │
│  ├─ 5 Dimensões AIMA                    │
│  └─ 7 Fatos AIMA                        │
├─────────────────────────────────────────┤
│  FASE 4: Validação Automática          │
│  └─ Verificação de 44 tabelas           │
└─────────────────────────────────────────┘
```

---

## 🏗️ Estrutura do Pipeline

### Scripts ETL Principais

| Script | Descrição | Entrada | Saída |
|--------|-----------|---------|-------|
| `ETL_EDUCACAO_CONSOLIDADO_v3.py` | Dados educacionais | `input/` + DP-01-A | 17 tabelas |
| `ETL_LABORAL_CONSOLIDADO.py` | Dados laborais | DP-01-A + DP-01-B1 | 11 tabelas |
| `ETL_AIMA_CONSOLIDADO.py` | Dados AIMA/SEF | DP-02-A2 | 13 tabelas |

### Scripts de Automação

| Script | Função |
|--------|--------|
| `preparar_dados.bat` | Verifica estrutura de pastas e disponibilidade de dados |
| `executar_pipeline_completo.bat` | **MASTER**: Executa todo o pipeline em sequência |
| `validar_tabelas.py` | Valida geração das 44 tabelas esperadas |

---

## ✅ Pré-requisitos

### Software
- **Python 3.8+** instalado e configurado no PATH
- **pandas** e **numpy** (instalação automática via `instalar_dependencias.bat`)

### Estrutura de Dados

```
3️⃣ Data Preparation/
├── scripts/
│   ├── input/                              # 12 CSVs de 2011
│   ├── ETL_EDUCACAO_CONSOLIDADO_v3.py
│   ├── ETL_LABORAL_CONSOLIDADO.py
│   ├── ETL_AIMA_CONSOLIDADO.py
│   ├── preparar_dados.bat
│   ├── executar_pipeline_completo.bat
│   ├── validar_tabelas.py
│   └── output/                             # Gerado automaticamente
│
└── data/processed/
    ├── DP-01-A/                            # Gerado pelo ETL Educação
    ├── DP-01-B/DP-01-B1/resultados_etl_laboral/  # Dados laborais
    └── DP-02-A/DP-02-A2/data/              # Dados AIMA
```

---

## 🚀 Guia Rápido de Execução

### Passo 1: Preparação (RECOMENDADO)

Execute para verificar se todos os dados estão disponíveis:

```batch
cd "3️⃣ Data Preparation\scripts"
preparar_dados.bat
```

**Verificações realizadas:**
- ✅ Estrutura de pastas (input/, output/)
- ✅ 12 arquivos CSV de 2011 em input/
- ✅ Pasta DP-01-A existe ou será criada
- ✅ Pasta DP-01-B1/resultados_etl_laboral/ com dados laborais
- ✅ Pasta DP-02-A2/data/ com dados AIMA (opcional)

### Passo 2: Execução do Pipeline Completo

Execute o pipeline master:

```batch
executar_pipeline_completo.bat
```

**O que acontece:**
1. ⏱️ Tempo estimado: **5-10 minutos**
2. 🔄 Execução automática de 3 fases (Educação → Laboral → AIMA)
3. ✅ Validação automática ao final
4. 📦 Geração de 3 arquivos ZIP em `output/`

### Passo 3: Verificação dos Resultados

Os resultados ficam em `scripts/output/`:

```
output/
├── ETL_EDUCACAO_CONSOLIDADO_2021_YYYYMMDD_HHMMSS.zip
├── ETL_LABORAL_CONSOLIDADO_2021_YYYYMMDD_HHMMSS.zip
└── ETL_AIMA_CONSOLIDADO_2020-2024_YYYYMMDD_HHMMSS.zip
```

**Validação automática mostrará:**
- ✅ Domínios completos (BASE, EDUCACAO, LABORAL, AIMA)
- ⚠️ Domínios parciais (% de cobertura)
- ❌ Domínios incompletos

---

## 📚 Scripts Disponíveis

### 1. preparar_dados.bat
**Função:** Verificação de pré-requisitos  
**Uso:**
```batch
preparar_dados.bat
```

**Saída esperada:**
```
========================================================================
  DADOS EDUCACAO (2011)
========================================================================
[OK] Dados de 2011 encontrados: 12 arquivos

========================================================================
  DADOS EDUCACAO (2021) - DP-01-A
========================================================================
[OK] Pasta DP-01-A encontrada
[OK] Arquivos em DP-01-A: XX

========================================================================
  DADOS LABORAL - DP-01-B1
========================================================================
[OK] Pasta resultados_etl_laboral encontrada
[OK] Arquivos laborais disponiveis: XX

========================================================================
  DADOS AIMA - DP-02-A2
========================================================================
[OK] Pasta DP-02-A2 encontrada
[OK] Arquivos AIMA disponiveis: XX
```

### 2. executar_pipeline_completo.bat
**Função:** Execução master do pipeline  
**Uso:**
```batch
executar_pipeline_completo.bat
```

**Fases de execução:**
```
FASE 1/3: ETL EDUCACAO (Dados Base + Educacionais)
└── Gera: 13 Dimensões + 4 Fatos = 17 tabelas

FASE 2/3: ETL LABORAL (Mercado de Trabalho)
└── Gera: 4 Dimensões + 4 Fatos = 8 tabelas (+3 compartilhadas)

FASE 3/3: ETL AIMA (Residencia e Concessoes)
└── Gera: 5 Dimensões + 7 Fatos = 12 tabelas (+1 compartilhada)

VALIDACAO: Checagem das 44 tabelas esperadas
```

### 3. validar_tabelas.py
**Função:** Validação de completude  
**Uso:**
```batch
python validar_tabelas.py
```

**Ou automaticamente ao final do pipeline completo.**

**Relatório gerado:**
```
========================================================================
  DOMINIO: BASE
========================================================================
  Dimensoes (5 esperadas):
    [OK] Encontradas: 5/5
  Fatos (8 esperados):
    [OK] Encontrados: 8/8
  Status: 13/13 (100.0%)
  [OK] Dominio COMPLETO

========================================================================
  RESUMO GERAL
========================================================================
  Total de tabelas esperadas: 44
  Total de tabelas encontradas: 44
  Percentual de cobertura: 100.0%
```

---

## 📊 Resultados Esperados

### Tabelas por Domínio

#### 🔹 BASE (13 tabelas)
**Dimensões (5):**
- Dim_PopulacaoResidente
- Dim_Nacionalidade
- Dim_Localidade
- Dim_Sexo
- Dim_GrupoEtario

**Fatos (8):**
- Fact_PopulacaoPorNacionalidade
- Fact_PopulacaoPorNacionalidadeSexo
- Fact_PopulacaoPorLocalidade
- Fact_PopulacaoPorLocalidadeNacionalidade
- Fact_PopulacaoPorGrupoEtario
- Fact_EvolucaoTemporal
- Fact_NacionalidadePrincipal
- Fact_DistribuicaoGeografica

#### 📚 EDUCACAO (4 tabelas)
**Dimensões (2):**
- Dim_NivelEducacao
- Dim_MapeamentoNacionalidades

**Fatos (2):**
- Fact_PopulacaoEducacao
- Fact_EstatisticasEducacao

#### 💼 LABORAL (15 tabelas)
**Dimensões (7):**
- Dim_CondicaoEconomica
- Dim_GrupoProfissional
- Dim_ProfissaoDigito1
- Dim_SetorEconomico
- Dim_SituacaoProfissional
- Dim_FonteRendimento
- Dim_RegiaoNUTS

**Fatos (8):**
- Fact_PopulacaoPorCondicao
- Fact_EmpregadosPorProfissao
- Fact_EmpregadosPorSetor
- Fact_EmpregadosPorSituacao
- Fact_EmpregadosProfSexo
- Fact_EmpregadosRegiaoSetor
- Fact_PopulacaoTrabalhoEscolaridade
- Fact_PopulacaoRendimentoRegiao

#### 🏛️ AIMA (12 tabelas)
**Dimensões (5):**
- Dim_AnoRelatorio
- Dim_TipoRelatorio
- Dim_Despacho
- Dim_MotivoConcessao
- Dim_NacionalidadeAIMA

**Fatos (7):**
- Fact_ConcessoesPorNacionalidadeSexo
- Fact_ConcessoesPorDespacho
- Fact_ConcessoesPorMotivoNacionalidade
- Fact_PopulacaoEstrangeiraPorNacionalidadeSexo
- Fact_DistribuicaoEtariaConcessoes
- Fact_EvolucaoPopulacaoEstrangeira
- Fact_PopulacaoResidenteEtaria

### Total: **44 tabelas**

---

## 🔧 Troubleshooting

### Problema: Python não encontrado
```
[ERRO] Python nao encontrado!
```
**Solução:**
1. Instale Python 3.8+ de [python.org](https://www.python.org/downloads/)
2. Durante instalação, marque "Add Python to PATH"
3. Reinicie o terminal

### Problema: Dependências faltando
```
[AVISO] Instalando dependencias...
```
**Solução:** Execute manualmente:
```batch
pip install pandas numpy
```

### Problema: Dados DP-01-A não encontrados
```
[ERRO] Pasta DP-01-A nao encontrada!
```
**Solução:**
- DP-01-A será criada automaticamente pelo ETL Educação
- Certifique-se de que os 12 CSVs de 2011 estão em `input/`

### Problema: ETL Laboral - arquivos não encontrados
```
[ERRO] Nenhuma pasta de resultados encontrada!
```
**Solução:** Verifique se existe:
```
3️⃣ Data Preparation/data/processed/DP-01-B/DP-01-B1/resultados_etl_laboral/
```
Ou pasta alternativa:
```
3️⃣ Data Preparation/data/processed/DP-01-B/DP-01-B1/Resultados_DP-01-B/
```

### Problema: ETL AIMA - dados não encontrados
```
[AVISO] Pasta DP-02-A2 nao encontrada
```
**Solução:**
- ETL AIMA é **opcional**
- Verifique se os dados foram processados em:
```
3️⃣ Data Preparation/data/processed/DP-02-A/DP-02-A2/data/
```

### Problema: Encoding Windows
```
UnicodeEncodeError: 'charmap' codec can't encode character...
```
**Solução:**
- Scripts já configurados para `chcp 65001` (UTF-8)
- Se persistir, execute manualmente:
```batch
chcp 65001
python ETL_EDUCACAO_CONSOLIDADO_v3.py
```

### Problema: Validação mostra tabelas faltantes
```
[FALTA] Fact_EmpregadosProfSexo
```
**Solução:**
1. Verifique qual ETL não gerou as tabelas
2. Execute o ETL individualmente para ver logs:
```batch
python ETL_LABORAL_CONSOLIDADO.py
```
3. Corrija dados de entrada conforme erro reportado

---

## 📞 Suporte

Para problemas adicionais:
1. Verifique logs detalhados durante execução
2. Execute `preparar_dados.bat` para diagnóstico
3. Execute ETLs individuais para isolar o problema
4. Consulte README específicos:
   - `README_ETL_EDUCACAO.md`
   - `README_ETL_LABORAL.md`

---

## 📝 Notas Técnicas

### Compatibilidade
- ✅ Windows 10/11
- ✅ Python 3.8, 3.9, 3.10, 3.11, 3.12
- ✅ Google Colab (scripts individuais)

### Encoding
- Todos os CSVs gerados em **UTF-8**
- Scripts configurados para Windows (cp1252 → UTF-8)

### Performance
- Pipeline completo: ~5-10 minutos
- ETL Educação: ~2-3 minutos
- ETL Laboral: ~1-2 minutos
- ETL AIMA: ~1-2 minutos

### Armazenamento
- ZIPs gerados: ~5-20 MB cada
- CSVs extraídos: ~10-50 MB total

---

## 🎉 Conclusão

Após execução bem-sucedida:

1. ✅ **44 tabelas** Star Schema geradas
2. ✅ **3 arquivos ZIP** prontos em `output/`
3. ✅ Dados validados e íntegros
4. ✅ Pronto para **import em banco de dados**
5. ✅ Pronto para **análises e dashboards**

**Próximos passos:**
- Extrair ZIPs
- Importar CSVs para PostgreSQL/MySQL/SQL Server
- Criar visualizações em Power BI/Tableau
- Desenvolver análises estatísticas com Python/R

---

**Versão:** 1.0  
**Última atualização:** Dezembro 2025  
**Projeto:** Concurso de Pesquisa Prepara Portugal 2025
