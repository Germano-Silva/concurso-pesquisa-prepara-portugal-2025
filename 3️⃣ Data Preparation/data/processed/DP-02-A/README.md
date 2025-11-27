# 🏠 DP-02-A: Dados de Motivos de Residência

## 🎯 Objetivo
Consolidar dados de concessão de títulos por motivos (2020-2024) como complemento aos dados censitários.

---

## ✅ Critérios de Aceitação

- ✅ Dados de motivos consolidados para 2020-2024
- ✅ Categorias padronizadas (atividade profissional, estudo, reagrupamento, AR CPLP)
- ✅ Percentagens calculadas por ano  
- ✅ Arquivo salvo em `/data/processed/`

---

## 📊 Dataset Gerado

**📁 Arquivo:** `dados_motivos_residencia.csv`  
**📂 Localização:** `3️⃣ Data Preparation/data/processed/DP-02-A/`  
**📅 Período:** 2020-2024 (5 anos)  
**📈 Registros:** 25 (5 anos × 5 categorias)

### 🗂️ Estrutura do Dataset

| Coluna | Tipo | Descrição |
| :----- | :--- | :--------- |
| Ano | string | Ano de referência (2020-2024) |
| Motivo | string | Categoria do motivo de residência |
| Total | integer | Número absoluto de concessões |
| Percentagem | float | Percentual anual (0-100%) |

### 🏷️ Categorias de Motivos

| Categoria | Descrição |
| :--------- | :--------- |
| 💼 ATIVIDADE PROFISSIONAL | Trabalho e atividade profissional |
| 🎓 ESTUDO | Motivos educacionais |
| 👨‍👩‍👧‍👦 REAGRUPAMENTO FAMILIAR | Reunificação familiar |
| 🌍 AR CPLP | Acordo de Residência para CPLP |
| 📦 OUTROS | Demais motivos |

---

## 🛠️ Scripts Desenvolvidos

| Script | Descrição |
| :----- | :--------- |
| `processar_motivos.py` | Versão inicial do script de processamento |
| `processar_motivos_corrigido.py` | Versão corrigida e robusta |
| `verificar.py` | Scripts de validação e verificação |

---

## 📈 Resultados Obtidos

- **📅 Período:** 2020-2024 (5 anos completos)
- **📊 Registros:** 25 (5 anos × 5 categorias)
- **🏷️ Categorias:** 5 motivos padronizados
- **📐 Métricas:** Totais absolutos e percentuais anuais

---

## 🔍 Fontes dos Dados

| Ano | Fonte | Tipo |
| :-- | :---- | :--- |
| 2020-2022 | RIFA - SEF | Relatórios de Imigração |
| 2023-2024 | RMA - AIMA | Relatórios de Migração |

**📍 Localização:** `2️⃣ Data Understanding/data/raw/aima/extraidas/`

---

## 📁 Estrutura de Arquivos
