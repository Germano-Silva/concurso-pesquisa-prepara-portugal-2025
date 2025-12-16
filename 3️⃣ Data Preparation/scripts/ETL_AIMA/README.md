# 📊 ETL_AIMA - Pipeline de Integração RIFA/RMA (2020-2024)

**Pipeline ETL modular para transformação de dados AIMA em Star Schema**  
*Compatível com Google Colab | Integração com ETL_EDUCACAO e ETL_LABORAL*

---

## 🎯 Visão Geral

O **ETL_AIMA** é o terceiro e último pipeline da série de transformação de dados do concurso de pesquisa sobre imigração em Portugal. Ele processa dados dos relatórios RIFA (SEF, 2020-2022) e RMA (AIMA, 2023-2024), transformando-os em um modelo Star Schema otimizado para análises temporais e integração com dados educacionais e laborais.

### 📈 Dados Processados
- **Período**: 2020-2024 (5 anos)
- **Fontes**: RIFA (Relatório de Imigração, Fronteiras e Asilo) + RMA (Relatório de Migração e Asilo)
- **Cobertura**: ~200 nacionalidades, 7 faixas etárias, múltiplos motivos de concessão

### 🏗️ Arquitetura do Pipeline

```
┌──────────────────────────────────────────────────────────┐
│              PIPELINE ETL_AIMA                           │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  FASE 1: Upload de Dados (Google Colab)                │
│    ↓                                                     │
│  FASE 2: Integração ETL_EDUCACAO/LABORAL               │
│    ↓                                                     │
│  FASE 3: Extração e Consolidação                       │
│    ↓                                                     │
│  FASE 4: Transformação → Dimensões (5 tabelas)         │
│    ↓                                                     │
│  FASE 5: Transformação → Fatos (7 tabelas)             │
│    ↓                                                     │
│  FASE 6: Validação de Integridade                      │
│    ↓                                                     │
│  FASE 7: Exportação (ZIP ou Individual)                │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## 📦 Estrutura de Arquivos

```
ETL_AIMA/
├── parte_01_imports_config.py          # Configurações e constantes
├── parte_02_classes_base_ref.py        # Classes base (import/fallback)
├── parte_03_transformador_dimensoes_aima.py  # Criação de dimensões
├── parte_04_transformador_fatos_aima.py      # Criação de fatos
├── parte_05_orquestrador_aima.py       # Orquestrador principal
└── README.md                            # Este arquivo
```

**Total**: 5 arquivos Python + 1 documentação  
**Linhas de código**: ~2.500 linhas

---

## 🗃️ Modelo de Dados - Star Schema

### 📁 Dimensões (5 tabelas)

| Tabela | Descrição | Registros Estimados |
|--------|-----------|---------------------|
| **Dim_AnoRelatorio** | Anos de 2020-2024 com fonte (RIFA/RMA) | 5 |
| **Dim_TipoRelatorio** | Tipos de relatórios (Concessões, Pop. Estrangeira, etc.) | 3 |
| **Dim_Despacho** | Códigos de despachos (AP, VLD, TR, CPLP) | ~5-10 |
| **Dim_MotivoConcessao** | Motivos de concessão (Trabalho, Estudo, Família, CPLP) | 5 |
| **Dim_NacionalidadeAIMA** | Nacionalidades com FK para Dim_Nacionalidade | ~200 |

### 📊 Fatos (7 tabelas)

| Tabela | Descrição | Granularidade |
|--------|-----------|---------------|
| **Fact_ConcessoesPorNacionalidadeSexo** | Concessões por país e sexo | Ano × Nacionalidade × Sexo |
| **Fact_ConcessoesPorDespacho** | Concessões por tipo de despacho | Ano × Despacho |
| **Fact_ConcessoesPorMotivoNacionalidade** | Concessões por motivo e nacionalidade | Ano × Motivo × Nacionalidade |
| **Fact_PopulacaoEstrangeiraPorNacionalidadeSexo** | População estrangeira residente | Ano × Nacionalidade × Sexo |
| **Fact_DistribuicaoEtariaConcessoes** | Distribuição etária de concessões | Ano × Faixa Etária × Sexo |
| **Fact_EvolucaoPopulacaoEstrangeira** | Evolução anual com variação % | Ano (série temporal) |
| **Fact_PopulacaoResidenteEtaria** | População residente por faixa etária | Ano × Faixa Etária |

**Total de Tabelas**: 12 (5 Dim + 7 Fact)

---

## 🚀 Guia de Uso no Google Colab

### 📋 Pré-requisitos

1. Conta Google com acesso ao Google Colab
2. Dados AIMA em formato CSV (por ano, 2020-2024)
3. *Opcional*: Dimensões do ETL_EDUCACAO/LABORAL para integração

### 🔧 Instalação

**Opção A: Upload Direto no Colab**

```python
# 1. Faça upload dos 5 arquivos .py para o Colab
# 2. Execute a célula:

from parte_05_orquestrador_aima import executar_pipeline_aima

# Executar pipeline standalone
orquestrador = executar_pipeline_aima()
```

**Opção B: Clone do Repositório**

```python
# 1. Clone o repositório
!git clone https://github.com/Germano-Silva/concurso-pesquisa-prepara-portugal-2025.git

# 2. Navegue até o diretório
import os
os.chdir('concurso-pesquisa-prepara-portugal-2025/3️⃣ Data Preparation/scripts/ETL_AIMA')

# 3. Execute
from parte_05_orquestrador_aima import executar_pipeline_aima
orquestrador = executar_pipeline_aima()
```

### 📤 Preparação dos Dados

**Estrutura Esperada dos Arquivos CSV:**

```
2020/
├── ConcessaoTitulosResidencia.csv         # Nacionalidade, Homens, Mulheres, Total
├── ConcessaoTitulosDespachos.csv          # Despacho, Concessoes
├── ConcessaoTitulosDistribuicaoEtaria.csv # FaixaEtaria, Homens, Mulheres
├── ConcessaoTitulosMotivo.csv             # Motivo, Nacionalidade, Total
├── DespachosDescricao.csv                 # Despacho, Descricao
├── PopulacaoEstrangeiraResidente.csv      # Nacionalidade, Homens, Mulheres
├── PopulacaoEstrangeiraResidenteEvolucao.csv  # Ano, TitulosRes, AP, VLD, Total
└── PopulacaoResidenteDistribuicaoEtaria.csv   # FaixaEtaria, Total

(Repetir estrutura para 2021, 2022, 2023, 2024)
```

---

## 💡 Exemplos de Uso

### Exemplo 1: Execução Básica (Standalone)

```python
from parte_05_orquestrador_aima import executar_pipeline_aima

# Pipeline sem integração com outros ETLs
orquestrador = executar_pipeline_aima(modo_download='zip')

# Acessar resultados
print(f"Dimensões criadas: {len(orquestrador.dimensoes)}")
print(f"Fatos criados: {len(orquestrador.fatos)}")

# Ver dimensão específica
df_anos = orquestrador.dimensoes['Dim_AnoRelatorio']
print(df_anos.head())
```

### Exemplo 2: Integração com ETL_EDUCACAO

```python
from parte_05_orquestrador_aima import executar_pipeline_aima

# Supondo que você já executou ETL_EDUCACAO anteriormente
# e tem as dimensões em memória

dimensoes_base = {
    'Dim_Nacionalidade': df_nacionalidade_educacao,
    'Dim_Sexo': df_sexo_educacao,
    'Dim_GrupoEtario': df_grupoetario_educacao
}

# Executar com integração
orquestrador = executar_pipeline_aima(
    dimensoes_base=dimensoes_base,
    modo_download='zip'
)
```

### Exemplo 3: Execução Fase a Fase (Controle Granular)

```python
from parte_05_orquestrador_aima import OrquestradorPipelineAIMA

# Criar orquestrador
orq = OrquestradorPipelineAIMA()

# Executar fases individualmente
orq.fase_1_upload_dados()
orq.fase_2_integracao_educacao_laboral(dimensoes_base)
orq.fase_3_extracao_consolidacao()
orq.fase_4_transformacao_dimensoes()
orq.fase_5_transformacao_fatos()
orq.fase_6_validacao()
orq.fase_7_exportacao(modo='individual')  # Download individual

# Acessar dados específicos
df_concessoes = orq.fatos['Fact_ConcessoesPorNacionalidadeSexo']
print(df_concessoes.describe())
```

### Exemplo 4: Análise Pós-Processamento

```python
import pandas as pd

# Após executar o pipeline
orq = executar_pipeline_aima()

# Análise 1: Top 10 nacionalidades por concessões (2024)
df_concessoes = orq.fatos['Fact_ConcessoesPorNacionalidadeSexo']
df_nac_aima = orq.dimensoes['Dim_NacionalidadeAIMA']
df_ano = orq.dimensoes['Dim_AnoRelatorio']

# Merge para análise
analise = df_concessoes.merge(df_nac_aima, on='nacionalidade_aima_id')
analise = analise.merge(df_ano, on='ano_id')

top10_2024 = (analise[analise['ano'] == 2024]
              .groupby('nome_nacionalidade_aima')['total_homens_mulheres']
              .sum()
              .nlargest(10))

print("🏆 Top 10 Nacionalidades - Concessões 2024:")
print(top10_2024)

# Análise 2: Evolução temporal
df_evolucao = orq.fatos['Fact_EvolucaoPopulacaoEstrangeira']
print("\n📈 Evolução da População Estrangeira:")
print(df_evolucao[['ano_id', 'total', 'variacao_percent']])
```

---

## 🔗 Integração com Outros Pipelines

### Dimensões Compartilhadas

O ETL_AIMA pode integrar com:

**ETL_EDUCACAO:**
- `Dim_Nacionalidade` → mapeada via `Dim_NacionalidadeAIMA`
- `Dim_Sexo` → FK em fatos de distribuição
- `Dim_GrupoEtario` → FK em fatos etários

**ETL_LABORAL:**
- `Dim_CondicaoEconomica` ↔ `Dim_MotivoConcessao` (N:M)
- `Dim_SetorEconomico` ↔ `Dim_MotivoConcessao` (profissional)

### Tabela de Mapeamento

| Dimensão Base | Dimensão AIMA | Tipo de Relação |
|---------------|---------------|-----------------|
| Dim_Nacionalidade | Dim_NacionalidadeAIMA | 1:N (ponte) |
| Dim_Sexo | (reutilizada) | 1:N (direta) |
| Dim_GrupoEtario | (reutilizada) | 1:N (direta) |
| Dim_CondicaoEconomica | Dim_MotivoConcessao | N:M (cross-domain) |

---

## ⚙️ Configurações Avançadas

### Personalizar Motivos de Concessão

Edite `parte_01_imports_config.py`:

```python
MOTIVOS_CONCESSAO = {
    'NOVO_MOTIVO': {
        'nome': 'Meu Novo Motivo',
        'categoria': 'Categoria',
        'variantes': ['variante1', 'variante2']
    }
}
```

### Ajustar Mapeamento de Nacionalidades

```python
NACIONALIDADES_VARIANTES = {
    'Nome Padrão': ['variante1', 'variante2', 'variante3']
}
```

### Modificar Validações

Em `parte_02_classes_base_ref.py`:

```python
Config.VALIDAR_FKS = True          # Validar foreign keys
Config.VALIDAR_INTEGRAÇÃO = False  # Desabilitar validação de integração
```

---

## 📊 Saída do Pipeline

### Formato de Exportação

**Opção 1: ZIP (Padrão)**
```
ETL_AIMA_StarSchema_20241216_001234.zip
├── Dim_AnoRelatorio.csv
├── Dim_TipoRelatorio.csv
├── Dim_Despacho.csv
├── Dim_MotivoConcessao.csv
├── Dim_NacionalidadeAIMA.csv
├── Fact_ConcessoesPorNacionalidadeSexo.csv
├── Fact_ConcessoesPorDespacho.csv
├── Fact_ConcessoesPorMotivoNacionalidade.csv
├── Fact_PopulacaoEstrangeiraPorNacionalidadeSexo.csv
├── Fact_DistribuicaoEtariaConcessoes.csv
├── Fact_EvolucaoPopulacaoEstrangeira.csv
└── Fact_PopulacaoResidenteEtaria.csv
```

**Opção 2: Arquivos Individuais**
- 12 arquivos CSV separados
- Download sequencial no Colab

### Exemplo de Dados Exportados

**Dim_AnoRelatorio.csv:**
```csv
ano_id,ano,fonte
2020,2020,RIFA
2021,2021,RIFA
2022,2022,RIFA
2023,2023,RMA
2024,2024,RMA
```

**Fact_ConcessoesPorNacionalidadeSexo.csv:**
```csv
concessao_nac_sexo_id,ano_id,tipo_id,nacionalidade_aima_id,sexo_id,total_homens_mulheres
1,2020,1,1,1,15234
2,2020,1,1,2,14123
...
```

---

## 🧪 Testes e Validação

### Executar Testes Unitários

Cada módulo tem testes integrados:

```python
# Testar configurações
!python parte_01_imports_config.py

# Testar classes base
!python parte_02_classes_base_ref.py

# Testar transformador de dimensões
!python parte_03_transformador_dimensoes_aima.py

# Testar transformador de fatos
!python parte_04_transformador_fatos_aima.py

# Testar orquestrador
!python parte_05_orquestrador_aima.py
```

### Relatórios de Validação

O pipeline gera automaticamente:

1. **Relatório de Dimensões**: Mostra estrutura e contagens
2. **Relatório de Fatos**: Estatísticas de registros
3. **Relatório de Integridade**: Validação FK → PK
4. **Relatório de Integração**: Mapeamento com outros pipelines
5. **Relatório Final**: Resumo completo da execução

---

## 📈 Métricas de Performance

### Estimativas de Processamento

| Fase | Duração Estimada | Memória |
|------|------------------|---------|
| Upload | 30-60s | ~50 MB |
| Extração | 10-20s | ~100 MB |
| Dimensões | 5-10s | ~10 MB |
| Fatos | 30-60s | ~200 MB |
| Validação | 10-20s | ~50 MB |
| Exportação | 20-40s | ~100 MB |
| **TOTAL** | **~2-4 min** | **~500 MB** |

*Baseado em: 5 anos × 8 arquivos/ano × ~1000 registros médios*

---

## 🛠️ Troubleshooting

### Problema: Erro ao importar módulos

**Solução:**
```python
import sys
sys.path.append('/content/')  # Ajuste o caminho se necessário
```

### Problema: Nacionalidades sem correspondência

**Verificação:**
```python
df_nac_aima = orq.dimensoes['Dim_NacionalidadeAIMA']
sem_mapeamento = df_nac_aima[df_nac_aima['nacionalidade_id'].isna()]
print(f"Nacionalidades sem mapeamento: {len(sem_mapeamento)}")
print(sem_mapeamento['nome_nacionalidade_aima'].tolist())
```

**Solução:** Adicionar variantes em `Constantes.NACIONALIDADES_VARIANTES`

### Problema: Erro de FK órfã

**Diagnóstico:**
```python
# A validação automática reportará os erros
# Verifique o relatório da Fase 6
```

**Solução:** Revisar dados de entrada ou ajustar lookups

---

## 📚 Referências

### Documentação Relacionada

- [ETL_EDUCACAO](../ETL_EDUCACAO/README.md) - Pipeline de dados educacionais
- [ETL_LABORAL](../ETL_LABORAL/README.md) - Pipeline de dados laborais
- [Diagrama ER Unificado](../../data/processed/diagrama-er-unificado-star-schema.mermaid)
- [Documentação AIMA](../../data/processed/DP-02-A/README.md)

### Fontes de Dados

- **AIMA** (Agência para a Integração, Migrações e Asilo)
  - RMA 2023: https://www.aima.gov.pt/
  - RMA 2024: https://www.aima.gov.pt/
- **SEF** (Serviço de Estrangeiros e Fronteiras - descontinuado)
  - RIFA 2020, 2021, 2022

---

## 👥 Contribuidores

**Projeto:** Concurso de Pesquisa Prepara Portugal 2025  
**Tema:** Estudo sobre Imigração em Portugal  
**Desenvolvedor:** Germano Silva  
**Data:** Dezembro 2025

---

## 📄 Licença

Este projeto é parte do concurso de pesquisa e está sujeito às regras do concurso.

---

## 🎯 Próximos Passos

Após a conclusão do ETL_AIMA:

1. ✅ **Integração Completa**: Unir dados de Educação + Laboral + AIMA
2. ✅ **Modelagem**: Aplicar modelos estatísticos e ML
3. ✅ **Visualização**: Criar dashboards interativos
4. ✅ **Análise**: Gerar insights para o relatório final

---

**🎉 ETL_AIMA - Transformando dados de imigração em conhecimento acionável!**
