# **7.2. PIPELINE DE PROCESSAMENTO E REPRODUTIBILIDADE TÉCNICA**

## **7.2.1. Arquitetura do Pipeline ETL**

Para garantir a reprodutibilidade técnica deste estudo, foi desenvolvido um **pipeline completo de ETL (Extract, Transform, Load)** que consolida dados de múltiplas fontes oficiais (INE Censos 2011/2021, AIMA/SEF RIFA 2020-2024) numa estrutura dimensional otimizada para análise.

### **Estrutura Modular do Pipeline**

O pipeline foi implementado em **Python 3.x** com arquitetura modular, dividido em três componentes principais:

1. **ETL_EDUCACAO_CONSOLIDADO_v3.py**
   - Processa dados censitários INE 2011 e 2021
   - Gera dimensões: Nacionalidade, Nível Educação, Grupo Etário, Localidade
   - Produz tabelas de fatos educacionais por nacionalidade e período

2. **ETL_AIMA_CONSOLIDADO.py**
   - Consolida relatórios RIFA/RMA 2020-2024
   - Gera dimensões: Motivo Concessão, Tipo Despacho, Ano Relatório
   - Produz fatos de concessões, população residente e distribuição etária

3. **ETL_LABORAL_CONSOLIDADO.py**
   - Processa dados laborais Censos 2021
   - Gera dimensões: Setor Econômico (CAE Rev.3), Ocupação
   - Produz fatos de inserção no mercado de trabalho

### **Diagrama de Fluxo do Pipeline**

```
┌─────────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS RAW                      │
├─────────────────────────────────────────────────────────────┤
│  INE Censos 2011  │  INE Censos 2021  │  AIMA RIFA/RMA     │
│  (CSV/Excel)      │  (CSV/Excel)      │  (Extrações PDF)   │
└──────────┬────────┴──────────┬─────────┴────────┬───────────┘
           │                   │                   │
           ▼                   ▼                   ▼
    ┌──────────────┐    ┌──────────────┐   ┌──────────────┐
    │ ETL EDUCACAO │    │ ETL LABORAL  │   │  ETL AIMA    │
    │  (2011-2021) │    │    (2021)    │   │ (2020-2024)  │
    └──────┬───────┘    └──────┬───────┘   └──────┬───────┘
           │                   │                   │
           └───────────────────┼───────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │  STAR SCHEMA MODEL  │
                    │  (Dimensional Data) │
                    └──────────┬──────────┘
                               │
                     ┌─────────┴──────────┐
                     │                    │
                ┌────▼────┐          ┌───▼────┐
                │DIMENSÕES│          │ FATOS  │
                ├─────────┤          ├────────┤
                │Dim_Nac  │          │Fact_Pop│
                │Dim_Educ │          │Fact_Lab│
                │Dim_CAE  │          │Fact_Con│
                │Dim_Ano  │          │   ...  │
                └─────────┘          └────────┘
                     │                    │
                     └──────────┬─────────┘
                                ▼
                       ┌─────────────────┐
                       │   POWER BI      │
                       │  (Visualização) │
                       └─────────────────┘
```

## **7.2.2. Modelo Dimensional (Star Schema)**

O pipeline produz um **modelo dimensional em estrela (Star Schema)** otimizado para análise OLAP, com separação clara entre tabelas de dimensões (descritivas) e tabelas de fatos (métricas):

### **Dimensões Principais**

| Dimensão | Arquivo | Atributos-Chave | Registros |
|----------|---------|-----------------|-----------|
| Nacionalidade | Dim_Nacionalidade.csv | nacionalidade_id, nome, continente, is_ue | 195 |
| Nível Educação | Dim_NivelEducacao.csv | nivel_educacao_id, descricao, hierarquia | 8 |
| Setor Econômico | Dim_SetorEconomico.csv | cae_id, codigo_cae, descricao | 21 |
| Grupo Etário | Dim_GrupoEtario.csv | grupo_etario_id, faixa_etaria | 18 |
| Motivo Concessão | Dim_MotivoConcessao.csv | motivo_id, descricao | 7 |
| Ano Relatório | Dim_AnoRelatorio.csv | ano_id, ano, fonte_relatorio | 14 |

### **Tabelas de Fatos**

| Tabela de Fato | Granularidade | Métricas principais |
|----------------|---------------|---------------------|
| Fact_PopulacaoEducacao | Nacionalidade × Nível Educação × Ano | total_populacao, percentagem |
| Fact_InsercaoLaboral | Nacionalidade × Setor CAE × Ano | empregados_estrangeiros, empregados_portugueses |
| Fact_ConcessoesPorMotivo | Nacionalidade × Motivo × Ano | total_concessoes, percentagem_motivo |
| Fact_PopulacaoResidenteEtaria | Nacionalidade × Grupo Etário × Ano | populacao_residente |

## **7.2.3. Tecnologias e Bibliotecas Utilizadas**

### **Requisitos do Sistema**
- **Python:** 3.8 ou superior
- **Sistema Operacional:** Windows 10/11 (scripts .bat) ou Linux/MacOS (adaptação necessária)

### **Bibliotecas Python**
```python
pandas >= 1.5.0        # Manipulação de dados tabulares
numpy >= 1.23.0        # Operações numéricas
openpyxl >= 3.0.0      # Leitura de arquivos Excel
pathlib                # Gestão de caminhos de arquivos (stdlib)
datetime               # Manipulação de datas (stdlib)
```

### **Instalação de Dependências**
```bash
pip install pandas numpy openpyxl
```

## **7.2.4. Execução do Pipeline**

### **Estrutura de Diretórios**

```
3️⃣ Data Preparation/
├── scripts/
│   ├── ETL_EDUCACAO_CONSOLIDADO_v3.py
│   ├── ETL_AIMA_CONSOLIDADO.py
│   ├── ETL_LABORAL_CONSOLIDADO.py
│   ├── EXECUTAR_TUDO.bat
│   ├── input/
│   │   ├── Angola.csv
│   │   ├── Brasil.csv
│   │   ├── ...
│   └── output/
│       ├── ETL_EDUCACAO_CONSOLIDADO_2011_2021/
│       ├── ETL_AIMA_CONSOLIDADO_2020-2024/
│       └── ETL_LABORAL_CONSOLIDADO_2021/
```

### **Execução Completa**

**Windows:**
```bash
cd "3️⃣ Data Preparation\scripts"
EXECUTAR_TUDO.bat
```

**Linux/MacOS:**
```bash
cd "3️⃣ Data Preparation/scripts"
python ETL_EDUCACAO_CONSOLIDADO_v3.py
python ETL_AIMA_CONSOLIDADO.py
python ETL_LABORAL_CONSOLIDADO.py
```

### **Execução Modular Individual**

```bash
# Apenas dados educacionais
python ETL_EDUCACAO_CONSOLIDADO_v3.py

# Apenas dados AIMA
python ETL_AIMA_CONSOLIDADO.py

# Apenas dados laborais
python ETL_LABORAL_CONSOLIDADO.py
```

## **7.2.5. Outputs Gerados**

### **ETL Educação (2011-2021)**
```
output/ETL_EDUCACAO_CONSOLIDADO_2011_2021/
├── Dim_Nacionalidade.csv
├── Dim_NivelEducacao.csv
├── Dim_GrupoEtario.csv
├── Dim_Localidade.csv
├── Fact_PopulacaoEducacao.csv
├── Fact_EvolucaoTemporal.csv
└── README.txt
```

### **ETL AIMA (2020-2024)**
```
output/ETL_AIMA_CONSOLIDADO_2020-2024/
├── Dim_MotivoConcessao.csv
├── Dim_AnoRelatorio.csv
├── Dim_NacionalidadeAIMA.csv
├── Fact_ConcessoesPorMotivo.csv
├── Fact_PopulacaoResidenteEtaria.csv
├── Fact_EvolucaoPopulacaoEstrangeira.csv
└── README_AIMA.txt
```

### **ETL Laboral (2021)**
```
output/ETL_LABORAL_CONSOLIDADO_2021/
├── Dim_SetorEconomico.csv
├── Fact_InsercaoMercadoTrabalho.csv
├── Fact_DistribuicaoSetorial.csv
└── README_LABORAL.txt
```

## **7.2.6. Validação e Controle de Qualidade**

### **Verificações Automáticas Implementadas**

1. **Integridade Referencial**
   - Validação de chaves estrangeiras entre tabelas de fatos e dimensões
   - Verificação de valores nulos em colunas obrigatórias

2. **Consistência Temporal**
   - Validação de anos (2011, 2021 para educação; 2020-2024 para AIMA)
   - Detecção de registros duplicados por chave composta

3. **Completude de Dados**
   - Contagem de registros processados vs. esperados
   - Relatório de supressões estatísticas (valores \<3 omitidos por confidencialidade INE)

4. **Validação de Percentagens**
   - Soma de percentagens por grupo = 100% (±0.1% tolerância arredondamento)
   - Detecção de outliers estatísticos

### **Logs de Execução**

Cada script gera logs detalhados no console:
```
[INFO] Iniciando ETL_EDUCACAO_CONSOLIDADO_v3.py
[INFO] Carregando arquivo: Brasil.csv (165,683 registros)
[INFO] Transformação: Harmonização níveis educacionais 2011↔2021
[WARN] Supressão estatística: 23 registros com população <3
[INFO] Dimensão Dim_Nacionalidade: 13 registros exportados
[INFO] Fato Fact_PopulacaoEducacao: 1,247 registros exportados
[SUCCESS] Pipeline concluído em 12.3s
```

## **7.2.7. Integração com Power BI**

### **Conexão de Dados**

1. Abrir Power BI Desktop
2. **Obter Dados** → **Texto/CSV**
3. Navegar até `3️⃣ Data Preparation/scripts/output/`
4. Carregar todas as tabelas Dim_* e Fact_*

### **Relações Configuradas**

```
Dim_Nacionalidade (1) ─── (*) Fact_PopulacaoEducacao
Dim_NivelEducacao (1) ─── (*) Fact_PopulacaoEducacao
Dim_SetorEconomico (1) ─── (*) Fact_InsercaoLaboral
Dim_MotivoConcessao (1) ─── (*) Fact_ConcessoesPorMotivo
```

### **Medidas DAX Implementadas**

```dax
// Percentagem de Ensino Superior
Pct_Ensino_Superior = 
DIVIDE(
    CALCULATE(SUM(Fact_PopulacaoEducacao[total_populacao]), 
              Dim_NivelEducacao[descricao] = "Superior"),
    SUM(Fact_PopulacaoEducacao[total_populacao]),
    0
)

// Índice de Representatividade Setorial
Indice_Representatividade = 
DIVIDE(
    DIVIDE([Empregados_Estrangeiros], [Total_Estrangeiros]),
    DIVIDE([Empregados_Portugueses], [Total_Portugueses]),
    BLANK()
)
```

## **7.2.8. Reprodutibilidade e Versionamento**

### **Controle de Versões**

- **Repositório Git:** github.com/Germano-Silva/concurso-pesquisa-prepara-portugal-2025
- **Commit Hash (último):** 9e84d49733170b83be8a444f2e933dd184316ef1
- **Branch:** main

### **Documentação Complementar**

| Documento | Localização | Descrição |
|-----------|-------------|-----------|
| README_PIPELINE_COMPLETO.md | scripts/ | Documentação técnica detalhada do pipeline |
| README_DIAGRAMA_ER_UNIFICADO.md | Diagrama-ER/ | Modelagem dimensional completa |
| diagrama-er-unificado-star-schema.mermaid | Diagrama-ER/ | Diagrama visual do modelo de dados |

### **Citação para Reprodução**

Para replicar este estudo:

```
1. Clonar repositório: 
   git clone https://github.com/Germano-Silva/concurso-pesquisa-prepara-portugal-2025.git

2. Instalar dependências:
   pip install pandas numpy openpyxl

3. Executar pipeline:
   cd "3️⃣ Data Preparation/scripts"
   python EXECUTAR_TUDO.bat  (Windows)
   # ou executar scripts individualmente (Linux/Mac)

4. Importar CSVs gerados em Power BI:
   output/ETL_*_CONSOLIDADO_*/

5. Carregar arquivo .pbix:
   4️⃣ Modeling/PowerBI/
```

## **7.2.9. Limitações Técnicas e Considerações**

### **Dados de Input**

- **Dependência de arquivos CSV manualmente extraídos** de plataformas INE e AIMA (não há API pública para automação completa)
- **Formato heterogêneo** entre diferentes relatórios AIMA exige configuração manual de mapeamentos

### **Performance**

- **Tempo médio de execução:** 35-45 segundos (conjunto completo de 3 ETLs)
- **Volume processado:** ~750MB de dados brutos consolidados em ~25MB de tabelas dimensionais

### **Escalabilidade**

O pipeline atual foi desenhado para o escopo deste estudo (2011-2024). Para extensões futuras:
- Incorporar Censos 2031 requer ajuste de harmonização educacional
- Adicionar micro dados exige re-arquitetura para processamento batch
- Integração com outras fontes (Eurostat, OIM) requer novos módulos ETL

---

**Nota:** Todos os scripts e documentação técnica completa estão disponíveis no repositório Git do projeto, garantindo total reprodutibilidade e transparência metodológica.
