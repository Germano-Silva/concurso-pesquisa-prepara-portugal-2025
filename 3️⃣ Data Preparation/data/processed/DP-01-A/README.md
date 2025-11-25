# DP-01-A - Dataset Populacional e Educacional Consolidado

## Resumo
Este diretório contém o dataset consolidado de população estrangeira por nacionalidade e nível de escolaridade para análise temporal (2011 vs 2021), resultado do processamento dos dados dos Censos 2021 do INE.

## Objetivo Cumprido
✅ **Extrair e organizar dados de população estrangeira por nacionalidade e nível de escolaridade para análise temporal (2011 vs 2021)**

## Estrutura de Arquivos Processados

### 📊 Tabelas Principais de População

#### **PopulacaoResidente.csv**
- **Descrição**: População residente total por ano de referência
- **Registros**: 2 | **Colunas**: 3
- **Conteúdo**: População total de Portugal em 2011 e 2021
- **Campos**: `populacao_id`, `total_populacao`, `ano_referencia`

#### **PopulacaoPorNacionalidade.csv**
- **Descrição**: População por nacionalidade e ano
- **Registros**: 38 | **Colunas**: 7
- **Conteúdo**: Dados detalhados de população por nacionalidade (2011/2021)
- **Campos**: `populacao_nacional_id`, `nacionalidade_id`, `populacao_id`, `populacao_total`, `masculino`, `feminino`, `percentagem_total`

#### **PopulacaoPorGrupoEtario.csv**
- **Descrição**: Distribuição etária por nacionalidade
- **Registros**: 76 | **Colunas**: 7
- **Conteúdo**: Estrutura etária detalhada por nacionalidade
- **Campos**: `populacao_grupoetario_id`, `populacao_id`, `grupoetario_id`, `nacionalidade_id`, `populacao_grupo`, `percentagem_grupo`, `idade_media`

#### **PopulacaoPorLocalidade.csv** 
- **Descrição**: População por divisão administrativa
- **Registros**: 344 | **Colunas**: 7
- **Conteúdo**: Distribuição populacional por município/região
- **Campos**: `populacao_local_id`, `localidade_id`, `populacao_id`, `populacao_total`, `populacao_portuguesa`, `populacao_estrangeira`, `apatridas`

### 📚 Tabelas de Estatísticas Educacionais

#### **EstatisticasEducacao.csv**
- **Descrição**: Estatísticas educacionais consolidadas por nacionalidade
- **Registros**: 19 | **Colunas**: 12
- **Conteúdo**: Indicadores educacionais completos (sem educação, ensino básico, secundário, superior)
- **Campos**: `estatistica_id`, `nacionalidade_id`, `populacao_total_educacao`, `sem_educacao`, `ensino_basico`, `ensino_secundario`, `ensino_superior`, percentuais por nível, `indice_educacional`, `ano_referencia`
- **Destaques**: 
  - Itália: 53.75% com ensino superior (maior percentual)
  - Nepal: 17.94% sem educação (maior percentual)
  - São Tomé e Príncipe: 55.59% com ensino básico

#### **PopulacaoEducacao.csv**
- **Descrição**: População por nacionalidade e nível de educação
- **Registros**: 133 | **Colunas**: 7
- **Conteúdo**: Detalhamento por nível educacional específico
- **Campos**: `populacao_educacao_id`, `nacionalidade_id`, `nivel_educacao_id`, `populacao_total`, `faixa_etaria`, `ano_referencia`, `percentual_nivel`

### 📈 Tabelas de Análise Temporal

#### **EvolucaoTemporal.csv**
- **Descrição**: Evolução temporal 2011-2021 por nacionalidade
- **Registros**: 19 | **Colunas**: 8
- **Conteúdo**: Variations populacionais entre censos
- **Campos**: `evolucao_id`, `nacionalidade_id`, `populacao_id`, `ano_inicio`, `populacao_inicio`, `variacao_absoluta`, `variacao_percentual`, `taxa_crescimento`
- **Destaques**:
  - Nepal: +1.278,9% crescimento
  - Itália: +301,7% crescimento
  - Brasil: +82,0% crescimento

#### **NacionalidadePrincipal.csv**
- **Descrição**: Ranking das 15 principais nacionalidades
- **Registros**: 15 | **Colunas**: 6
- **Conteúdo**: Top nacionalidades por população
- **Ranking**: 
  1. População Estrangeira (542.165)
  2. Brasil (199.810)
  3. Angola (31.556)

### 🗺️ Tabelas de Distribuição Geográfica

#### **DistribuicaoGeografica.csv**
- **Descrição**: Concentração geográfica por nacionalidade
- **Registros**: 4.053 | **Colunas**: 6
- **Conteúdo**: Distribuição detalhada por localidade e nacionalidade
- **Campos**: `distribuicao_geo_id`, `localidade_id`, `nacionalidade_id`, `populacao_nacional_local`, `concentracao_relativa`, `dominio_regional`

### 🏗️ Tabelas de Referência

#### **Nacionalidade.csv**
- **Descrição**: Cadastro de nacionalidades com códigos e continentes
- **Registros**: 19 | **Colunas**: 4
- **Conteúdo**: Mapeamento de nacionalidades estudadas
- **Campos**: `nacionalidade_id`, `nome_nacionalidade`, `codigo_pais`, `continente`

#### **Localidade.csv**
- **Descrição**: Divisões administrativas (NUTS I/II/III/Municípios)
- **Registros**: 344 | **Colunas**: 4
- **Conteúdo**: Hierarquia territorial portuguesa
- **Campos**: `localidade_id`, `nome_localidade`, `nivel_administrativo`, `codigo_regiao`

#### **GrupoEtario.csv**, **Sexo.csv**
- **Descrição**: Classificações demográficas padronizadas
- **Conteúdo**: Grupos etários (4 faixas) e classificação por sexo

#### **MapeamentoNacionalidades.csv**
- **Descrição**: Compatibilidade entre dados educacionais e populacionais
- **Registros**: 19 | **Colunas**: 4
- **Conteúdo**: Ligação entre datasets de educação e população

## 📋 Índice e Documentação

#### **INDICE_TABELAS.csv**
- **Descrição**: Índice completo de todas as tabelas
- **Conteúdo**: Documentação de estrutura (arquivo, tabela, registros, colunas, descrição)

## 🔄 Processos Realizados

### **Script DP-01-A1.py**
```python
# Localização: ./script/DP-01-A1.py
# Função: Processamento inicial dos dados dos Censos 2021
# Processo:
# 1. Extração de dados de população por nacionalidade
# 2. Limpeza e padronização de dados
# 3. Criação de estruturas relacionais
# 4. Geração de tabelas de referência
```

### **Script DP-01-A2.py** 
```python
# Localização: ./script/DP-01-A2.py
# Função: Processamento de dados educacionais
# Processo:
# 1. Integração de dados educacionais com população
# 2. Cálculo de estatísticas educacionais
# 3. Criação de índices educacionais
# 4. Mapeamento de compatibilidade entre datasets
```

## 🗺️ Modelo de Dados

#### **diagrama-er-completo-educacao.mermaid**
- **Descrição**: Diagrama Entidade-Relacionamento completo
- **Conteúdo**: Modelo de dados integrado população + educação
- **Relacionamentos**: 15+ entidades com relacionamentos 1:N e 1:1

## 📊 Critérios de Aceitação Cumpridos

- [x] **Dataset educacional consolidado criado**: ✅ EstatisticasEducacao.csv e PopulacaoEducacao.csv
- [x] **Dados incluem pelo menos 15 nacionalidades principais**: ✅ 19 nacionalidades processadas
- [x] **Variáveis padronizadas (nacionalidade, nível escolaridade, população)**: ✅ Estrutura relacional implementada
- [x] **Arquivo salvo em `/data/processed/`**: ✅ Todos os arquivos organizados em DP-01-A/
- [x] **Documentação das etapas realizadas**: ✅ Scripts documentados e README.md criado

## 🔗 Links entre Arquivos e Processos

### **Fluxo de Processamento**:
```
Dados Brutos (INE Censos 2021) 
    ↓
DP-01-A1.py → [PopulacaoResidente, Nacionalidade, Localidade, PopulacaoPorNacionalidade, etc.]
    ↓
DP-01-A2.py → [EstatisticasEducacao, PopulacaoEducacao, MapeamentoNacionalidades]
    ↓
Dataset Consolidado DP-01-A
```

### **Relacionamentos Principais**:
- `Nacionalidade.csv` ←→ Todas as tabelas populacionais (chave estrangeira)
- `PopulacaoResidente.csv` ←→ `EvolucaoTemporal.csv` (comparação temporal)
- `EstatisticasEducacao.csv` ⟷ `PopulacaoEducacao.csv` (dados educacionais)
- `DistribuicaoGeografica.csv` ←→ `Localidade.csv` (distribuição territorial)

## 📈 Principais Indicadores Extraídos

- **Population Growth**: +37.4% crescimento da população estrangeira (2011-2021)
- **Educational Integration**: Variação significativa nos níveis educacionais por nacionalidade
- **Geographic Distribution**: Concentração em áreas metropolitanas
- **Demographic Structure**: Perfil etário jovem da população imigrante

---
**Processamento realizado em**: Nov 2024  
**Fonte**: INE - Censos 2021  
**Metodologia**: CRISP-DM - Data Preparation Phase
