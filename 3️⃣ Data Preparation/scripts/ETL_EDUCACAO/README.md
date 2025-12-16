# Pipeline ETL - Educação (DP-01-A)
## INE Censos 2011 → Star Schema

> **Transformação de dados educacionais do INE Censos 2011 para modelo dimensional Star Schema**

---

## 📋 Visão Geral

Este pipeline ETL modular transforma dados brutos do INE Censos 2011 em um modelo dimensional Star Schema otimizado para análises educacionais sobre imigração em Portugal.

### Características Principais

- ✅ **Modular**: 8 módulos Python independentes e reutilizáveis
- ✅ **Orientado a Objetos**: Arquitetura escalável com classes bem definidas
- ✅ **Validação Completa**: Integridade referencial (FK → PK) garantida
- ✅ **Google Colab Ready**: Upload/download interativo via `files.upload()` e `files.download()`
- ✅ **Star Schema**: 7 dimensões + 10 fatos (17 tabelas)
- ✅ **Sem Sistema de Arquivos Local**: Tudo em memória no Colab

---

## 📁 Estrutura dos Arquivos

```
ETL_EDUCACAO/
│
├── parte_01_imports_config.py        # Configurações, constantes e utilitários
├── parte_02_classes_base.py          # Classes abstratas e validadores
├── parte_03_extracao.py              # Extração de dados CSV do INE
├── parte_04_transformador_dimensoes.py  # Criação de dimensões
├── parte_05_transformador_educacao.py   # Fatos educacionais
├── parte_06_transformador_fatos_base.py # Fatos populacionais base
├── parte_07_carregamento_exportacao.py  # Exportação e validação final
├── parte_08_orquestrador_principal.py   # Script principal (orquestrador)
│
└── README.md                         # Este arquivo
```

### Módulos Detalhados

| Módulo | Linhas | Descrição | Classes Principais |
|--------|--------|-----------|-------------------|
| **Parte 1** | ~350 | Configurações e utilitários | `Config`, `Constantes`, `Formatadores`, `Logger` |
| **Parte 2** | ~350 | Classes base e validadores | `TabelaBase`, `DimensaoBase`, `FatoBase`, `ValidadorDados` |
| **Parte 3** | ~350 | Extração de dados | `ExtratorDados`, `ParserINE2011` |
| **Parte 4** | ~400 | Transformação de dimensões | `TransformadorDimensoesBase`, `LookupDimensoes` |
| **Parte 5** | ~450 | Fatos educacionais | `TransformadorEducacao`, `AnalisadorEducacao` |
| **Parte 6** | ~450 | Fatos populacionais | `TransformadorFatosBase` |
| **Parte 7** | ~400 | Carregamento e exportação | `GerenciadorExportacao`, `ValidadorFinal` |
| **Parte 8** | ~450 | Orquestrador principal | `OrquestradorPipelineEducacao` |

**Total**: ~3.200 linhas de código Python

---

## 🚀 Como Usar no Google Colab

### Passo 1: Upload dos Scripts

1. Abra um novo notebook no [Google Colab](https://colab.research.google.com/)
2. Faça upload de **todos os 8 arquivos** `parte_*.py`:

```python
from google.colab import files

# Upload dos scripts do pipeline
print("📤 Faça upload dos 8 arquivos parte_*.py")
uploaded = files.upload()
```

### Passo 2: Executar o Pipeline

```python
# Importar e executar o orquestrador
from parte_08_orquestrador_principal import executar_pipeline_educacao

# Executar pipeline completo
orquestrador = executar_pipeline_educacao(modo_download='zip')
```

### Passo 3: Upload dos Dados INE 2011

Quando solicitado, faça upload dos arquivos CSV:

- `Angola.csv`
- `Brasil.csv`
- `Cabo Verde.csv`
- `Espanha.csv`
- `França.csv`
- `Guiné-Bissau.csv`
- `Reino Unido.csv`
- `República da Moldávia.csv`
- `República Popular da China.csv`
- `Romenia.csv`
- `Sao tome e Principe.csv`
- `Ucrânia.csv`

### Passo 4: Download dos Resultados

O pipeline irá gerar automaticamente um arquivo **ZIP** contendo todos os CSVs processados:

- `ETL_Educacao_DP-01-A.zip` (~17 arquivos CSV)

**Modo Alternativo** (downloads individuais):

```python
orquestrador = executar_pipeline_educacao(modo_download='individual')
```

---

## 📊 Tabelas Geradas

### Dimensões (7 tabelas)

| Tabela | Descrição | Campos Principais | Registros Esperados |
|--------|-----------|-------------------|---------------------|
| `Dim_PopulacaoResidente` | Anos de referência | `populacao_id`, `ano_referencia` | 2 (2011, 2001) |
| `Dim_Nacionalidade` | Nacionalidades | `nacionalidade_id`, `nome_nacionalidade`, `codigo_pais`, `continente` | ~14 |
| `Dim_Localidade` | Municípios e regiões | `localidade_id`, `nome_localidade`, `nivel_administrativo` | ~300 |
| `Dim_Sexo` | Tipos de sexo | `sexo_id`, `tipo_sexo` | 2 |
| `Dim_GrupoEtario` | Faixas etárias | `grupoetario_id`, `faixa_etaria` | 4 |
| `Dim_NivelEducacao` | Níveis educacionais | `nivel_educacao_id`, `nome_nivel`, `categoria` | 4 |
| `Dim_MapeamentoNacionalidades` | Variações de nomes | `nacionalidade_educacao_id`, `nome_nacionalidade_educacao` | ~50 |

### Fatos (10 tabelas)

| Tabela | Descrição | Métricas Principais |
|--------|-----------|---------------------|
| `Fact_PopulacaoEducacao` | População por nível educacional | `populacao_total`, `percentual_nivel` |
| `Fact_EstatisticasEducacao` | Estatísticas agregadas de educação | `percentual_ensino_superior`, `indice_educacional` |
| `Fact_PopulacaoPorNacionalidade` | População total por nacionalidade | `populacao_total`, `masculino`, `feminino` |
| `Fact_PopulacaoPorNacionalidadeSexo` | População por nacionalidade e sexo | `populacao_masculino`, `populacao_feminino` |
| `Fact_PopulacaoPorGrupoEtario` | População por faixa etária | `populacao_grupo`, `percentagem_grupo`, `idade_media` |
| `Fact_PopulacaoPorLocalidade` | População por município | `populacao_total`, `populacao_portuguesa`, `populacao_estrangeira` |
| `Fact_PopulacaoPorLocalidadeNacionalidade` | Cruzamento localidade × nacionalidade | `populacao_nacional` |
| `Fact_EvolucaoTemporal` | Evolução 2001-2011 | `variacao_absoluta`, `taxa_crescimento` |
| `Fact_NacionalidadePrincipal` | Ranking de nacionalidades | `posicao_ranking`, `percentagem_variacao` |
| `Fact_DistribuicaoGeografica` | Distribuição geográfica | `concentracao_relativa`, `dominio_regional` |

---

## ⚙️ Configurações Avançadas

### Personalizar Validações

Edite `parte_01_imports_config.py`:

```python
class Config:
    # Ativar/desativar validações
    VALIDAR_FKS = True        # Validação de integridade referencial
    VALIDAR_TIPOS = True      # Validação de tipos de dados
    VALIDAR_RANGES = True     # Validação de ranges (ex: percentuais)
```

### Adicionar Novas Nacionalidades

Edite `Constantes` em `parte_01_imports_config.py`:

```python
CODIGOS_PAIS = {
    'Angola': 'AGO',
    'Brasil': 'BRA',
    'Novo País': 'XXX',  # Adicionar aqui
}

CONTINENTES = {
    'Angola': 'África',
    'Novo País': 'Continente',  # Adicionar aqui
}
```

---

## 🔍 Fases do Pipeline

### 1️⃣ EXTRAÇÃO (`parte_03_extracao.py`)

- Upload interativo de CSVs via `files.upload()`
- Parsing de formato INE 2011 (separador `;`, decimal `,`)
- Normalização de nomes de nacionalidades
- Extração de dados educacionais por categoria

### 2️⃣ TRANSFORMAÇÃO (`parte_04`, `parte_05`, `parte_06`)

**Dimensões Base**:
- Criação de 7 dimensões com chaves primárias
- Sistema de lookup para resolução rápida de IDs

**Fatos Educacionais**:
- Cálculo de percentuais educacionais
- Índice educacional ponderado
- Coeficiente de Gini educacional

**Fatos Populacionais**:
- Agregações por sexo, grupo etário, localidade
- Ranking de nacionalidades
- Concentração geográfica

### 3️⃣ VALIDAÇÃO (`parte_07_carregamento_exportacao.py`)

- ✅ Validação de tabelas obrigatórias
- ✅ Integridade referencial (FK → PK)
- ✅ Completude de dados (sem PKs nulas)
- ✅ Ranges de valores (percentuais 0-100)

### 4️⃣ CARREGAMENTO (`parte_07_carregamento_exportacao.py`)

- Exportação para CSV (UTF-8, separador `,`)
- Empacotamento em ZIP
- Download automático via `files.download()`
- Relatório de estatísticas

---

## 📈 Exemplo de Uso Avançado

### Executar Apenas Algumas Etapas

```python
from parte_08_orquestrador_principal import OrquestradorPipelineEducacao

# Criar orquestrador
orq = OrquestradorPipelineEducacao()

# Executar etapa por etapa
orq._executar_extracao()
orq._executar_transformacao()

# Acessar dados intermediários
print(orq.dimensoes.keys())  # Ver dimensões criadas
print(orq.fatos.keys())      # Ver fatos criados
```

### Inspecionar Tabelas

```python
# Ver uma dimensão específica
dim_nacionalidade = orq.dimensoes['Dim_Nacionalidade']
print(dim_nacionalidade.head())

# Ver um fato específico
fact_educacao = orq.fatos['Fact_PopulacaoEducacao']
print(f"Total de registros: {len(fact_educacao)}")
```

### Análise Personalizada

```python
# Usar o analisador de educação
from parte_05_transformador_educacao import AnalisadorEducacao

analisador = AnalisadorEducacao()

# Calcular Gini educacional
dados_educacao = [...]  # Seus dados
gini = analisador.calcular_coeficiente_gini_educacao(dados_educacao)
print(f"Coeficiente de Gini: {gini:.4f}")
```

---

## 🛠️ Solução de Problemas

### Erro: "Módulo não encontrado"

**Problema**: Scripts não foram carregados corretamente.

**Solução**:
```python
# Verificar se todos os arquivos estão no diretório
!ls -la parte_*.py

# Reinstalar se necessário
from google.colab import files
uploaded = files.upload()
```

### Erro: "Nacionalidade não encontrada"

**Problema**: Nome de nacionalidade não mapeado.

**Solução**: Adicione o mapeamento em `parte_04_transformador_dimensoes.py`:
```python
@staticmethod
def _normalizar_nome_pais(nome):
    mapeamento = {
        'Nome Variante': 'Nome Oficial',
        # Adicionar aqui
    }
    return mapeamento.get(nome, nome)
```

### Erro: "FK órfã detectada"

**Problema**: Violação de integridade referencial.

**Solução**: Verificar se todas as dimensões foram criadas antes dos fatos:
```python
# Ordem correta no orquestrador:
# 1. Criar dimensões
# 2. Criar lookup
# 3. Criar fatos (usando lookup)
```

---

## 📊 Diagramas

### Fluxo do Pipeline

```
┌─────────────┐
│   EXTRAÇÃO  │  Upload CSVs + Parsing INE 2011
└──────┬──────┘
       │
       ▼
┌─────────────┐
│TRANSFORMAÇÃO│  Dimensões → Lookup → Fatos
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  VALIDAÇÃO  │  Integridade + Qualidade
└──────┬──────┘
       │
       ▼
┌─────────────┐
│CARREGAMENTO │  CSV Export → ZIP → Download
└─────────────┘
```

### Star Schema - Educação

```
        Dim_Nacionalidade
               │
               │
        ┌──────┴──────┐
        │             │
   Dim_NivelEducacao  Dim_PopulacaoResidente
        │             │
        └──────┬──────┘
               │
        Fact_PopulacaoEducacao  ←─ Fact Central
               │
               ├─→ Fact_EstatisticasEducacao
               ├─→ Fact_PopulacaoPorNacionalidade
               └─→ Fact_EvolucaoTemporal
```

---

## 📝 Metadados

- **Versão**: 1.0
- **Autor**: Pipeline ETL Automatizado
- **Data**: Dezembro 2025
- **Fonte**: INE Censos 2011
- **Licença**: Ver LICENSE no repositório
- **Total de Linhas**: ~3.200 linhas Python
- **Tabelas Geradas**: 17 (7 Dim + 10 Fact)

---

## 🔗 Links Úteis

- [Google Colab](https://colab.research.google.com/)
- [INE Portugal - Censos 2011](https://censos.ine.pt/)
- [Diagrama ER Unificado](../data/processed/diagrama-er-unificado-star-schema.mermaid)
- [Documentação do Projeto](../../../README.md)

---

## 📞 Suporte

Para questões ou problemas:

1. Verifique a seção **Solução de Problemas** acima
2. Revise os logs de erro detalhados no Colab
3. Consulte a documentação inline em cada módulo
4. Abra uma issue no repositório do projeto

---

## ✅ Checklist de Execução

- [ ] Upload dos 8 arquivos `parte_*.py` no Colab
- [ ] Upload dos CSVs do INE 2011
- [ ] Execução do `executar_pipeline_educacao()`
- [ ] Validação bem-sucedida (0 erros críticos)
- [ ] Download do ZIP com os resultados
- [ ] Verificação das 17 tabelas geradas

---

**🎓 Pronto para transformar dados educacionais em insights valiosos!**
