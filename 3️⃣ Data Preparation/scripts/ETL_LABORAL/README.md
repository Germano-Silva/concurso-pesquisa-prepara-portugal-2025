# Pipeline ETL - Laboral (DP-01-B)
## INE Censos 2011 → Star Schema Laboral

> **Transformação de dados laborais/profissionais do INE Censos 2011 para modelo dimensional**

---

## 📋 Visão Geral

Este pipeline ETL processa dados sobre condições de trabalho, profissões, setores econômicos e rendimentos da população imigrante em Portugal (Censos 2011).

### Características Principais

- ✅ **Reutiliza** classes base do ETL_EDUCACAO
- ✅ **7 Dimensões Laborais** específicas do domínio profissional
- ✅ **8 Tabelas de Fatos** com métricas laborais
- ✅ **Integração** com dimensões base (Nacionalidade, Sexo, etc.)
- ✅ **Google Colab Ready**

---

## 📁 Estrutura dos Arquivos

```
ETL_LABORAL/
│
├── parte_01_imports_config.py                    # Config laborais
├── parte_02_classes_base_ref.py                  # Ref ao ETL_EDUCACAO
├── parte_03_transformador_dimensoes_laborais.py  # 7 dimensões
├── parte_04_transformador_fatos_laborais.py      # 8 fatos (A CRIAR)
├── parte_05_orquestrador_laboral.py              # Orquestrador (A CRIAR)
│
└── README.md                                      # Este arquivo
```

---

## 📊 Tabelas Geradas

### Dimensões Laborais (7 tabelas)

| Tabela | Descrição | Registros |
|--------|-----------|-----------|
| `Dim_CondicaoEconomica` | Condições perante o trabalho | 7 |
| `Dim_GrupoProfissional` | Grandes grupos profissionais (CNP) | 10 |
| `Dim_ProfissaoDigito1` | Profissão simplificada (1º dígito) | 10 |
| `Dim_SetorEconomico` | Setores econômicos (CAE Rev.3) | 21 |
| `Dim_SituacaoProfissional` | Situação na profissão | 6 |
| `Dim_FonteRendimento` | Fontes de rendimento | 7 |
| `Dim_RegiaoNUTS`| Regiões NUTS II e III | ~32 |

### Fatos Laborais (8 tabelas)

| Tabela | Descrição | Métricas Principais |
|--------|-----------|---------------------|
| `Fact_PopulacaoPorCondicao` | População por condição econômica | `quantidade`, `percentual` |
| `Fact_EmpregadosPorProfissao` | Empregados por grande grupo profissional | `quantidade` |
| `Fact_EmpregadosPorSetor` | Empregados por setor econômico | `quantidade` |
| `Fact_EmpregadosPorSituacao` | Empregados por situação profissional | `quantidade` |
| `Fact_EmpregadosProfSexo` | Empregados por profissão e sexo | `quantidade_homens`, `quantidade_mulheres` |
| `Fact_EmpregadosRegiaoSetor` | Empregados por região NUTS e setor | `quantidade` |
| `Fact_PopulacaoTrabalhoEscolaridade` | População por condição de trabalho e escolaridade | `quantidade_hm`, `quantidade_h`, `quantidade_m` |
| `Fact_PopulacaoRendimentoRegiao` | População por fonte de rendimento e região | `quantidade` |

---

## 🚀 Como Usar no Google Colab

### Passo 1: Preparar Ambiente

```python
from google.colab import files

# Upload dos scripts do ETL_LABORAL
print("📤 Faça upload dos arquivos parte_*.py do ETL_LABORAL")
uploaded = files.upload()
```

### Passo 2: Executar Pipeline

```python
# NOTA: Implementação completa em desenvolvimento
# Por enquanto, pode criar dimensões diretamente:

from parte_01_imports_config import Config, Constantes, Logger
from parte_03_transformador_dimensoes_laborais import (
    TransformadorDimensoesLaborais,
    LookupDimensoesLaborais
)

# Inicializar
logger = Logger("ETL-LABORAL")
config = Config()
constantes = Constantes()

# Criar transformador
transformador = TransformadorDimensoesLaborais(logger, config, constantes)

# Criar todas as 7 dimensões laborais
dimensoes = transformador.criar_todas_dimensoes()

print(f"✓ {len(dimensoes)} dimensões laborais criadas")
for nome, df in dimensoes.items():
    print(f"  - {nome}: {len(df)} registros")
```

### Passo 3: Criar Lookup

```python
# Sistema de lookup para FKs
lookup = LookupDimensoesLaborais(dimensoes)

# Exemplos de uso
condicao_id = lookup.get_condicao_id('População empregada')
setor_id = lookup.get_setor_id('C')  # Indústrias transformadoras
grupo_id = lookup.get_grupo_prof_id('2')  # Especialistas

print(f"IDs encontrados: condicao={condicao_id}, setor={setor_id}, grupo={grupo_id}")
```

---

## 🔗 Integração com ETL_EDUCACAO

O ETL_LABORAL **reutiliza** dimensões base do ETL_EDUCACAO:

- `Dim_Nacionalidade` (do ETL_EDUCACAO)
- `Dim_Sexo` (do ETL_EDUCACAO)
- `Dim_PopulacaoResidente` (do ETL_EDUCACAO)
- `Dim_NivelEducacao` (do ETL_EDUCACAO) → para `Fact_PopulacaoTrabalhoEscolaridade`

**Fluxo Recomendado:**
1. Executar ETL_EDUCACAO primeiro
2. Importar dimensões base geradas
3. Criar dimensões laborais específicas
4. Criar fatos laborais (usando ambos os conjuntos de dimensões)

---

## 📈 Análises Possíveis

Com os dados laborais você pode responder:

### Mercado de Trabalho
- Qual a taxa de emprego por nacionalidade?
- Quais nacionalidades têm maior percentual de desemprego?
- Como se distribui a população ativa vs inativa?

### Profissões e Setores
- Quais são as profissões mais comuns para cada nacionalidade?
- Em quais setores econômicos os imigrantes estão mais presentes?
- Há concentração em setores específicos (ex: construção, serviços)?

### Geografia
- Como se distribuem os trabalhadores por região NUTS?
- Quais regiões têm maior concentração de imigrantes empregados?
- Há diferenças entre Lisboa, Porto e outras regiões?

### Rendimentos
- Quais são as principais fontes de rendimento por nacionalidade?
- Percentual de trabalho por conta própria vs conta de outrem?
- Dependência de pensões e subsídios?

### Cruzamentos
- Educação × Profissão: pessoas com ensino superior em que profissões?
- Sexo × Setor: distribuição de homens e mulheres por setores?
- Região × Nacionalidade: quais nacionalidades em cada região?

---

## ⚙️ Mapeamentos Importantes

### Grandes Grupos Profissionais (CNP)

| Código | Descrição |
|--------|-----------|
| 0 | Forças Armadas |
| 1 | Dirigentes e gestores executivos |
| 2 | Especialistas das atividades intelectuais e científicas |
| 3 | Técnicos e profissões de nível intermédio |
| 4 | Pessoal administrativo |
| 5 | Trabalhadores dos serviços e vendedores |
| 6 | Agricultores e trabalhadores qualificados |
| 7 | Trabalhadores qualificados da indústria e construção |
| 8 | Operadores de instalações e máquinas |
| 9 | Trabalhadores não qualificados |

### Setores Econômicos (CAE Rev.3 - Principais)

| Código | Setor | Agregado |
|--------|-------|----------|
| A | Agricultura, floresta e pesca | Primário |
| C | Indústrias transformadoras | Secundário |
| F | Construção | Secundário |
| G | Comércio por grosso e a retalho | Terciário |
| I | Alojamento e restauração | Terciário |
| P | Educação | Terciário |
| Q | Saúde e apoio social | Terciário |

---

## 🔍 Status do Desenvolvimento

### ✅ Completo
- [x] Configurações e constantes laborais
- [x] Referência a classes base
- [x] 7 Dimensões laborais
- [x] Sistema de lookup de IDs

### 🚧 Em Desenvolvimento
- [ ] 8 Transformadores de fatos laborais
- [ ] Extração de dados laborais dos CSVs
- [ ] Orquestrador principal
- [ ] Validação e exportação
- [ ] Testes integrados

---

## 📝 Metadados

- **Versão**: 1.0 (Em desenvolvimento)
- **Domínio**: Laboral/Profissional
- **Fonte**: INE Censos 2011
- **Tabelas**: 15 (7 Dim + 8 Fact)
- **Integração**: ETL_EDUCACAO (dimensões base)

---

## 🔗 Links Úteis

- [ETL_EDUCACAO](../ETL_EDUCACAO/README.md) - Pipeline base
- [Diagrama ER Unificado](../../data/processed/diagrama-er-unificado-star-schema.mermaid)
- [CNP 2010](https://www.ine.pt/xportal/xmain?xpid=INE&xpgid=ine_cont_inst&INST=6251013) - Classificação Nacional de Profissões
- [CAE Rev.3](https://www.ine.pt/xportal/xmain?xpid=INE&xpgid=ine_cont_inst&INST=6251018) - Classificação de Atividades Econômicas

---

## ⚠️ Nota Importante

Este pipeline está em **desenvolvimento ativo**. Para uso em produção:

1. Complete a implementação dos transformadores de fatos
2. Integre com dados reais do INE 2011
3. Execute testes de validação completos
4. Documente padrões de dados encontrados

Para questões ou contribuições, consulte a documentação principal do projeto.

---

**💼 Pronto para analisar o mercado de trabalho imigrante em Portugal!**
