# Documentação de Modelagem ER - Integração Dados AIMA (RIFA/RMA 2020-2024)

## 1. Análise da Estrutura dos Dados Brutos
Os CSVs AIMA seguem padrões semelhantes por ano (2020-2024), com variações mínimas:
- **ConcessaoTitulosResidencia.csv**: Nacionalidades x Total/Homens/Mulheres (redundância nacionalidade).
- **ConcessaoTitulosDespachos.csv**: Despacho x Concessões (lookup despachos).
- **ConcessaoTitulosDistribuicaoEtaria.csv**: FaixaEtaria x Homens/Mulheres/Total.
- **ConcessaoTitulosMotivo.csv**: País/Motivo x Totais (parcial por top países).
- **DespachosDescricao.csv**: Despacho x Descrição (dimensão).
- **PopulacaoEstrangeiraResidente.csv**: Nacionalidades x Total/Homens/Mulheres.
- **PopulacaoEstrangeiraResidenteEvolucao.csv**: Ano x Métricas evolutivas.
- **PopulacaoResidenteDistribuicaoEtaria.csv**: FaixaEtaria x Total.
- **Evoluções 2023/2024**: Novos motivos/setoriais (Acordo CPLP, Atividade Profissional).

**Redundâncias detectadas**: Nacionalidades repetidas; motivos parciais; etárias fixas. **Anomalias**: Nulos raros; inconsistências nomes (ex: "Reino Unido (British Subject)"). **Evolução temporal**: Estrutura estável, mas 2024 adiciona atividade profissional.

## 2. Análise do Diagrama Existente (Laboral)
- **Entidades chave compartilhadas**: `Nacionalidade`, `Sexo`, `GrupoEtario`, `PopulaçãoResidente` (ano_referencia), `NivelEducacao`.
- **Laboral**: `CondicaoEconomica`, `SetorEconomico` para linkar motivos profissionais.
- **Pontos de integração**: Nacionalidade (mapeamento nomes), Sexo/Etario para distribuições, Motivos -> Condicoes/Setores.

## 3. Diagrama ER Integrado (diagrama-er-completo-aima-integrado.mermaid)
**Novas entidades**:
- `AnoRelatorio`: PK ano + fonte (RIFA/RMA).
- `TipoRelatorio`: Concessao/PopEstrangeira/PopResidente.
- `Despacho`, `MotivoConcessao`: Dimensões normalizadas.
- `NacionalidadeAIMA`: Mapeamento para entidade existente (evita duplicação).

**Tabelas fato** (medidas normalizadas):
- `ConcessoesPorNacionalidadeSexo`, `PopulacaoEstrangeiraPorNacionalidadeSexo`: Nacionalidade x Sexo x Totais.
- `ConcessoesPorDespacho`, `ConcessoesPorMotivoNacionalidade`: Agrupamentos.
- `DistribuicaoEtariaConcessoes`, `EvolucaoPopulacaoEstrangeira`, `PopulacaoResidenteEtaria`.

## 4. Normalização e Otimização (3FN/BCNF)
- **1FN**: Listas atomicas (nacionalidades separadas).
- **2FN/3FN**: Dependências funcionais eliminadas (fato sem attrs não-chave dependentes parcial/transitivamente).
- **BCNF**: Chaves candidatas únicas.
- **Índices sugeridos**: `ano_id + nacionalidade_aima_id` (queries tempo/nacional); `motivo_id` (análises profissionais).
- **Particionamento**: Por `ano_id` (dados temporais volumosos).
- **Constraints**: FK obrigatórias; UNIQUE em dims (ex: nome_motivo).
- **Joins otimizados**: Máx 3-4 tabelas/query via dims compartilhadas; views para % calculados.
- **Desnormalização**: Nenhuma (performance via índices).

## 5. Justificativas de Integração
- **NacionalidadeAIMA**: Ponte para padronizar ~200 nomes variantes sem alterar existente.
- **MotivoConcessao -> CondicaoEconomica/SetorEconomico**: N:M para linkar "Atividade Profissional" (ativa/subordinada) sem perda.
- **AnoRelatorio -> PopulaçãoResidente**: Temporal consistente.
- **Inconsistências tratadas**: Mapeamento ETL futuro (ex: Brasil variações -> ID único).

## 6. Análise Comparativa Temporal (2020-2024)
| Ano | Arquivos | Destaques |
|----|----------|-----------|
| 2020-2022 (RIFA) | 8 CSVs/ano | Foco concessões + pop residente/estrangeira |
| 2023 (RMA) | 8 CSVs | Acordo CPLP dominante |
| 2024 (RMA) | 9 CSVs | + Atividade Profissional setorial |

**Recomendações ETL**: Scripts Python para load dims/fatos; validação mapeamento nacionalidades.

Modelo pronto para implementação relacional (PostgreSQL/SQLite) ou OLAP (DuckDB). Joins eficientes para análises imigração-laboral.
