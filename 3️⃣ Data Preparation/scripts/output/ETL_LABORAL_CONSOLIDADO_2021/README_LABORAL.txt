
========================================================================
ETL LABORAL CONSOLIDADO - DATASET CENSOS 2021
========================================================================
Data de Geracao: 2025-12-16 12:03:37
Versao: 1.0-LABORAL
Fonte: INE Censos 2021 - Populacao Estrangeira
Modelo: Star Schema Unificado

========================================================================
DIMENSOES (7 tabelas)
========================================================================

Dim_Nacionalidade.csv - 19 registros
Dim_Sexo.csv - 3 registros
Dim_PopulacaoResidente.csv - 2 registros
Dim_CondicaoEconomica.csv - 11 registros
Dim_GrupoProfissional.csv - 10 registros
Dim_SetorEconomico.csv - 27 registros
Dim_SituacaoProfissional.csv - 4 registros

========================================================================
FATOS (4 tabelas)
========================================================================

Fact_PopulacaoPorCondicao.csv - 209 registros
Fact_EmpregadosPorProfissao.csv - 188 registros
Fact_EmpregadosPorSetor.csv - 391 registros
Fact_EmpregadosPorSituacao.csv - 76 registros

========================================================================
ESTRUTURA DOS DADOS
========================================================================

Fact_PopulacaoPorCondicao:
- População por nacionalidade e condição econômica (Ativa/Inativa)
- Vincula: Dim_Nacionalidade, Dim_CondicaoEconomica, Dim_PopulacaoResidente

Fact_EmpregadosPorProfissao:
- Empregados por nacionalidade e grupo profissional
- Vincula: Dim_Nacionalidade, Dim_GrupoProfissional

Fact_EmpregadosPorSetor:
- Empregados por nacionalidade e setor econômico (CAE)
- Vincula: Dim_Nacionalidade, Dim_SetorEconomico

Fact_EmpregadosPorSituacao:
- Empregados por nacionalidade e situação profissional
- Vincula: Dim_Nacionalidade, Dim_SituacaoProfissional

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Integra com dados de Educacao (DP-01-A)
- Integra com dados AIMA (DP-02-A)
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
