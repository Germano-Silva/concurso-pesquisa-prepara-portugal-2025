
========================================================================
ETL EDUCACAO CONSOLIDADO - DATASET TEMPORAL 2011 + 2021
========================================================================
Data de Geracao: 2025-12-18 22:52:56
Versao: 3.0-CONSOLIDADO-TEMPORAL
Fonte: INE Censos 2011 + 2021
Modelo: Star Schema Unificado

========================================================================
DIMENSOES (7 tabelas)
========================================================================

Dim_Nacionalidade.csv - 20 registros
Dim_Sexo.csv - 3 registros
Dim_Localidade.csv - 344 registros
Dim_GrupoEtario.csv - 4 registros
Dim_PopulacaoResidente.csv - 2 registros
Dim_NivelEducacao.csv - 7 registros
Dim_MapeamentoNacionalidades.csv - 19 registros

========================================================================
FATOS (8 tabelas)
========================================================================

Fact_PopulacaoPorNacionalidade.csv - 38 registros
Fact_PopulacaoPorLocalidade.csv - 344 registros
Fact_PopulacaoPorGrupoEtario.csv - 76 registros
Fact_EvolucaoTemporal.csv - 19 registros
Fact_NacionalidadePrincipal.csv - 15 registros
Fact_DistribuicaoGeografica.csv - 4053 registros
Fact_PopulacaoEducacao.csv - 181 registros
Fact_EstatisticasEducacao.csv - 31 registros

========================================================================
ESTRUTURA DOS DADOS
========================================================================

Fact_PopulacaoEducacao:
- Registros com ano_referencia = 2011 (dados historicos)
- Registros com ano_referencia = 2021 (dados atuais)
- Permite analise de evolucao temporal por nacionalidade e nivel

Fact_EstatisticasEducacao:
- Estatisticas agregadas por nacionalidade e ano
- Comparacao de indicadores educacionais 2011 vs 2021

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Compativel com analises temporais
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
