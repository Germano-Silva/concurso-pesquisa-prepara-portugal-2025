
========================================================================
ETL AIMA CONSOLIDADO - DATASET AIMA/SEF 2020-2024
========================================================================
Data de Geracao: 2025-12-16 12:03:38
Versao: 1.0-AIMA
Fonte: AIMA/SEF - RIFA 2020-2022, RMA 2023-2024
Modelo: Star Schema Unificado

========================================================================
DIMENSOES (7 tabelas)
========================================================================

Dim_Nacionalidade.csv - 19 registros
Dim_Sexo.csv - 3 registros
Dim_AnoRelatorio.csv - 5 registros
Dim_TipoRelatorio.csv - 3 registros
Dim_Despacho.csv - 29 registros
Dim_MotivoConcessao.csv - 10 registros
Dim_NacionalidadeAIMA.csv - 227 registros

========================================================================
FATOS (7 tabelas)
========================================================================

Fact_ConcessoesPorNacionalidadeSexo.csv - 1910 registros
Fact_ConcessoesPorDespacho.csv - 43 registros
Fact_ConcessoesPorMotivoNacionalidade.csv - 150 registros
Fact_PopulacaoEstrangeiraPorNacionalidadeSexo.csv - 1910 registros
Fact_DistribuicaoEtariaConcessoes.csv - 142 registros
Fact_PopulacaoResidenteEtaria.csv - 72 registros
Fact_EvolucaoPopulacaoEstrangeira.csv - 5 registros

========================================================================
ESTRUTURA DOS DADOS
========================================================================

Fact_ConcessoesPorNacionalidadeSexo:
- Concessões de títulos de residência por nacionalidade e sexo (2020-2024)
- Vincula: Dim_AnoRelatorio, Dim_NacionalidadeAIMA, Dim_Sexo

Fact_ConcessoesPorDespacho:
- Concessões por tipo de despacho administrativo
- Vincula: Dim_AnoRelatorio, Dim_Despacho

Fact_ConcessoesPorMotivoNacionalidade:
- Concessões por motivo (trabalho, família, estudo, etc) e nacionalidade
- Vincula: Dim_AnoRelatorio, Dim_MotivoConcessao, Dim_NacionalidadeAIMA

Fact_PopulacaoEstrangeiraPorNacionalidadeSexo:
- População estrangeira residente por nacionalidade e sexo
- Vincula: Dim_AnoRelatorio, Dim_NacionalidadeAIMA, Dim_Sexo

Fact_DistribuicaoEtariaConcessoes:
- Distribuição etária das concessões de residência
- Vincula: Dim_AnoRelatorio, Dim_Sexo

Fact_PopulacaoResidenteEtaria:
- População residente por faixa etária
- Vincula: Dim_AnoRelatorio

Fact_EvolucaoPopulacaoEstrangeira:
- Evolução temporal agregada da população estrangeira
- Vincula: Dim_AnoRelatorio

========================================================================
COBERTURA TEMPORAL
========================================================================
2020: RIFA 2020 (SEF)
2021: RIFA 2021 (SEF)
2022: RIFA 2022 (SEF)
2023: RMA 2023 (AIMA)
2024: RMA 2024 (AIMA)

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Integra com dados de Educacao (DP-01-A)
- Integra com dados Laborais (DP-01-B)
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
