# Análise de Dados Europeus para Pesquisa sobre Imigração em Portugal

## Índice
1. [Descrição Geral](#descrição-geral)
2. [Metodologia de Análise](#metodologia-de-análise)
3. [Documentação dos Datasets](#documentação-dos-datasets)
4. [Viabilidade das Perguntas de Pesquisa](#viabilidade-das-perguntas-de-pesquisa)
5. [Recomendações](#recomendações)

## Descrição Geral
Este repositório contém dados extraídos dos relatórios anuais do AIMA (Agência para a Integração, Migração e Asilo) e anteriormente do SEF (Serviço de Estrangeiros e Fronteiras), abrangendo o período de 2020 a 2024. Os datasets incluem informações sobre população estrangeira residente, concessão de títulos de residência, despachos e dados de atividade profissional.

Os dados são organizados por ano em pastas separadas (RIFA2020_csv, RIFA2021_csv, RIFA2022_csv, RMA2023_csv, RMA2024_csv), com cada ano contendo múltiplos arquivos CSV que detalham diferentes aspectos da imigração em Portugal.

## Metodologia de Análise
A análise dos datasets foi realizada através da seguinte metodologia:
1. **Exame sistemático de cada arquivo CSV**: Identificação de campos, tipos de dados e estrutura
2. **Mapeamento de variáveis**: Relacionamento dos campos presentes com as variáveis relevantes para as perguntas de pesquisa
3. **Avaliação de completude**: Verificação da cobertura temporal, geográfica e temática dos dados
4. **Análise de limitações**: Identificação de lacunas e restrições nos dados disponíveis
5. **Classificação da viabilidade**: Avaliação da adequação dos dados para responder às perguntas de pesquisa

## Documentação dos Datasets

### RIFA2020_csv - Relatório de Imigração e Fronteiras 2020

#### RIFA2020 - populacao-estrangeira-residente.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2020
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com população estrangeira residente em Portugal por sexo
- Dimensões temporais: Dados pontuais para 2020
- Variáveis relevantes: Nacionalidade, gênero, total populacional

#### RIFA2020 - concessao-titulos-residencia.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2020
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com concessão de títulos de residência por sexo
- Dimensões temporais: Dados pontuais para 2020
- Variáveis relevantes: Nacionalidade, gênero, concessão de residência

#### RIFA2020 - populacao-estrangeira-residente_evolucao.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2020
**Estrutura:**
- Campos principais: ANO, "Títulos de Residência", "Concessão e Prorrogação de AP's", "Prorrogação de VLD's", TOTAL, "VARIAÇÃO %"
- Hierarquia dos dados: Série temporal da evolução da população residente
- Dimensões temporais: 1980-2020
- Variáveis relevantes: Ano, número de títulos, variações percentuais

#### RIFA2020 - despachos-descricao.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2020
**Estrutura:**
- Campos principais: Despacho, Descrição
- Hierarquia dos dados: Tipologia dos despachos e suas descrições
- Dimensões temporais: Não temporal (referencial)
- Variáveis relevantes: Códigos de despacho, descrição de motivos legais

### RIFA2021_csv - Relatório de Imigração e Fronteiras 2021

#### RIFA2021 - populacao-estrangeira-residente.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2021
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com população estrangeira residente em Portugal por sexo
- Dimensões temporais: Dados pontuais para 2021
- Variáveis relevantes: Nacionalidade, gênero, total populacional

#### RIFA2021 - concessao-titulos-residencia.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2021
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com concessão de títulos de residência por sexo
- Dimensões temporais: Dados pontuais para 2021
- Variáveis relevantes: Nacionalidade, gênero, concessão de residência

#### RIFA2021 - populacao-estrangeira-residente_evolucao.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2021
**Estrutura:**
- Campos principais: ANO, "Títulos de Residência", "Concessão e Prorrogação de AP's", "Prorrogação de VLD's", TOTAL, "VARIAÇÃO %"
- Hierarquia dos dados: Série temporal da evolução da população residente
- Dimensões temporais: 1980-2021
- Variáveis relevantes: Ano, número de títulos, variações percentuais

### RIFA2022_csv - Relatório de Imigração e Fronteiras 2022

#### RIFA2022 - populacao-estrangeira-residente.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2022
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com população estrangeira residente em Portugal por sexo
- Dimensões temporais: Dados pontuais para 2022
- Variáveis relevantes: Nacionalidade, gênero, total populacional

#### RIFA2022 - concessao-titulos-residencia.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2022
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Homens, Mulheres
- Hierarquia dos dados: Lista de nacionalidades com concessão de títulos de residência por sexo
- Dimensões temporais: Dados pontuais para 2022
- Variáveis relevantes: Nacionalidade, gênero, concessão de residência

#### RIFA2022 - populacao-estrangeira-residente_evolucao.csv
**Fonte:** SEF/AIMA - Relatório de Imigração e Fronteiras 2022
**Estrutura:**
- Campos principais: ANO, "Títulos de Residência", "Concessão e Prorrogação de AP's", "Prorrogação de VLD's", TOTAL, "VARIAÇÃO %"
- Hierarquia dos dados: Série temporal da evolução da população residente
- Dimensões temporais: 1980-2022
- Variáveis relevantes: Ano, número de títulos, variações percentuais

### RMA2023_csv - Relatório de Migração e Asilo 2023

#### RMA2023 - populacao-estrangeira-residente.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2023
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Masculino, Feminino
- Hierarquia dos dados: Lista de nacionalidades com população estrangeira residente em Portugal por sexo
- Dimensões temporais: Dados pontuais para 2023
- Variáveis relevantes: Nacionalidade, gênero, total populacional

#### RMA2023 - concessao-titulos-residencia.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2023
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Masculino, Feminino
- Hierarquia dos dados: Lista de nacionalidades com concessão de títulos de residência por sexo
- Dimensões temporais: Dados pontuais para 2023
- Variáveis relevantes: Nacionalidade, gênero, concessão de residência

#### RMA2023 - populacao-residente_evolucao.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2023
**Estrutura:**
- Campos principais: ANO, "Títulos de Residência", "Concessão e Prorrogação de AP's", "Prorrogação de VLD's", TOTAL, "VARIAÇÃO %"
- Hierarquia dos dados: Série temporal da evolução da população residente
- Dimensões temporais: 1980-2023
- Variáveis relevantes: Ano, número de títulos, variações percentuais

### RMA2024_csv - Relatório de Migração e Asilo 2024

#### RMA2024 - populacao-estrangeira-residente.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2024
**Estrutura:**
- Campos principais: NACIONALIDADES, TOTAL, Masculino, Feminino
- Hierarquia dos dados: Lista de nacionalidades com população estrangeira residente em Portugal por sexo
- Dimensões temporais: Dados pontuais para 2024
- Variáveis relevantes: Nacionalidade, gênero, total populacional

#### RMA2024 - concessao-titulos_atividade-profission.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2024
**Estrutura:**
- Campos principais: Motivo de Atividade Profissional, Número de Pessoas
- Hierarquia dos dados: Tipos de atividades profissionais concedidas
- Dimensões temporais: Dados pontuais para 2024
- Variáveis relevantes: Tipo de atividade profissional, quantidade

#### RMA2024 - concessao-titulos_evolucao.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2024
**Estrutura:**
- Campos principais: Ano, Concessões, Variação (%)
- Hierarquia dos dados: Evolução anual das concessões de títulos
- Dimensões temporais: 2017-2024
- Variáveis relevantes: Ano, número de concessões, variação percentual

#### RMA2024 - populacao-residente_atividade-profissional.csv
**Fonte:** AIMA - Relatório de Migração e Asilo 2024
**Estrutura:**
- Campos principais: Motivo de Atividade Profissional, Número de Pessoas
- Hierarquia dos dados: População residente por tipo de atividade profissional
- Dimensões temporais: Dados pontuais para 2024
- Variáveis relevantes: Tipo de atividade profissional, quantidade populacional

## Viabilidade das Perguntas de Pesquisa

| Pergunta | Dataset(s) Relevante(s) | Viabilidade | Limitações |
|----------|------------------------|-------------|------------|
| 1. Evolução do nível de escolaridade dos imigrantes | Não diretamente disponível | Não viável | Os datasets não contêm informações sobre nível de escolaridade |
| 2. Distribuição dos imigrantes por setores de atividade econômica | RMA2024 - concessao-titulos_atividade-profission.csv, RMA2024 - populacao-residente_atividade-profissional.csv | Parcial | Dados limitados a 2024, sem detalhamento por setor econômico específico (apenas categorias gerais) |
| 3. Perfil educacional por setor | Não diretamente disponível | Não viável | Ausência de dados sobre escolaridade e setores econômicos detalhados |
| 4. Diferenças entre nacionalidades | Todos os arquivos de população estrangeira residente | Completa | Dados robustos por nacionalidade para 2020-2024, com detalhamento por sexo |

## Recomendações

1. **Para análise de escolaridade:**
   - Buscar fontes complementares como dados do INE (Instituto Nacional de Estatística) sobre educação e imigração
   - Considerar a utilização de dados do Censos 2021 que podem conter informações sobre escolaridade da população estrangeira

2. **Para análise de setores econômicos:**
   - Os dados disponíveis são muito agregados (Ativ. Profissional Subordinada, Independente, etc.)
   - Recomenda-se buscar dados mais detalhados do Instituto do Emprego e Formação Profissional (IEFP) ou Quadros de Pessoal
   - Considerar a integração com dados de segurança social para melhor detalhamento setorial

3. **Para análise de nacionalidades:**
   - Os datasets são excelentes para análise comparativa entre nacionalidades
   - A série temporal de 2020-2024 permite análises de evolução
   - Sugere-se agregar nacionalidades por regiões para análise mais macro

4. **Para pesquisa longitudinal:**
   - Os dados de evolução populacional (arquivos *_evolucao.csv) fornecem uma perspectiva histórica desde 1980
   - Estes são particularmente úteis para análise de tendências de longo prazo

5. **Limitações gerais:**
   - Ausência de variáveis demográficas detalhadas (idade, educação, profissão específica)
   - Dados limitados a informações administrativas do AIMA/SEF
   - Necessidade de triangulação com outras fontes para análise mais completa

6. **Sugestões para futuras coletas de dados:**
   - Incluir variáveis de nível educativo
   - Detalhar melhor os setores de atividade económica
   - Incluir informações sobre duração da residência
   - Adicionar variáveis de profissão e qualificação
