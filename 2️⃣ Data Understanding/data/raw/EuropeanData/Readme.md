# Análise de Viabilidade - Dados Europeus

## Índice

- [1. Contexto e Metodologia](#1-contexto-e-metodologia)
- [2. Perguntas de Pesquisa e Requisitos](#2-perguntas-de-pesquisa-e-requisitos)
- [3. Análise Estrutural dos Arquivos JSON](#3-análise-estrutural-dos-arquivos-json)
- [4. Avaliação de Viabilidade dos Datasets](#4-avaliação-de-viabilidade-dos-datasets)
- [5. Mapeamento e Avaliação Crítica](#5-mapeamento-e-avaliação-crítica)
- [6. Síntese](#6-síntese)

---

## 1. Contexto e Metodologia

### Situação
Este trabalhando em um projeto de pesquisa acadêmica sobre imigração em Portugal que requer análise de dados europeus para responder questões sobre o perfil sócio-profissional de imigrantes.

### Metodologia de Análise para Arquivos Grandes
- Examinar apenas amostras representativas de cada JSON (primeiros 50-100 registros)
- Focar na identificação de metadados estruturais (nomes de campos, hierarquias)
- Inferir tipos de dados a partir da amostra
- Identificar presença de campos-chave sem analisar o dataset completo
- Verificar dimensões temporais e geográficas na estrutura

### Tema da Pesquisa
**"O Perfil Sócio-profissional do Imigrante em Portugal: Uma Análise da Relação entre Nível Educacional e Inserção no Mercado de Trabalho"**

## 2. Perguntas de Pesquisa e Requisitos

### Perguntas de Pesquisa

1. **Evolução da Escolaridade:** Qual a evolução, nos últimos 5-10 anos, do nível de escolaridade da população estrangeira residente em Portugal?

2. **Distribuição Setorial:** Como se distribui a população imigrante ativa por setores de atividade económica e como essa distribuição se compara com a população nacional?

3. **Perfil Educacional por Setor:** Qual é o perfil educacional predominante dentro dos principais setores que absorvem mão-de-obra imigrante?

4. **Diferenças por Nacionalidade:** Existem diferenças significativas no nível educacional médio entre as nacionalidades mais representativas?

### Critérios Mínimos de Viabilidade
- [ ] Cobertura: Portugal especificamente
- [ ] Temporal: Séries de 5+ anos
- [ ] Variáveis: Educação/escolaridade
- [ ] Variáveis: Setores económicos
- [ ] Variáveis: Nacionalidade/origem
- [ ] Diferenciação: Imigrantes vs. nacionais

---

## 3. Análise Estrutural dos Arquivos JSON

A baixo segue a análise detalhada da estrutura de cada arquivo JSON encontrado no diretório EuropeanData, com base em amostragem representativa dos registros.

### 3.1 Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json

#### Informações Gerais
- **Nome do arquivo:** `Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json`
- **Indicador Principal:** "Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida (N.º) por Sexo e Nível de escolaridade mais elevado completo; Quinquenal - INE, Inquérito à educação e formação de adultos"
- **Documentação oficial:** https://www.ine.pt/bddXplorer/htdocs/minfo.jsp?var_cd=0005900&lingua=PT
- **Amostra analisada:** Primeiros 100 registros

#### Estrutura de Dados
```json
{
  "IndicadorDsg": "Descrição do indicador",
  "MetaInfUrl": "URL para metadados",
  "Dados": {
    "2007": [
      {
        "geocod": "PT",
        "geodsg": "Portugal",
        "dim_3": "1", // Sexo (1: Homem, 2: Mulher, T: Total)
        "dim_4": "1", // Nível de escolaridade (1: Até básico 3º ciclo, 2: Secundário/pós-secundário, 3: Superior, T: Total)
        "valor": "1234567", // Número de indivíduos (string, converter para numérico)
        "ind_string": "1.234.567"
      }
    ]
  }
}
```

#### Dimensões e Variáveis
- **Dimensão Temporal:** Dados agrupados por ano (ex: "2007")
- **Dimensão Geográfica:** Nacional (geocod: "PT", geodsg: "Portugal")
- **dim_3:** Sexo (1: Homem, 2: Mulher, T: Total)
- **dim_4:** Nível de escolaridade (1: Até básico 3º ciclo, 2: Secundário/pós-secundário, 3: Superior, T: Total)
- **valor:** Número de indivíduos (formato string, requires conversão para numérico)
- **ind_string:** Versão formatada do valor

#### Cobertura
- **Temporal:** Quinquenal. Última atualização em 2009
- **Geográfica:** Portugal

---

### 3.2 População empregada.json

#### Informações Gerais
- **Nome do arquivo:** `População empregada.json`
- **Indicador Principal:** "População empregada (Série 2011 - N.º) por Sexo, Setor de atividade económica (CAE Rev. 3) e Situação na profissão; Anual - INE, Inquérito ao emprego"
- **Documentação oficial:** https://www.ine.pt/bddXplorer/htdocs/minfo.jsp?var_cd=0006339&lingua=PT
- **Amostra analisada:** Primeiros 100 registros

#### Estrutura de Dados
```json
{
  "IndicadorDsg": "Descrição do indicador",
  "MetaInfUrl": "URL para metadados",
  "Dados": {
    "2020": [
      {
        "geocod": "PT",
        "geodsg": "Portugal",
        "dim_3": "1", // Sexo (1: Homem, 2: Mulher, T: Total)
        "dim_4": "A0", // Setor de atividade económica (A0: Agricultura, B-F: Indústria, G-U: Serviços, TOT: Total)
        "dim_5": "1", // Situação na profissão (1: Trabalhador por conta de outrem, 2: Trabalhador por conta própria/isolado, T: Total)
        "valor": "1234.5", // Número em milhares (string com ponto decimal)
        "ind_string": "1.234,5",
        "sinal_conv": "x" // Indicador de dados não disponíveis
      }
    ]
  }
}
```

#### Dimensões e Variáveis
- **Dimensão Temporal:** Dados agrupados por ano (ex: "2020")
- **Dimensão Geográfica:** Nacional (geocod: "PT", geodsg: "Portugal")
- **dim_3:** Sexo (1: Homem, 2: Mulher, T: Total)
- **dim_4:** Setor de atividade económica (CAE Rev. 3)
- **dim_5:** Situação na profissão
- **valor:** Número de indivíduos em milhares (string com ponto decimal)
- **sinal_conv:** Indicador de qualidade de dados (ex: "x" para dados não disponíveis)

#### Cobertura
- **Temporal:** Anual (Série 2011)
- **Geográfica:** Portugal

---

### 3.3 Proporção da população residente com ensino superior completo.json

#### Informações Gerais
- **Nome do arquivo:** `Proporção da população residente com ensino superior completo.json`
- **Indicador Principal:** "Proporção da população residente com ensino superior completo (%) por Local de residência (NUTS - 2013) e Sexo; Decenal - INE, Recenseamento da população e habitação - Censos 2011"
- **Documentação oficial:** https://www.ine.pt/bddXplorer/htdocs/minfo.jsp?var_cd=0008865&lingua=PT
- **Amostra analisada:** Primeiros 100 registros

#### Estrutura de Dados
```json
{
  "IndicadorDsg": "Descrição do indicador",
  "MetaInfUrl": "URL para metadados",
  "Dados": {
    "2011": [
      {
        "geocod": "PT",
        "geodsg": "Portugal",
        "dim_3": "1", // Sexo (1: Homem, 2: Mulher, T: Total)
        "valor": "12.3", // Proporção em percentagem (string)
        "ind_string": "12,3%" // Versão formatada com vírgula decimal
      }
    ]
  }
}
```

#### Dimensões e Variáveis
- **Dimensão Temporal:** Dados decenais (ex: "2011" - Censos 2011)
- **Dimensão Geográfica:** Alta granularidade (até nível de freguesia)
- **dim_3:** Sexo (1: Homem, 2: Mulher, T: Total)
- **valor:** Proporção em percentagem (formato string, requires conversão para numérico)
- **ind_string:** Versão formatada com vírgula decimal

#### Cobertura
- **Temporal:** Decenal (Censos 2011)
- **Geográfica:** Portugal com alta granularidade (freguesias)

---

### 3.4 Taxa de emprego.json

#### Informações Gerais
- **Nome do arquivo:** `Taxa de emprego.json`
- **Indicador Principal:** "Taxa de emprego (Série 1998 - %) por Local de residência (NUTS - 2002), Sexo, Grupo etário e Nível de escolaridade mais elevado completo; Trimestral - INE, Inquérito ao emprego"
- **Documentação oficial:** https://www.ine.pt/bddXplorer/htdocs/minfo.jsp?var_cd=0000420&lingua=PT
- **Amostra analisada:** Primeiros 100 registros

#### Estrutura de Dados
```json
{
  "IndicadorDsg": "Descrição do indicador",
  "MetaInfUrl": "URL para metadados",
  "Dados": {
    "4.º Trimestre de 2010": [
      {
        "geocod": "PT",
        "geodsg": "Portugal",
        "dim_3": "1", // Sexo (1: Homem, 2: Mulher, T: Total)
        "dim_4": "15 - 24 anos", // Grupo etário
        "dim_5": "Básico - 1º Ciclo", // Nível de escolaridade
        "valor": "45.6", // Taxa de emprego em percentagem (string com ponto decimal)
        "ind_string": "45,6%",
        "sinal_conv": "§" // Indicador de problemas de qualidade
      }
    ]
  }
}
```

#### Dimensões e Variáveis
- **Dimensão Temporal:** Dados trimestrais (ex: "4.º Trimestre de 2010")
- **Dimensão Geográfica:** Regional (NUTS II) e nacional
- **dim_3:** Sexo (1: Homem, 2: Mulher, T: Total)
- **dim_4:** Grupo etário (ex: 15 - 24 anos, 25 - 34 anos, etc.)
- **dim_5:** Nível de escolaridade (ex: Básico - 1º Ciclo, Secundário e pós-secundário, Superior)
- **valor:** Taxa de emprego em percentagem (string com ponto decimal)
- **sinal_conv:** Indicador de qualidade de dados (ex: §, o, -)

#### Cobertura
- **Temporal:** Trimestral (Série 1998)
- **Geográfica:** Portugal e NUTS II

---

## 4. Avaliação de Viabilidade dos Datasets

A baixo segue análise detalhada de cada arquivo em relação aos critérios mínimos definidos para responder às perguntas de pesquisa.

### 4.1 Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json

Este dataset mede a participação em atividades de aprendizagem, não o nível de emprego ou a distribuição setorial.

#### Análise de Viabilidade
- [x] **Cobertura (Portugal):** Sim, os dados são para Portugal
- [ ] **Temporal (5+ anos):** Não. A amostra contém apenas o ano de 2007. A fonte é quinquenal e a última atualização foi em 2009
- [x] **Variáveis (Educação):** Sim, possui a dimensão "Nível de escolaridade"
- [ ] **Variáveis (Setores económicos):** Não
- [ ] **Variáveis (Nacionalidade):** Não. Não há distinção entre população nacional e estrangeira
- [ ] **Diferenciação (Imigrantes vs. Nacionais):** Não

#### Conclusão
**Inviável.** O dataset não possui a dimensão temporal necessária, nem as variáveis de nacionalidade ou setor económico.

---

### 4.2 População empregada.json

Este dataset foca na população empregada por setor económico e situação na profissão.

#### Análise de Viabilidade
- [x] **Cobertura (Portugal):** Sim
- [x] **Temporal (5+ anos):** Sim. A fonte é anual (Inquérito ao Emprego), o que permite construir uma série temporal
- [ ] **Variáveis (Educação):** Não. Não há desagregação por nível de escolaridade
- [x] **Variáveis (Setores económicos):** Sim, possui a dimensão "Setor de atividade económica"
- [ ] **Variáveis (Nacionalidade):** Não
- [ ] **Diferenciação (Imigrantes vs. Nacionais):** Não

#### Conclusão
**Inviável.** Embora tenha a dimensão setorial e temporal, a ausência das variáveis de escolaridade e nacionalidade impede a resposta a todas as perguntas de pesquisa.

---

### 4.3 Proporção da população residente com ensino superior completo.json

Este dataset é um retrato da população com ensino superior em 2011.

#### Análise de Viabilidade
- [x] **Cobertura (Portugal):** Sim
- [ ] **Temporal (5+ anos):** Não. Os dados são decenais e referem-se apenas ao Censo de 2011
- [x] **Variáveis (Educação):** Parcialmente. Foca-se apenas na proporção com "ensino superior completo"
- [ ] **Variáveis (Setores económicos):** Não
- [ ] **Variáveis (Nacionalidade):** Não
- [ ] **Diferenciação (Imigrantes vs. Nacionais):** Não

#### Conclusão
**Inviável.** É um ponto único no tempo (2011) e não contém as variáveis essenciais de setor económico ou nacionalidade.

---

### 4.4 Taxa de emprego.json

Este dataset cruza a taxa de emprego com múltiplas dimensões socioeconómicas.

#### Análise de Viabilidade
- [x] **Cobertura (Portugal):** Sim
- [x] **Temporal (5+ anos):** Sim. A fonte é o Inquérito ao Emprego, com dados trimestrais
- [x] **Variáveis (Educação):** Sim, possui a dimensão "Nível de escolaridade mais elevado completo"
- [ ] **Variáveis (Setores económicos):** Não
- [ ] **Variáveis (Nacionalidade):** Não
- [ ] **Diferenciação (Imigrantes vs. Nacionais):** Não

#### Conclusão
**Parcialmente viável, mas insuficiente.** Este é o dataset mais promissor, pois cruza Educação e Emprego com boa granularidade temporal. Contudo, falha em dois critérios cruciais: não distingue a população por nacionalidade e não inclui a distribuição por setores económicos.

---

### 4.5 Sumário da Avaliação

Nenhum dos quatro datasets fornecidos permite, isoladamente, responder a todas as perguntas de pesquisa, pois **falta a variável-chave Nacionalidade/Origem** em todos eles.

- **Pergunta 1 (Evolução da Escolaridade):** Precisaríamos de um dataset como `Taxa de emprego.json`, mas com um campo que diferenciasse a população estrangeira
- **Perguntas 2 e 3 (Distribuição Setorial e Perfil Educacional):** Seria necessário um dataset que combinasse as variáveis de `População empregada.json` (setores) e `Taxa de emprego.json` (escolaridade), além da distinção por nacionalidade
- **Pergunta 4 (Diferenças por Nacionalidade):** Totalmente inviável com os dados atuais

**Recomendação:** Para prosseguir com a análise, será indispensável buscar novos conjuntos de dados em Eurostat, que contenham a desagregação da população por nacionalidade cruzada com as variáveis de escolaridade e setor de atividade.

---

## 5. Mapeamento e Avaliação Crítica

**Mapeamento de Variáveis por Pergunta de Pesquisa:**

*   **P1 (Evolução Escolaridade):**
    *   `educação`: `dim_4` (Nível de escolaridade) em `Taxa de emprego.json` e `Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json`.
    *   `ano`: Chaves dentro de `Dados` (ex: "2007", "2020", "4.º Trimestre de 2010") em todos os arquivos.
    *   `nacionalidade`: **Não encontrada.**
    *   `país_residência`: Indiretamente via `geodsg` ("Portugal") e `geocod` ("PT") em todos os arquivos. Não permite distinguir imigrantes.
*   **P2 (Distribuição Setorial):**
    *   `setor_emprego` / `atividade_económica`: `dim_4` (Setor de atividade económica) em `População empregada.json`.
    *   `status_migratório`: **Não encontrada.**
*   **P3 (Educação × Setor):**
    *   Campos de educação: `dim_4` ou `dim_5` (Nível de escolaridade) em `Taxa de emprego.json` e `Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json`.
    *   Campos de setor: `dim_4` (Setor de atividade económica) em `População empregada.json`.
    *   **Nota:** Não há um único arquivo que contenha ambos os tipos de campos para o mesmo registro.
*   **P4 (Nacionalidades):**
    *   `nacionalidade`: **Não encontrada.**
    *   `país_origem`: **Não encontrada.**
    *   `educação`: `dim_4` (Nível de escolaridade) em `Taxa de emprego.json` e `Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json`.

**Avaliação de Viabilidade por Pergunta:**

*   **P1: Qual a evolução, nos últimos 5-10 anos, do nível de escolaridade da população estrangeira residente em Portugal?**
    *   **Classificação: NÃO VIÁVEL**
    *   Justificativa: Embora existam variáveis de `educação` e `ano`, a ausência de `nacionalidade` impede a identificação da população estrangeira.
*   **P2: Como se distribui a população imigrante ativa por setores de atividade económica e como essa distribuição se compara com a população nacional?**
    *   **Classificação: NÃO VIÁVEL**
    *   Justificativa: A variável `atividade_económica` está presente no `População empregada.json`, mas a ausência de `nacionalidade` e `status_migratório` impede a diferenciação entre imigrantes e nacionais.
*   **P3: Qual é o perfil educacional predominante dentro dos principais setores que absorvem mão-de-obra imigrante?**
    *   **Classificação: NÃO VIÁVEL**
    *   Justificativa: Seria necessário cruzar dados de educação (de `Taxa de emprego.json`) e setor (de `População empregada.json`). Ambos carecem da variável `nacionalidade` para focar a análise em imigrantes, e não há um arquivo único com ambas as dimensões.
*   **P4: Existem diferenças significativas no nível educacional médio entre as nacionalidades mais representativas?**
    *   **Classificação: NÃO VIÁVEL**
    *   Justificativa: A ausência completa das variáveis `nacionalidade` e `país_origem` torna impossível responder a esta pergunta.

## 6. Síntese

### 6.1 Visão Geral dos Dados Disponíveis

Foram analisados quatro datasets em formato JSON, provenientes do INE (Instituto Nacional de Estatística), Portugal:

- **`Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json`**: Foco em participação em educação/formação, desagregado por sexo e escolaridade. Granularidade quinquenal. Cobertura temporal limitada (ex: 2007).
- **`População empregada.json`**: Foco em número de empregados, desagregado por sexo, setor de atividade económica e situação na profissão. Granularidade anual.
- **`Proporção da população residente com ensino superior completo.json`**: Foco na proporção de pessoas com ensino superior, desagregada por local de residência (muito granular) e sexo. Dados pontuais (ex: Censo 2011).
- **`Taxa de emprego.json`**: Foco na taxa de emprego em percentagem, desagregada por sexo, grupo etário, escolaridade e local de residência. Granularidade trimestral.

**A abordagem de amostragem (primeiros 50-100 registros por arquivo) revelou eficaz para identificar a estrutura geral, tipos de dados e dimensões-chave, sem a necessidade de processamento de arquivos de grande volume.**

### 6.2 Tabela Síntese de Viabilidade Baseada em Evidências

| Pergunta de Pesquisa | Dataset(s) Relevantes | Potencial de Resposta | Evidências e Limitações Específicas |
| :--- | :--- | :--- | :--- |
| **P1. Evolução da Escolaridade da população estrangeira** | `Taxa de emprego.json` (principal)<br>`Indivíduos com idade entre 18 e 64 anos que participaram em atividades de aprendizagem ao longo da vida.json` (secundário) | **NÃO VIÁVEL** | **Evidências:** Ambos possuem variáveis de `educação` (`dim_4`/`dim_5`: Nível de escolaridade) e `ano/tempo` (chaves dentro de `Dados`).<br>**Limitação Crítica:** Ausência de qualquer variável de `nacionalidade` ou `país de origem` impede a identificação e diferenciação da população estrangeira. A cobertura temporal, embora presente em `Taxa de emprego.json`, é inútil sem a discriminação necessária. |
| **P2. Distribuição Setorial da população imigrante ativa** | `População empregada.json` | **NÃO VIÁVEL** | **Evidências:** Possui variável `atividade_económica` (`dim_4`: Setor de atividade económica) e `ano`.<br>**Limitação Crítica:** Ausência de `nacionalidade`/`status_migratório` impede a distinção entre imigrantes e nacionais. Também não possui variável de `escolaridade` para enriquecer a análise setorial. |
| **P3. Perfil Educacional predominante em setores para imigrantes** | `Taxa de emprego.json` (educação)<br>`População empregada.json` (setor) | **NÃO VIÁVEL** | **Evidências:** `Taxa de emprego.json` tem `educação`; `População empregada.json` tem `setor`.<br>**Limitações Críticas:** (1) Não há um único dataset que cruze `educação` e `setor` para os mesmos indivíduos. (2) Ambos carecem da variável `nacionalidade`, tornando impossível focar a análise exclusivamente em imigrantes, mesmo que o cruzamento fosse possível. |
| **P4. Diferenças educacionais por nacionalidade** | Nenhum dos datasets analisados | **NÃO VIÁVEL** | **Evidência:** Nenhum dos quatro datasets contém a variável `nacionalidade` ou `país de origem`.<br>**Limitação:** A ausência é total e direta, tornando a pergunta impossível de responder com os dados disponíveis. |

### 6.3 Recomendações Acionáveis para os Pesquisadores

#### Conclusão Central
A principal lacuna de dados que impede todas as perguntas de pesquisa é a **ausência sistemática de uma variável que permita a desagregação da população por nacionalidade (ou distinção entre nacionais e estrangeiros)** nos datasets europeus analisados.

#### Ações Recomendadas

1. **Prioridade Máxima: Busca por Dados com Desagregação Nacional**
   - **Critérios Essenciais para Novos Datasets:**
     - Variável de **Nacionalidade** (ou País de Origem/Cidadania) detalhada
     - Variável de **Nível de Escolaridade**
     - Variável de **Setor de Atividade Económica** (CAE)
     - Dimensão **Temporal** (séries de, idealmente, 10+ anos)
     - Cobertura geográfica de **Portugal** (preferencialmente regional)

2. **Exploração de Dados Nacionais (INE)**
   - Os datasets analisados são valiosos para o contexto socioeconómico geral de Portugal, mas não para o foco em imigração
   - Recomenda-se uma investigação aprofundada nos dados do Censo 2021 (seção "População estrangeira" já identificada no projeto) e em inquéritos específicos do INE que possam ter perguntas sobre origem imigratória

3. **Consideração sobre Metodologia de Amostragem**
   - A adoção de uma estratégia de amostragem inicial (ex: primeiros 100 registros) demonstrou ser eficiente para a fase de *data understanding* e viabilidade, permitindo uma rápida avaliação dos recursos e limitações dos datasets sem sobrecarga computacional

#### Próximos Passos Sugeridos
- Se novos datasets forem encontrados, repetir a análise de estrutura e viabilidade apresentada neste documento
- Se nenhum dataset adequado for encontrado, reconsiderar o escopo das perguntas de pesquisa com base nos dados disponíveis, ou buscar fontes alternativas (ex: dados administrativos, outras instituições de estatística)

---

### Conclusão Final

Este relatório conclui a análise de viabilidade dos dados europeus para o projeto de pesquisa sobre "O Perfil Sócio-profissional do Imigrante em Portugal". A análise estrutural detalhada demonstrou que, embora os datasets contenham informações valiosas sobre o mercado de trabalho e educação em Portugal, a **ausência de desagregação por nacionalidade impede completamente** a resposta às quatro perguntas de pesquisa fundamentais.

Os investigadores devem priorizar a busca de fontes de dados que incluam variáveis de nacionalidade cruzadas com escolaridade e setor de atividade económica, caso pretendam avançar com a análise proposta conforme os objetivos originais da pesquisa.
