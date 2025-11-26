# Documento de Síntese - Fase de Data Understanding (Compreensão dos Dados)

## 1. Introdução

Este documento consolida todas as descobertas realizadas durante a fase de Data Understanding (Compreensão dos Dados) do projeto de pesquisa sobre imigração em Portugal. A análise exploratória abrangeu múltiplas fontes de dados, incluindo Pordata, AIMA (Agência para a Integração, Migrações e Asilo), dados europeus e INE (Instituto Nacional de Estatística), visando responder a quatro perguntas de pesquisa centrais sobre o perfil sócio-profissional da população imigrante residente em Portugal.

O objetivo desta síntese é fornecer uma base clara e informada para a decisão de prosseguir para a fase de Data Preparation, identificando os dados disponíveis, lacunas existentes, problemas de qualidade e ajustes necessários no escopo do projeto.

## 2. Tabelas Resumo

### Tabela 1: Quais dados temos?

| Fonte de Dados | Período Temporal | Granularidade | Principais Variáveis Disponíveis | Observações |
|----------------|------------------|---------------|----------------------------------|-------------|
| **INE (Censos 2021)** | 2011, 2021 | Nacional, regional (NUTS I/II/III), municipal | Nacionalidade, Nível de escolarização, Situação profissional, Setor de atividade econômica (CAE), Idade, Sexo | Dados detalhados e confiáveis. Permite cruzamento de variáveis. |
| **INE (Ficheiros de Uso Público)** | 2011, 2021 | Amostra de 5% de indivíduos e alojamentos | Mesmas variáveis dos Censos, mas em formato anonimizado | Requer credenciamento. Permite análises mais detalhadas. |
| **Pordata** | Séries históricas (até 2023) | Anual, agregada | População residente (nacional/estrangeira), Emigrantes temporários, Adquirentes de nacionalidade, População estrangeira com estatuto legal | Dados agregados e atualizados. Facilidade de exportação. |
| **AIMA/SEF** | 2020-2024 | Nacional | População estrangeira residente, Concessões de títulos de residência, Motivos de concessão, Distribuição etária | Dados administrativos. Úteis para análise de fluxos e motivos de imigração. |
| **Dados Europeus (Eurostat/INE)** | Diversos (2011-2020) | Variável (trimestral, anual, decenal) | População empregada, Taxa de emprego, Participação em aprendizagem, Ensino superior | Limitações na desagregação por nacionalidade. |

### Tabela 2: Quais dados estão faltando?

| Fonte de Dados | Lacuna Crítica | Impacto na Pesquisa | Possível Solução |
|----------------|----------------|---------------------|------------------|
| **Todas as Fontes** | Dados históricos detalhados (anuais) de escolaridade e setor por nacionalidade | Limita análise de tendências ao longo do tempo | Priorizar análise comparativa 2011 vs 2021 |
| **Pordata** | Variável de setor de ativ económica e profissão | Impede análise sobre distribuição setorial dos imigrantes | Integrar com dados do IEFP ou Quadros de Pessoal |
| **AIMA/SEF** | Nível de escolaridade formal da população residente | Impede análise direta sobre qualificação da mão de obra | Cruzar com dados do INE Censos 2021 |
| **Dados Europeus** | Desagregação por nacionalidade | Dados não permitem distinguir imigrantes de nacionais | Buscar fontes específicas com variável de nacionalidade |
| **Fontes Complementares** | Dados sobre subemprego, qualificação profissão e salários | Limitação na análise da qualidade de inserção no mercado | Investigar fontes do IEFP e Inquéritos ao Emprego |

### Tabela 3: Há problemas de qualidade críticos?

| Fonte de Dados | Problema Identificado | Severidade | Observações |
|----------------|----------------------|------------|-------------|
| **Dados Europeus** | Ausência sistemática de variável de nacionalidade | Crítica | Impede completamente responder às perguntas de pesquisa |
| **INE (Censos)** | Disponibilidade apenas de dados pontuais (2011, 2021) | Importante | Limita análise de tendências contínuas |
| **AIMA/SEF** | Dados de 2023 e 2024 refletem revisões estatísticas em alta | Moderada | Afeta comparabilidade entre anos |
| **Pordata** | Dados agregados não permitem análise detalhada | Moderada | Necessidade de cruzamento com outras fontes |
| **Todas as Fontes** | Inconsistências na definição de "estrangeiro" vs "imigrante | Moderada | Reclarificação de conceitos necessária |

## 3. Problemas Identificados

1. **Limitação Temporal nos Dados do INE**
   - **Descrição:** Os dados do INE Censos só estão disponíveis para dois pontos no tempo (2011 e 2021), impedindo a análise de tendências anuais ou quinzenais.
   - **Quantificação:** Apenas 2 observações no período de 10 anos.
   - **Fonte afetada:** INE Censos 2021
   - **Impacto potencial:** Limita a capacidade de analisar mudanças graduais no perfil sócio-profissional dos imigrantes.

2. **Ausência de Variável de Nacionalidade em Dados Europeus**
   - **Descrição:** Todos os datasets europeus analisados carecem de uma variável que permita desagregar a população por nacionalidade.
   - **Quantificação:** 4 datasets europeus totalmente inutilizáveis para as perguntas de pesquisa.
   - **Fonte afetada:** Dados Europeus (Eurostat/INE)
   - **Impacto potencial:** Torna impossível responder a todas as perguntas de pesquisa com base nestes dados.

3. **Falta de Dados Setoriais Detalhados na Pordata e AIMA/SEF**
   - **Descrição:** As fontes Pordata e AIMA/SEF não fornecem desagregação da população imigrante por setores de atividade económica detalhados (CAE Rev.3).
   - **Quantificação:** 22 setores económicos não cobertos nestas fontes.
   - **Fonte afetada:** Pordata, AIMA/SEF
   - **Impacto potencial:** Impede a análise da distribuição dos imigrantes por setores económicos.

4. **Dados de Escolaridade Indisponíveis na Fonte AIMA/SEF**
   - **Descrição:** A fonte AIMA/SEF não contém informações sobre o nível de escolaridade formal da população estrangeira residente.
   - **Quantificação:** Dados de escolaridade ausentes para todo o período 2020-2024.
   - **Fonte afetada:** AIMA/SEF
   - **Impacto potencial:** Impede a análise da evolução do nível de escolaridade dos imigrantes com base nesta fonte.

5. **Revisões Estatísticas em Dados Recentes (2023-2024)**
   - **Descrição:** Os dados de 2023 e 2024 do AIMA/SEF refletem revisões estatísticas em alta, afetando a comparabilidade temporal.
   - **Quantificação:** Variação significativa nos totais de população estrangeira entre 2022 e 2023 (+33,6%).
   - **Fonte afetada:** AIMA/SEF (RMA 2023, 2024)
   - **Impacto potencial:** Compromete a análise de tendências recentes e a precisão de projeções.

6. **Limitada Disponibilidade de Dados Históricos**
   - **Descrição:** A maioria das fontes não possui dados históricos consistentes para períodos longos (10+ anos).
   - **Quantificação:** Apenas Pordata e algumas séries do INE cobrem mais de 5 anos.
   - **Fonte afetada:** Várias fontes
   - **Impacto potencial:** Limita a capacidade de analisar mudanças estruturais ao longo do tempo.

7. **Inconsistências Conceituais entre Fontes**
   - **Descrição:** Diferentes fontes utilizam definições variadas para conceitos como "estrangeiro", "imigrante" e "população residente".
   - **Quantificação:** Pelo menos 3 definições distintas identificadas entre as fontes.
   - **Fonte afetada:** Todas as fontes
   - **Impacto potencial:** Dificulta a integração e comparabilidade de dados entre diferentes fontes.

8. **Acesso Restito a Microdados do INE**
   - **Descrição:** Os microdados dos Censos requerem credenciamento específico do INE para acesso.
   - **Quantificação:** Dados amostrais de 5% disponíveis, mas processamento requer recursos adicionais.
   - **Fonte afetada:** INE (Ficheiros de Uso Público)
   - **Impacto potencial:** Atrasa potencialmente a análise mais detalhada.

## 4. Soluções e Adaptações Propostas

### Para Problema 1: Limitação Temporal nos Dados do INE
- **Solução técnica:** Focar na análise comparativa entre 2011 e 2021, identificando mudanças estruturais em vez de tendências contínuas.
- **Adaptação de escopo:** Reformular a pergunta de pesquisa para focar em "mudanças decenais" em vez de "evolução anual".
- **Alternativa de abordagem:** Utilizar dados da Pordata e AIMA/SEF para preencher lacunas temporais onde viável.
- **Priorização:** Crítico

### Para Problema 2: Ausência de Variável de Nacionalidade em Dados Europeus
- **Solução técnica:** Descartar o uso de dados europeus para as perguntas centrais do projeto.
- **Adaptação de escopo:** Basear a análise quase que exclusivamente em dados nacionais (INE, Pordata, AIMA/SEF).
- **Alternativa de abordagem:** Investigar outras fontes europeias que possam conter a variável de nacionalidade.
- **Priorização:** Crítico

### Para Problema 3: Falta de Dados Setoriais Detalhados
- **Solução técnica:** Priorizar o uso dos dados do INE Censos 2021 que contêm informações sobre setor de atividade económica.
- **Adaptação de escopo:** Utilizar os dados da AIMA/SEF sobre motivos de residência como proxy para inserção profissional.
- **Alternativa de abordagem:** Buscar fontes complementares como IEFP (Quadros de Pessoal) ou inquéritos ao emprego.
- **Priorização:** Importante

### Para Problema 4: Dados de Escolaridade Indisponíveis na AIMA/SEF
- **Solução técnica:** Utilizar exclusivamente dados do INE Censos 2021 para análise de escolaridade.
- **Adaptação de escopo:** Combinar dados de fluxos da AIMA/SEF com dados de estoque do INE para análise integrada.
- **Alternativa de abordagem:** Utilizar dados da Pordata sobre população residente por nacionalidade como base para inferências.
- **Priorização:** Importante

### Para Problema 5: Revisões Estatísticas em Dados Recentes
- **Solução técnica:** Tratar 2023 como um ponto de inflexão devido às revisões metodológicas, evitando comparações diretas com anos anteriores.
- **Adaptação de escopo:** Focar a análise de tendências recentes (2023-2024) como um período distinto.
- **Alternativa de abordagem:** Investigar a natureza das revisões e ajustar os dados quando possível.
- **Priorização:** Moderado

### Para Problema 6: Limitada Disponibilidade de Dados Históricos
- **Solução técnica:** Combinar séries temporais de diferentes fontes para construir a cobertura mais longa possível.
- **Adaptação de escopo:** Aceitar que algumas análises só poderão ser feitas com dados limitados historicamente.
- **Alternativa de abordagem:** Utilizar técnicas de interpolação ou projeções onde metodologicamente justificado.
- **Priorização:** Moderado

### Para Problema 7: Inconsistências Conceituais
- **Solução técnica:** Criar um glossário de conceitos padronizado para o projeto e documentar adaptações feitas.
- **Adaptação de escopo:** Limitar comparações diretas entre fontes com definições diferentes.
- **Alternativa de abordagem:** Priorizar fontes que utilizem conceitos alinhados com a literatura académica do campo.
- **Priorização:** Moderado

### Para Problema 8: Acesso Restito a Microdados do INE
- **Solução técnica:** Utilizar os Ficheiros de Uso Público (FUPs) já disponíveis como alternativa inicial.
- **Adaptação de escopo:** Planejar o processo de credenciamento para acesso a microdados para futuras iterações.
- **Alternativa de abordagem:** Utilizar os dados agregados já analisados como base para a análise inicial.
- **Priorização:** Desejável

## 5. Recomendações Finais

### Avaliação sobre Viabilidade de Prosseguir
**Projeto Viável com Restrições:** As perguntas de pesquisa são respondidas de forma limitada mas robusta com base nos dados disponíveis, especialmente nos Censos 2021 do INE. A principal limitação é a natureza pontual dos dados do INE (apenas 2011 e 2021), mas a qualidade e detalhe destes dados permitem uma análise comparativa significativa.

### Condições para Avançar
1. **Priorizar análise com base em dados do INE Censos 2021** como fonte principal para escolaridade e setor de atividade.
2. **Utilizar dados da AIMA/SEF para contextualizar fluxos recentes** e motivos de residência.
3. **Aceitar limitações temporais** e focar em mudanças estruturais entre 2011 e 2021.
4. **Padronizar conceitos** entre diferentes fontes para garantir comparabilidade.
5. **Planejar integração com fontes complementares** (IEFP, Quadros de Pessoal) para análises futuras.

### Riscos Residuais
1. **Risco de viés de atualidade:** A dependência de dados de 2021 pode não capturar mudanças muito recentes no perfil do imigrante.
2. **Risco de sub-representação:** Dados censitários podem não capturar totalmente populações mais vulneráveis ou em situação irregular.
3. **Risco de limitação geográfica:** Foco nacional pode esconder disparidades regionais importantes.
4. **Risco de obsolescência:** Mudanças rápidas nos fluxos migratórios podem tornar dados de 2021 menos representativos rapidamente.

### Próximos Passos Recomendados
1. **Preparar dados dos Censos 2021** para análise detalhada das variáveis-chave.
2. **Desenvolver script de integração** entre dados do INE, Pordata e AIMA/SEF.
3. **Criar dashboard visual** para exploração inicial dos dados consolidados.
4. **Documentar metodologia de padronização** de conceitos entre fontes.
5. **Estabelecer protocolo** para atualização regular dos dados à medida que novas versões forem publicadas.
6. **Planestrar análise comparativa** com dados futuros quando disponíveis (próximos Censos).

## 6. Conclusão

Apesar das limitações identificadas, especialmente no que diz respeito à cobertura temporal e à ausência de variáveis-chave em algumas fontes, o projeto demonstra viabilidade para prosseguir para a fase de Data Preparation. Os dados dos Censos 2021 do INE, em particular, fornecem uma base sólida para responder às perguntas de pesquisa de forma robusta, permitindo análises comparativas detalhadas sobre o perfil sócio-profissional da população imigrante em Portugal.

A chave para o sucesso da fase subsequente será a criatividade na integração de múltiplas fontes, a clareza na documentação das limitações e a flexibilidade no ajuste do escopo com base nas restrições dos dados disponíveis. Com as adaptações propostas, o projeto tem potencial para gerar insights valiosos sobre dinâmicas migratórias e inserção sócio-profissional em Portugal.

---
*Este documento foi gerado com base na análise das seguintes fontes:*
- *INE (Censos 2021, Ficheiros de Uso Público)*
- *Pordata (Contemporary Portugal Database)*
- *AIMA/SEF (Relatórios de Imigração, Fronteiras e Asilo)*
- *Dados Europeus (Eurostat/INE)*
