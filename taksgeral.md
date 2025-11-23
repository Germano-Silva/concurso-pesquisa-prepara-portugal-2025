Vamos estruturar esta iniciativa como um verdadeiro projeto de Data Science, aplicando a metodologia CRISP-DM de forma pragmática e adaptada ao nosso objetivo final: a produção de um estudo descritivo robusto e elegível para publicação.

Para gerir todo este fluxo de trabalho, foi criado um **Quadro Kanban no GitHub Projects** com as seguintes colunas:
*   **Backlog:** Todas as tasks identificadas.
*   **In Progress:** Tasks em desenvolvimento.
*   **Review:** Tasks concluídas, aguardando validação por outro membro da equipa.
*   **Done:** Tasks finalizadas e validadas.

Abaixo, detalho a **Estrutura Inicial de Tasks e Subtasks** organizada pelas fases do CRISP-DM. Esta será a base do nosso backlog do produto.

---

### **Fase 1: Business Understanding (Compreensão do Negócio)**

**Objetivo:** Definir com clareza e precisão o recorte do nosso estudo, alinhando-o ao tema "Imigração em Portugal" e aos critérios do concurso.

*   **Task 1.1: Brainstorming e Definição do Tema Específico**
    *   Subtask 1.1.1: Realizar uma sessão de brainstorming com a equipa para levantar possíveis recortes (ex: "Evolução dos Fluxos Migratórios em Portugal (2010-2023)", "Perfil Demográfico dos Imigrantes Residentes", "Impacto da Imigração no Mercado de Trabalho por Setor").
    *   Subtask 1.1.2: Avaliar a viabilidade de cada recorte com base na provável disponibilidade de dados nas fontes oficiais.
    *   Subtask 1.1.3: Decidir coletivamente o tema específico e formular a pergunta de pesquisa principal.
    *   **Critério de Aceitação:** Definição formal de um tema específico, aprovado por todos os membros da equipa e documentado no README do projeto.

*   **Task 1.2: Definição dos Objetivos Descritivos e Metodologia de Análise**
    *   Subtask 1.2.1: Listar as perguntas descritivas que o estudo buscará responder (ex: "Qual a evolução do número de residentes estrangeiros?", "Quais as principais nacionalidades e como sua representatividade mudou?", "Como se distribui a população imigrante por distrito?").
    *   Subtask 1.2.2: Definir os tipos de análise que serão utilizados (séries temporais, análise de composição, análise geográfica, etc.).
    *   **Critério de Aceitação:** Documento com os objetivos de pesquisa e a metodologia analítica descritiva a ser empregada.

*   **Task 1.3: Estabelecer Critérios de Sucesso e Validação**
    *   Subtask 1.3.1: Revisar os critérios eliminatórios do concurso e transformá-los em uma checklist de verificação.
    *   Subtask 1.3.2: Definir o que constitui um "estudo descritivo bem-sucedido" para a nossa equipa.
    *   **Critério de Aceitação:** Checklist formalizada e partilhada com a equipa.

---

### **Fase 2: Data Understanding (Compreensão dos Dados)**

**Objetivo:** Explorar e mapear as fontes de dados oficiais, compreendendo a estrutura, disponibilidade e limitações dos dados relevantes para o nosso tema.

*   **Task 2.1: Mapeamento e Acesso às Fontes Oficiais**
    *   Subtask 2.1.1: Catalogar os conjuntos de dados específicos no **INE** (ex: Estatísticas Demográficas, Censos), **Pordata** (ex: base de dados "População Estrangeira em Portugal") e **AIMA/SEF** (ex: Relatórios de Imigração, Fronteiras e Asilo).
    *   Subtask 2.1.2: Aceder e fazer o download inicial dos datasets identificados.
    *   Subtask 2.1.3: **Crítica Eliminatória:** Verificar e documentar a origem oficial de CADA dado a ser utilizado, anotando URL e data de acesso.
    *   **Critério de Aceitação:** Lista de URLs e datasets salvos localmente, com data de acesso documentada.

*   **Task 2.2: Descrição e Exploração Inicial dos Dados**
    *   Subtask 2.2.1: Para cada dataset, realizar uma análise exploratória inicial (usando Python/Pandas, R, ou Excel) para entender as variáveis, tipos de dados e estrutura.
    *   Subtask 2.2.2: Identificar o período temporal coberto, a granularidade geográfica e as principais métricas disponíveis (ex: número de vistos, residentes, autorizações de residência).
    *   Subtask 2.2.3: Gerar estatísticas sumárias (contagem, valores únicos, etc.) para uma visão geral.
    *   **Critério de Aceitação:** Relatório de exploração inicial (ex: Jupyter Notebook) com as primeiras impressões sobre cada fonte de dados.

*   **Task 2.3: Identificação de Lacunas e Questões de Qualidade**
    *   Subtask 2.3.1: Verificar a presença de valores em falta (NA/Null), inconsistências ou quebras nas séries temporais.
    *   Subtask 2.3.2: Avaliar se os dados são suficientes para responder às perguntas definidas na Fase 1.
    *   **Critério de Aceitação:** Lista de potenciais problemas de dados e lacunas identificadas.

---

### **Fase 3: Data Preparation (Preparação dos Dados)**

**Objetivo:** Construir um dataset final, limpo e consolidado, pronto para análise.

*   **Task 3.1: Limpeza de Dados**
    *   Subtask 3.1.1: Tratar valores em falta (decidir por remoção ou imputação, documentando a escolha).
    *   Subtask 3.1.2: Corrigir inconsistências de formatação (datas, nomes de países, gênero).
    *   Subtask 3.1.3: Uniformizar nomenclaturas ao longo de diferentes fontes (ex: garantir que "Brasil" não aparece como "BR" ou "Brasil ").
    *   **Critério de Aceitação:** Dataset(s) limpo(s) e com qualidade reportada.

*   **Task 3.2: Integração e Transformação de Dados**
    *   Subtask 3.2.1: Combinar dados de diferentes fontes (ex: juntar dados do INE com do AIMA/SEF através de chaves comuns como "Ano" ou "Nacionalidade").
    *   Subtask 3.2.2: Criar novas variáveis derivadas, se necessário (ex: percentagens, taxas de crescimento, agregações por região).
    *   Subtask 3.2.3: Estruturar os dados em um formato ideal para análise (tidy data).
    *   **Critério de Aceitação:** Dataset final e único, ou conjunto de datasets relacionados, pronto para modelagem.

*   **Task 3.3: Documentação do Processo de ETL**
    *   Subtask 3.3.1: Criar um script (ex: Python ou R) reprodutível que execute todo o processo de limpeza e transformação.
    *   Subtask 3.3.2: Documentar todas as decisões de transformação tomadas.
    *   **Critério de Aceitação:** Script e documentação commitados no repositório GitHub.

---

### **Fase 4: Modeling (Modelagem - Análise Descritiva)**

**Objetivo:** Executar as análises descritivas planeadas para extrair os factos, padrões e números que comporão o cerne do nosso estudo.

*   **Task 4.1: Análise Univariada e Bivariada**
    *   Subtask 4.1.1: Realizar análises de tendência temporal para as principais variáveis (ex: gráfico de linha da população estrangeira ao longo dos anos).
    *   Subtask 4.1.2: Realizar análises de composição (ex: gráficos de pizza ou barras das principais nacionalidades).
    *   Subtask 4.1.3: Realizar análises de distribuição geográfica (ex: criação de mapas temáticos por distrito/concelho).
    *   **Critério de Aceitação:** Conjunto de visualizações e tabelas sumárias que respondem às perguntas descritivas iniciais.

*   **Task 4.2: Consolidação dos Principais Insights**
    *   Subtask 4.2.1: Sintetizar os principais factos e números extraídos das análises.
    *   Subtask 4.2.2: **Crítica Eliminatória:** Revisar todas as conclusões para garantir que são estritamente descritivas, sem interpretações subjetivas ou políticas.
    *   **Critério de Aceitação:** Lista de "Insights Descritivos" chave, com referência cruzada aos gráficos/tabelas que os suportam.

---

### **Fase 5: Evaluation (Avaliação)**

**Objetivo:** Validar todo o trabalho perante os critérios do concurso e refinar o estudo.

*   **Task 5.1: Validação Interna da Análise**
    *   Subtask 5.1.1: Realizar uma revisão cruzada entre os membros da equipa para verificar a consistência e precisão das análises.
    *   Subtask 5.1.2: Validar se os resultados fazem sentido face ao conhecimento do domínio.
    *   **Critério de Aceitação:** Relatório de validação interna assinado por todos.

*   **Task 5.2: Verificação Final dos Critérios do Concurso**
    *   Subtask 5.2.1: Executar a checklist criada na Task 1.3.
    *   Subtask 5.2.2: Verificar se TODAS as fontes de dados estão citadas corretamente no formato exigido.
    *   Subtask 5.2.3: Revisar a linguagem do estudo para eliminar qualquer vestígio de opinião pessoal.
    *   **Critério de Aceitação:** Checklist preenchida e aprovada com 100% de conformidade.

---

### **Fase 6: Deployment (Implementação - Relatório Final)**

**Objetivo:** Formatizar e entregar o estudo no formato de relatório descritivo, pronto para submissão.

*   **Task 6.1: Redação do Relatório Descritivo**
    *   Subtask 6.1.1: Estruturar o relatório (Introdução, Objetivos, Metodologia, Análise de Resultados, Conclusões Descritivas, Referências).
    *   Subtask 6.1.2: Integrar as visualizações e tabelas no texto, com a devida contextualização.
    *   Subtask 6.1.3: Redigir o texto de forma clara, objetiva e estritamente factual.
    *   **Critério de Aceitação:** Rascunho completo do relatório.

*   **Task 6.2: Revisão Final e Formatação**
    *   Subtask 6.2.1: Revisão ortográfica e de coerência.
    *   Subtask 6.2.2: Aplicar o formato específico exigido pela revista Estrela do Atlântico (se disponível).
    *   Subtask 6.2.3: Compilar o relatório final em PDF.
    *   **Critério de Aceitação:** Relatório final em PDF, pronto para submissão.

*   **Task 6.3: Preparação para Publicação e Submissão**
    *   Subtask 6.3.1: Submeter o relatório ao concurso.
    *   Subtask 6.3.2: Preparar um resumo do projeto para possível divulgação.
    *   **Critério de Aceitação:** Comprovante de submissão e versão final arquivada no GitHub.

---
