
# **[BU-01] Refinamento do Tema & Definição dos Objetivos de Pesquisa**

**Descrição:**
Este épico tem como objetivo finalizar a Fase 1 (Business Understanding) do CRISP-DM, definindo com clareza o tema, as perguntas de pesquisa e os objetivos descritivos que guiarão todo o projeto, garantindo o alinhamento com os critérios do concurso.

**Tema Principal Definido pelo PO:**
"**O Perfil Sócio-profissional do Imigrante em Portugal: Uma Análise da Relação entre Nível Educacional e Inserção no Mercado de Trabalho**"

**Perguntas de Pesquisa Propostas:**
1.  Qual a evolução, nos últimos 5-10 anos, do nível de escolaridade da população estrangeira residente em Portugal?
2.  Como se distribui a população imigrante ativa por setores de atividade económica e como essa distribuição se compara com a população nacional?
3.  Qual é o perfil educacional predominante dentro dos principais setores que absorvem mão-de-obra imigrante?
4.  Existem diferenças significativas no nível educacional médio entre as nacionalidades mais representativas da imigração em Portugal?

**Critérios de Aceitação (Definition of Done para este Épico):**
- [✅] Todas as perguntas de pesquisa foram validadas e aprovadas por todos os 5 membros da equipa.
- [✅] O tema e as perguntas foram revisados para garantir que são estritamente descritivos.
- [✅] Foi documentado no README do projeto GitHub o tema final e a lista de perguntas de pesquisa.
- [✅] As próximas tasks de "Data Understanding" foram identificadas e priorizadas no backlog.

---

*   **Task 1.1: [BU-01-A] Validação Colaborativa das Perguntas de Pesquisa**
    *   **Descrição:** Realizar uma breve reunião (15-20 min) ou uma discussão assíncrona para que todos os membros da equipa possam expressar se as perguntas capturam o interesse do grupo e sugerir ajustes.
    *   **Habilidade Primária:** **Soft Skill** (Comunicação, Colaboração).
    *   **Critério de Aceitação:** Consenso ou votação majoritária sobre a lista final de perguntas.

*   **Task 1.2: [BU-01-B] Tradução dos Objetivos para Requisitos de Dados**
    *   **Descrição:** Para cada pergunta de pesquisa aprovada, listar quais variáveis de dados serão necessárias. Ex: Para a pergunta 1, precisamos de "Nível de Escolaridade" e "Ano". Para a pergunta 2, precisamos de "Setor de Atividade", "Nacionalidade (PT/EST)", "Condição perante o trabalho".
    *   **Habilidade Primária:** **Pensamento Lógico** (Hard/Soft Skill).
    *   **Critério de Aceitação:** Uma lista simples, em formato de tabela, vinculando cada pergunta às suas variáveis necessárias.

*   **Task 1.3: [BU-01-C] Documentação Final no README.md**
    *   **Descrição:** Atualizar o ficheiro README.md do repositório GitHub com o tema, as perguntas de pesquisa finais e os objetivos. Esta é uma tarefa crucial para a documentação do projeto.
    *   **Habilidade Primária:** **Hard Skill Básica** (Edição de Markdown no GitHub).
    *   **Critério de Aceitação:** README atualizado, com formatação clara e sem erros de ortografia.

*   **Task 1.4: [BU-01-D] Brainstorming Inicial de Fontes de Dados**
    *   **Descrição:** Com base na lista de variáveis da Task 1.2, a equipa deve fazer um brainstorming simples: "Onde podemos encontrar esses dados?". Não é para aceder ainda, apenas pensar. Ex: "Dados de educação provavelmente estão no INE ou Pordata. Dados setoriais de trabalho também."
    *   **Habilidade Primária:** **Pesquisa e Investigação** (Soft Skill).
    *   **Critério de Aceitação:** Lista informal de fontes oficiais (INE, Pordata, AIMA) que *provavelmente* contêm os dados que precisamos.

---

# **[DU-01] Exploração e Confirmação das Fontes de Dados**

**Descrição:**
Este épico tem como objetivo realizar a Fase 2 (Data Understanding) do CRISP-DM. Iremos acessar, explorar e documentar a disponibilidade e a qualidade dos dados nas fontes oficiais identificadas, criando um "Dicionário de Dados" inicial que nos permitirá avançar para a preparação.

**Critérios de Aceitação (Definition of Done para este Épico):**
- [✅] Todas as fontes prioritárias (INE, Pordata, AIMA) foram acessadas e os datasets relevantes, identificados.
- [✅] O "Log de Fontes" está completo com URL, data de acesso e descrição para cada dataset.
- [ ] Foi criado um "Relatório de Viabilidade" inicial, indicando quais perguntas de pesquisa podem ser respondidas e com quais dados.
- [ ] Os primeiros datasets foram baixados e armazenados na pasta `data/raw/` do repositório.

---

**Tarefas de Nível 1 (Baixa Complexidade - Foco em Soft Skills de Pesquisa e Organização)**

*   **Task 2.1: [DU-01-A] Criação do Log de Fontes e Estrutura de Pastas**
    *   **Descrição:** Criar uma planilha partilhada (Google Sheets ou Excel no GitHub) com as colunas: `Fonte`, `URL`, `Data de Acesso`, `Dataset Encontrado`, `Variáveis Relevantes`, `Observações`. Também criar a estrutura de pastas no repositório: `data/raw/`, `data/processed/`, `docs/`, `scripts/`.
    *   **Habilidade Necessária:** Organização, Atenção aos Detalhes.
    *   **Critério de Aceitação:** Planilha criada e link partilhado com a equipe. Estrutura de pastas commitada no GitHub.

*   **Task 2.2: [DU-01-B] Exploração e Documentação dos Relatórios do AIMA**
    *   **Descrição:** Acessar o site do AIMA, baixar os Relatórios de Imigração, Fronteiras e Asilo (RIFA) dos últimos 5 anos. Navegar pelos PDFs e identificar tabelas sobre **autorizações de residência por motivo (trabalho, estudo, etc.)** e **nacionalidades**. Registrar as descobertas no Log de Fontes.
    *   **Habilidade Necessária:** Leitura atenta de documentos, Persistência.
    *   **Critério de Aceitação:** PDFs baixados para `data/raw/aima/`. Log de Fontes atualizado com pelo menos 3 tabelas relevantes identificadas.

*   **Task 2.3: [DU-01-C] Exploração Guiada na Pordata**
    *   **Descrição:** Usar a interface da Pordata para buscar séries temporais relacionadas ao tema. Sugestões de busca: "População estrangeira com educação superior", "Empregados estrangeiros por setor de atividade". Exportar os resultados em CSV e documentar no Log de Fontes.
    *   **Habilidade Necessária:** Navegação em websites, Noção de filtros e exportação de dados.
    *   **Critério de Aceitação:** Pelo menos 3 séries temporais diferentes exportadas para `data/raw/pordata/` e documentadas.

*   **Task 2.4: [DU-01-D] Investigação de Fontes Secundárias (BPstat e European Data)**
    *   **Descrição:** Explorar o BPstat e o portal europeu de dados (`data.europa.eu`), filtrando por Portugal. O foco é encontrar dados complementares, como estatísticas económicas por setor que possam ser úteis. Documentar qualquer descoberta promissora no Log de Fontes.
    *   **Habilidade Necessária:** Curiosidade, Capacidade de Investigação.
    *   **Critério de Aceitação:** Log de Fontes atualizado com pelo menos 2 fontes secundárias investigadas e uma breve descrição do que foi encontrado.

*   **Task 2.5: [DU-01-E] Acesso e Análise dos Microdados do INE**
    *   **Descrição:** Esta é uma task crítica. Envolve acessar o portal do INE, localizar os microdados dos **Censos 2021** e do **Inquérito ao Emprego**. Verificar a disponibilidade das variáveis-chave (nacionalidade, habilitações literárias, profissão, setor de atividade) e os procedimentos para download (que podem exigir registo).
    *   **Habilidade Necessária:** Persistência, Capacidade de seguir instruções técnicas, Noção de estrutura de dados (o que são microdados).
    *   **Critério de Aceitação:** Documentação do INE sobre os microdados anexada ao projeto. Confirmação de que as variáveis necessárias estão presentes. Se possível, download de um ficheiro de exemplo ou de metadados.

*   **Task 2.6: [DU-01-F] Consolidação do Relatório de Viabilidade**
    *   **Descrição:** Com base nas descobertas de todas as outras tasks, criar um documento sumário (no `docs/`). Para cada pergunta de pesquisa, responder: "Com os dados encontrados, é possível respondê-la? Qual a fonte principal?".
    *   **Habilidade Necessária:** Síntese de Informação, Visão Sistémica.
    *   **Critério de Aceitação:** Relatório de 1-2 páginas criado e partilhado, servindo como guia para a próxima fase.

---

Excelente! Parabéns à equipe pela conclusão da fase de mapeamento. Avançar com dados concretos em mãos é um grande passo. Agora, partimos para o coração da **Fase 2 (Data Understanding)**, onde vamos "conversar" com os dados que coletamos.

Vamos estruturar as próximas tasks de forma muito prática, criando um novo **Épico** para esta etapa de exploração detalhada. Novamente, vamos distribuir as tarefas considerando os diferentes níveis de habilidade.

---

**Título do Épico: [DU-02] Análise Exploratória Inicial e Avaliação de Qualidade dos Dados**

**Descrição:**
Este épico tem como objetivo realizar uma análise técnica inicial de cada dataset baixado. Iremos gerar um relatório de exploração para compreender a estrutura, conteúdo e qualidade dos dados, identificando oportunidades e riscos para a fase de preparação.

**Critérios de Aceitação (Definition of Done para este Épico):**
- [ ] Um Jupyter Notebook (ou script R) de exploração foi criado para cada fonte de dados principal (ex: `notebooks/01_Exploracao_INE.ipynb`, `notebooks/02_Exploracao_Pordata.ipynb`).
- [ ] Um relatório sumário consolidado foi produzido, listando as lacunas e problemas de qualidade identificados.
- [ ] A equipe tem clareza sobre a viabilidade de responder a cada pergunta de pesquisa com os dados disponíveis.

---

### **Tasks Detalhadas para o Épico [DU-02]**

Aqui estão as tasks desdobradas, focando na exploração técnica e na avaliação de qualidade.

**Tarefa Principal: Análise Exploratória por Fonte de Dados**

*   **Task 2.4: [DU-02-A] Análise Exploratória dos Dados do INE (Censos/Inquérito ao Emprego)**
    *   **Descrição:** Criar um Jupyter Notebook para explorar os microdados do INE. Focar nos datasets que contêm `Nacionalidade`, `Habilitações Literárias` e `Setor de Atividade`.
    *   **Subtasks:**
        *   **2.4.1:** Carregar os dados e usar `.info()`, `.describe()` e `.head()` para entender a estrutura (número de linhas/colunas, tipos de dados).
        *   **2.4.2:** Identificar o período temporal e a granularidade geográfica (ex: é a nível nacional, distrital?).
        *   **2.4.3:** Gerar estatísticas sumárias para as variáveis-chave: contagem de valores únicos para `Nacionalidade` e `Habilitações Literárias`, frequência dos principais setores de atividade.
        *   **2.4.4:** **Verificar a presença de valores NA/Null** nas colunas críticas e calcular a percentagem de missing data.
    *   **Habilidade Necessária:** Python/Pandas (Nível Básico-Intermediário).
    *   **Critério de Aceitação:** Notebook commitado no GitHub, contendo código executável, os outputs das análises e comentários explicando os achados.

*   **Task 2.5: [DU-02-B] Análise Exploratória dos Dados da Pordata**
    *   **Descrição:** Criar um notebook separado para analisar as séries temporais exportadas da Pordata.
    *   **Subtasks:**
        *   **2.5.1:** Carregar os CSVs e verificar a consistência das colunas (ex: `Ano`, `Valor`, `Nacionalidade`).
        *   **2.5.2:** Plotar gráficos de linha simples (`matplotlib` ou `seaborn`) para visualizar a evolução temporal das séries (ex: evolução da população estrangeira com ensino superior).
        *   **2.5.3:** Identificar se há quebras ou mudanças de metodologia nas séries (isso pode ser notado por quedas ou picos abruptos sem explicação no contexto).
        *   **2.5.4:** Verificar a consistência das categorias ao longo do tempo (ex: os nomes dos setores mudaram?).
    *   **Habilidade Necessária:** Python/Pandas (Nível Básico), Noção de visualização de dados.
    *   **Critério de Aceitação:** Notebook commitado com as visualizações temporais e anotações sobre a qualidade das séries.

*   **Task 2.6: [DU-02-C] Análise e Estruturação dos Dados do AIMA (PDF)**
    *   **Descrição:** Esta task é diferente devido à fonte ser em PDF. O objetivo é extrair as tabelas dos relatórios RIFA para um formato estruturado (CSV/Excel).
    *   **Subtasks:**
        *   **2.6.1:** (Automática) Tentar extrair tabelas dos PDFs usando uma biblioteca como `tabula-py` ou `camelot`.
        *   **2.6.2:** (Manual) Para tabelas complexas, realizar a extração manual para uma planilha, criando colunas claras como `Ano`, `Nacionalidade`, `Motivo da Residência`, `Total`.
        *   **2.6.3:** Documentar no Log de Fontes quais tabelas foram extraídas com sucesso e quais apresentaram problemas.
        *   **2.6.4:** Realizar a mesma análise de valores missing e inconsistências nas tabelas extraídas.
    *   **Habilidade Necessária:** Paciência, Atenção aos Detalhes, Conhecimento em extração de PDF (ou disposição para aprender/realizar manualmente).
    *   **Critério de Aceitação:** Tabelas extraídas e salvas em `data/raw/aima/extraidas/`. Planilha de documentação atualizada.

**Tarefa de Síntese e Decisão**

*   **Task 2.7: [DU-02-D] Consolidação do Relatório de Lacunas e Viabilidade**
    *   **Descrição:** Sintetizar as descobertas de todas as análises exploratórias em um único documento.
    *   **Subtasks:**
        *   **2.7.1:** Criar uma tabela resumo para cada pergunta de pesquisa: "Quais dados temos?", "Quais estão faltando?", "Há problemas de qualidade críticos?".
        *   **2.7.2:** Listar claramente todos os problemas identificados (ex: "Série do INE tem 30% de NA em `Habilitações Literárias` para anos anteriores a 2015", "Dados setoriais do AIMA não são diretamente compatíveis com a CAE do INE").
        *   **2.7.3:** Propor soluções ou adaptações para o escopo, se necessário (ex: "Focar a análise a partir de 2015", "Agrupar nacionalidades em grandes grupos devido à baixa contagem").
    *   **Habilidade Necessária:** **Soft Skill** (Síntese, Pensamento Crítico, Comunicação Clara).
    *   **Critério de Aceitação:** Documento formal (em `docs/`) aprovado pelo PO, que servirá como guia para a fase de Data Preparation. Este documento responde à pergunta: "Podemos prosseguir? Com que ajustes?".

---

### **Próximos Passos Imediatos**

1.  **Mover o Épico [DU-02]** para **"In Progress"**.
2.  **Distribuir as Tasks 2.4, 2.5 e 2.6** entre os membros com mais afinidade técnica ou vontade de aprender. Estas tasks podem ser feitas em paralelo.
3.  A **Task 2.7** deve ser assumida por alguém com uma visão geral do projeto (potencialmente o Scrum Master ou o PO), com inputs de todos.

**Dica para a Equipe:** Não se assustem com os problemas que vão encontrar. Encontrar lacunas e inconsistências **é o objetivo desta fase**. É melhor descobrir isso agora do que no meio da análise descritiva.

A equipe está preparada para mergulhar nos dados? Vamos transformar esses arquivos brutos em informação!