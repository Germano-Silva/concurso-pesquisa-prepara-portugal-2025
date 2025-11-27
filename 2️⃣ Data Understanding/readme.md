### **Data Understanding (Entendimento dos Dados) - Primeira Exploração**

**Fontes Primárias de Dados Sugeridas:**

1.  **INE (Instituto Nacional de Estatística):**
    *   **Censos 2021:** Fonte riquíssima para cruzar variáveis como **Nacionalidade**, **Nível de Escolarização**, **Situação Profissional**, **Profissão**, e **Setor de Atividade**. É um retrato detalhado, mas é uma foto de um momento (2021).
    *   **Inquérito ao Emprego (trimestral/anuário):** Fornece dados contínuos sobre a população estrangeira no mercado de trabalho. Podemos extrair séries temporais sobre **taxa de atividade, desemprego e subemprego** por nacionalidade.
    *   **Estatísticas Demográficas:** Para dados gerais de evolução da população estrangeira residente.

2.  **Pordata:** Funciona como uma interface amigável para muitos dos dados do INE. É excelente para obter séries históricas consolidadas e fazer comparações iniciais. A vantagem é a facilidade de exportação.

3.  **AIMA (Agência para a Integração, Migrações e Asilo) - ex-SEF:** 
    *   **Relatórios de Imigração, Fronteiras e Asilo (RIFA):** São a fonte mais autorizada para dados administrativos sobre imigração. Trazem informações cruciais sobre **novas autorizações de residência**, muitas vezes com breakdown por **motivo** (ex: trabalho altamente qualificado, trabalho não qualificado, estudo, reagrupamento familiar).

**Mapeamento Inicial de Dados vs. Perguntas:**

| Pergunta de Pesquisa | Possível Fonte de Dados | Variáveis Chave a Buscar |
| :--- | :--- | :--- |
| **1. Evolução do nível educacional** | INE (Censos; Inquérito ao Emprego) / Pordata | `Nacionalidade` + `Habilitações Literárias` + `Ano` |
| **2. Distribuição por setores** | INE (Inquérito ao Emprego; Censos) / Pordata | `Nacionalidade` + `Setor de Atividade Económica (CAE)` |
| **3. Perfil educacional por setor** | INE (Censos - cruzamento ideal) | `Nacionalidade` + `Setor de Atividade` + `Habilitações Literárias` |
| **4. Diferenças entre nacionalidades** | INE (Censos; Inquérito ao Emprego) / AIMA | `Nacionalidade` + `Habilitações Literárias` |

---

### **Próximos Passos Imediatos (Backlog do Sprint 1)**

Sugiro que nosso **Sprint 1** tenha como objetivo a **exploração e confirmação da disponibilidade dos dados**.

**Backlog do Sprint 1 - Objetivo: "Confirmar Fontes de Dados"**

*   **Tarefa 1:** Acessar o site da Pordata e buscar séries sobre "População Estrangeira", "Habilitações Literárias" e "Emprego por Setor", filtrando por "Estrangeiros".
*   **Tarefa 2:** Acessar o portal do INE e localizar os microdados dos Censos 2021 e os dados do Inquérito ao Emprego. Verificar se as variáveis necessárias estão disponíveis para download.
*   **Tarefa 3:** Acessar o site do AIMA e baixar os últimos 3-5 relatórios anuais (RIFA). Localizar as tabelas sobre autorizações de residência por motivo.
*   **Tarefa 4:** (Importante) Criar um "Log de Fontes" compartilhado (uma planilha simples) para registrar: **URL do dado, Data de Acesso, Descrição do Dataset, e Variáveis Encontradas**.

**Atenção aos Riscos:**
*   Os dados dos Censos são muito detalhados, mas o acesso aos microdados pode requerer um registo. Vamos verificar isso na Tarefa 2.
*   Os relatórios do AIMA/SEF são em PDF, o que pode exigir um trabalho de extração manual ou com ferramentas. Já vamos antevendo isso.

---

**Para seguirmos em frente, preciso que o time valide:**

1.  O plano de ação para o Sprint 1 (a exploração inicial das bases de dados) parece claro e exequível?

---

## **Relatório de Progresso – Fase de Data Understanding (CRISP-DM)**


### **Objetivo da Fase Atual**

Inicia-se a Fase 2 do CRISP-DM – **Data Understanding**, cujo objetivo é explorar, avaliar e documentar a disponibilidade e a qualidade das fontes de dados identificadas. O produto final dessa etapa será um **Dicionário de Dados inicial** e um **Relatório de Viabilidade**, que indicarão as possibilidades de resposta às perguntas de pesquisa com base nas fontes acessadas.

---

### **Estrutura de Trabalho**

Foi criado um **Épico no Kanban** intitulado **[DU-01] Exploração e Confirmação das Fontes de Dados**, com tasks distribuídas entre os cinco membros da equipe, considerando diferentes níveis de complexidade e competências.

---

### **Critérios de Conclusão (Definition of Done)**

* Todas as fontes prioritárias (**INE**, **Pordata**, **AIMA**) foram acessadas e os datasets relevantes identificados.
* O **Log de Fontes** foi completado com URL, data de acesso e descrição de cada dataset.
* Foi produzido um **Relatório de Viabilidade** inicial.
* Os primeiros datasets foram armazenados na pasta `data/raw/` do repositório do projeto.

---

### **Tarefas Definidas**

**Nível 1 – Baixa Complexidade**

* **[DU-01-A]** Criação do Log de Fontes e estrutura de pastas do repositório.
* **[DU-01-B]** Exploração e documentação dos relatórios do AIMA (RIFA dos últimos 5 anos).

**Nível 2 – Média Complexidade**

* **[DU-01-C]** Exploração guiada na **Pordata**, exportando séries temporais relevantes.
* **[DU-01-D]** Investigação de fontes secundárias no **BPstat** e **data.europa.eu**.

**Nível 3 – Alta Complexidade**

* **[DU-01-E]** Acesso e análise dos **microdados do INE** (Censos 2021 e Inquérito ao Emprego).
* **[DU-01-F]** Consolidação do **Relatório de Viabilidade** com base nos resultados das tasks anteriores.

---

### **Distribuição e Priorização**

1. O Épico **[DU-01]** foi movido para a coluna **“In Progress”** do Kanban.
2. A **Task 2.1** é prioritária por ser a base estrutural das demais.
3. As **Tasks 2.2, 2.3 e 2.4** serão executadas em paralelo.
4. A **Task 2.5** requer maior familiaridade técnica e será atribuída a um membro com experiência em acesso a microdados.
5. A **Task 2.6** será finalizada após a conclusão das demais, consolidando o relatório.

---
