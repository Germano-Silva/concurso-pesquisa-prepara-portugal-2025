### **Data Understanding (Entendimento dos Dados) - Primeira Exploração**

Agora, como PO e Scrum Master, vou levantar os **datasets oficiais portugueses** que possivelmente responderão a essas perguntas. Este é o início da nossa "caça aos dados".

**Fontes Primárias de Dados Sugeridas:**

1.  **INE (Instituto Nacional de Estatística):**
    *   **Censos 2021:** Fonte riquíssima para cruzar variáveis como **Nacionalidade**, **Nível de Escolarização**, **Situação Profissional**, **Profissão**, e **Setor de Atividade**. É um retrato detalhado, mas é uma foto de um momento (2021).
    *   **Inquérito ao Emprego (trimestral/anuário):** Fornece dados contínuos sobre a população estrangeira no mercado de trabalho. Podemos extrair séries temporais sobre **taxa de atividade, desemprego e subemprego** por nacionalidade.
    *   **Estatísticas Demográficas:** Para dados gerais de evolução da população estrangeira residente.

2.  **Pordata:** Funciona como uma interface amigável para muitos dos dados do INE. É excelente para obter séries históricas consolidadas e fazer comparações iniciais. A vantagem é a facilidade de exportação.

3.  **AIMA (Agência para a Integração, Migrações e Asilo) - ex-SEF:** 
    *   **Relatórios de Imigração, Fronteiras e Asilo (RIFA):** São a fonte mais autorizada para dados administrativos sobre imigração. Trazem informações cruciais sobre **novas autorizações de residência**, muitas vezes com breakdown por **motivo** (ex: trabalho altamente qualificado, trabalho não qualificado, estudo, reagrupamento familiar). Isso é **OURO** para o seu tema.

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
