# Task [DP-01-C] - Cruzamento de Variáveis

## Mapeamento dos dados para as perguntas

Task: [DP-01-C] Cruzamento de Variáveis

**Projeto:** Concurso Prepara Portugal 2025

### PERGUNTA 1: Evolução do Nível de Escolaridade (2011-2021)

#### **Arquivo Final planejado:** `pergunta1_evolucao_educacional.csv`

#### TABELAS NECESSÁRIAS


| Tabela de Origem                       | Campos Utilizados                                                                                                                                                                                        | Propósito                                                                      | Status               |
| :--------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------- | :--------------------- |
| **EvolucaoTemporal.csv**               | `nacionalidade_id`<br/>`ano_inicio` (2011)<br/>`populacao_inicio`<br/>`variacao_absoluta`<br/>`variacao_percentual`<br/>`taxa_crescimento`                                                               | Dados de crescimento populacional 2011→2021                                    | ✅ Disponível       |
| **EstatisticasEducacao.csv** (2021)    | `nacionalidade_id`<br/>`percentual_sem_educacao`<br/>`percentual_ensino_basico`<br/>`percentual_ensino_secundario`<br/>`percentual_ensino_superior`<br/>`indice_educacional`<br/>`ano_referencia` (2021) | Perfil educacional atual (2021)                                                 | ✅ Disponível       |
| **EstatisticasEducacao_2011.csv** ⚠️ | `nacionalidade_id`<br/>`percentual_ensino_superior`<br/>`indice_educacional`<br/>`ano_referencia` (2011)                                                                                                 | Perfil educacional histórico (2011)<br/>**ESSENCIAL para calcular evolução** | ❌**Não encontrei** |
| **Nacionalidade.csv**                  | `nacionalidade_id` (PK)<br/>`nome_nacionalidade`<br/>`continente`                                                                                                                                        | Identificação e classificação das nacionalidades                            | ✅ Disponível       |

#### **Estrutura do Arquivo Final (planejada):**

Nacionalidade, Populacao_2011, Populacao_2021, Variacao_Populacional_%, Superior_2011_%, Superior_2021_%, Evolucao_Superior_pp, Basico_2011_%, Basico_2021_%, Evolucao_Basico_pp, Sem_Educacao_2011_%, Sem_Educacao_2021_%, Evolucao_Sem_Educacao_pp, Indice_Educacional_2011, Indice_Educacional_2021, Evolucao_Indice, Continente

Brasil,105622,199810,89.2,20.0,28.6,+8.6,55.0,48.2,-6.8,8.0,5.8,-2.2,2.95,3.45,+0.50,América
Nepal,15641,215451,1278.9,8.5,12.3,+3.8,60.2,52.8,-7.4,22.0,17.9,-4.1,1.85,2.15,+0.30,Ásia

...

---

### PERGUNTA 2: Distribuição por Setores de Atividade Económica

#### **Arquivo Final:** `pergunta2_distribuicao_setorial.csv`


| Tabela de Origem           | Campos Utilizados                                                                                      | Propósito                              |
| :--------------------------- | :------------------------------------------------------------------------------------------------------- | :---------------------------------------- |
| **EmpregadosporSetor.csv** | `nacionalidade_id`<br/>`setor_id`<br/>`populacao_setor`(quantidade)<br/>`percentagem_setor` (calcular) | Distribuição por setor económico     |
| **Nacionalidade.csv**      | `nacionalidade_id` (PK)<br/>`nome_nacionalidade`                                                       | Identificação das nacionalidades      |
| **SetorEconomico.csv**     | `setor_id`<br/>`descricao`<br/>`codigo_cae`                                                            | Identificação dos setores econômicos |

#### **Estrutura do Arquivo Final (planejada):**

Nacionalidade, Setor_Atividade, Populacao_Setor, Percentagem, Comparacao_Nacionais

Brasil, Construção (F), 45000, 22.5%, +8.3pp
Brasil, Alojamento (I), 38000, 19.0%, +12.1pp ...

---

### PERGUNTA 3: Perfil Educacional dentro dos Setores

#### **Arquivo Final:** `pergunta3_perfil_educacional_setor.csv`


| Tabela de Origem                            | Campos Utilizados                                                                                                                                                                                           | Propósito                                                                        |
| :-------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :---------------------------------------------------------------------------------- |
| **EstatisticasEducacao.csv**                | `nacionalidade_id`<br/>`populacao_total_educacao`<br/>`indice_educacional`<br/>`percentual_sem_educacao`<br/>`percentual_ensino_basico`<br/>`percentual_ensino_secundario`<br/>`percentual_ensino_superior` | Perfil educacional por nacionalidade                                              |
| **SetorEconomico.csv**                      | `setor_id`<br/>`descricao`<br/>`codigo_cae`                                                                                                                                                                 | Identificação dos setores econômicos                                           |
| **distribuicao_setorial_nacionalidade.csv** | `codigo_cae`<br/>`setor_economico`<br/>`nacionalidade`<br/>`num_empregados`<br/>`indice_educacional`<br/>`percentual_da_nacionalidade`<br/>`percentual_do_setor`                                            | Distribuição setorial de empregados portugueses e estrangeiros pelos 22 setores |
| **Nacionalidade.csv**                       | `nacionalidade_id` (PK)<br/>`nome_nacionalidade`                                                                                                                                                            | Identificação das nacionalidades                                                |
| **EmpregadosporSetor.csv**                  | `nacionalidade_id`<br/>`setor_id`<br/>`populacao_setor`(quantidade)<br/>`percentagem_setor` (calcular)                                                                                                      | Distribuição setorial (para cruzamento)                                         |
| **PopulacaoTrabalhoEscolaridade.csv**       | `condicao_trabalho`<br/>`nivel_educacao_id`<br/>`quantidade_hm`                                                                                                                                             | Nível educacional por população empregada/Desempregada/Não Ativa              |
| **NivelEducacao.csv**                       | `nivel_educacao_id`<br/>`nome_nivel`<br/>`categoria`                                                                                                                                                        | Identificação do nivel educacional                                              |

#### **Estrutura do Arquivo Final (Planejada):**

Setor_Atividade, Nacionalidade, Nivel_Educacional, Percentagem, Indice_Educacional

Construção, Brasil, Ensino Básico, 45.2%, 2.8
Construção, Brasil, Ensino Superior, 8.5%, 2.8
TI, Índia, Ensino Superior, 78.3%, 5.2 ...

---

### PERGUNTA 4: Diferenças Educacionais entre Nacionalidades

#### **Arquivo Final:** `pergunta4_diferencas_nacionalidades.csv`


| Tabela de Origem             | Campos Utilizados                                                                                                                                                                                           | Propósito                           |
| :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------- |
| **EstatisticasEducacao.csv** | `nacionalidade_id`<br/>`populacao_total_educacao`<br/>`indice_educacional`<br/>`percentual_sem_educacao`<br/>`percentual_ensino_basico`<br/>`percentual_ensino_secundario`<br/>`percentual_ensino_superior` | Estatísticas educacionais completas |
| **Nacionalidade.csv**        | `nacionalidade_id` (PK)<br/>`nome_nacionalidade`<br/>`continente`                                                                                                                                           | Identificação das nacionalidades   |

#### **Estrutura do Arquivo Final (Planejada):**

Nacionalidade, Indice_Educacional, Percentual_Superior, Percentual_Basico, Percentual_Sem_Educacao, Populacao_Total, Continente

Itália, 4.82, 53.75%, 28.50%, 2.10%, 5234, Europa
Nepal, 2.15, 12.30%, 52.80%, 17.94%, 3847, Ásia
Brasil, 3.45, 28.60%, 48.20%, 5.80%, 199810, América ...

---

### **OBSERVAÇÕES**

- Dados educacionais de 2011 para comparação temporal não encontrados até o momento da criação desse arquivo.
- Arquivo de referência sujeito a mudanças.
- Somente adicionadas tabelas essenciais para a resposta as perguntas, possível adição de análises extra (região, Condição econômica, Grupo etário, etc).

---

### NOTAS TÉCNICAS

#### **Chave de Relacionamento**

- **Campo:** `nacionalidade_id` (tipo: Integer)
- **Cardinalidade:** 1:N (Dimensão → Fato)

#### **Tratamento de Dados**

- Valores nulos: 0 (dados do INE completos)
- Padronização: Feita nas tasks anteriores, não foram encontrados dados inconsistentes até o momento

---

**Documento gerado para:** Task [DP-01-C] - Cruzamento de Variáveis

**Projeto:** Concurso de Pesquisa Prepara Portugal 2025

**Fonte:** INE - Censos 2021 / Dados processados nas tasks anteriores

**Data:** 3 de Dezembro de 2025
