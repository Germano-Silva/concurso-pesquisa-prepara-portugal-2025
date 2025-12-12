# Relatório de Viabilidade Técnica: Censos 2011 para Estudo sobre Imigração em Portugal

**Título do Estudo:** O Perfil Sócio-profissional do Imigrante em Portugal: Uma Análise da Relação entre Nível Educacional e Inserção no Mercado de Trabalho

**Fonte de Dados:** Censos 2011 - Instituto Nacional de Estatística (INE)  
**Total de Arquivos Analisados:** 115 arquivos de metadados (Q1.01 a Q6.49)  
**Data de Análise:** 12 de dezembro de 2024

---

## 1. RESUMO EXECUTIVO

Os dados dos Censos 2011 apresentam **limitações significativas** para responder completamente às quatro perguntas de pesquisa propostas. A principal limitação é a **natureza transversal dos dados** (fotografam apenas o ano de 2011), impossibilitando análises evolutivas dos últimos 5-10 anos. Adicionalmente, embora existam dados sobre escolaridade e sobre nacionalidade, **faltam cruzamentos diretos** entre estas variáveis e a inserção setorial no mercado de trabalho.

**Viabilidade Global:** **PARCIAL** - Apenas as perguntas 2 e 4 podem ser respondidas com os dados disponíveis, ainda que com limitações metodológicas importantes. As perguntas 1 e 3 são **inviáveis** com os dados atuais.

---

## 2. ANÁLISE POR PERGUNTA DE PESQUISA

### Pergunta 1: Evolução do Nível de Escolaridade da População Estrangeira (5-10 anos)

**Classificação de Viabilidade:** ❌ **INVIÁVEL**

#### Fontes de Dados Identificadas
- **Q1.03**: População residente por nível de escolaridade e sexo (sem distinção de nacionalidade)
- **Q6.03 a Q6.05**: Nível de escolaridade por grupo etário (sem distinção de nacionalidade)
- **Q6.06 a Q6.08**: População por naturalidade/nacionalidade e grupo etário (sem dados de escolaridade)

#### Análise de Suficiência
Os Censos 2011 fornecem **dados de um único momento temporal** (21 de março de 2011), tornando impossível realizar análise evolutiva dos últimos 5-10 anos. Os arquivos identificados contêm:
- Escolaridade da população total sem desagregação por nacionalidade (Q1.03)
- Nacionalidade da população sem cruzamento com escolaridade (Q6.06-Q6.08)

#### Lacunas e Limitações
1. **Limitação temporal crítica**: Dados transversais de 2011 não permitem análise de tendências
2. **Ausência de série histórica**: Não há dados de 2001-2006 ou 2006-2011 nos arquivos fornecidos
3. **Falta de cruzamento**: Não existe tabela que cruze simultaneamente nacionalidade × escolaridade × tempo
4. **Dados comparativos inexistentes**: Arquivo Q1.02 compara 2001 vs 2011 apenas para grupos etários, não escolaridade

#### Recomendação
**Esta pergunta NÃO pode ser respondida** com os dados dos Censos 2011 isoladamente. Seria necessário:
- Acesso aos microdados do Censo 2001 para comparação
- Ou dados anuais do PORDATA/SEF para período 2006-2016
- Ou reformulação para análise cross-sectional (perfil educacional em 2011 por tempo de residência)

---

### Pergunta 2: Distribuição da População Imigrante por Setores vs População Nacional

**Classificação de Viabilidade:** ⚠️ **PARCIALMENTE VIÁVEL**

#### Fontes de Dados Identificadas
- **Q1.04**: População economicamente ativa por setor (primário, secundário, terciário) e sexo
- **Q6.31**: População ativa segundo situação na profissão por grupo etário
- **Q6.33**: População empregada segundo situação na profissão por profissões
- **Q6.34**: População empregada por situação na profissão e ramos de atividade econômica (DETALHADO)
- **Q6.39**: População empregada por setor econômico e sexo
- **Q6.47**: Desempregados à procura de novo emprego por ramos de atividade

#### Análise de Suficiência
Os dados permitem caracterizar a **distribuição setorial da população total**, mas apresentam limitação crítica:
- **Q6.34 e Q6.39** fornecem distribuição detalhada por 237 ramos de atividade econômica (agricultura, construção, hotelaria, TI, saúde, etc.)
- **Limitação**: Os dados não separam população nacional de estrangeira nos setores

#### Dados Disponíveis
✅ Setores disponíveis (exemplos do Q6.34):
- Construção
- Hotelaria (Alojamento e restauração)
- Tecnologias de Informação
- Saúde (Atividades de saúde humana e apoio social)
- Indústria transformadora
- Comércio por grosso e a retalho
- Educação

#### Lacunas e Limitações
1. **Ausência de cruzamento nacionalidade × setor**: Dados setoriais não distinguem nacionais de estrangeiros
2. **Solução parcial possível**: Se houver acesso aos **microdados** (dados individuais não agregados), seria possível realizar o cruzamento
3. **Proxy viável**: Cruzar dados de Q6.06-Q6.08 (população por naturalidade) com Q6.34 (setores) através de análise geográfica

#### Recomendação
**Resposta PARCIAL possível** mediante:
- Acesso aos microdados dos Censos 2011 para cruzamento nacionalidade × setor
- Ou análise comparativa usando dados PORDATA/SEF que já contenham este cruzamento
- Ou limitação da análise à população total (sem distinção nacional/estrangeira)

---

### Pergunta 3: Perfil Educacional dentro dos Principais Setores de Mão-de-Obra Imigrante

**Classificação de Viabilidade:** ❌ **INVIÁVEL**

#### Fontes de Dados Identificadas
- **Q6.33**: População empregada por profissões (10 grupos CNP)
- **Q6.34**: População empregada por ramos de atividade (237 setores)
- **Q6.38**: População empregada por grupo etário, nível de escolaridade e sexo (SEM setor)
- **Q1.03**: Escolaridade da população (SEM nacionalidade ou setor)

#### Análise de Suficiência
Esta pergunta requer **triplo cruzamento** (nacionalidade × setor × escolaridade), que **não existe** nos dados agregados fornecidos:
- Q6.38 cruza escolaridade × idade, mas não inclui setor nem nacionalidade
- Q6.34 cruza setor × situação profissional, mas não inclui escolaridade nem nacionalidade
- Q6.06 cruza nacionalidade × idade, mas não inclui setor nem escolaridade

#### Lacunas e Limitações
1. **Cruzamento triplo inexistente**: Nenhum arquivo fornece nacionalidade × setor × escolaridade simultaneamente
2. **Impossibilidade de inferência**: Mesmo combinando múltiplos arquivos, não há variável comum que permita o cruzamento
3. **Limitação estrutural**: Dados agregados por zona geográfica não permitem reconstruir relações individuais

#### Recomendação
**Esta pergunta NÃO pode ser respondida** com dados agregados dos Censos 2011. Seria necessário:
- Acesso obrigatório aos **microdados** (base individual completa)
- Ou utilização de Inquérito ao Emprego (IE) do INE que contenha estes cruzamentos
- Ou reformulação para análise bivariada (educação × setor OU educação × nacionalidade)

---

### Pergunta 4: Diferenças no Nível Educacional entre Nacionalidades Representativas

**Classificação de Viabilidade:** ⚠️ **PARCIALMENTE VIÁVEL**

#### Fontes de Dados Identificadas
- **Q6.06 a Q6.08**: População residente por naturalidade/nacionalidade e grupo etário
- **Q6.03 a Q6.05**: População por nível de escolaridade e grupo etário
- **Q1.03**: População por escolaridade (total, sem nacionalidade)

#### Análise de Suficiência
Os dados permitem caracterização **indireta** através de análise por naturalidade:
- **Q6.06** lista 50+ países de naturalidade com distribuição etária
- **Q6.03-Q6.05** fornecem escolaridade por grupo etário
- **Possibilidade**: Correlacionar padrões etários de nacionalidades com padrões educacionais etários

#### Dados Disponíveis - Nacionalidades Principais
Segundo Q6.06, as nacionalidades mais representativas incluem:
- Brasil
- Cabo Verde
- Angola
- Guiné-Bissau
- Moçambique
- Ucrânia
- Romênia
- Reino Unido
- França
- Espanha
- China
- Índia

#### Lacunas e Limitações
1. **Cruzamento direto ausente**: Não há arquivo que cruze diretamente nacionalidade × escolaridade
2. **Solução por proxy etária**: Assumindo padrões etários distintos por nacionalidade, pode-se inferir diferenças educacionais
3. **Precisão limitada**: Análise indireta tem menor robustez estatística
4. **Microdados essenciais**: Resposta precisa requer acesso aos dados individuais

#### Recomendação
**Resposta PARCIAL possível** mediante:
- Análise indireta correlacionando idade × nacionalidade com idade × escolaridade
- Ou acesso aos microdados para cruzamento direto
- Ou uso de dados PORDATA que já contenham população estrangeira por nacionalidade × escolaridade
- **Limitação metodológica**: Resultados devem ser apresentados com cautela estatística

---

## 3. MAPEAMENTO DADOS-PERGUNTA

### Matriz de Correspondência

| Pergunta | Arquivos Relevantes | Variáveis Necessárias | Variáveis Disponíveis | Gap Crítico |
|----------|---------------------|----------------------|----------------------|-------------|
| P1 (Evolução escolaridade) | Q1.02, Q1.03, Q6.03-Q6.08 | Nacionalidade × Escolaridade × Tempo | Escolaridade (2011 apenas) | Série temporal + Cruzamento |
| P2 (Distribuição setorial) | Q1.04, Q6.34, Q6.39 | Nacionalidade × Setor | Setor (população total) | Nacionalidade nos setores |
| P3 (Perfil educacional setorial) | Q6.34, Q6.38, Q1.03 | Nacionalidade × Setor × Escolaridade | Setores, Escolaridade (separados) | Cruzamento triplo |
| P4 (Diferenças por nacionalidade) | Q6.06-Q6.08, Q6.03-Q6.05 | Nacionalidade × Escolaridade | Nacionalidade, Escolaridade (separados) | Cruzamento direto |

### Arquivos-Chave por Tema

**Escolaridade:**
- Q1.03: Escolaridade da população (agregada)
- Q6.03-Q6.05: Escolaridade por grupo etário

**Nacionalidade/Naturalidade:**
- Q6.06: População por naturalidade e grupo etário (50+ países)
- Q6.07: População residente por países de nacionalidade
- Q6.08: População que residiu no estrangeiro por país de proveniência

**Mercado de Trabalho:**
- Q1.04: Atividade econômica por setor (primário, secundário, terciário)
- Q6.31: População ativa segundo situação profissional
- Q6.33: População empregada por profissões (CNP)
- Q6.34: **População empregada por 237 ramos de atividade** (CRÍTICO)
- Q6.38: Empregados por idade, escolaridade e sexo
- Q6.39: Empregados por setor e sexo

**Desemprego:**
- Q1.05: População desempregada
- Q645-Q648: Desempregados por diferentes características

---

## 4. LACUNAS GERAIS DE DADOS

### 4.1 Limitações Temporais
- **Dados transversais**: Censo de 2011 é uma fotografia única, impossibilitando análise de tendências
- **Ausência de comparabilidade**: Q1.02 compara 2001-2011 apenas para idade, não escolaridade ou nacionalidade
- **Período inadequado**: Dados de 2011 estão defasados 13 anos (desatualizados para 2024)

### 4.2 Limitações de Cruzamento
- **Variáveis isoladas**: Escolaridade, nacionalidade e setor aparecem em tabelas separadas
- **Agregação excessiva**: Dados agregados por zona geográfica impedem reconstrução de relações individuais
- **Microdados inacessíveis**: Arquivos fornecidos são apenas metadados agregados

### 4.3 Limitações de Granularidade
- **Nacionalidade**: Q6.06-Q6.08 fornecem países principais, mas sem cruzamento com variáveis laborais
- **Setores**: Q6.34 tem 237 setores detalhados, mas sem distinção nacional/estrangeira
- **Educação**: Q1.03 e Q6.03-Q6.05 têm níveis de escolaridade completos, mas sem nacionalidade

### 4.4 Inconsistências Metodológicas
- **Definições diferentes**: "Naturalidade" (Q6.06) vs "Nacionalidade" (Q6.07) vs "Proveniência" (Q6.08)
- **Períodos de referência variados**: Algumas variáveis referem-se a momentos distintos
- **População de referência**: Alguns arquivos incluem apenas população ativa, outros incluem total

---

## 5. CONCLUSÕES E RECOMENDAÇÕES

### 5.1 Viabilidade por Pergunta

| Pergunta | Viabilidade | Justificativa |
|----------|-------------|---------------|
| P1 | ❌ Baixa (10%) | Dados transversais impossibilitam análise evolutiva |
| P2 | ⚠️ Média (50%) | Possível com microdados ou dados complementares |
| P3 | ❌ Baixa (20%) | Cruzamento triplo inexistente nos dados agregados |
| P4 | ⚠️ Média (60%) | Possível análise indireta ou com microdados |

### 5.2 Cenário Atual (Apenas Censos 2011 - Dados Agregados)
**Viabilidade Global: 35%** - Pesquisa NÃO é viável com dados atuais

**Perguntas que podem ser respondidas:**
- ❌ P1: Não
- ⚠️ P2: Parcialmente (sem distinção nacional/estrangeira)
- ❌ P3: Não
- ⚠️ P4: Parcialmente (análise indireta)

### 5.3 Cenário com Microdados dos Censos 2011
**Viabilidade Global: 60%** - Pesquisa PARCIALMENTE viável

**Perguntas que podem ser respondidas:**
- ❌ P1: Não (limitação temporal persiste)
- ✅ P2: Sim (cruzamento direto possível)
- ✅ P3: Sim (cruzamento triplo possível)
- ✅ P4: Sim (cruzamento direto possível)

### 5.4 Cenário com Fontes Complementares
**Viabilidade Global: 90%** - Pesquisa VIÁVEL

**Fontes complementares recomendadas:**
1. **PORDATA** - Séries temporais de população estrangeira por nacionalidade
2. **AIMA/SEF** - Dados anuais de imigrantes por nacionalidade e escolaridade (2006-2023)
3. **Inquérito ao Emprego (INE)** - Dados trimestrais sobre mercado de trabalho
4. **Censos 2021** - Dados mais recentes para comparação evolutiva
5. **Eurostat** - Dados europeus para contextualização

### 5.5 Reformulações Possíveis do Estudo

#### Opção A: Estudo Transversal (2011)
**Título reformulado:** "Perfil Sócio-profissional do Imigrante em Portugal em 2011: Análise da Relação entre Nível Educacional e Inserção no Mercado de Trabalho"

**Perguntas reformuladas:**
1. ~~Evolução~~ → **Qual o perfil educacional da população estrangeira residente em Portugal em 2011?**
2. Como se distribui a população ~~imigrante~~ **total** ativa por setores em 2011? (sem distinção)
3. ~~Perfil educacional nos setores~~ → **Qual a relação entre nível educacional e setor de atividade da população ativa em 2011?** (sem distinção de nacionalidade)
4. Qual o perfil etário-educacional das principais nacionalidades residentes em Portugal em 2011? (análise indireta)

#### Opção B: Estudo Evolutivo (2011-2021)
Requerer **Censos 2021** + **dados AIMA/SEF** para análise comparativa

#### Opção C: Estudo com Microdados
Solicitar acesso à **base de microdados** dos Censos 2011 ao INE (procedimento formal)

---

## 6. RECOMENDAÇÕES FINAIS

### 6.1 Ação Imediata Recomendada
1. **Solicitar acesso aos microdados** dos Censos 2011 ao INE (processo pode levar 2-4 meses)
2. **Obter dados PORDATA/AIMA** para série temporal 2006-2021
3. **Reformular perguntas** para estudo transversal se prazo for curto
4. **Considerar Censos 2021** como fonte principal (dados mais recentes)

### 6.2 Estratégia Híbrida Recomendada
Para maximizar viabilidade com prazo limitado:

**Fase 1 - Análise com Dados Públicos (1-2 meses):**
- Caracterização geral usando Censos 2011 agregados (P2 e P4 parciais)
- Dados complementares PORDATA para série temporal (P1 alternativa)
- Produzir relatório preliminar

**Fase 2 - Aprofundamento com Microdados (3-4 meses):**
- Solicitar e obter microdados Censos 2011
- Análise completa P2, P3 e P4
- Relatório final

### 6.3 Avaliação de Risco

| Cenário | Viabilidade | Prazo | Qualidade Científica |
|---------|-------------|-------|---------------------|
| Apenas dados agregados 2011 | 35% | 1 mês | Baixa (muitas limitações) |
| Com microdados 2011 | 60% | 3-4 meses | Média (sem evolução temporal) |
| Com fontes complementares | 90% | 2-3 meses | Alta (análise completa) |
| Com Censos 2021 + AIMA | 95% | 4-6 meses | Muito Alta (dados atuais) |

---

## 7. CONCLUSÃO FINAL

Os dados dos **Censos 2011 isoladamente NÃO são suficientes** para responder às quatro perguntas de pesquisa na forma originalmente proposta. As limitações críticas são:

1. ✘ **Temporal**: Impossibilidade de análise evolutiva de 5-10 anos
2. ✘ **Cruzamento**: Ausência de tabelas que combinem nacionalidade × setor × escolaridade
3. ✘ **Atualidade**: Dados de 2011 estão defasados (13 anos)

**Recomendação final:** 
- **Cenário ideal**: Utilizar **Censos 2021** + **dados AIMA 2015-2024** + **microdados** para estudo completo
- **Cenário alternativo**: Reformular para estudo transversal 2011 com análises parciais
- **Não recomendado**: Prosseguir apenas com dados agregados Censos 2011

**Próximo passo crítico:** Definir se o objetivo é análise histórica (2011) ou contemporânea (2021-2024), pois isso determinará as fontes de dados necessárias.

---

**Analista Responsável:** [Sistema de Análise de Viabilidade]  
**Data do Relatório:** 12 de dezembro de 2024  
**Versão:** 1.0 - Relatório Técnico Final
