# 📊 PLANO REVISADO - DASHBOARD POWER BI POR PERGUNTA DE PESQUISA

---

## 🎯 ESTRUTURA GERAL DO DASHBOARD

**Formato:** 4 Páginas (Tabs) - Uma página dedicada para cada pergunta de pesquisa

---

## 📄 PÁGINA 1: EVOLUÇÃO DA ESCOLARIDADE (2011-2024)

### **Pergunta:** Qual a evolução, nos últimos 5-10 anos, do nível de escolaridade da população estrangeira residente em Portugal?

### **🔢 5 KPIs**

1. **📈 Taxa de Crescimento - Ensino Superior**
   - Variação % (2011 → 2024)
   - *Fonte: Fact_EstatisticasEducacao*

2. **📉 Taxa de Redução - Sem Educação**
   - Variação % (2011 → 2024)
   - *Fonte: Fact_EstatisticasEducacao*

3. **🎓 População com Ensino Superior (2024)**
   - Número absoluto
   - *Fonte: Fact_PopulacaoEducacao*

4. **📊 Índice Educacional Médio (2024)**
   - Escala 0-10
   - *Fonte: Fact_EstatisticasEducacao*

5. **🔄 Taxa de Crescimento Anual Média**
   - CAGR (Compound Annual Growth Rate)
   - *Cálculo personalizado*

### **📊 5 GRÁFICOS**

**1. Linha Temporal - Evolução dos 4 Níveis (2011-2024)**
- Eixo X: Anos | Eixo Y: % População
- 4 linhas: Sem Educação / Básico / Secundário / Superior

**2. Barras Empilhadas - Comparativo 2011 vs 2024**
- 2 barras verticais mostrando a mudança na distribuição
- Segmentos coloridos por nível educacional

**3. Gráfico de Área - Crescimento Absoluto por Nível**
- Variação no número de pessoas (não %)
- Destaque para qual nível teve maior crescimento absoluto

**4. Funil - Pirâmide Educacional 2024**
- Visualização tipo funil mostrando a distribuição atual
- Maior base (Básico) → Menor topo (Superior)

**5. Velocímetro/Gauge - Meta de Qualificação**
- Compara % atual com meta europeia (30% Ensino Superior)
- Indicador visual de progresso

**Tabelas Usadas:**
- `Fact_EstatisticasEducacao`
- `Fact_PopulacaoEducacao`
- `Dim_NivelEducacao`
- `Dim_AnoRelatorio`

---

## 📄 PÁGINA 2: DISTRIBUIÇÃO SETORIAL

### **Pergunta:** Como se distribui a população imigrante ativa por setores de atividade económica e como essa distribuição se compara com a população nacional?

### **🔢 5 KPIs**

1. **🏭 Setor com Maior Concentração de Imigrantes**
   - Nome do setor + % de imigrantes
   - *Fonte: Fact_EmpregadosPorSetor*

2. **👥 Total de Imigrantes Ativos**
   - Número absoluto
   - *Fonte: Fact_PopulacaoPorCondicao*

3. **⚖️ Índice de Sobre-representação**
   - Setor onde imigrantes são mais representados vs nacionais
   - *Cálculo: (% Imigrantes / % Nacionais)*

4. **📊 Diversidade Setorial**
   - Número de setores com +5% de imigrantes
   - *Fonte: Fact_EmpregadosPorSetor*

5. **🔄 Taxa de Empregabilidade Imigrante**
   - % de imigrantes empregados vs desempregados
   - *Fonte: Fact_PopulacaoPorCondicao + Dim_CondicaoEconomica*

### **📊 5 GRÁFICOS**

**1. Barras Agrupadas Horizontais - Top 10 Setores**
- Comparação lado a lado: Imigrantes (laranja) vs Nacionais (azul)
- Ordenado por maior diferença

**2. Treemap - Proporção de Imigrantes por Setor**
- Tamanho do bloco = nº de trabalhadores
- Cor = intensidade do % de imigrantes

**3. Scatter Plot - Correlação Setorial**
- Eixo X: % Imigrantes | Eixo Y: Salário Médio do Setor
- Cada ponto = um setor econômico
- Bolhas coloridas por agregado setorial (Primário/Secundário/Terciário)

**4. Gráfico de Barras 100% Empilhadas**
- Cada barra = 1 setor (Top 8)
- Segmentos: Portugueses vs Estrangeiros

**5. Heatmap - Matriz Nacionalidade x Setor**
- Linhas: Top 10 Nacionalidades
- Colunas: Top 8 Setores
- Cor: Concentração de trabalhadores

**Tabelas Usadas:**
- `Fact_EmpregadosPorSetor`
- `Dim_SetorEconomico`
- `Dim_Nacionalidade`
- `Fact_PopulacaoPorCondicao`
- `Dim_CondicaoEconomica`

---

## 📄 PÁGINA 3: EDUCAÇÃO POR SETOR

### **Pergunta:** Qual é o perfil educacional predominante dentro dos principais setores que absorvem mão-de-obra imigrante?

### **🔢 5 KPIs**

1. **🎓 Setor com Maior Qualificação**
   - Nome do setor + % com Ensino Superior
   - *Fonte: Fact_PopulacaoTrabalhoEscolaridade*

2. **🏗️ Setor com Menor Qualificação**
   - Nome do setor + % sem Educação/Básico
   - *Fonte: Fact_PopulacaoTrabalhoEscolaridade*

3. **📊 Gap Educacional Médio**
   - Diferença média entre setores (desvio padrão)
   - *Cálculo estatístico*

4. **🔝 Setor com Mais Crescimento Educacional**
   - Setor que mais aumentou % Superior (2011-2021)
   - *Fonte: comparação temporal*

5. **⚖️ Alinhamento Educação-Setor**
   - % de trabalhadores com qualificação adequada ao setor
   - *Cálculo personalizado*

### **📊 5 GRÁFICOS**

**1. Barras Empilhadas 100% - Distribuição Educacional por Setor**
- Cada barra = 1 setor (Top 10)
- Segmentos: Sem Educação / Básico / Secundário / Superior

**2. Radar Chart - Perfil Multidimensional**
- 4 eixos: % por nível educacional
- Múltiplas séries (um polígono por setor principal)

**3. Gráfico de Colunas Agrupadas - Setores Críticos**
- Foco em 5 setores-chave
- Barras agrupadas: cada nível educacional

**4. Sunburst - Hierarquia Setor → Educação → Nacionalidade**
- Nível 1: Agregado setorial (3 grandes grupos)
- Nível 2: Nível educacional
- Nível 3: Top 5 nacionalidades

**5. Small Multiples - Mini Gráficos por Setor**
- Grade com 6-8 setores
- Cada célula: pizza ou barra mostrando distribuição educacional

**Tabelas Usadas:**
- `Fact_PopulacaoTrabalhoEscolaridade`
- `Dim_NivelEducacao`
- `Dim_SetorEconomico`
- `Fact_EmpregadosPorSetor`
- `Dim_Sexo` (opcional - análise de género)

---

## 📄 PÁGINA 4: DIFERENÇAS POR NACIONALIDADE

### **Pergunta:** Existem diferenças significativas no nível educacional médio entre as nacionalidades mais representativas da imigração em Portugal?

### **🔢 5 KPIs**

1. **🌍 Nacionalidade Mais Qualificada**
   - País + % com Ensino Superior
   - *Fonte: Fact_EstatisticasEducacao*

2. **🌍 Nacionalidade Menos Qualificada**
   - País + % sem Educação
   - *Fonte: Fact_EstatisticasEducacao*

3. **📊 Amplitude (Range) Educacional**
   - Diferença entre maior e menor % Superior
   - *Cálculo: Max - Min*

4. **🇵🇹 Comparação com Portugal**
   - % de nacionalidades acima/abaixo da média portuguesa
   - *Fonte: dados INE populacionais*

5. **📈 Nacionalidade com Maior Evolução**
   - País que mais melhorou índice educacional
   - *Fonte: comparação temporal*

### **📊 5 GRÁFICOS**

**1. Barras Ordenadas - Ranking de Qualificação (Top 15 Nacionalidades)**
- Ordenado por % com Ensino Superior
- Linha de referência: Média de Portugal
- Cores: Verde (acima média) / Vermelho (abaixo média)

**2. Box Plot - Distribuição Estatística por Continente**
- Agrupamento por continente de origem
- Mostra mediana, quartis e outliers
- Visualiza desigualdade intra-continental

**3. Matriz de Correlação - Educação vs Outros Indicadores**
- Eixo X: Nacionalidades (Top 12)
- Eixo Y: 4 Níveis Educacionais
- Heatmap de intensidade

**4. Gráfico de Dispersão - Qualificação vs População**
- Eixo X: % com Ensino Superior
- Eixo Y: Tamanho da comunidade
- Bolhas: cada nacionalidade
- Cor: Continente de origem

**5. Gráfico de Barras Divergente (Butterfly Chart)**
- Centro: zero
- Esquerda: % Sem Educação/Básico (vermelho)
- Direita: % Secundário/Superior (verde)
- Mostra simetria educacional

**Tabelas Usadas:**
- `Fact_EstatisticasEducacao`
- `Dim_Nacionalidade`
- `Fact_NacionalidadePrincipal` (Top nacionalidades)
- `Fact_PopulacaoPorNacionalidade`
- `Fact_EvolucaoTemporal` (análise temporal)

---

## 🎨 NAVEGAÇÃO E LAYOUT

### **Estrutura de Navegação**
```
┌────────────────────────────────────────────────────────┐
│ [HOME] [P1: Evolução] [P2: Setores] [P3: Ed×Setor] [P4: Nacionalidades] │
└────────────────────────────────────────────────────────┘
```

### **Layout Padrão (Cada Página)**
```
┌───────────────────────────────────────────────────────────┐
│ TÍTULO DA PERGUNTA                     [Filtros Globais▼] │
├───────────────────────────────────────────────────────────┤
│ KPI 1    │ KPI 2    │ KPI 3    │ KPI 4    │ KPI 5        │
├───────────────────────────────────────────────────────────┤
│                    GRÁFICO 1 (Principal)                  │
│                      [Tela cheia]                         │
├─────────────────────────────┬─────────────────────────────┤
│      GRÁFICO 2              │      GRÁFICO 3              │
├─────────────────────────────┼─────────────────────────────┤
│      GRÁFICO 4              │      GRÁFICO 5              │
└─────────────────────────────┴─────────────────────────────┘
```

---

## 📊 RESUMO QUANTITATIVO

| Pergunta | KPIs | Gráficos | Tabelas Principais |
|----------|------|----------|-------------------|
| **P1: Evolução Educacional** | 5 | 5 | 4 tabelas |
| **P2: Distribuição Setorial** | 5 | 5 | 5 tabelas |
| **P3: Educação por Setor** | 5 | 5 | 5 tabelas |
| **P4: Diferenças Nacionalidade** | 5 | 5 | 5 tabelas |
| **TOTAL** | **20 KPIs** | **20 Gráficos** | **40 tabelas** |

---

## ✅ BENEFÍCIOS DESTA ESTRUTURA

✅ **Organização Clara:** Cada pergunta tem sua página dedicada  
✅ **Profundidade Analítica:** 5 gráficos permitem explorar múltiplas perspectivas  
✅ **KPIs Focados:** Cada KPI responde diretamente à pergunta  
✅ **Navegação Intuitiva:** Usuário pode saltar entre análises  
✅ **Escalável:** Fácil adicionar novas páginas ou drill-downs  

---