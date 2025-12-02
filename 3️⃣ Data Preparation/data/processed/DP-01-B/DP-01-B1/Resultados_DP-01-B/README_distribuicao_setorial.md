# Distribuição Setorial por Nacionalidade
## Dataset de Análise - CAE Rev.3

**Data de Geração:** 02/12/2025 15:27:28  
**Fonte:** Censos 2021 - INE Portugal  
**Processamento:** Script de Análise Setorial

---

## 📋 Descrição

Este dataset contém a distribuição de empregados portugueses e imigrantes pelos 22 setores de atividade económica segundo a Classificação das Atividades Económicas (CAE Rev.3).

## 📊 Estrutura do Dataset

**Total de Registros:** 42  
**Setores CAE Rev.3:** 22 (A até U)  
**Nacionalidades:** 2 (Portuguesa e Estrangeira)

### Colunas:

1. **codigo_cae** (string)
   - Código do setor económico (A, B, C, ..., U)
   - Corresponde à CAE Rev.3

2. **setor_economico** (string)
   - Descrição completa do setor em português
   - Exemplo: "Agricultura, produção animal, caça, floresta e pesca"

3. **nacionalidade** (string)
   - "Portuguesa": Cidadãos portugueses
   - "Estrangeira (Imigrantes)": Cidadãos estrangeiros residentes

4. **num_empregados** (integer)
   - Número absoluto de empregados
   - Baseado nos dados dos Censos 2021

5. **percentual_da_nacionalidade** (float)
   - Percentagem que este setor representa do total de empregados dessa nacionalidade
   - Fórmula: (empregados do setor / total da nacionalidade) × 100
   - A soma para cada nacionalidade é 100%

6. **percentual_do_setor** (float)
   - Percentagem que essa nacionalidade representa no setor
   - Fórmula: (empregados da nacionalidade / total do setor) × 100
   - Mostra a composição de cada setor

---

## 🎯 Casos de Uso

### Análise de Integração Laboral
- Identificar setores com maior inserção de imigrantes
- Comparar padrões de emprego entre portugueses e imigrantes
- Avaliar concentração setorial por nacionalidade

### Estudos de Mercado de Trabalho
- Análise de setores dependentes de mão-de-obra estrangeira
- Identificação de nichos de emprego para imigrantes
- Avaliação de diversidade setorial

### Políticas Públicas
- Orientação para políticas de integração
- Planeamento de formação profissional
- Estratégias de atração de talento

---

## 📈 Principais Insights

### Distribuição Total
- **Portugueses:** 4,162,122 empregados
- **Estrangeiros:** 264,271 empregados

### Setores com Maior Concentração de Imigrantes
1. Setor U: 25.44% imigrantes
2. Setor T: 13.20% imigrantes
3. Setor I: 12.87% imigrantes

---

## ⚠️ Considerações Metodológicas

### Definição de "Imigrantes"
- Baseado em "Nacionalidade estrangeira" dos Censos 2021
- Inclui cidadãos estrangeiros com residência em Portugal
- Não distingue entre diferentes países de origem nesta versão agregada

### Cobertura de Setores
- Todos os 22 setores CAE Rev.3 estão representados (A-U)
- Setores com zero empregados estão incluídos para completude
- Não inclui setores agregados (apenas desagregados)

### Cálculos de Percentagens
- Percentagens arredondadas a 2 casas decimais
- Totais podem divergir ligeiramente de 100% devido a arredondamento
- Valores baseados em dados censitários de 2021

---

## 🔗 Fontes de Dados

**Arquivos de Origem:**
1. `EmpregadosPorSetor.csv` - Dados de emprego por setor
2. `SetorEconomico.csv` - Classificação CAE Rev.3
3. `Nacionalidade.csv` - Mapeamento de nacionalidades

**Referência:**
INE - Instituto Nacional de Estatística  
Censos da População e Habitação 2021  
[www.ine.pt](https://www.ine.pt)

---

## 📝 Como Citar

```
Distribuição Setorial por Nacionalidade - CAE Rev.3
Baseado em: INE, Censos 2021
Processado em: 02/12/2025
```

---

## 📧 Informações Adicionais

Para mais informações sobre:
- **CAE Rev.3:** Consulte a documentação oficial do INE
- **Censos 2021:** [censos.ine.pt](https://censos.ine.pt)
- **Metodologia ETL:** Veja documentação do projeto

---

**Última Atualização:** 02/12/2025 às 15:27
