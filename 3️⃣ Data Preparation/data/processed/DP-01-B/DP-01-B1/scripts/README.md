# 📊 Scripts de Análise Setorial - CAE Rev.3
## Distribuição de Imigrantes e Nacionais por Setor Económico

---

## 📁 Conteúdo do Diretório

Este diretório contém scripts e documentação para processar e analisar a distribuição de empregados portugueses e estrangeiros pelos 22 setores de atividade económica segundo a Classificação CAE Rev.3.

### Arquivos Disponíveis:

1. **`distribuicao_setorial_colab.py`** (Script Principal)
   - Script Python completo para Google Colab
   - Processamento end-to-end de dados setoriais
   - Download automático dos resultados

2. **`INSTRUCOES_COLAB.md`** (Guia de Uso)
   - Instruções passo a passo para Google Colab
   - Lista de arquivos necessários
   - Troubleshooting e resolução de problemas

3. **`README.md`** (Este Arquivo)
   - Visão geral da solução
   - Documentação técnica
   - Referências e recursos

---

## 🎯 Objetivo da Solução

Criar um dataset consolidado e padronizado que possibilita análise comparativa da inserção laboral de imigrantes versus nacionais portugueses em todos os 22 setores económicos (CAE Rev.3), com métricas de distribuição percentual e concentração setorial.

---

## 📋 Arquivos de Entrada Necessários

Para executar o script, você precisará de **3 arquivos CSV**:

### 1. EmpregadosPorSetor.csv
- **Localização:** `3️⃣ Data Preparation/data/processed/DP-01-B/DP-01-B1/resultados_etl_laboral/`
- **Conteúdo:** Dados de empregados por setor e nacionalidade
- **Colunas:** `emp_setor_id`, `nacionalidade_id`, `setor_id`, `quantidade`
- **Registros:** ~391 linhas

### 2. SetorEconomico.csv
- **Localização:** `3️⃣ Data Preparation/data/processed/DP-01-B/DP-01-B1/resultados_etl_laboral/`
- **Conteúdo:** Classificação completa CAE Rev.3 (22 setores + agregados)
- **Colunas:** `setor_id`, `codigo_cae`, `descricao`, `agregado`
- **Registros:** 27 linhas (21-22 setores individuais + 5-6 agregados)

### 3. Nacionalidade.csv
- **Localização:** `3️⃣ Data Preparation/data/processed/DP-01-A/`
- **Conteúdo:** Mapeamento de IDs de nacionalidades
- **Colunas:** `nacionalidade_id`, `nome_nacionalidade`, `codigo_pais`, `continente`
- **Registros:** 19 nacionalidades

---

## 🚀 Como Usar

### Opção Recomendada: Google Colab

1. **Preparar Arquivos**
   - Reunir os 3 arquivos CSV listados acima
   - Tê-los acessíveis no seu computador

2. **Abrir Google Colab**
   - Acessar: https://colab.research.google.com
   - Criar novo notebook

3. **Copiar e Executar Script**
   - Copiar todo o conteúdo de `distribuicao_setorial_colab.py`
   - Colar numa célula do Colab
   - Executar a célula (▶️)

4. **Upload dos Arquivos**
   - O script solicitará cada arquivo
   - Fazer upload quando solicitado

5. **Download Automático**
   - Aguardar processamento (1-2 minutos)
   - Arquivos serão baixados automaticamente:
     - `distribuicao_setorial_nacionalidade.csv`
     - `README_distribuicao_setorial.md`

**Para instruções detalhadas, consulte:** `INSTRUCOES_COLAB.md`

---

## 📊 Arquivos de Saída

### 1. distribuicao_setorial_nacionalidade.csv

**Dataset principal** com análise completa da distribuição setorial.

**Estrutura:**
- **44 registros** (22 setores × 2 nacionalidades)
- **6 colunas:**

| Coluna | Descrição |
|--------|-----------|
| `codigo_cae` | Código do setor (A, B, C, ..., U) |
| `setor_economico` | Descrição completa do setor em português |
| `nacionalidade` | "Portuguesa" ou "Estrangeira (Imigrantes)" |
| `num_empregados` | N.º absoluto de empregados no setor |
| `percentual_da_nacionalidade` | % que o setor representa na nacionalidade |
| `percentual_do_setor` | % que a nacionalidade representa no setor |

**Exemplo de dados:**

```csv
codigo_cae,setor_economico,nacionalidade,num_empregados,percentual_da_nacionalidade,percentual_do_setor
A,"Agricultura, produção animal, caça, floresta e pesca",Portuguesa,115478,2.78,88.73
A,"Agricultura, produção animal, caça, floresta e pesca",Estrangeira (Imigrantes),14663,5.56,11.27
B,Indústrias extractivas,Portuguesa,9887,0.24,96.41
B,Indústrias extractivas,Estrangeira (Imigrantes),366,0.14,3.59
...
```

### 2. README_distribuicao_setorial.md

**Documentação completa** do dataset gerado, incluindo:
- Descrição detalhada das colunas
- Metodologia de cálculo
- Principais insights estatísticos
- Casos de uso
- Considerações metodológicas
- Fontes de dados e referências

---

## 🔍 Metodologia de Processamento

### Etapas do Script:

1. **Carregamento de Dados**
   - Leitura dos 3 arquivos CSV
   - Validação de estrutura

2. **Mapeamento de Nacionalidades**
   - Identificação de IDs relevantes:
     - `nacionalidade_id = 12` → Portuguesa
     - `nacionalidade_id = 11` → Estrangeira (Imigrantes)

3. **Filtro de Setores CAE Rev.3**
   - Exclusão de setores agregados
   - Garantia de 22 setores (A até U)

4. **Agregação de Dados**
   - Soma de empregados por setor e nacionalidade
   - Criação de matriz completa (22 setores × 2 nacionalidades)

5. **Cálculo de Métricas**
   - **Percentual da Nacionalidade:**  
     `(empregados_setor / total_nacionalidade) × 100`
   - **Percentual do Setor:**  
     `(empregados_nacionalidade / total_setor) × 100`

6. **Validação**
   - Verificação de soma de percentuais (≈ 100% por nacionalidade)
   - Identificação de setores com maior concentração

7. **Exportação**
   - Geração de CSV UTF-8
   - Criação de documentação Markdown
   - Download automático

---

## 📈 Principais Métricas Geradas

### Distribuição por Nacionalidade

Cada setor mostra:
- **Número absoluto** de empregados portugueses e estrangeiros
- **Percentual na nacionalidade:** Quanto % do total de empregados dessa nacionalidade trabalha neste setor
- **Percentual no setor:** Quanto % deste setor é composto por essa nacionalidade

### Análise de Concentração

O script identifica automaticamente:
- **Top 5 setores** para cada nacionalidade
- **Setores com maior concentração** de imigrantes
- **Padrões de distribuição** setorial

---

## 🎯 Casos de Uso

### 1. Integração Laboral de Imigrantes
- Identificar setores com maior inserção de estrangeiros
- Avaliar diversidade da força de trabalho por setor
- Comparar padrões de emprego entre grupos

### 2. Políticas Públicas
- Orientar programas de integração profissional
- Planejar formação específica por setor
- Desenvolver estratégias de atração de talento

### 3. Estudos de Mercado de Trabalho
- Analisar dependência setorial de mão-de-obra estrangeira
- Identificar nichos de emprego
- Avaliar competitividade setorial

### 4. Pesquisa Académica
- Estudos de imigração e trabalho
- Análise de segregação ocupacional
- Padrões de mobilidade laboral

---

## ⚙️ Requisitos Técnicos

### Google Colab (Recomendado)
- ✅ Navegador web moderno
- ✅ Conta Google
- ✅ Conexão à internet
- ✅ Permissão para downloads no navegador

**Bibliotecas (já incluídas no Colab):**
- pandas
- numpy
- google.colab.files

### Execução Local (Opcional)
```bash
# Requisitos
Python 3.7+
pandas >= 1.0.0
numpy >= 1.18.0

# Instalação
pip install pandas numpy
```

---

## 📚 Estrutura de Dados CAE Rev.3

### Os 22 Setores Económicos (A-U):

| Código | Descrição |
|--------|-----------|
| **A** | Agricultura, produção animal, caça, floresta e pesca |
| **B** | Indústrias extractivas |
| **C** | Indústrias transformadoras |
| **D** | Electricidade, gás, vapor, água quente e fria e ar frio |
| **E** | Captação, tratamento e distribuição de água; saneamento, gestão de resíduos |
| **F** | Construção |
| **G** | Comércio por grosso e a retalho; reparação de veículos |
| **H** | Transportes e armazenagem |
| **I** | Alojamento, restauração e similares |
| **J** | Actividades de informação e de comunicação |
| **K** | Actividades financeiras e de seguros |
| **L** | Actividades imobiliárias |
| **M** | Actividades de consultoria, científicas, técnicas e similares |
| **N** | Actividades administrativas e dos serviços de apoio |
| **O** | Administração Pública e Defesa; Segurança Social Obrigatória |
| **P** | Educação |
| **Q** | Actividades de saúde humana e apoio social |
| **R** | Actividades artísticas, de espectáculos, desportivas e recreativas |
| **S** | Outras actividades de serviços |
| **T** | Atividades das famílias empregadoras de pessoal doméstico |
| **U** | Actividades dos organismos internacionais e instituições extra-territoriais |

---

## 🔗 Referências e Fontes

### Dados Originais
- **INE - Instituto Nacional de Estatística**
- **Censos da População e Habitação 2021**
- Website: https://www.ine.pt
- Portal Censos: https://censos.ine.pt

### Classificação CAE
- **CAE Rev.3** - Classificação Portuguesa das Actividades Económicas
- Baseado na NACE Rev.2 (Nomenclatura Europeia)
- Documentação: https://www.ine.pt/cae

### Documentação do Projeto
- `documentacaoetl.md` - Metodologia ETL completa
- `diagrama-er-completo-laboral.mermaid` - Modelo de dados

---

## 📝 Notas Importantes

### Definição de "Imigrantes"
- Utiliza "Nacionalidade estrangeira" (ID 11) dos Censos 2021
- Inclui todos os cidadãos não-portugueses residentes em Portugal
- Não distingue países de origem específicos nesta análise agregada

### Cobertura Temporal
- Dados referentes a **2021** (ano censitário)
- Momento específico da recolha dos Censos

### Limitações
- Não inclui trabalhadores informais não registados
- Não distingue entre diferentes países de origem dos imigrantes
- Setores agregados (AGR, IND, CON, COM, FIN, SER) não são processados

---

## 🆘 Suporte e Troubleshooting

### Problemas Comuns

**1. Erro de Upload de Arquivos**
- Verificar nomes exatos dos arquivos
- Confirmar encoding UTF-8
- Validar estrutura de colunas

**2. Dados Incorretos**
- Confirmar versão mais recente dos CSV
- Verificar integridade dos dados de origem
- Consultar logs de processamento

**3. Erro de Download**
- Permitir downloads no navegador
- Autorizar downloads múltiplos do Colab
- Verificar espaço em disco

### Para Mais Ajuda

Consulte:
- `INSTRUCOES_COLAB.md` - Guia detalhado de uso
- `documentacaoetl.md` - Documentação técnica ETL
- Logs do script durante execução

---

## 📊 Estatísticas Esperadas

Com base nos Censos 2021, o dataset final deverá apresentar:

- **~4,2 milhões** de empregados portugueses
- **~264 mil** empregados estrangeiros
- **44 registros** totais (22 setores × 2 nacionalidades)
- **Percentuais somam 100%** para cada nacionalidade

---

## 🔄 Histórico de Versões

### Versão 1.0 (Dezembro 2024)
- ✅ Script completo para Google Colab
- ✅ Processamento de 22 setores CAE Rev.3
- ✅ Cálculo de métricas comparativas
- ✅ Download automático de resultados
- ✅ Documentação completa

---

## 📧 Informações de Contato

Para questões sobre:
- **Dados originais:** INE - www.ine.pt
- **Metodologia CAE:** Documentação INE CAE Rev.3
- **Script e processamento:** Consultar documentação do projeto

---

## ✅ Checklist de Uso Rápido

- [ ] Reunir 3 arquivos CSV necessários
- [ ] Abrir Google Colab
- [ ] Copiar script `distribuicao_setorial_colab.py`
- [ ] Executar e fazer upload dos arquivos quando solicitado
- [ ] Aguardar processamento (1-2 min)
- [ ] Verificar download dos 2 arquivos de resultado
- [ ] Consultar `README_distribuicao_setorial.md` para interpretar dados

---

**Desenvolvido para:** Concurso de Pesquisa Prepara Portugal 2025  
**Última Atualização:** Dezembro 2024  
**Versão:** 1.0  

---

🎯 **Pronto para processar seus dados!** Siga as instruções em `INSTRUCOES_COLAB.md` para começar.
