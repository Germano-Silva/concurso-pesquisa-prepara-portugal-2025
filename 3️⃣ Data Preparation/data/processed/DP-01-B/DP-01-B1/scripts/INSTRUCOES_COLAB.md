# 📘 Instruções de Uso - Google Colab
## Processamento de Distribuição Setorial por Nacionalidade

---

## 🎯 Objetivo

Este guia explica como executar o script de processamento de dados setoriais no Google Colab, que gera análises comparativas da distribuição de portugueses e imigrantes pelos 22 setores económicos (CAE Rev.3).

---

## 📋 Pré-requisitos

### Arquivos Necessários para Upload:

Você precisará de **3 arquivos CSV** localizados no diretório:  
`3️⃣ Data Preparation/data/processed/DP-01-B/DP-01-B1/resultados_etl_laboral/`

**Lista dos arquivos:**

1. ✅ **EmpregadosPorSetor.csv**
   - Caminho completo: `resultados_etl_laboral/EmpregadosPorSetor.csv`
   - Conteúdo: Dados de empregados por setor e nacionalidade

2. ✅ **SetorEconomico.csv**
   - Caminho completo: `resultados_etl_laboral/SetorEconomico.csv`
   - Conteúdo: Classificação CAE Rev.3 (22 setores A-U)

3. ✅ **Nacionalidade.csv**
   - Caminho completo: `3️⃣ Data Preparation/data/processed/DP-01-A/Nacionalidade.csv`
   - Conteúdo: Mapeamento de IDs de nacionalidades

---

## 🚀 Passo a Passo no Google Colab

### **Passo 1: Aceder ao Google Colab**

1. Abra o navegador e aceda a: [https://colab.research.google.com](https://colab.research.google.com)
2. Faça login com a sua conta Google
3. Clique em **"Novo Notebook"** ou **"New Notebook"**

### **Passo 2: Upload do Script**

1. No menu superior do Colab, clique em **"Ficheiro" → "Enviar notebook"** (ou **"File" → "Upload notebook"**)
2. OU: Crie uma nova célula de código e copie todo o conteúdo do arquivo:
   - `distribuicao_setorial_colab.py`

### **Passo 3: Executar o Script**

1. Clique no botão ▶️ (play) à esquerda da célula de código
2. O script iniciará automaticamente

### **Passo 4: Upload dos Arquivos CSV**

O script solicitará o upload de cada arquivo sequencialmente:

**Upload 1:**
```
Por favor, faça upload do arquivo: EmpregadosPorSetor.csv
```
- Clique em **"Escolher arquivos"** ou **"Choose Files"**
- Navegue até o diretório: `resultados_etl_laboral/`
- Selecione: **EmpregadosPorSetor.csv**
- Aguarde a confirmação: `✓ Arquivo carregado`

**Upload 2:**
```
Por favor, faça upload do arquivo: SetorEconomico.csv
```
- Clique em **"Escolher arquivos"**
- Selecione: **SetorEconomico.csv**
- Aguarde a confirmação: `✓ Arquivo carregado`

**Upload 3:**
```
Por favor, faça upload do arquivo: Nacionalidade.csv
```
- Clique em **"Escolher arquivos"**
- Navegue até: `3️⃣ Data Preparation/data/processed/DP-01-A/`
- Selecione: **Nacionalidade.csv**
- Aguarde a confirmação: `✓ Arquivo carregado`

### **Passo 5: Processamento Automático**

Após o upload dos 3 arquivos, o script executará automaticamente todas as etapas:

1. ✅ Carregamento e preparação dos dados
2. ✅ Mapeamento de nacionalidades
3. ✅ Processamento setorial
4. ✅ Cobertura completa dos 22 setores
5. ✅ Cálculo de métricas e percentagens
6. ✅ Análise de concentração setorial
7. ✅ Preparação do dataset final
8. ✅ Exportação dos resultados
9. ✅ Download automático

### **Passo 6: Download dos Resultados**

O script fará download automático de **2 arquivos** para o seu computador:

1. 📊 **distribuicao_setorial_nacionalidade.csv**
   - Dataset principal com análise completa
   - 44 registros (22 setores × 2 nacionalidades)
   - Colunas: codigo_cae, setor_economico, nacionalidade, num_empregados, percentual_da_nacionalidade, percentual_do_setor

2. 📄 **README_distribuicao_setorial.md**
   - Documentação completa do dataset
   - Metodologia e casos de uso
   - Principais insights e estatísticas

---

## 📊 Saída Esperada

### Durante a Execução:

O script exibirá informações detalhadas sobre cada etapa:

```
============================================================
PROCESSAMENTO DE DISTRIBUIÇÃO SETORIAL POR NACIONALIDADE
CAE Rev.3 - Censos 2021 Portugal
============================================================

📁 ETAPA 1: Upload dos Arquivos de Entrada
------------------------------------------------------------
✓ Arquivo carregado: EmpregadosPorSetor.csv
✓ Arquivo carregado: SetorEconomico.csv
✓ Arquivo carregado: Nacionalidade.csv

📊 ETAPA 2: Carregamento e Preparação dos Dados
------------------------------------------------------------
✓ Empregados por Setor: 391 registros
✓ Setores Econômicos: 27 setores
✓ Nacionalidades: 19 nacionalidades
✓ Setores CAE Rev.3 (A-U): 21 setores

🗺️  ETAPA 3: Mapeamento de Nacionalidades
------------------------------------------------------------
✓ ID Nacionalidade Portuguesa: 12
✓ ID Nacionalidade Estrangeira: 11

[... mais etapas ...]

📈 TOP 5 SETORES POR NACIONALIDADE:

Portuguesa:
  G. Comércio por grosso e a retalho; reparação de veícu... - 16.23%
  C. Indústrias transformadoras... - 15.99%
  [...]

🌍 SETORES COM MAIOR CONCENTRAÇÃO DE IMIGRANTES:
  [...]

✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!
```

### Arquivos Baixados:

Os arquivos serão baixados automaticamente para a pasta de **Downloads** do seu navegador.

---

## 🔧 Estrutura do Dataset Final

### Arquivo: `distribuicao_setorial_nacionalidade.csv`

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| **codigo_cae** | string | Código do setor (A-U) | "G" |
| **setor_economico** | string | Descrição completa do setor | "Comércio por grosso e a retalho..." |
| **nacionalidade** | string | Portuguesa ou Estrangeira | "Portuguesa" |
| **num_empregados** | int | Número absoluto de empregados | 674229 |
| **percentual_da_nacionalidade** | float | % do setor na nacionalidade | 16.23 |
| **percentual_do_setor** | float | % da nacionalidade no setor | 94.82 |

**Total de Registros:** 44 (22 setores × 2 nacionalidades)

---

## ⚠️ Resolução de Problemas

### Erro: "Arquivo não encontrado"
**Solução:** Certifique-se de que está fazendo upload do arquivo correto com o nome exato.

### Erro: "ModuleNotFoundError"
**Solução:** O Google Colab já inclui pandas e numpy. Não é necessário instalar nada.

### O download não iniciou
**Solução:** 
- Verifique se o navegador está bloqueando downloads
- Permita downloads múltiplos do site colab.research.google.com

### Dados incorretos ou zeros
**Solução:**
- Verifique se os arquivos CSV foram carregados corretamente
- Confirme que os arquivos são os mais recentes da pasta `resultados_etl_laboral/`

---

## 📧 Suporte

Para mais informações sobre:
- **CAE Rev.3:** Consulte a documentação do INE
- **Censos 2021:** https://censos.ine.pt
- **Metodologia:** Veja o arquivo `documentacaoetl.md` no diretório DP-01-B1

---

## ✅ Checklist de Execução

Antes de executar, confirme:

- [ ] Tenho acesso aos 3 arquivos CSV necessários
- [ ] Estou logado no Google Colab
- [ ] Copiei o script `distribuicao_setorial_colab.py` para uma célula
- [ ] Li as instruções de upload
- [ ] Tenho permissão para downloads no navegador

Durante a execução:

- [ ] Upload do arquivo 1: EmpregadosPorSetor.csv
- [ ] Upload do arquivo 2: SetorEconomico.csv
- [ ] Upload do arquivo 3: Nacionalidade.csv
- [ ] Aguardar o processamento completo
- [ ] Verificar download dos 2 arquivos de resultado

---

**Última Atualização:** Dezembro 2024  
**Versão do Script:** 1.0  
**Compatibilidade:** Google Colab (Python 3.10+)

---

🎯 **Pronto para começar!** Execute o script no Google Colab e os arquivos serão baixados automaticamente para o seu computador.
