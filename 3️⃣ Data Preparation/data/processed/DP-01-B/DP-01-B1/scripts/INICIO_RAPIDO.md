# 🚀 INÍCIO RÁPIDO
## Processamento de Distribuição Setorial - Google Colab

---

## ⚡ Execução em 5 Passos

### 1️⃣ Preparar Arquivos (2 min)

Localize e tenha prontos estes **3 arquivos CSV**:

```
✅ EmpregadosPorSetor.csv
   📁 Localização: resultados_etl_laboral/EmpregadosPorSetor.csv

✅ SetorEconomico.csv  
   📁 Localização: resultados_etl_laboral/SetorEconomico.csv

✅ Nacionalidade.csv
   📁 Localização: 3️⃣ Data Preparation/data/processed/DP-01-A/Nacionalidade.csv
```

---

### 2️⃣ Abrir Google Colab

1. Acesse: **https://colab.research.google.com**
2. Clique em **"Novo Notebook"** (New Notebook)

---

### 3️⃣ Copiar Script

1. Abra o arquivo: **`distribuicao_setorial_colab.py`** (neste diretório)
2. **Copie TODO o conteúdo** (Ctrl+A, Ctrl+C)
3. **Cole** na célula do Colab (Ctrl+V)

---

### 4️⃣ Executar e Fazer Upload

1. Clique no botão **▶️ (play)** à esquerda da célula
2. O script solicitará upload de cada arquivo:
   - Upload do **EmpregadosPorSetor.csv** → Aguarde confirmação
   - Upload do **SetorEconomico.csv** → Aguarde confirmação
   - Upload do **Nacionalidade.csv** → Aguarde confirmação

---

### 5️⃣ Aguardar Download Automático

- Processamento: **~1-2 minutos**
- Download automático de **2 arquivos**:
  - ✅ `distribuicao_setorial_nacionalidade.csv` (dataset principal)
  - ✅ `README_distribuicao_setorial.md` (documentação)

---

## 💾 Onde Estão os Resultados?

Os arquivos são baixados automaticamente para:
- **Windows:** `C:\Users\[seu_usuario]\Downloads\`
- **Mac:** `/Users/[seu_usuario]/Downloads/`
- **Linux:** `/home/[seu_usuario]/Downloads/`

---

## 📊 O Que Você Terá?

### Arquivo Principal: `distribuicao_setorial_nacionalidade.csv`

Um dataset com **44 linhas** contendo:

| Informação | Detalhes |
|------------|----------|
| **Setores CAE Rev.3** | Todos os 22 setores (A até U) |
| **Nacionalidades** | Portuguesa + Estrangeira (Imigrantes) |
| **Métricas** | Número de empregados + Percentuais comparativos |

**Exemplo de dados:**
```
Setor G (Comércio): 
  - Portugueses: 674,229 (16.23% do total português)
  - Estrangeiros: 36,794 (13.93% do total estrangeiro)
```

---

## 🎯 Para Que Serve?

✅ Comparar distribuição de portugueses vs. imigrantes por setor  
✅ Identificar setores com maior concentração de imigrantes  
✅ Analisar padrões de inserção laboral  
✅ Fundamentar políticas de integração  
✅ Pesquisa académica sobre mercado de trabalho  

---

## ❓ Precisa de Mais Detalhes?

### Documentação Completa:
- **`INSTRUCOES_COLAB.md`** → Passo a passo detalhado com screenshots
- **`README.md`** → Documentação técnica completa
- **`distribuicao_setorial_colab.py`** → Código comentado

### Estrutura do Projeto:
```
scripts/
├── 🚀 INICIO_RAPIDO.md (ESTE ARQUIVO - Comece aqui!)
├── 📘 INSTRUCOES_COLAB.md (Guia detalhado)
├── 📚 README.md (Documentação técnica)
└── 🐍 distribuicao_setorial_colab.py (Script principal)
```

---

## ⚠️ Problemas Comuns

| Problema | Solução |
|----------|---------|
| **Erro ao fazer upload** | Verificar nome exato do arquivo |
| **Não baixou nada** | Permitir downloads no navegador |
| **Dados estranhos** | Confirmar arquivos CSV corretos |
| **Erro de módulo** | Não precisa instalar nada, o Colab já tem tudo |

---

## 📞 Precisa de Ajuda?

1. **Problemas técnicos:** Consulte `INSTRUCOES_COLAB.md`
2. **Dúvidas sobre dados:** Veja `README.md`
3. **Interpretação de resultados:** Abra o `README_distribuicao_setorial.md` gerado

---

## ✅ Checklist Rápido

Antes de executar:
- [ ] Tenho os 3 arquivos CSV prontos
- [ ] Abri o Google Colab
- [ ] Copiei o script completo

Durante:
- [ ] Upload: EmpregadosPorSetor.csv ✓
- [ ] Upload: SetorEconomico.csv ✓
- [ ] Upload: Nacionalidade.csv ✓

Depois:
- [ ] Baixei: distribuicao_setorial_nacionalidade.csv ✓
- [ ] Baixei: README_distribuicao_setorial.md ✓
- [ ] Verifiquei os dados ✓

---

## 🎉 Pronto!

**Tempo total:** ~5 minutos  
**Resultado:** Dataset profissional pronto para análise  

---

**Última Atualização:** Dezembro 2024  
**Dificuldade:** ⭐ Fácil (não precisa conhecimento de Python)  

🚀 **Boa sorte com sua análise!**
