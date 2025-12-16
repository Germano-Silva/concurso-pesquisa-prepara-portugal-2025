# 📦 Arquivos Legados - ETL Educação

Esta pasta contém versões **antigas/obsoletas** dos scripts ETL mantidas apenas para **histórico e referência**.

## ⚠️ NÃO USE ESTES ARQUIVOS

Os scripts nesta pasta foram **substituídos** pela versão consolidada:
- **Use**: `ETL_EDUCACAO_CONSOLIDADO_v3.py` (na pasta pai)
- **Execute**: `executar_consolidacao.bat` (na pasta pai)

## 📁 Arquivos Arquivados

### **executar_etl.bat** (Obsoleto)
- **Data**: Dezembro 2025
- **Função**: Script batch simples que executava apenas ETL de 2011
- **Substituído por**: `executar_consolidacao.bat`
- **Motivo**: Não consolida dados 2011+2021

### **ETL_EDUCACAO_HIBRIDO.py** (Obsoleto)
- **Versão**: 2.1-HIBRIDO-WIN
- **Data**: Dezembro 2025
- **Função**: Processava apenas dados de 2011 (12 nacionalidades)
- **Substituído por**: `ETL_EDUCACAO_CONSOLIDADO_v3.py`
- **Limitações**:
  - Processava APENAS dados de 2011
  - Gerava apenas 3 tabelas de dimensão
  - Não consolidava com dados de 2021
  - Estrutura mais simples

## ✅ Versão Atual (Ativa)

**Localização**: `../ETL_EDUCACAO_CONSOLIDADO_v3.py`

**Características**:
- ✅ Consolida dados 2011 + 2021
- ✅ Harmoniza 12 nacionalidades de 2011 → 19 de 2021
- ✅ Gera Star Schema completo
- ✅ Campo `ano_referencia` para análise temporal
- ✅ Compatible com Diagrama ER Unificado

## 📊 Evolução do Projeto

```
v1.0 → ETL básico apenas 2011
  ↓
v2.1 → ETL_EDUCACAO_HIBRIDO.py (este arquivo)
  ↓
v3.0 → ETL_EDUCACAO_CONSOLIDADO_v3.py ⭐ (ATUAL)
```

## 🗑️ Por que não foi deletado?

1. **Histórico do projeto** - Evidência da evolução
2. **Documentação para relatório** - Mostra progresso
3. **Backup de segurança** - Caso necessário rollback temporário
4. **Referência técnica** - Comparação de abordagens

---

**Data de Arquivamento**: 16/12/2025  
**Projeto**: Concurso Pesquisa Prepara Portugal 2025  
**Status**: ARQUIVADO - NÃO USAR
