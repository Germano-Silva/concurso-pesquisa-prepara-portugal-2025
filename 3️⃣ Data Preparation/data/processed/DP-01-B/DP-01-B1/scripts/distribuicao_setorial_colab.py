"""
============================================================
SCRIPT DE PROCESSAMENTO - DISTRIBUIÇÃO SETORIAL POR NACIONALIDADE
Análise CAE Rev.3 - Censos 2021 Portugal
Para uso no Google Colab
============================================================

Este script processa dados de emprego por setor econômico e nacionalidade,
gerando um dataset consolidado comparando imigrantes e portugueses.

ARQUIVOS NECESSÁRIOS PARA UPLOAD:
1. EmpregadosPorSetor.csv
2. SetorEconomico.csv
3. Nacionalidade.csv

SAÍDA:
- distribuicao_setorial_nacionalidade.csv (download automático)
- README_distribuicao_setorial.md (download automático)
============================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
from google.colab import files
import io

print("="*60)
print("PROCESSAMENTO DE DISTRIBUIÇÃO SETORIAL POR NACIONALIDADE")
print("CAE Rev.3 - Censos 2021 Portugal")
print("="*60)
print()

# ============================================================
# ETAPA 1: UPLOAD DOS ARQUIVOS NECESSÁRIOS
# ============================================================
print("📁 ETAPA 1: Upload dos Arquivos de Entrada")
print("-" * 60)
print()

print("Por favor, faça upload do arquivo: EmpregadosPorSetor.csv")
uploaded_files_1 = files.upload()
empregados_setor_file = list(uploaded_files_1.keys())[0]
print(f"✓ Arquivo carregado: {empregados_setor_file}\n")

print("Por favor, faça upload do arquivo: SetorEconomico.csv")
uploaded_files_2 = files.upload()
setor_economico_file = list(uploaded_files_2.keys())[0]
print(f"✓ Arquivo carregado: {setor_economico_file}\n")

print("Por favor, faça upload do arquivo: Nacionalidade.csv")
uploaded_files_3 = files.upload()
nacionalidade_file = list(uploaded_files_3.keys())[0]
print(f"✓ Arquivo carregado: {nacionalidade_file}\n")

# ============================================================
# ETAPA 2: CARREGAMENTO E PREPARAÇÃO DOS DADOS
# ============================================================
print("📊 ETAPA 2: Carregamento e Preparação dos Dados")
print("-" * 60)

# Carregar os arquivos CSV
df_empregados_setor = pd.read_csv(empregados_setor_file, encoding='utf-8')
df_setores = pd.read_csv(setor_economico_file, encoding='utf-8')
df_nacionalidades = pd.read_csv(nacionalidade_file, encoding='utf-8')

print(f"✓ Empregados por Setor: {len(df_empregados_setor)} registros")
print(f"✓ Setores Econômicos: {len(df_setores)} setores")
print(f"✓ Nacionalidades: {len(df_nacionalidades)} nacionalidades")
print()

# Filtrar apenas setores CAE Rev.3 (A-U), excluindo agregados
df_setores_cae = df_setores[df_setores['agregado'] == False].copy()
print(f"✓ Setores CAE Rev.3 (A-U): {len(df_setores_cae)} setores")
print()

# ============================================================
# ETAPA 3: MAPEAMENTO DE NACIONALIDADES
# ============================================================
print("🗺️  ETAPA 3: Mapeamento de Nacionalidades")
print("-" * 60)

# Identificar IDs relevantes
portuguesa_id = df_nacionalidades[
    df_nacionalidades['nome_nacionalidade'] == 'Nacionalidade portuguesa'
]['nacionalidade_id'].values[0]

estrangeira_id = df_nacionalidades[
    df_nacionalidades['nome_nacionalidade'] == 'Nacionalidade estrangeira'
]['nacionalidade_id'].values[0]

print(f"✓ ID Nacionalidade Portuguesa: {portuguesa_id}")
print(f"✓ ID Nacionalidade Estrangeira: {estrangeira_id}")
print()

# Criar mapeamento de nacionalidade
def mapear_nacionalidade(nac_id):
    if nac_id == portuguesa_id:
        return 'Portuguesa'
    elif nac_id == estrangeira_id:
        return 'Estrangeira (Imigrantes)'
    else:
        return None

# ============================================================
# ETAPA 4: PROCESSAMENTO SETORIAL
# ============================================================
print("⚙️  ETAPA 4: Processamento e Agregação Setorial")
print("-" * 60)

# Filtrar apenas para nacionalidades relevantes
df_filtrado = df_empregados_setor[
    df_empregados_setor['nacionalidade_id'].isin([portuguesa_id, estrangeira_id])
].copy()

# Adicionar mapeamento de nacionalidade
df_filtrado['nacionalidade'] = df_filtrado['nacionalidade_id'].apply(mapear_nacionalidade)

# Fazer merge com setores (apenas CAE Rev.3)
df_processado = df_filtrado.merge(
    df_setores_cae[['setor_id', 'codigo_cae', 'descricao']],
    on='setor_id',
    how='inner'
)

print(f"✓ Registros processados: {len(df_processado)}")
print()

# Agregar por setor e nacionalidade
df_agregado = df_processado.groupby(
    ['codigo_cae', 'descricao', 'nacionalidade'],
    as_index=False
)['quantidade'].sum()

# Renomear colunas
df_agregado.rename(columns={
    'descricao': 'setor_economico',
    'quantidade': 'num_empregados'
}, inplace=True)

print(f"✓ Setores agregados: {len(df_agregado)} registros")
print()

# ============================================================
# ETAPA 5: GARANTIR TODOS OS 22 SETORES CAE REV.3
# ============================================================
print("📋 ETAPA 5: Garantindo Cobertura Completa (22 Setores)")
print("-" * 60)

# Lista completa dos 22 códigos CAE Rev.3
codigos_cae_completos = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                         'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

nacionalidades = ['Portuguesa', 'Estrangeira (Imigrantes)']

# Criar DataFrame completo com todas as combinações
todas_combinacoes = []
for codigo in codigos_cae_completos:
    # Buscar descrição do setor
    setor_info = df_setores_cae[df_setores_cae['codigo_cae'] == codigo]
    if not setor_info.empty:
        descricao = setor_info['descricao'].values[0]
        for nac in nacionalidades:
            todas_combinacoes.append({
                'codigo_cae': codigo,
                'setor_economico': descricao,
                'nacionalidade': nac,
                'num_empregados': 0
            })

df_completo = pd.DataFrame(todas_combinacoes)

# Atualizar com dados reais
for idx, row in df_agregado.iterrows():
    mask = (
        (df_completo['codigo_cae'] == row['codigo_cae']) &
        (df_completo['nacionalidade'] == row['nacionalidade'])
    )
    df_completo.loc[mask, 'num_empregados'] = row['num_empregados']

print(f"✓ Dataset completo com {len(df_completo)} registros")
print(f"✓ Setores únicos: {df_completo['codigo_cae'].nunique()}")
print(f"✓ Nacionalidades: {df_completo['nacionalidade'].nunique()}")
print()

# ============================================================
# ETAPA 6: CÁLCULO DE MÉTRICAS COMPARATIVAS
# ============================================================
print("📊 ETAPA 6: Cálculo de Métricas e Percentagens")
print("-" * 60)

# Calcular totais por nacionalidade
totais_nac = df_completo.groupby('nacionalidade')['num_empregados'].sum()

print("Totais por Nacionalidade:")
for nac, total in totais_nac.items():
    print(f"  • {nac}: {total:,} empregados")
print()

# Calcular totais por setor
totais_setor = df_completo.groupby('codigo_cae')['num_empregados'].sum()

# Adicionar percentuais
df_completo['percentual_da_nacionalidade'] = df_completo.apply(
    lambda row: (row['num_empregados'] / totais_nac[row['nacionalidade']] * 100)
    if totais_nac[row['nacionalidade']] > 0 else 0,
    axis=1
)

df_completo['percentual_do_setor'] = df_completo.apply(
    lambda row: (row['num_empregados'] / totais_setor[row['codigo_cae']] * 100)
    if totais_setor[row['codigo_cae']] > 0 else 0,
    axis=1
)

# Arredondar percentuais
df_completo['percentual_da_nacionalidade'] = df_completo['percentual_da_nacionalidade'].round(2)
df_completo['percentual_do_setor'] = df_completo['percentual_do_setor'].round(2)

print("✓ Métricas calculadas:")
print("  • Percentual da nacionalidade: % de empregados dessa nacionalidade no setor")
print("  • Percentual do setor: % de participação da nacionalidade no setor")
print()

# Verificar soma de percentuais (deve ser ~100% para cada nacionalidade)
soma_percentuais = df_completo.groupby('nacionalidade')['percentual_da_nacionalidade'].sum()
print("Verificação de Consistência:")
for nac, soma in soma_percentuais.items():
    print(f"  • {nac}: {soma:.2f}% (deve ser ~100%)")
print()

# ============================================================
# ETAPA 7: ANÁLISE E INSIGHTS
# ============================================================
print("🔍 ETAPA 7: Análise de Concentração Setorial")
print("-" * 60)

# Top 5 setores para cada nacionalidade
print("\n📈 TOP 5 SETORES POR NACIONALIDADE:\n")

for nac in nacionalidades:
    df_nac = df_completo[df_completo['nacionalidade'] == nac].copy()
    df_nac = df_nac.sort_values('percentual_da_nacionalidade', ascending=False).head(5)
    
    print(f"{nac}:")
    for idx, row in df_nac.iterrows():
        print(f"  {row['codigo_cae']}. {row['setor_economico'][:50]}... - {row['percentual_da_nacionalidade']:.2f}%")
    print()

# Setores com maior concentração de imigrantes
print("\n🌍 SETORES COM MAIOR CONCENTRAÇÃO DE IMIGRANTES:\n")
df_imigrantes = df_completo[df_completo['nacionalidade'] == 'Estrangeira (Imigrantes)'].copy()
df_imigrantes = df_imigrantes.sort_values('percentual_do_setor', ascending=False).head(5)

for idx, row in df_imigrantes.iterrows():
    print(f"  {row['codigo_cae']}. {row['setor_economico'][:50]}... - {row['percentual_do_setor']:.2f}% do setor")
print()

# ============================================================
# ETAPA 8: PREPARAÇÃO DO DATASET FINAL
# ============================================================
print("📦 ETAPA 8: Preparação do Dataset Final")
print("-" * 60)

# Ordenar por código CAE e nacionalidade
df_final = df_completo.sort_values(['codigo_cae', 'nacionalidade']).reset_index(drop=True)

# Reorganizar colunas na ordem desejada
df_final = df_final[[
    'codigo_cae',
    'setor_economico',
    'nacionalidade',
    'num_empregados',
    'percentual_da_nacionalidade',
    'percentual_do_setor'
]]

print(f"✓ Dataset final preparado: {len(df_final)} registros")
print(f"✓ Colunas: {', '.join(df_final.columns)}")
print()

print("Amostra do Dataset Final:")
print(df_final.head(10).to_string(index=False))
print()

# ============================================================
# ETAPA 9: EXPORTAÇÃO DOS RESULTADOS
# ============================================================
print("💾 ETAPA 9: Exportação dos Resultados")
print("-" * 60)

# Salvar CSV
output_filename = 'distribuicao_setorial_nacionalidade.csv'
df_final.to_csv(output_filename, index=False, encoding='utf-8')
print(f"✓ CSV gerado: {output_filename}")

# Gerar README de documentação
readme_content = f"""# Distribuição Setorial por Nacionalidade
## Dataset de Análise - CAE Rev.3

**Data de Geração:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  
**Fonte:** Censos 2021 - INE Portugal  
**Processamento:** Script de Análise Setorial

---

## 📋 Descrição

Este dataset contém a distribuição de empregados portugueses e imigrantes pelos 22 setores de atividade económica segundo a Classificação das Atividades Económicas (CAE Rev.3).

## 📊 Estrutura do Dataset

**Total de Registros:** {len(df_final)}  
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
- **Portugueses:** {totais_nac['Portuguesa']:,} empregados
- **Estrangeiros:** {totais_nac['Estrangeira (Imigrantes)']:,} empregados

### Setores com Maior Concentração de Imigrantes
{chr(10).join([f"{i+1}. Setor {row['codigo_cae']}: {row['percentual_do_setor']:.2f}% imigrantes" 
               for i, (idx, row) in enumerate(df_imigrantes.head(3).iterrows())])}

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
Processado em: {datetime.now().strftime('%d/%m/%Y')}
```

---

## 📧 Informações Adicionais

Para mais informações sobre:
- **CAE Rev.3:** Consulte a documentação oficial do INE
- **Censos 2021:** [censos.ine.pt](https://censos.ine.pt)
- **Metodologia ETL:** Veja documentação do projeto

---

**Última Atualização:** {datetime.now().strftime('%d/%m/%Y às %H:%M')}
"""

readme_filename = 'README_distribuicao_setorial.md'
with open(readme_filename, 'w', encoding='utf-8') as f:
    f.write(readme_content)

print(f"✓ README gerado: {readme_filename}")
print()

# ============================================================
# ETAPA 10: DOWNLOAD AUTOMÁTICO DOS RESULTADOS
# ============================================================
print("⬇️  ETAPA 10: Download dos Resultados")
print("-" * 60)

print(f"\nBaixando arquivo: {output_filename}...")
files.download(output_filename)

print(f"Baixando arquivo: {readme_filename}...")
files.download(readme_filename)

print()
print("="*60)
print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("="*60)
print()
print("📁 Arquivos gerados e baixados:")
print(f"  1. {output_filename}")
print(f"  2. {readme_filename}")
print()
print("📊 Resumo Final:")
print(f"  • Total de Registros: {len(df_final)}")
print(f"  • Setores CAE Rev.3: 22")
print(f"  • Portugueses: {totais_nac['Portuguesa']:,} empregados")
print(f"  • Estrangeiros: {totais_nac['Estrangeira (Imigrantes)']:,} empregados")
print()
print("🎯 Os arquivos foram baixados para o seu computador!")
print("="*60)
