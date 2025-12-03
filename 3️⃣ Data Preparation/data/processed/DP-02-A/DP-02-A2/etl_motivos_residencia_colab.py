"""
ETL - Motivos de Concessão de Títulos de Residência (2020-2024)
Google Colab - Script Modular
Concurso Prepara Portugal 2025

Objetivo: Consolidar dados históricos de motivos de concessão de títulos de residência
como complemento aos dados censitários, permitindo análise temporal das razões
de imigração em Portugal com categorias padronizadas e métricas percentuais.

"""

# ============================================================================
# PARTE 1 - CONFIGURAÇÃO E IMPORTAÇÕES
# ============================================================================
print("="*70)
print("ETL - MOTIVOS DE CONCESSÃO DE TÍTULOS DE RESIDÊNCIA (2020-2024)")
print("="*70)
print("\n📦 Instalando e importando bibliotecas...\n")

# Instalação silenciosa
!pip install pandas numpy -q

# Importações
import pandas as pd
import numpy as np
from google.colab import files
import io
import warnings
warnings.filterwarnings('ignore')

# Configuração de display do pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', None)

print("✅ Bibliotecas carregadas com sucesso!")
print("\n📂 Criando estrutura de diretórios...\n")

# Criar estrutura de diretórios
import os
os.makedirs('/content/data/processed', exist_ok=True)

print("✅ Diretório '/content/data/processed/' criado!")


# ============================================================================
# PARTE 2 - FUNÇÃO DE UPLOAD DE ARQUIVOS
# ============================================================================
print("\n" + "="*70)
print("PARTE 2: FUNÇÃO DE UPLOAD DE ARQUIVOS")
print("="*70 + "\n")

def solicitar_arquivo_csv(nome_arquivo, descricao=""):
    """
    Solicita upload individual de arquivo CSV com nome específico.
    
    Args:
        nome_arquivo (str): Nome esperado do arquivo CSV
        descricao (str): Descrição opcional do arquivo
        
    Returns:
        pd.DataFrame: DataFrame pandas com os dados do arquivo
        
    Raises:
        ValueError: Se o upload falhar ou arquivo inválido
    """
    print(f"📤 Por favor, faça upload do arquivo: {nome_arquivo}.csv")
    if descricao:
        print(f"   Descrição: {descricao}")
    print(f"   {'-'*60}")
    
    try:
        # Solicitar upload
        uploaded = files.upload()
        
        # Validar upload
        if not uploaded:
            raise ValueError("❌ Nenhum arquivo foi carregado!")
        
        # Obter primeiro arquivo carregado
        arquivo_carregado = list(uploaded.keys())[0]
        conteudo = uploaded[arquivo_carregado]
        
        # Ler CSV
        df = pd.read_csv(io.BytesIO(conteudo))
        
        print(f"   ✅ Arquivo '{arquivo_carregado}' carregado com sucesso!")
        print(f"   📊 Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas\n")
        
        return df
        
    except Exception as e:
        print(f"   ❌ ERRO ao carregar arquivo: {str(e)}\n")
        raise

print("✅ Função solicitar_arquivo_csv() definida!")


# ============================================================================
# PARTE 3 - CARREGAMENTO DE DADOS
# ============================================================================
print("\n" + "="*70)
print("PARTE 3: CARREGAMENTO DE DADOS")
print("="*70 + "\n")

# Dicionário para armazenar DataFrames
dados_anos = {}

# Lista de anos a processar
ANOS = [2020, 2021, 2022, 2023, 2024]

print("📋 Serão solicitados arquivos CSV para os anos: 2020-2024")
print("   Formato esperado: ConcessoesPorMotivoNacionalidade_AAAA.csv\n")

# Carregar dados de cada ano
for ano in ANOS:
    print(f"\n{'─'*70}")
    print(f"📅 ANO {ano}")
    print(f"{'─'*70}\n")
    
    # Solicitar arquivo do ano
    nome_arquivo = f"ConcessoesPorMotivoNacionalidade_{ano}"
    descricao = f"Dados de motivos de concessão para o ano {ano}"
    
    try:
        df = solicitar_arquivo_csv(nome_arquivo, descricao)
        
        # Exibir primeiras linhas
        print(f"   🔍 Primeiras 3 linhas do arquivo:\n")
        print(df.head(3).to_string(index=False))
        print(f"\n   📋 Colunas disponíveis: {list(df.columns)}\n")
        
        # Armazenar DataFrame
        dados_anos[ano] = df
        print(f"   ✅ Dados de {ano} armazenados com sucesso!\n")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao carregar dados de {ano}: {str(e)}")
        print(f"   Continuando sem este ano...\n")

print(f"\n{'='*70}")
print(f"✅ CARREGAMENTO CONCLUÍDO!")
print(f"   Total de anos carregados: {len(dados_anos)}/{len(ANOS)}")
print(f"={'*70}\n")


# ============================================================================
# PARTE 4 - PADRONIZAÇÃO DE COLUNAS
# ============================================================================
print("\n" + "="*70)
print("PARTE 4: PADRONIZAÇÃO DE COLUNAS")
print("="*70 + "\n")

def padronizar_colunas_motivos(df, ano):
    """
    Padroniza nomes de colunas relacionadas a motivos de residência.
    
    Args:
        df (pd.DataFrame): DataFrame com dados brutos
        ano (int): Ano de referência dos dados
        
    Returns:
        pd.DataFrame: DataFrame com colunas padronizadas
    """
    # Copiar DataFrame
    df_padronizado = df.copy()
    
    # Dicionário de mapeamento de colunas
    mapeamento_colunas = {
        # Variações de motivo
        'motivo_raw': 'motivo',
        'Motivo': 'motivo',
        'MOTIVO': 'motivo',
        'motivo_concessao': 'motivo',
        
        # Variações de nacionalidade
        'nacionalidade_aima_raw': 'nacionalidade',
        'Nacionalidade': 'nacionalidade',
        'NACIONALIDADE': 'nacionalidade',
        'pais': 'nacionalidade',
        'país': 'nacionalidade',
        
        # Variações de total
        'total_motivo': 'total',
        'Total': 'total',
        'TOTAL': 'total',
        'quantidade': 'total',
        'concessoes': 'total'
    }
    
    # Aplicar mapeamento
    df_padronizado.rename(columns=mapeamento_colunas, inplace=True)
    
    # Garantir colunas mínimas necessárias
    colunas_obrigatorias = ['motivo', 'total']
    colunas_faltantes = [col for col in colunas_obrigatorias if col not in df_padronizado.columns]
    
    if colunas_faltantes:
        raise ValueError(f"Colunas obrigatórias faltando: {colunas_faltantes}")
    
    # Adicionar coluna de ano se não existir
    if 'ano' not in df_padronizado.columns:
        df_padronizado['ano'] = ano
    
    # Limpar espaços em branco
    df_padronizado.columns = df_padronizado.columns.str.strip()
    if 'motivo' in df_padronizado.columns:
        df_padronizado['motivo'] = df_padronizado['motivo'].str.strip()
    
    print(f"   ✅ Ano {ano}: {len(df_padronizado)} registros padronizados")
    print(f"      Colunas: {list(df_padronizado.columns)}")
    
    return df_padronizado

# Aplicar padronização a todos os DataFrames
print("🔄 Padronizando colunas de todos os anos...\n")

dados_padronizados = {}

for ano, df in dados_anos.items():
    try:
        df_padronizado = padronizar_colunas_motivos(df, ano)
        dados_padronizados[ano] = df_padronizado
        
    except Exception as e:
        print(f"   ❌ Erro ao padronizar {ano}: {str(e)}")

print(f"\n{'='*70}")
print(f"✅ PADRONIZAÇÃO CONCLUÍDA!")
print(f"   Anos padronizados: {len(dados_padronizados)}")
print(f"={'*70}\n")


# ============================================================================
# PARTE 5 - MAPEAMENTO DE CATEGORIAS
# ============================================================================
print("\n" + "="*70)
print("PARTE 5: MAPEAMENTO DE CATEGORIAS")
print("="*70 + "\n")

# Dicionário de mapeamento para as 4 categorias principais
MAPEAMENTO_MOTIVOS = {
    # Atividade Profissional
    'Atividade Profissional': 'Atividade Profissional',
    'Atividade Profissional (%)': 'Atividade Profissional',
    'atividade profissional': 'Atividade Profissional',
    'Trabalho': 'Atividade Profissional',
    'trabalho': 'Atividade Profissional',
    
    # Estudo
    'Estudo': 'Estudo',
    'Estudo (%)': 'Estudo',
    'estudo': 'Estudo',
    'Educação': 'Estudo',
    'educação': 'Estudo',
    
    # Reagrupamento Familiar
    'Reagrupamento Familiar': 'Reagrupamento Familiar',
    'Reagrupamento Familiar (%)': 'Reagrupamento Familiar',
    'reagrupamento familiar': 'Reagrupamento Familiar',
    'Família': 'Reagrupamento Familiar',
    'família': 'Reagrupamento Familiar',
    
    # AR CPLP
    'CRs': 'AR CPLP',
    'CRs (%)': 'AR CPLP',
    'Certificado de Residência': 'AR CPLP',
    'Acordo CPLP': 'AR CPLP',
    'acordo cplp': 'AR CPLP',
    'AR CPLP': 'AR CPLP'
}

def aplicar_mapeamento_categorias(df, mapeamento):
    """
    Aplica mapeamento de categorias aos dados de motivos.
    
    Args:
        df (pd.DataFrame): DataFrame com coluna 'motivo'
        mapeamento (dict): Dicionário de mapeamento
        
    Returns:
        pd.DataFrame: DataFrame com categoria mapeada
    """
    df_mapeado = df.copy()
    
    # Aplicar mapeamento
    df_mapeado['categoria'] = df_mapeado['motivo'].map(mapeamento)
    
    # Tratar não mapeados como "Outros"
    df_mapeado['categoria'].fillna('Outros', inplace=True)
    
    # Estatísticas de mapeamento
    total = len(df_mapeado)
    mapeados = (df_mapeado['categoria'] != 'Outros').sum()
    outros = (df_mapeado['categoria'] == 'Outros').sum()
    
    print(f"      Total de registros: {total}")
    print(f"      Mapeados: {mapeados} ({mapeados/total*100:.1f}%)")
    print(f"      Outros: {outros} ({outros/total*100:.1f}%)")
    
    return df_mapeado

print("📋 Categorias definidas:")
categorias_unicas = set(MAPEAMENTO_MOTIVOS.values())
for i, cat in enumerate(sorted(categorias_unicas), 1):
    motivos_grupo = [k for k, v in MAPEAMENTO_MOTIVOS.items() if v == cat]
    print(f"   {i}. {cat}")
    print(f"      Variações: {', '.join(motivos_grupo[:3])}...")

print(f"\n🔄 Aplicando mapeamento de categorias...\n")

dados_categorizados = {}

for ano, df in dados_padronizados.items():
    print(f"   📅 Ano {ano}:")
    df_categorizado = aplicar_mapeamento_categorias(df, MAPEAMENTO_MOTIVOS)
    dados_categorizados[ano] = df_categorizado
    print()

print(f"{'='*70}")
print(f"✅ MAPEAMENTO DE CATEGORIAS CONCLUÍDO!")
print(f"={'*70}\n")


# ============================================================================
# PARTE 6 - CONSOLIDAÇÃO TEMPORAL
# ============================================================================
print("\n" + "="*70)
print("PARTE 6: CONSOLIDAÇÃO TEMPORAL")
print("="*70 + "\n")

print("🔄 Consolidando dados de todos os anos...\n")

# Concatenar todos os DataFrames
df_consolidado = pd.concat(dados_categorizados.values(), ignore_index=True)

print(f"   ✅ Dados consolidados:")
print(f"      Total de registros: {len(df_consolidado)}")
print(f"      Anos cobertos: {sorted(df_consolidado['ano'].unique())}")

# Agrupar por Ano e Categoria
print(f"\n🔄 Agrupando dados por Ano e Categoria...\n")

df_agrupado = df_consolidado.groupby(['ano', 'categoria']).agg({
    'total': 'sum'
}).reset_index()

df_agrupado.rename(columns={'total': 'total_concessoes'}, inplace=True)

print(f"   ✅ Agrupamento concluído:")
print(f"      Total de grupos: {len(df_agrupado)}")
print(f"\n   📊 Amostra dos dados agrupados:\n")
print(df_agrupado.head(10).to_string(index=False))

print(f"\n{'='*70}")
print(f"✅ CONSOLIDAÇÃO TEMPORAL CONCLUÍDA!")
print(f"={'*70}\n")


# ============================================================================
# PARTE 7 - CÁLCULO DE PERCENTAGENS
# ============================================================================
print("\n" + "="*70)
print("PARTE 7: CÁLCULO DE PERCENTAGENS")
print("="*70 + "\n")

print("🔄 Calculando percentagens anuais...\n")

# Calcular total por ano
totais_anuais = df_agrupado.groupby('ano')['total_concessoes'].sum().reset_index()
totais_anuais.rename(columns={'total_concessoes': 'total_ano'}, inplace=True)

print("   📊 Totais por ano:")
for _, row in totais_anuais.iterrows():
    print(f"      {row['ano']}: {row['total_ano']:,} concessões")

# Merge para adicionar total anual
df_com_percentagens = df_agrupado.merge(totais_anuais, on='ano', how='left')

# Calcular percentagem
df_com_percentagens['percentagem'] = (
    df_com_percentagens['total_concessoes'] / df_com_percentagens['total_ano'] * 100
).round(2)

# Remover coluna auxiliar
df_final = df_com_percentagens[['ano', 'categoria', 'total_concessoes', 'percentagem']].copy()

# Ordenar por Ano (crescente) e Percentagem (decrescente)
df_final = df_final.sort_values(['ano', 'percentagem'], ascending=[True, False])

print(f"\n   ✅ Percentagens calculadas!")
print(f"\n   📊 Amostra dos dados finais:\n")
print(df_final.head(15).to_string(index=False))

print(f"\n{'='*70}")
print(f"✅ CÁLCULO DE PERCENTAGENS CONCLUÍDO!")
print(f"={'*70}\n")


# ============================================================================
# PARTE 8 - GERAÇÃO DE SAÍDA
# ============================================================================
print("\n" + "="*70)
print("PARTE 8: GERAÇÃO DE SAÍDA")
print("="*70 + "\n")

# Definir caminho de saída
ARQUIVO_SAIDA = '/content/data/processed/motivos_residencia_2020_2024.csv'

print(f"💾 Salvando dados finais em: {ARQUIVO_SAIDA}\n")

# Salvar CSV
df_final.to_csv(ARQUIVO_SAIDA, index=False, encoding='utf-8')

print(f"   ✅ Arquivo salvo com sucesso!")
print(f"   📊 Dimensões: {df_final.shape[0]} registros x {df_final.shape[1]} colunas")

# Preview do arquivo final
print(f"\n   🔍 PREVIEW - Primeiras 10 linhas:\n")
print(df_final.head(10).to_string(index=False))

print(f"\n   🔍 PREVIEW - Últimas 10 linhas:\n")
print(df_final.tail(10).to_string(index=False))

print(f"\n{'='*70}")
print(f"✅ GERAÇÃO DE SAÍDA CONCLUÍDA!")
print(f"={'*70}\n")


# ============================================================================
# PARTE 9 - DOWNLOAD
# ============================================================================
print("\n" + "="*70)
print("PARTE 9: DOWNLOAD DO ARQUIVO FINAL")
print("="*70 + "\n")

print(f"📥 Preparando download de: {ARQUIVO_SAIDA}\n")

try:
    # Download do arquivo
    files.download(ARQUIVO_SAIDA)
    
    print(f"   ✅ Download iniciado com sucesso!")
    print(f"   📂 Arquivo: motivos_residencia_2020_2024.csv")
    print(f"   📍 Localização: /content/data/processed/")
    
except Exception as e:
    print(f"   ❌ Erro ao fazer download: {str(e)}")

# Estatísticas finais
print(f"\n{'='*70}")
print(f"📊 ESTATÍSTICAS FINAIS")
print(f"{'='*70}\n")

print(f"   Total de registros processados: {len(df_final):,}")
print(f"   Anos cobertos: {df_final['ano'].min()} - {df_final['ano'].max()}")
print(f"   Categorias identificadas: {df_final['categoria'].nunique()}")

print(f"\n   📋 Distribuição por categoria:\n")
for categoria, count in df_final['categoria'].value_counts().items():
    percentual = count / len(df_final) * 100
    print(f"      • {categoria}: {count} registros ({percentual:.1f}%)")

print(f"\n{'='*70}")
print(f"🎉 ETL CONCLUÍDO COM SUCESSO!")
print(f"{'='*70}\n")
print("✨ Dados consolidados e prontos para análise!")
print("📊 Arquivo disponível para download: motivos_residencia_2020_2024.csv\n")
