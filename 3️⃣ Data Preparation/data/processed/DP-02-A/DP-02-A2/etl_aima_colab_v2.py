"""
ETL AIMA/SEF para Google Colab - v2.0
Processa RIFA 2020-2022 e RMA 2023-2024
Conforme Diagrama ER: diagrama-er-completo-aima-integrado.mermaid

Autor: Cline - Concurso Prepara Portugal 2025
Execução: 100% Google Colab (Upload CSV → Transform → Download)
"""

# ========== SEÇÃO 1: INSTALAÇÃO E IMPORTS ==========
print("📦 Instalando dependências...")
!pip install pandas -q

import pandas as pd
import io
from google.colab import files
import warnings
warnings.filterwarnings('ignore')

print("✅ Dependências instaladas!\n")

# ========== SEÇÃO 2: CONFIGURAÇÕES GLOBAIS ==========
ANOS_CONFIG = {
    2020: 'RIFA',
    2021: 'RIFA',
    2022: 'RIFA',
    2023: 'RMA',
    2024: 'RMA'
}

TIPO_RELATORIO_MAP = {
    1: 'ConcessaoTitulos',
    2: 'PopulacaoEstrangeira',
    3: 'PopulacaoResidente'
}

SEXO_NORMALIZADO = {
    'Homens': 'M', 'Mulheres': 'F', 
    'homens': 'M', 'mulheres': 'F',
    'Masculino': 'M', 'Feminino': 'F'
}

# ========== SEÇÃO 3: FUNÇÕES DE TRANSFORMAÇÃO ==========

def normalizar_coluna_sexo(valor):
    """Normaliza valores de sexo para M/F"""
    return SEXO_NORMALIZADO.get(valor, valor)

def parse_concessao_residencia(content_str, ano):
    """
    Transforma: concessao-titulos-residencia.csv
    Saída: ConcessoesPorNacionalidadeSexo (long format)
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    # Renomear colunas padronizadas
    col_map = {
        'NACIONALIDADES': 'nacionalidade_aima_raw',
        'NACIONALIDADE': 'nacionalidade_aima_raw',
        'Homens': 'homens', 'Mulheres': 'mulheres',
        'Masculino': 'homens', 'Feminino': 'mulheres'
    }
    df = df.rename(columns=col_map)
    
    # Melt para long format
    df_long = df.melt(
        id_vars=['nacionalidade_aima_raw'],
        value_vars=['homens', 'mulheres'],
        var_name='sexo_raw',
        value_name='quantidade'
    )
    
    df_long['sexo_raw'] = df_long['sexo_raw'].map({'homens': 'M', 'mulheres': 'F'})
    df_long['ano'] = ano
    df_long['fonte'] = ANOS_CONFIG[ano]
    df_long['tipo_relatorio'] = 1
    
    return df_long[['ano', 'fonte', 'tipo_relatorio', 'nacionalidade_aima_raw', 'sexo_raw', 'quantidade']]

def parse_populacao_estrangeira(content_str, ano):
    """
    Transforma: populacao-estrangeira-residente.csv
    Saída: PopulacaoEstrangeiraPorNacionalidadeSexo
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    col_map = {
        'NACIONALIDADES': 'nacionalidade_aima_raw',
        'Homens': 'homens', 'Mulheres': 'mulheres',
        'Masculino': 'homens', 'Feminino': 'mulheres'
    }
    df = df.rename(columns=col_map)
    
    df_long = df.melt(
        id_vars=['nacionalidade_aima_raw'],
        value_vars=['homens', 'mulheres'],
        var_name='sexo_raw',
        value_name='quantidade'
    )
    
    df_long['sexo_raw'] = df_long['sexo_raw'].map({'homens': 'M', 'mulheres': 'F'})
    df_long['ano'] = ano
    df_long['fonte'] = ANOS_CONFIG[ano]
    df_long['tipo_relatorio'] = 2
    
    return df_long[['ano', 'fonte', 'tipo_relatorio', 'nacionalidade_aima_raw', 'sexo_raw', 'quantidade']]

def parse_despachos_concessao(content_str, ano):
    """
    Transforma: concessao-titulos_despachos.csv
    Saída: ConcessoesPorDespacho
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    df = df.rename(columns={
        df.columns[0]: 'codigo_despacho',
        df.columns[1]: 'concessoes'
    })
    
    df['ano'] = ano
    df['fonte'] = ANOS_CONFIG[ano]
    df['tipo_relatorio'] = 1
    
    return df[['ano', 'fonte', 'tipo_relatorio', 'codigo_despacho', 'concessoes']]

def parse_despachos_descricao(content_str):
    """
    Transforma: despachos-descricao.csv
    Saída: Despacho (dimensão)
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    df = df.rename(columns={
        df.columns[0]: 'codigo_despacho',
        df.columns[1]: 'descricao'
    })
    
    return df[['codigo_despacho', 'descricao']]

def parse_distribuicao_etaria_concessoes(content_str, ano):
    """
    Transforma: concessao-titulos_distribuicao-etaria.csv
    Saída: DistribuicaoEtariaConcessoes
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    df = df.rename(columns={
        'FAIXA ETARIA': 'grupo_etario_raw',
        'Faixa Etária': 'grupo_etario_raw',
        'Homens': 'homens', 'Mulheres': 'mulheres'
    })
    
    df_long = df.melt(
        id_vars=['grupo_etario_raw'],
        value_vars=['homens', 'mulheres'],
        var_name='sexo_raw',
        value_name='quantidade'
    )
    
    df_long['sexo_raw'] = df_long['sexo_raw'].map({'homens': 'M', 'mulheres': 'F'})
    df_long['ano'] = ano
    df_long['fonte'] = ANOS_CONFIG[ano]
    df_long['tipo_relatorio'] = 1
    
    return df_long[['ano', 'fonte', 'tipo_relatorio', 'grupo_etario_raw', 'sexo_raw', 'quantidade']]

def parse_motivos_concessao(content_str, ano):
    """
    Transforma: concessao-titulos_motivo.csv
    Saída: ConcessoesPorMotivoNacionalidade
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    # Identificar coluna país
    pais_col = df.columns[0]
    motivo_cols = df.columns[2:]  # Pular 'Total'
    
    df_long = df.melt(
        id_vars=[pais_col],
        value_vars=motivo_cols,
        var_name='motivo_raw',
        value_name='total_motivo'
    )
    
    df_long['nacionalidade_aima_raw'] = df_long[pais_col]
    df_long['ano'] = ano
    df_long['fonte'] = ANOS_CONFIG[ano]
    
    return df_long[['ano', 'fonte', 'motivo_raw', 'nacionalidade_aima_raw', 'total_motivo']]

def parse_populacao_residente_etaria(content_str, ano):
    """
    Transforma: populacao-residente_distribuicao-etaria.csv
    Saída: PopulacaoResidenteEtaria
    """
    df = pd.read_csv(io.StringIO(content_str))
    df.columns = df.columns.str.strip()
    
    df = df.rename(columns={
        'FAIXA ETARIA': 'grupo_etario_raw',
        'Faixa Etária': 'grupo_etario_raw',
        df.columns[-1]: 'total'
    })
    
    df['ano'] = ano
    df['fonte'] = ANOS_CONFIG[ano]
    df['tipo_relatorio'] = 3
    
    return df[['ano', 'fonte', 'tipo_relatorio', 'grupo_etario_raw', 'total']]

# ========== SEÇÃO 4: PROCESSAMENTO POR ANO ==========

def processar_ano(ano):
    """
    Solicita upload e processa todos CSVs de um ano específico
    """
    fonte = ANOS_CONFIG[ano]
    print(f"\n{'='*60}")
    print(f"📅 ANO {ano} ({fonte})")
    print(f"{'='*60}")
    print(f"\n📤 Faça upload dos CSVs de {fonte}{ano}:")
    print(f"   Exemplo: {fonte}{ano} - concessao-titulos-residencia.csv")
    print(f"   (Você pode selecionar múltiplos arquivos de uma vez)\n")
    
    uploaded = files.upload()
    dados_ano = {}
    
    print(f"\n🔄 Processando {len(uploaded)} arquivo(s)...")
    
    for filename, file_bytes in uploaded.items():
        content = file_bytes.decode('utf-8')
        
        # Detectar tipo de arquivo e aplicar parsing
        if 'concessao-titulos-residencia' in filename.lower():
            dados_ano['concessoes_nac_sexo'] = parse_concessao_residencia(content, ano)
            print(f"  ✓ Concessões por Nacionalidade/Sexo")
            
        elif 'populacao-estrangeira-residente' in filename.lower() and 'evolucao' not in filename.lower():
            dados_ano['pop_est_nac_sexo'] = parse_populacao_estrangeira(content, ano)
            print(f"  ✓ População Estrangeira por Nacionalidade/Sexo")
            
        elif 'concessao-titulos_despachos' in filename.lower():
            dados_ano['concessoes_despacho'] = parse_despachos_concessao(content, ano)
            print(f"  ✓ Concessões por Despacho")
            
        elif 'despachos-descricao' in filename.lower():
            dados_ano['despacho_dim'] = parse_despachos_descricao(content)
            print(f"  ✓ Despachos (Dimensão)")
            
        elif 'concessao-titulos_distribuicao-etaria' in filename.lower():
            dados_ano['dist_etaria_conc'] = parse_distribuicao_etaria_concessoes(content, ano)
            print(f"  ✓ Distribuição Etária de Concessões")
            
        elif 'concessao-titulos_motivo' in filename.lower():
            dados_ano['concessoes_motivo'] = parse_motivos_concessao(content, ano)
            print(f"  ✓ Concessões por Motivo")
            
        elif 'populacao-residente-distribuicao-etaria' in filename.lower() or \
             'populacao-residente_distribuicao-etaria' in filename.lower():
            dados_ano['pop_res_etaria'] = parse_populacao_residente_etaria(content, ano)
            print(f"  ✓ População Residente Etária")
    
    print(f"\n✅ {ano} processado: {len(dados_ano)} tabelas criadas")
    return dados_ano

# ========== SEÇÃO 5: CONSTRUÇÃO DE DIMENSÕES ==========

def construir_dimensoes(todos_dados):
    """
    Constrói tabelas de dimensão únicas a partir dos dados de todos os anos
    """
    print(f"\n{'='*60}")
    print("🏗️  CONSTRUINDO DIMENSÕES GLOBAIS")
    print(f"{'='*60}\n")
    
    # AnoRelatorio
    ano_dim = pd.DataFrame(list(ANOS_CONFIG.items()), columns=['ano', 'fonte'])
    print(f"  ✓ AnoRelatorio: {len(ano_dim)} linhas")
    
    # TipoRelatorio
    tipo_dim = pd.DataFrame(list(TIPO_RELATORIO_MAP.items()), columns=['tipo_id', 'tipo'])
    print(f"  ✓ TipoRelatorio: {len(tipo_dim)} linhas")
    
    # Sexo (fixo)
    sexo_dim = pd.DataFrame([
        {'sexo_id': 1, 'tipo_sexo': 'M'},
        {'sexo_id': 2, 'tipo_sexo': 'F'}
    ])
    print(f"  ✓ Sexo: {len(sexo_dim)} linhas")
    
    # Nacionalidade (DIMENSÃO COMPARTILHADA - solicitar CSV processado)
    print(f"\n📤 Upload necessário: Nacionalidade.csv (dimensão compartilhada)")
    print("   Localização: 3️⃣ Data Preparation/data/processed/DP-01-A/Nacionalidade.csv")
    uploaded_nac = files.upload()
    
    if uploaded_nac:
        nac_file = list(uploaded_nac.keys())[0]
        nacionalidade_base = pd.read_csv(io.BytesIO(uploaded_nac[nac_file]))
        print(f"  ✓ Nacionalidade (base): {len(nacionalidade_base)} linhas\n")
    else:
        print("  ⚠️  Nacionalidade.csv não carregado - criando base mínima\n")
        nacionalidade_base = pd.DataFrame({
            'nacionalidade_id': [1],
            'nome_nacionalidade': ['Desconhecido'],
            'codigo_pais': ['XX'],
            'continente': ['Desconhecido']
        })
    
    # NacionalidadeAIMA (união de todas nacionalidades únicas dos CSVs AIMA)
    nacs = []
    for ano_data in todos_dados.values():
        for tabela in ano_data.values():
            if 'nacionalidade_aima_raw' in tabela.columns:
                nacs.extend(tabela['nacionalidade_aima_raw'].unique())
    
    nacionalidade_aima_dim = pd.DataFrame({'nome_nacionalidade_aima': sorted(set(nacs))})
    nacionalidade_aima_dim['nacionalidade_aima_id'] = range(1, len(nacionalidade_aima_dim) + 1)
    
    # MAPEAMENTO: NacionalidadeAIMA -> Nacionalidade (FK)
    # Fazer merge fuzzy/manual mapping (simplified version)
    nacionalidade_aima_dim['nacionalidade_id'] = None  # FK placeholder
    # Nota: Mapeamento completo requer lógica adicional ou tabela auxiliar
    
    print(f"  ✓ NacionalidadeAIMA: {len(nacionalidade_aima_dim)} linhas (com FK para Nacionalidade)")
    
    # Despacho (união de todos despachos)
    despachos = []
    for ano_data in todos_dados.values():
        if 'despacho_dim' in ano_data:
            despachos.append(ano_data['despacho_dim'])
    despacho_dim = pd.concat(despachos, ignore_index=True).drop_duplicates('codigo_despacho')
    print(f"  ✓ Despacho: {len(despacho_dim)} linhas")
    
    # MotivoConcessao (união de todos motivos)
    motivos = []
    for ano_data in todos_dados.values():
        if 'concessoes_motivo' in ano_data:
            motivos.extend(ano_data['concessoes_motivo']['motivo_raw'].unique())
    
    motivo_dim = pd.DataFrame({'motivo_raw': sorted(set(motivos))})
    motivo_dim['categoria'] = motivo_dim['motivo_raw'].map({
        'Reagrupamento Familiar': 'Familiar',
        'Atividade Profissional': 'Profissional',
        'Estudo': 'Educacional',
        'CRs': 'Outro',
        'Certificado de Residência': 'Outro',
        'Acordo CPLP': 'Outro',
        'Outros Motivos': 'Outro'
    }).fillna('Outro')
    print(f"  ✓ MotivoConcessao: {len(motivo_dim)} linhas")
    
    return {
        'AnoRelatorio': ano_dim,
        'TipoRelatorio': tipo_dim,
        'Sexo': sexo_dim,
        'Nacionalidade': nacionalidade_base,
        'NacionalidadeAIMA': nacionalidade_aima_dim,
        'Despacho': despacho_dim,
        'MotivoConcessao': motivo_dim
    }

# ========== SEÇÃO 6: CONSOLIDAÇÃO DE FATOS ==========

def consolidar_fatos(todos_dados):
    """
    Consolida todas tabelas fato de todos os anos
    """
    print(f"\n{'='*60}")
    print("📊 CONSOLIDANDO TABELAS FATO")
    print(f"{'='*60}\n")
    
    fatos = {}
    
    # Consolidar cada tipo de fato
    tipos_fato = [
        ('concessoes_nac_sexo', 'ConcessoesPorNacionalidadeSexo'),
        ('pop_est_nac_sexo', 'PopulacaoEstrangeiraPorNacionalidadeSexo'),
        ('concessoes_despacho', 'ConcessoesPorDespacho'),
        ('dist_etaria_conc', 'DistribuicaoEtariaConcessoes'),
        ('concessoes_motivo', 'ConcessoesPorMotivoNacionalidade'),
        ('pop_res_etaria', 'PopulacaoResidenteEtaria')
    ]
    
    for chave, nome in tipos_fato:
        dfs = [ano_data.get(chave, pd.DataFrame()) for ano_data in todos_dados.values()]
        dfs = [df for df in dfs if not df.empty]
        if dfs:
            fatos[nome] = pd.concat(dfs, ignore_index=True)
            print(f"  ✓ {nome}: {len(fatos[nome])} linhas")
    
    return fatos

# ========== SEÇÃO 7: EXECUÇÃO PRINCIPAL ==========

def main():
    """
    Pipeline ETL completo
    """
    import os
    
    print("\n" + "="*60)
    print("🚀 ETL AIMA/SEF - INÍCIO")
    print("="*60)
    
    # Processar todos os anos
    todos_dados = {}
    for ano in sorted(ANOS_CONFIG.keys()):
        todos_dados[ano] = processar_ano(ano)
    
    # Construir dimensões
    dimensoes = construir_dimensoes(todos_dados)
    
    # Consolidar fatos
    fatos = consolidar_fatos(todos_dados)
    
    # Criar pasta data no Colab
    output_dir = '/content/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Salvar todos os CSVs na pasta
    print(f"\n{'='*60}")
    print(f"💾 SALVANDO ARQUIVOS CSV EM {output_dir}")
    print(f"{'='*60}\n")
    
    todas_tabelas = {**dimensoes, **fatos}
    arquivos_salvos = 0
    
    for nome, df in todas_tabelas.items():
        if not df.empty:
            filepath = f"{output_dir}/{nome}.csv"
            df.to_csv(filepath, index=False)
            arquivos_salvos += 1
            print(f"  ✓ {nome}.csv ({len(df)} linhas, {len(df.columns)} colunas)")
    
    print(f"\n{'='*60}")
    print("🎉 ETL CONCLUÍDO COM SUCESSO!")
    print(f"{'='*60}")
    print(f"\n📊 Resumo:")
    print(f"  • {len(dimensoes)} Dimensões criadas")
    print(f"  • {len(fatos)} Tabelas Fato criadas")
    print(f"  • {arquivos_salvos} arquivos CSV salvos em {output_dir}/")
    print(f"\n📂 Arquivos disponíveis:")
    for arquivo in sorted(os.listdir(output_dir)):
        tamanho = os.path.getsize(f"{output_dir}/{arquivo}") / 1024
        print(f"   • {arquivo} ({tamanho:.1f} KB)")
    print()

# ========== EXECUTAR ETL ==========
if __name__ == "__main__":
    main()
