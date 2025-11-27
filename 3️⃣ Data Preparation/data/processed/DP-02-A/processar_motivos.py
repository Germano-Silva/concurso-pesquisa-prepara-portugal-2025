import os
import pandas as pd

def processar_motivos_residencia():
    print("🚀 INICIANDO PROCESSAMENTO DE MOTIVOS DE RESIDÊNCIA")
    print("=" * 50)
    
    # Configuração do caminho
    caminho_base = "2️⃣ Data Understanding/data/raw/aima/extraidas/"
    
    # Anos para processar
    anos_config = [
        ('2020', 'RIFA2020_csv'),
        ('2021', 'RIFA2021_csv'),
        ('2022', 'RIFA2022_csv'), 
        ('2023', 'RMA2023_csv'),
        ('2024', 'RMA2024_csv')
    ]
    
    dados_todos = []
    
    for ano, pasta in anos_config:
        print(f"\n📅 PROCESSANDO ANO {ano}...")
        
        caminho_pasta = os.path.join(caminho_base, pasta)
        
        # Encontrar arquivo de concessões por motivo
        arquivos = os.listdir(caminho_pasta)
        arquivo_concessao = None
        
        for arquivo in arquivos:
            if 'concessao' in arquivo.lower() and 'motivo' in arquivo.lower():
                arquivo_concessao = arquivo
                break
        
        if not arquivo_concessao:
            print(f"   ❌ Arquivo de motivos não encontrado para {ano}")
            continue
            
        print(f"   📂 Arquivo: {arquivo_concessao}")
        
        try:
            # Ler o arquivo
            caminho_arquivo = os.path.join(caminho_pasta, arquivo_concessao)
            
            # Tentar diferentes encodings
            try:
                df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8')
            except:
                try:
                    df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1')
                except:
                    df = pd.read_csv(caminho_arquivo, sep=';', encoding='iso-8859-1')
            
            print(f"   ✅ Carregado: {len(df)} registros")
            print(f"   📊 Colunas: {list(df.columns)}")
            print(f"   🔍 Primeiras linhas:")
            print(df.head(2))
            
            # Adicionar coluna de ano
            df['Ano'] = ano
            dados_todos.append(df)
            
        except Exception as e:
            print(f"   ❌ Erro ao ler arquivo: {e}")
    
    # VERIFICAR SE TEMOS DADOS
    if not dados_todos:
        print("\n❌ NENHUM DADO FOI CARREGADO!")
        return
    
    print(f"\n✅ DADOS CARREGADOS: {len(dados_todos)} anos")
    
    # PROCESSAR CADA ANO SEPARADAMENTE (estruturas diferentes)
    resultados_finais = []
    
    for df in dados_todos:
        ano = df['Ano'].iloc[0]
        print(f"\n🎯 PROCESSANDO ESTRUTURA DO ANO {ano}...")
        
        # Ver estrutura específica de cada ano
        colunas = df.columns.tolist()
        primeira_linha = df.iloc[0].tolist() if len(df) > 0 else []
        
        print(f"   Colunas: {colunas}")
        print(f"   Primeira linha: {primeira_linha[:3]}...")  # Mostra só os primeiros valores
        
        # ANOS 2020, 2021, 2022 - Estrutura com países nas linhas
        if ano in ['2020', '2021', '2022']:
            # Esses arquivos têm países nas linhas e motivos nas colunas
            # Precisamos transformar: de wide para long format
            try:
                # A primeira coluna geralmente é o país
                coluna_pais = colunas[0]
                
                # Identificar colunas de motivos (não são 'Ano' e não são a primeira)
                colunas_motivos = []
                for col in colunas:
                    if col != 'Ano' and col != coluna_pais:
                        # Verificar se a coluna tem valores numéricos
                        if pd.to_numeric(df[col].iloc[0], errors='coerce') is not None:
                            colunas_motivos.append(col)
                
                print(f"   🎯 Colunas de motivos identificadas: {colunas_motivos}")
                
                if colunas_motivos:
                    # Transformar para formato longo
                    df_long = pd.melt(df, 
                                    id_vars=[coluna_pais, 'Ano'], 
                                    value_vars=colunas_motivos,
                                    var_name='Motivo', 
                                    value_name='Total')
                    
                    # Converter total para numérico
                    df_long['Total'] = pd.to_numeric(df_long['Total'], errors='coerce')
                    
                    # Limpar dados
                    df_long = df_long.dropna(subset=['Total'])
                    
                    resultados_finais.append(df_long)
                    print(f"   ✅ {ano}: {len(df_long)} registros processados")
                
            except Exception as e:
                print(f"   ❌ Erro ao processar {ano}: {e}")
        
        # ANOS 2023, 2024 - Estrutura com motivos nas linhas
        elif ano in ['2023', '2024']:
            try:
                # Esses anos têm motivos diretamente nas linhas
                # A primeira coluna é o motivo, a segunda é o total
                coluna_motivo = colunas[0]
                coluna_total = colunas[1] if len(colunas) > 1 else None
                
                if coluna_total:
                    # Criar dataframe padronizado
                    df_processed = df[[coluna_motivo, coluna_total, 'Ano']].copy()
                    df_processed.columns = ['Motivo', 'Total', 'Ano']
                    
                    # Converter total para numérico
                    df_processed['Total'] = pd.to_numeric(df_processed['Total'], errors='coerce')
                    df_processed = df_processed.dropna(subset=['Total'])
                    
                    resultados_finais.append(df_processed)
                    print(f"   ✅ {ano}: {len(df_processed)} registros processados")
                    
            except Exception as e:
                print(f"   ❌ Erro ao processar {ano}: {e}")
    
    # COMBINAR TODOS OS RESULTADOS
    if not resultados_finais:
        print("\n❌ NENHUM DADO FOI PROCESSADO!")
        return
    
    df_final = pd.concat(resultados_finais, ignore_index=True)
    print(f"\n📊 TOTAL DE REGISTROS FINAIS: {len(df_final)}")
    
    # LIMPAR E CATEGORIZAR MOTIVOS
    print("\n🎯 CATEGORIZANDO MOTIVOS...")
    
    df_final['Motivo_Limpo'] = df_final['Motivo'].astype(str).str.strip().str.upper()
    
    # Mostrar motivos únicos
    motivos_unicos = df_final['Motivo_Limpo'].unique()
    print(f"🎯 Motivos encontrados: {list(motivos_unicos)}")
    
    # Mapeamento para categorias
    mapeamento = {
        'TRABALHO': 'ATIVIDADE PROFISSIONAL',
        'TRABALHO POR CONTA DE OUTREM': 'ATIVIDADE PROFISSIONAL', 
        'TRABALHO INDEPENDENTE': 'ATIVIDADE PROFISSIONAL',
        'ATIVIDADE PROFISSIONAL': 'ATIVIDADE PROFISSIONAL',
        'ATIVIDADE PROFISSIONAL ': 'ATIVIDADE PROFISSIONAL',
        'ESTUDO': 'ESTUDO',
        'REAGRUPAMENTO FAMILIAR': 'REAGRUPAMENTO FAMILIAR',
        'REAGRUPAMENTO FAMILIAR (%)': 'REAGRUPAMENTO FAMILIAR',
        'ARTIGO 89': 'REAGRUPAMENTO FAMILIAR',
        'ARTIGO 87A': 'AR CPLP',
        'AR CPLP': 'AR CPLP',
        'CRS': 'OUTROS',
        'CRS (%)': 'OUTROS',
        'OUTROS MOTIVOS': 'OUTROS',
        'OUTROS MOTIVOS (%)': 'OUTROS'
    }
    
    # Aplicar mapeamento
    df_final['Motivo_Categoria'] = df_final['Motivo_Limpo'].map(mapeamento)
    df_final['Motivo_Categoria'] = df_final['Motivo_Categoria'].fillna('OUTROS')
    
    # AGRUPAR DADOS FINAIS
    print("\n📊 GERANDO RESULTADOS FINAIS...")
    
    df_agrupado = df_final.groupby(['Ano', 'Motivo_Categoria'])['Total'].sum().reset_index()
    
    # Calcular percentuais
    df_agrupado['Percentagem'] = df_agrupado.groupby('Ano')['Total'].transform(
        lambda x: round(x / x.sum() * 100, 2)
    )
    
    # CRIAR PASTA DE DESTINO
    os.makedirs("3️⃣ Data Preparation/data/processed/", exist_ok=True)
    
    # SALVAR RESULTADO
    caminho_saida = "3️⃣ Data Preparation/data/processed/dados_motivos_residencia.csv"
    df_agrupado.to_csv(caminho_saida, index=False, encoding='utf-8')
    
    print(f"\n💾 ARQUIVO SALVO: {caminho_saida}")
    
    # MOSTRAR RESULTADO
    print("\n📋 RESULTADO FINAL:")
    print("=" * 50)
    print(df_agrupado)
    
    print("\n🎉 PROCESSAMENTO CONCLUÍDO COM SUCESSO!")

# Executar o script
if __name__ == "__main__":
    processar_motivos_residencia()