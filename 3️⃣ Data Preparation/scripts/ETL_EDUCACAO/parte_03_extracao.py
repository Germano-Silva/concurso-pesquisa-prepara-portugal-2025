"""
============================================================
PARTE 3: MÓDULO DE EXTRAÇÃO (UPLOAD)
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
"""

import pandas as pd
import io
from google.colab import files


# ============================================================
# CLASSE DE EXTRAÇÃO DE DADOS
# ============================================================

class ExtratorDados:
    """Gerencia o upload e carregamento de arquivos CSV"""
    
    def __init__(self, logger):
        self.logger = logger
        self.arquivos_carregados = {}
        self.dataframes = {}
    
    def solicitar_upload(self, nome_arquivo, descricao=""):
        """
        Solicita upload de um arquivo específico
        Retorna: DataFrame carregado ou None se falhar
        """
        self.logger.info(f"Solicitando upload: {nome_arquivo}")
        if descricao:
            print(f"📄 {descricao}")
        
        print(f"\n👉 Por favor, faça upload do arquivo: {nome_arquivo}")
        print("-" * 60)
        
        try:
            uploaded = files.upload()
            
            if not uploaded:
                self.logger.erro(f"Nenhum arquivo foi carregado")
                return None
            
            # Pega o primeiro arquivo enviado
            arquivo_nome = list(uploaded.keys())[0]
            conteudo = uploaded[arquivo_nome]
            
            # Carrega o CSV
            df = pd.read_csv(
                io.BytesIO(conteudo),
                encoding='utf-8',
                sep=';',
                decimal=',',
                thousands='.'
            )
            
            self.arquivos_carregados[nome_arquivo] = arquivo_nome
            self.dataframes[nome_arquivo] = df
            
            self.logger.sucesso(
                f"Arquivo carregado: {arquivo_nome} ({len(df)} linhas, "
                f"{len(df.columns)} colunas)"
            )
            
            return df
            
        except Exception as e:
            self.logger.erro(f"Erro ao carregar {nome_arquivo}: {str(e)}")
            return None
    
    def solicitar_uploads_em_lote(self, lista_arquivos, descricao_categoria=""):
        """
        Solicita upload de múltiplos arquivos sequencialmente
        Retorna: dict {nome_arquivo: dataframe}
        """
        if descricao_categoria:
            self.logger.subsecao(descricao_categoria)
        
        resultados = {}
        total = len(lista_arquivos)
        
        for i, nome_arquivo in enumerate(lista_arquivos, 1):
            self.logger.progresso(i, total, f"Carregando arquivos ({i}/{total})")
            
            df = self.solicitar_upload(
                nome_arquivo,
                descricao=f"Arquivo {i} de {total}"
            )
            
            if df is not None:
                resultados[nome_arquivo] = df
            else:
                self.logger.aviso(f"Arquivo {nome_arquivo} não foi carregado")
        
        print()  # Nova linha após progresso
        return resultados
    
    def get_dataframe(self, nome_arquivo):
        """Retorna DataFrame de um arquivo carregado"""
        return self.dataframes.get(nome_arquivo)
    
    def listar_arquivos_carregados(self):
        """Lista todos os arquivos carregados"""
        self.logger.subsecao("Arquivos Carregados")
        
        if not self.arquivos_carregados:
            print("Nenhum arquivo carregado ainda.")
            return
        
        for nome_esperado, nome_real in self.arquivos_carregados.items():
            df = self.dataframes.get(nome_esperado)
            print(f"✓ {nome_esperado}")
            if df is not None:
                print(f"    Arquivo: {nome_real}")
                print(f"    Registros: {len(df)}, Colunas: {len(df.columns)}")
                mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                print(f"    Memória: {mem_mb:.2f} MB")
    
    def validar_estrutura_arquivo(self, nome_arquivo, colunas_obrigatorias):
        """
        Valida se arquivo possui colunas obrigatórias
        Retorna: True se válido, False caso contrário
        """
        df = self.get_dataframe(nome_arquivo)
        
        if df is None:
            self.logger.erro(f"Arquivo {nome_arquivo} não encontrado")
            return False
        
        colunas_faltantes = set(colunas_obrigatorias) - set(df.columns)
        
        if colunas_faltantes:
            self.logger.erro(
                f"Arquivo {nome_arquivo}: colunas obrigatórias faltantes: "
                f"{colunas_faltantes}"
            )
            return False
        
        return True


# ============================================================
# CLASSE DE PARSER DE DADOS INE 2011
# ============================================================

class ParserINE2011:
    """Parser específico para arquivos CSV do INE 2011"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def extrair_por_categoria(self, df, categorias):
        """
        Extrai linhas de uma ou mais categorias específicas
        df: DataFrame do arquivo país
        categorias: lista de categorias ou string única
        Retorna: DataFrame filtrado
        """
        if isinstance(categorias, str):
            categorias = [categorias]
        
        if 'Categoria' not in df.columns:
            self.logger.aviso("Coluna 'Categoria' não encontrada no DataFrame")
            return pd.DataFrame()
        
        df_filtrado = df[df['Categoria'].isin(categorias)].copy()
        
        return df_filtrado
    
    def extrair_subcategoria(self, df, categoria, subcategoria):
        """
        Extrai uma subcategoria específica de uma categoria
        """
        df_cat = self.extrair_por_categoria(df, categoria)
        
        if 'Subcategoria' not in df_cat.columns:
            return pd.DataFrame()
        
        df_sub = df_cat[df_cat['Subcategoria'] == subcategoria].copy()
        
        return df_sub
    
    def extrair_dados_educacao(self, df_pais, nacionalidade):
        """
        Extrai dados educacionais de um arquivo de país
        Retorna: dict com dados estruturados
        """
        resultado = {
            'nacionalidade': nacionalidade,
            'niveis': []
        }
        
        # Tentar ambas as categorias (alguns países usam faixas diferentes)
        categorias_educacao = [
            'NÍVEL DE ENSINO (15-64 anos)',
            'NÍVEL DE ENSINO (45-66 anos)'
        ]
        
        df_educacao = self.extrair_por_categoria(df_pais, categorias_educacao)
        
        if df_educacao.empty:
            self.logger.aviso(
                f"Dados educacionais não encontrados para {nacionalidade}"
            )
            return resultado
        
        # Extrair faixa etária usada
        if 'Categoria' in df_educacao.columns:
            faixa_etaria = df_educacao['Categoria'].iloc[0]
            resultado['faixa_etaria'] = faixa_etaria
        
        # Extrair total da população na faixa
        linha_total = df_educacao[
            df_educacao['Subcategoria'].str.contains('Total', case=False, na=False)
        ]
        
        if not linha_total.empty:
            total = self._limpar_numero(linha_total['Dados 2011'].iloc[0])
            resultado['total_populacao'] = total
        
        # Extrair dados por nível
        niveis_mapeamento = {
            'Inferior ao básico 3º ciclo': 'inferior_basico',
            'Básico 3º ciclo': 'basico',
            'Secundário e pós-secundário': 'secundario',
            'Superior': 'superior'
        }
        
        for nivel_nome, nivel_chave in niveis_mapeamento.items():
            linha_nivel = df_educacao[
                df_educacao['Subcategoria'].str.contains(nivel_nome, case=False, na=False)
            ]
            
            if not linha_nivel.empty:
                quantidade = self._limpar_numero(linha_nivel['Dados 2011'].iloc[0])
                percentual = self._limpar_numero(linha_nivel['Percentagem (2011)'].iloc[0])
                
                resultado['niveis'].append({
                    'nivel': nivel_nome,
                    'quantidade': quantidade,
                    'percentual': percentual
                })
        
        return resultado
    
    def extrair_populacao_residente(self, df_pais, nacionalidade):
        """Extrai dados de população residente total"""
        df_pop = self.extrair_por_categoria(df_pais, 'POPULAÇÃO RESIDENTE')
        
        if df_pop.empty:
            return None
        
        resultado = {'nacionalidade': nacionalidade}
        
        # Total
        linha_total = df_pop[df_pop['Subcategoria'] == 'Total']
        if not linha_total.empty:
            resultado['total_2011'] = self._limpar_numero(linha_total['Dados 2011'].iloc[0])
            resultado['total_2001'] = self._limpar_numero(linha_total['Dados 2001'].iloc[0])
        
        # Homens
        linha_homens = df_pop[df_pop['Subcategoria'] == 'Homens']
        if not linha_homens.empty:
            resultado['homens_2011'] = self._limpar_numero(linha_homens['Dados 2011'].iloc[0])
            resultado['homens_2001'] = self._limpar_numero(linha_homens['Dados 2001'].iloc[0])
        
        # Mulheres
        linha_mulheres = df_pop[df_pop['Subcategoria'] == 'Mulheres']
        if not linha_mulheres.empty:
            resultado['mulheres_2011'] = self._limpar_numero(linha_mulheres['Dados 2011'].iloc[0])
            resultado['mulheres_2001'] = self._limpar_numero(linha_mulheres['Dados 2001'].iloc[0])
        
        # Idade média
        linha_idade = df_pop[df_pop['Subcategoria'].str.contains('Idade média', na=False)]
        if not linha_idade.empty:
            idade_texto = linha_idade['Dados 2011'].iloc[0]
            resultado['idade_media'] = self._extrair_numero_de_texto(idade_texto)
        
        return resultado
    
    def extrair_municipios_top(self, df_pais, nacionalidade, top_n=5):
        """Extrai top N municípios por população"""
        df_municipios = self.extrair_por_categoria(df_pais, 'MUNICÍPIOS (Top 5)')
        
        if df_municipios.empty:
            return []
        
        municipios = []
        
        for _, row in df_municipios.iterrows():
            municipios.append({
                'nacionalidade': nacionalidade,
                'municipio': row['Subcategoria'],
                'populacao': self._limpar_numero(row['Dados 2011']),
                'percentual': self._limpar_numero(row['Percentagem (2011)'])
            })
        
        return municipios[:top_n]
    
    @staticmethod
    def _limpar_numero(valor):
        """Limpa e converte valor numérico"""
        if pd.isna(valor) or valor == '':
            return None
        
        valor_str = str(valor).replace('%', '').replace(' ', '').strip()
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            return float(valor_str)
        except:
            return None
    
    @staticmethod
    def _extrair_numero_de_texto(texto):
        """Extrai número de texto como '34,9 anos'"""
        if pd.isna(texto):
            return None
        
        import re
        numeros = re.findall(r'[\d,]+', str(texto))
        if numeros:
            return ParserINE2011._limpar_numero(numeros[0])
        return None


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Este módulo deve ser executado no Google Colab")
    print("Teste manual de parsers:")
    
    # Dados de teste simulados
    df_teste = pd.DataFrame({
        'Nacionalidade': ['Brasil'] * 5,
        'Categoria': ['NÍVEL DE ENSINO (15-64 anos)'] * 5,
        'Subcategoria': [
            'Total População 15-64',
            'Inferior ao básico 3º ciclo',
            'Básico 3º ciclo',
            'Secundário e pós-secundário',
            'Superior'
        ],
        'Dados 2011': [93545, 24498, 21132, 38411, 9504],
        'Percentagem (2011)': ['100,00%', '26,19%', '22,59%', '41,05%', '10,16%']
    })
    
    # Teste do parser
    class LoggerTeste:
        def aviso(self, msg): print(f"AVISO: {msg}")
    
    parser = ParserINE2011(LoggerTeste())
    resultado = parser.extrair_dados_educacao(df_teste, 'Brasil')
    
    print(f"\nResultado da extração:")
    print(f"  Nacionalidade: {resultado['nacionalidade']}")
    print(f"  Total populaçã: {resultado.get('total_populacao')}")
    print(f"  Níveis: {len(resultado['niveis'])}")
    
    print("\n✓ Módulo parte_03_extracao.py carregado com sucesso!")
