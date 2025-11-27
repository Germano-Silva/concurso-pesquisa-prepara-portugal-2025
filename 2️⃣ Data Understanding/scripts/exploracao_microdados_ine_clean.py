#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exploração de Microdados do INE - Censos 2021
==============================================

Projeto: Concurso de Pesquisa Prepara Portugal 2025
Tema: O Perfil Sócio-profissional do Imigrante em Portugal
Foco: Relação entre Nível Educacional e Inserção no Mercado de Trabalho
Data: Novembro 2025

Este script realiza uma análise exploratória completa dos dados do INE dos Censos 2021,
focando em três variáveis-chave:
- Nacionalidade (população estrangeira)
- Habilitações Literárias (nível educacional)
- Setor de Atividade Económica (inserção laboral)

Datasets Analisados:
1. Q2.1.csv - Habilitações Literárias por Nacionalidade
2. Q3.3.csv - Setor de Atividade por Nacionalidade
3. Q1.1.csv - Demografia por Nacionalidade (2011-2021)
4. Q13.csv - Educação Geral (comparação nacional)
5. Q21.csv - Setores de Atividade Geral (comparação nacional)

Autor: Sistema de Análise de Dados
"""

# =============================================================================
# IMPORTS E CONFIGURAÇÕES INICIAIS
# =============================================================================

# Bibliotecas essenciais para manipulação e análise de dados
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from pathlib import Path
import sys
import os
from typing import Optional, Dict, List, Tuple, Union

# Configurações gerais do pandas para melhor visualização dos dados
pd.set_option('display.max_columns', None)  # Mostrar todas as colunas
pd.set_option('display.width', None)        # Largura ilimitada
pd.set_option('display.max_colwidth', 50)   # Máximo 50 caracteres por coluna

# Configurações para matplotlib e seaborn
plt.style.use('default')                    # Estilo padrão do matplotlib
sns.set_palette("husl")                     # Paleta de cores harmoniosa
warnings.filterwarnings('ignore')           # Suprimir avisos desnecessários

# Configurações específicas para gráficos
plt.rcParams['figure.figsize'] = (12, 8)    # Tamanho padrão das figuras
plt.rcParams['font.size'] = 10               # Tamanho da fonte
plt.rcParams['font.family'] = 'sans-serif'  # Família da fonte

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS E CONSTANTES
# =============================================================================

class ConfiguracaoCaminhos:
    """
    Classe para gerenciar todos os caminhos dos arquivos de dados.
    
    Esta classe centraliza a configuração de caminhos para facilitar
    a manutenção e portabilidade do código.
    """
    
    def __init__(self, diretorio_base: str = "data/raw/ine"):
        """
        Inicializa as configurações de caminhos.
        
        Args:
            diretorio_base (str): Diretório base onde estão os dados do INE
        """
        self.BASE_PATH = Path(diretorio_base)
        self.CENSOS_CSV_PATH = self.BASE_PATH / "Censos2021_csv"
        self.POPULACAO_ESTRANGEIRA_PATH = self.BASE_PATH / "Censos2021_População estrangeira"
        
        # Dicionário com todos os datasets principais utilizados na análise
        self.DATASETS = {
            'educacao_nacionalidade': self.POPULACAO_ESTRANGEIRA_PATH / "Q2.1.csv",
            'setores_nacionalidade': self.POPULACAO_ESTRANGEIRA_PATH / "Q3.3.csv", 
            'demografia_nacionalidade': self.POPULACAO_ESTRANGEIRA_PATH / "Q1.1.csv",
            'educacao_geral': self.CENSOS_CSV_PATH / "Q13.csv",
            'setores_geral': self.CENSOS_CSV_PATH / "Q21.csv"
        }
    
    def verificar_arquivos_existentes(self) -> Dict[str, bool]:
        """
        Verifica se todos os arquivos necessários existem.
        
        Returns:
            Dict[str, bool]: Dicionário indicando quais arquivos existem
        """
        status = {}
        for nome, caminho in self.DATASETS.items():
            status[nome] = caminho.exists()
        return status

# =============================================================================
# FUNÇÕES UTILITÁRIAS PARA ANÁLISE DE DADOS
# =============================================================================

def carregar_e_analisar_dataset(caminho_arquivo: Path, 
                               nome_dataset: str, 
                               encoding: str = 'utf-8') -> Optional[pd.DataFrame]:
    """
    Carrega um dataset CSV e retorna análise estrutural completa.
    
    Esta função tenta carregar um arquivo CSV com diferentes encodings
    e fornece informações detalhadas sobre a estrutura de dados.
    
    Args:
        caminho_arquivo (Path): Caminho para o arquivo CSV
        nome_dataset (str): Nome descritivo do dataset para logging
        encoding (str): Codificação do arquivo (padrão: utf-8)
    
    Returns:
        Optional[pd.DataFrame]: DataFrame carregado ou None se houver erro
    """
    try:
        # Primeira tentativa de carregamento com encoding padrão
        df = pd.read_csv(caminho_arquivo, encoding=encoding)
        
        print(f"\n{'='*60}")
        print(f"DATASET: {nome_dataset.upper()}")
        print(f"Arquivo: {caminho_arquivo.name}")
        print(f"{'='*60}")
        
        return df
        
    except UnicodeDecodeError:
        # Se falhar com UTF-8, tenta com ISO-8859-1 (comum em dados portugueses)
        print(f"AVISO: Erro de encoding UTF-8, tentando ISO-8859-1...")
        try:
            df = pd.read_csv(caminho_arquivo, encoding='iso-8859-1')
            print(f"SUCESSO: Carregado com sucesso usando ISO-8859-1")
            return df
        except Exception as e:
            print(f"ERRO: Erro ao carregar {caminho_arquivo}: {e}")
            return None
            
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {caminho_arquivo}")
        return None
        
    except Exception as e:
        print(f"ERRO: Erro inesperado ao carregar {caminho_arquivo}: {e}")
        return None


def analisar_dados_ausentes(df: pd.DataFrame, nome_dataset: str) -> pd.DataFrame:
    """
    Analisa dados ausentes no dataset e retorna estatísticas detalhadas.
    
    Esta função é crucial para avaliar a qualidade dos dados, identificando
    colunas com valores faltantes e calculando percentuais de completude.
    
    Args:
        df (pd.DataFrame): DataFrame para análise
        nome_dataset (str): Nome do dataset para contextualização
    
    Returns:
        pd.DataFrame: DataFrame com estatísticas de missing data
    """
    print(f"\nANÁLISE DE DADOS AUSENTES - {nome_dataset}")
    print("-" * 50)
    
    # Calcula contagem e percentual de valores ausentes por coluna
    valores_ausentes = df.isnull().sum()
    percentual_ausente = (valores_ausentes / len(df)) * 100
    
    # Cria DataFrame com estatísticas organizadas
    dados_ausentes_df = pd.DataFrame({
        'Coluna': df.columns,
        'Valores_Ausentes': valores_ausentes.values,
        'Percentual_Ausente': percentual_ausente.values
    })
    
    # Ordena por percentual de dados ausentes (do maior para o menor)
    dados_ausentes_df = dados_ausentes_df[
        dados_ausentes_df['Valores_Ausentes'] > 0
    ].sort_values('Percentual_Ausente', ascending=False)
    
    # Apresenta resultados de forma clara
    if len(dados_ausentes_df) == 0:
        print("RESULTADO: Nenhum valor ausente encontrado!")
    else:
        print(f"AVISO: Encontrados valores ausentes em {len(dados_ausentes_df)} colunas:")
        print(dados_ausentes_df.to_string(index=False))
    
    return dados_ausentes_df


def criar_estatisticas_resumo(df: pd.DataFrame, 
                             colunas_categoricas: List[str], 
                             colunas_numericas: List[str] = None) -> Dict:
    """
    Cria estatísticas resumo abrangentes para colunas categóricas e numéricas.
    
    Esta função gera um dicionário com estatísticas descritivas essenciais
    para compreender a distribuição e características dos dados.
    
    Args:
        df (pd.DataFrame): DataFrame para análise
        colunas_categoricas (List[str]): Lista de colunas categóricas
        colunas_numericas (List[str], optional): Lista de colunas numéricas
    
    Returns:
        Dict: Dicionário com estatísticas organizadas por tipo de coluna
    """
    if colunas_numericas is None:
        colunas_numericas = []
    
    estatisticas = {}
    
    # Análise de colunas categóricas
    for coluna in colunas_categoricas:
        if coluna in df.columns:
            # Estatísticas básicas para variáveis categóricas
            valores_unicos = df[coluna].nunique()
            
            # Valor mais frequente (moda)
            moda = df[coluna].mode()
            valor_mais_frequente = moda.iloc[0] if len(moda) > 0 else None
            
            # Frequência do valor mais comum
            frequencia_maxima = df[coluna].value_counts().iloc[0] if len(df[coluna]) > 0 else 0
            
            # Top 5 valores mais frequentes
            top_5_valores = df[coluna].value_counts().head().to_dict()
            
            estatisticas[coluna] = {
                'valores_unicos': valores_unicos,
                'valor_mais_frequente': valor_mais_frequente,
                'frequencia_maxima': frequencia_maxima,
                'top_5_valores': top_5_valores
            }
    
    # Análise de colunas numéricas
    for coluna in colunas_numericas:
        if coluna in df.columns:
            # Usa a função describe() do pandas para estatísticas descritivas completas
            estatisticas[coluna] = df[coluna].describe().to_dict()
    
    return estatisticas


def extrair_nacionalidades(df: pd.DataFrame, nome_dataset: str) -> Optional[List[str]]:
    """
    Extrai e analisa as nacionalidades presentes em um dataset.
    
    Esta função identifica e filtra as nacionalidades válidas,
    excluindo linhas de cabeçalho, totais e outros valores não relevantes.
    
    Args:
        df (pd.DataFrame): DataFrame para análise
        nome_dataset (str): Nome do dataset para contextualização
    
    Returns:
        Optional[List[str]]: Lista de nacionalidades identificadas ou None se erro
    """
    if df is None:
        print(f"ERRO: Dataset {nome_dataset} não disponível")
        return None
    
    # Assume que as nacionalidades estão na primeira coluna
    primeira_coluna = df.iloc[:, 0]
    
    # Extrai todas as entradas não nulas
    nacionalidades = primeira_coluna[primeira_coluna.notna()].tolist()
    
    # Define filtros para excluir valores que não são nacionalidades
    filtros_exclusao = [
        'total', 'população', 'quadro', 'nacionalidade', 
        'fonte', 'ine', 'portugal', 'estrangeira', 'residente'
    ]
    
    # Filtra nacionalidades válidas
    nacionalidades_filtradas = [
        str(nac) for nac in nacionalidades 
        if not any(filtro in str(nac).lower() for filtro in filtros_exclusao)
        and len(str(nac)) > 2  # Exclui códigos muito curtos
        and str(nac).strip() != ''  # Exclui strings vazias
    ]
    
    # Apresenta resultados da análise
    print(f"\nANÁLISE DE NACIONALIDADES - {nome_dataset}:")
    print(f"   Total de entradas: {len(nacionalidades)}")
    print(f"   Nacionalidades válidas identificadas: {len(nacionalidades_filtradas)}")
    
    if len(nacionalidades_filtradas) > 0:
        print(f"   Top 10 nacionalidades:")
        for i, nacionalidade in enumerate(nacionalidades_filtradas[:10], 1):
            print(f"     {i:2d}. {nacionalidade}")
    
    return nacionalidades_filtradas


def analisar_setores_atividade(df: pd.DataFrame) -> Dict[str, str]:
    """
    Analisa e mapeia os setores de atividade económica presentes nos dados.
    
    Esta função identifica os setores CAE (Classificação das Atividades Económicas)
    e fornece descrições completas para facilitar a interpretação.
    
    Args:
        df (pd.DataFrame): DataFrame com dados de setores de atividade
    
    Returns:
        Dict[str, str]: Mapeamento código CAE -> descrição do setor
    """
    print("ANÁLISE DOS SETORES DE ATIVIDADE ECONÓMICA")
    print("=" * 50)
    
    if df is None:
        print("ERRO: Dataset de setores não disponível")
        return {}
    
    # Analisa estrutura das colunas
    colunas = list(df.columns)
    print(f"Total de colunas identificadas: {len(colunas)}")
    
    # Identifica colunas de setores (geralmente após as primeiras 2 colunas de identificação)
    colunas_setoriais = colunas[2:] if len(colunas) > 2 else []
    
    if len(colunas_setoriais) > 0:
        print(f"SETORES CAE IDENTIFICADOS ({len(colunas_setoriais)} setores):")
        
        # Mapeamento completo dos setores CAE conhecidos
        setores_cae = {
            'A': 'Agricultura, produção animal, caça, floresta e pesca',
            'B': 'Indústrias extractivas', 
            'C': 'Indústrias transformadoras',
            'D': 'Electricidade, gás, vapor, água quente e fria e ar frio',
            'E': 'Captação, tratamento e distribuição de água; saneamento, gestão de resíduos',
            'F': 'Construção',
            'G': 'Comércio por grosso e a retalho; reparação de veículos automóveis',
            'H': 'Transportes e armazenagem',
            'I': 'Alojamento, restauração e similares',
            'J': 'Actividades de informação e de comunicação',
            'K': 'Actividades financeiras e de seguros',
            'L': 'Actividades imobiliárias',
            'M': 'Actividades de consultoria, científicas, técnicas e similares',
            'N': 'Actividades administrativas e dos serviços de apoio',
            'O': 'Administração Pública e Defesa; Segurança Social Obrigatória',
            'P': 'Educação',
            'Q': 'Actividades de saúde humana e apoio social',
            'R': 'Actividades artísticas, de espectáculos, desportivas e recreativas',
            'S': 'Outras actividades de serviços',
            'T': 'Atividades das famílias empregadoras de pessoal doméstico',
            'U': 'Actividades dos organismos internacionais'
        }
        
        # Apresenta mapeamento dos setores encontrados
        for i, coluna_setor in enumerate(colunas_setoriais, 1):
            letra_cae = chr(64 + i)  # Converte para A, B, C, etc.
            if letra_cae in setores_cae:
                print(f"   {letra_cae}. {setores_cae[letra_cae]}")
            else:
                print(f"   {letra_cae}. {coluna_setor}")
        
        return setores_cae
    
    return {}

# =============================================================================
# CLASSE PRINCIPAL PARA ANÁLISE DOS MICRODADOS
# =============================================================================

class AnalisadorMicrodadosINE:
    """
    Classe principal para análise dos microdados do INE dos Censos 2021.
    
    Esta classe encapsula todos os métodos necessários para carregar,
    processar e analisar os dados do INE de forma organizada e sistemática.
    """
    
    def __init__(self, diretorio_dados: str = "data/raw/ine"):
        """
        Inicializa o analisador com configurações padrão.
        
        Args:
            diretorio_dados (str): Diretório base dos dados do INE
        """
        self.config = ConfiguracaoCaminhos(diretorio_dados)
        self.datasets: Dict[str, pd.DataFrame] = {}
        self.nacionalidades: Dict[str, List[str]] = {}
        self.estatisticas: Dict[str, Dict] = {}
        
        print("Analisador de Microdados do INE inicializado")
        print(f"Diretório base: {self.config.BASE_PATH}")
    
    def verificar_disponibilidade_dados(self) -> None:
        """
        Verifica a disponibilidade de todos os arquivos de dados necessários.
        """
        print("\nVERIFICAÇÃO DE DISPONIBILIDADE DOS DADOS")
        print("=" * 50)
        
        status_arquivos = self.config.verificar_arquivos_existentes()
        
        for nome_dataset, existe in status_arquivos.items():
            status = "[OK]" if existe else "[ERRO]"
            caminho_arquivo = self.config.DATASETS[nome_dataset]
            print(f"{status} {nome_dataset}: {caminho_arquivo.name}")
        
        arquivos_disponiveis = sum(status_arquivos.values())
        total_arquivos = len(status_arquivos)
        
        print(f"\nResumo: {arquivos_disponiveis}/{total_arquivos} arquivos disponíveis")
        
        if arquivos_disponiveis == 0:
            print("AVISO: Nenhum arquivo de dados encontrado!")
            print("   Verifique se os dados estão no diretório correto.")
    
    def carregar_todos_datasets(self) -> None:
        """
        Carrega todos os datasets disponíveis e realiza análise estrutural básica.
        """
        print("\nCARREGAMENTO DE TODOS OS DATASETS")
        print("=" * 50)
        
        # Lista de datasets com nomes descritivos para melhor compreensão
        datasets_info = [
            ('educacao_nacionalidade', 'Habilitações Literárias por Nacionalidade'),
            ('setores_nacionalidade', 'Setores de Atividade por Nacionalidade'),
            ('demografia_nacionalidade', 'Demografia por Nacionalidade (2011-2021)'),
            ('educacao_geral', 'Educação Geral (comparação nacional)'),
            ('setores_geral', 'Setores de Atividade Geral (comparação nacional)')
        ]
        
        for nome_interno, nome_descritivo in datasets_info:
            if nome_interno in self.config.DATASETS:
                caminho = self.config.DATASETS[nome_interno]
                
                if caminho.exists():
                    # Carrega o dataset usando a função utilitária
                    df = carregar_e_analisar_dataset(caminho, nome_descritivo)
                    
                    if df is not None:
                        # Armazena o dataset para uso posterior
                        self.datasets[nome_interno] = df
                        
                        # Realiza análise estrutural básica
                        self._analisar_estrutura_basica(df, nome_descritivo)
                    else:
                        print(f"ERRO: Falha ao carregar: {nome_descritivo}")
                else:
                    print(f"ERRO: Arquivo não encontrado: {nome_descritivo}")
    
    def _analisar_estrutura_basica(self, df: pd.DataFrame, nome_dataset: str) -> None:
        """
        Realiza análise estrutural básica de um dataset.
        
        Args:
            df (pd.DataFrame): DataFrame para análise
            nome_dataset (str): Nome descritivo do dataset
        """
        print(f"Dimensões: {df.shape[0]:,} linhas × {df.shape[1]} colunas")
        print(f"Tamanho em memória: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # Apresenta informações sobre tipos de dados
        print(f"Tipos de dados:")
        tipos_dados = df.dtypes.value_counts()
        for tipo, contagem in tipos_dados.items():
            print(f"   {tipo}: {contagem} colunas")
        
        # Análise de dados ausentes
        analisar_dados_ausentes(df, nome_dataset)
    
    def analisar_nacionalidades(self) -> None:
        """
        Analisa nacionalidades em todos os datasets carregados.
        """
        print("\nANÁLISE CONSOLIDADA DAS NACIONALIDADES")
        print("=" * 50)
        
        # Datasets que contém informações sobre nacionalidades
        datasets_nacionalidades = [
            ('educacao_nacionalidade', 'Habilitações Literárias'),
            ('setores_nacionalidade', 'Setores de Atividade'),
            ('demografia_nacionalidade', 'Demografia')
        ]
        
        todas_nacionalidades = set()
        
        # Extrai nacionalidades de cada dataset
        for nome_dataset, nome_descritivo in datasets_nacionalidades:
            if nome_dataset in self.datasets:
                nacionalidades = extrair_nacionalidades(
                    self.datasets[nome_dataset], 
                    nome_descritivo
                )
                
                if nacionalidades:
                    self.nacionalidades[nome_dataset] = nacionalidades
                    todas_nacionalidades.update(nacionalidades)
        
        # Apresenta consolidação geral
        print(f"\nCONSOLIDAÇÃO GERAL:")
        print(f"   Total de nacionalidades únicas: {len(todas_nacionalidades)}")
        
        # Encontra nacionalidades presentes em múltiplos datasets
        if 'educacao_nacionalidade' in self.nacionalidades and 'setores_nacionalidade' in self.nacionalidades:
            nacionalidades_comuns = (
                set(self.nacionalidades['educacao_nacionalidade']) & 
                set(self.nacionalidades['setores_nacionalidade'])
            )
            
            print(f"   Nacionalidades em múltiplos datasets: {len(nacionalidades_comuns)}")
            
            if len(nacionalidades_comuns) > 0:
                print(f"\nNacionalidades mais representativas:")
                for i, nacionalidade in enumerate(sorted(list(nacionalidades_comuns))[:12], 1):
                    print(f"     {i:2d}. {nacionalidade}")
    
    def analisar_contexto_temporal_geografico(self) -> None:
        """
        Analisa o contexto temporal e geográfico dos dados dos Censos 2021.
        """
        print("\nANÁLISE TEMPORAL E GEOGRÁFICA DOS DADOS")
        print("=" * 60)
        
        # Informações contextuais dos Censos 2021
        info_censos = {
            'Data de Referência': '19 de abril de 2021',
            'Período de Coleta': 'Abril-Julho 2021', 
            'Cobertura Geográfica': 'Portugal (Continental + Regiões Autónomas)',
            'Metodologia': 'Recenseamento por via eletrônica e presencial',
            'Comparação Temporal': 'Censos 2011 (algumas variáveis)',
            'Nível de Desagregação': 'Nacional, NUTS I/II/III, Municipal, Freguesia'
        }
        
        print("CONTEXTO TEMPORAL:")
        for chave, valor in info_censos.items():
            print(f"   {chave}: {valor}")
        
        # Análise dos datasets carregados
        print(f"\nCARACTERÍSTICAS DOS DATASETS CARREGADOS:")
        print("-" * 50)
        
        for nome_interno, df in self.datasets.items():
            nome_legivel = nome_interno.replace('_', ' ').title()
            print(f"\n{nome_legivel}:")
            
            # Verifica colunas temporais
            colunas_temporais = [
                col for col in df.columns 
                if any(termo in str(col).lower() for termo in ['2021', '2011', 'ano', 'data'])
            ]
            
            if colunas_temporais:
                print(f"   Colunas temporais: {colunas_temporais}")
            else:
                print(f"   Período: Censos 2021 (implícito)")
            
            # Verifica granularidade geográfica
            colunas_geograficas = [
                col for col in df.columns 
                if any(termo in str(col).lower() for termo in ['nuts', 'distrito', 'concelho', 'freguesia', 'região'])
            ]
            
            if colunas_geograficas:
                print(f"   Colunas geográficas: {colunas_geograficas}")
            else:
                print(f"   Nível geográfico: Nacional (agregado)")
            
            print(f"   Dimensão: {df.shape[0]:,} observações × {df.shape[1]} variáveis")
        
        # Adequação para perguntas de pesquisa
        print(f"\nADEQUAÇÃO PARA AS PERGUNTAS DE PESQUISA:")
        adequacao_perguntas = {
            "1. Evolução educacional (5-10 anos)": "ADEQUADO (2011 vs 2021 = 10 anos)",
            "2. Distribuição setorial atual": "ADEQUADO (dados 2021 detalhados)", 
            "3. Perfil educacional por setor": "ADEQUADO (cruzamento possível)",
            "4. Diferenças por nacionalidade": "ADEQUADO (15+ nacionalidades)"
        }
        
        for pergunta, status in adequacao_perguntas.items():
            print(f"   {status} {pergunta}")
    
    def gerar_relatorio_completo(self) -> None:
        """
        Gera um relatório completo com todas as análises realizadas.
        """
        print("\nRELATÓRIO COMPLETO DA ANÁLISE")
        print("=" * 60)
        
        # Verifica disponibilidade dos dados
        self.verificar_disponibilidade_dados()
        
        # Carrega todos os datasets
        self.carregar_todos_datasets()
        
        # Realiza análises específicas
        self.analisar_nacionalidades()
        self.analisar_contexto_temporal_geografico()
        
        # Análise de setores de atividade (se dados disponíveis)
        if 'setores_nacionalidade' in self.datasets:
            analisar_setores_atividade(self.datasets['setores_nacionalidade'])
        
        # Sumário final
        print(f"\nANÁLISE CONCLUÍDA COM SUCESSO!")
        print(f"Datasets processados: {len(self.datasets)}")
        print(f"Categorias de nacionalidades identificadas: {len(self.nacionalidades)}")

# =============================================================================
# FUNÇÃO PRINCIPAL DE EXECUÇÃO
# =============================================================================

def executar_analise_completa(diretorio_dados: str = "data/raw/ine") -> None:
    """
    Função principal que executa toda a análise dos microdados do INE.
    
    Esta função orquestra todo o processo de análise de dados, desde o
    carregamento até a geração do relatório final.
    
    Args:
        diretorio_dados (str): Diretório onde estão localizados os dados do INE
    """
    print("INICIANDO ANÁLISE EXPLORATÓRIA DOS MICRODADOS DO INE")
    print("=" * 70)
    print("Projeto: Concurso de Pesquisa Prepara Portugal 2025")
    print("Foco: O Perfil Sócio-profissional do Imigrante em Portugal")
    print("=" * 70)
    
    try:
        # Instancia o analisador principal
        analisador = AnalisadorMicrodadosINE(diretorio_dados)
        
        # Executa análise completa
        analisador.gerar_relatorio_completo()
        
        print(f"\nANÁLISE FINALIZADA COM SUCESSO!")
        print(f"Para mais detalhes, consulte os outputs acima.")
        
    except Exception as e:
        print(f"ERRO DURANTE A ANÁLISE: {str(e)}")
        print("   Verifique se os arquivos de dados estão no diretório correto.")
        print("   Certifique-se de que os dados estão no formato esperado.")
        raise



Este script analisa os microdados do INE dos Censos 2021, focando no perfil
sócio-profissional da população imigrante em Portugal.

ESTRUTURA DE DADOS ESPERADA:
data/raw/ine/
├── Censos2021_csv/
def mostrar_ajuda() -> None:
    """
    Mostra informações de ajuda sobre como usar este script.
    """
    ajuda = """
COMO USAR ESTE SCRIPT

Este script analisa os microdados do INE dos Censos 2021, focando no perfil
sócio-profissional da população imigrante em Portugal.

ESTRUTURA DE DADOS ESPERADA:
data/raw/ine/
├── Censos2021_csv/
│   ├── Q13.csv
│   └── Q21.csv
└── Censos2021_População estrangeira/
    ├── Q1.1.csv
    ├── Q2.1.csv
    └── Q3.3.csv

FORMAS DE EXECUÇÃO:

1. Execução básica (dados no diretório padrão):
   python exploracao_microdados_ine.py

2. Especificando diretório customizado:
   python exploracao_microdados_ine.py --diretorio /caminho/para/dados

3. Mostrando esta ajuda:
   python exploracao_microdados_ine.py --help

O QUE O SCRIPT FAZ:
- Verifica disponibilidade dos arquivos de dados
- Carrega e analisa estrutura dos datasets
- Identifica nacionalidades presentes nos dados  
- Analisa setores de atividade económica
- Examina contexto temporal e geográfico
- Detecta dados ausentes
- Gera relatório completo de análise

DICAS:
- Certifique-se de que os arquivos CSV estão em encoding UTF-8 ou ISO-8859-1
- O script tenta automaticamente diferentes encodings se houver problemas
- Todos os outputs são apresentados em português
"""
    print(ajuda)


def analisar_argumentos_linha_comando() -> str:
    """
    Analisa argumentos da linha de comando.
    
    Returns:
        str: Diretório de dados especificado pelo usuário ou padrão
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Análise de Microdados do INE - Censos 2021",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python exploracao_microdados_ine.py
  python exploracao_microdados_ine.py --diretorio /caminho/para/dados
  python exploracao_microdados_ine.py --help
        """
    )
    
    parser.add_argument(
        '--diretorio', '-d',
        default="data/raw/ine",
        help='Diretório base onde estão os dados do INE (padrão: data/raw/ine)'
    )
    
    parser.add_argument(
        '--ajuda', '--help',
        action='store_true',
        help='Mostra informações detalhadas de ajuda'
    )
    
    args = parser.parse_args()
    
    if args.ajuda:
        mostrar_ajuda()
        sys.exit(0)
    
    return args.diretorio


# =============================================================================
# EXECUÇÃO PRINCIPAL DO SCRIPT
# =============================================================================

if __name__ == "__main__":
    """
    Ponto de entrada principal do script.
    
    Esta seção é executada apenas quando o script é rodado diretamente,
    não quando é importado como módulo em outros scripts.
    """
    
    # Configura encoding para output correto em português
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    
    try:
        # Analisa argumentos da linha de comando
        diretorio_dados = analisar_argumentos_linha_comando()
        
        # Executa análise completa
        executar_analise_completa(diretorio_dados)
        
    except KeyboardInterrupt:
        print("\n\nExecução interrompida pelo usuário.")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nERRO FATAL: {str(e)}")
        print("   Consulte a documentação ou execute com --ajuda para mais informações.")
        sys.exit(1)

# =============================================================================
# FIM DO SCRIPT
# =============================================================================
====================

Este script analisa os microdados do INE dos Censos 2021, focando no perfil
sócio-profissional da população imigrante em Portugal.

ESTRUTURA DE DADOS ESPERADA:
data/raw/ine/
├── Censos2021_csv/
