"""
============================================================
PARTE 1: IMPORTS E CONFIGURAÇÕES
Pipeline ETL - AIMA Integrado (DP-02-A)
Google Colab
============================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
from google.colab import files
import io
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURAÇÕES GLOBAIS
# ============================================================

class Config:
    """Configurações centralizadas do pipeline ETL AIMA"""
    
    # Informações do projeto
    PROJETO_NOME = "ETL AIMA - Integração RIFA/RMA"
    VERSAO = "1.0"
    ANOS_REFERENCIA = [2020, 2021, 2022, 2023, 2024]
    FONTE_DADOS = "AIMA - RIFA/RMA 2020-2024"
    
    # Arquivos de entrada necessários (por ano)
    ARQUIVOS_POR_ANO = [
        'ConcessaoTitulosResidencia.csv',
        'ConcessaoTitulosDespachos.csv',
        'ConcessaoTitulosDistribuicaoEtaria.csv',
        'ConcessaoTitulosMotivo.csv',
        'DespachosDescricao.csv',
        'PopulacaoEstrangeiraResidente.csv',
        'PopulacaoEstrangeiraResidenteEvolucao.csv',
        'PopulacaoResidenteDistribuicaoEtaria.csv'
    ]
    
    # Arquivo adicional de 2024
    ARQUIVOS_2024_EXTRA = [
        'ConcessaoTitulosAtividadeProfissional.csv'
    ]
    
    # Mapeamento de fontes por ano
    FONTES_ANO = {
        2020: 'RIFA',
        2021: 'RIFA',
        2022: 'RIFA',
        2023: 'RMA',
        2024: 'RMA'
    }
    
    # Tipos de relatório
    TIPOS_RELATORIO = [
        'Concessão de Títulos',
        'População Estrangeira Residente',
        'População Residente - Distribuição Etária'
    ]
    
    # Códigos de despachos conhecidos
    DESPACHOS_CONHECIDOS = {
        'AP': 'Autorização de Permanência',
        'VLD': 'Prorrogação de Validade',
        'TR': 'Título de Residência',
        'CPLP': 'Acordo CPLP',
        'OUTRO': 'Outros Despachos'
    }
    
    # Motivos de concessão padronizados
    MOTIVOS_CONCESSAO = {
        'ATIVIDADE_PROFISSIONAL': {
            'nome': 'Atividade Profissional',
            'categoria': 'Trabalho',
            'variantes': [
                'Atividade profissional subordinada',
                'Atividade profissional independente',
                'Trabalho',
                'Profissional'
            ]
        },
        'ESTUDO': {
            'nome': 'Estudo',
            'categoria': 'Educação',
            'variantes': [
                'Estudo',
                'Estágio profissional',
                'Investigação',
                'Ensino'
            ]
        },
        'REAGRUPAMENTO_FAMILIAR': {
            'nome': 'Reagrupamento Familiar',
            'categoria': 'Família',
            'variantes': [
                'Reagrupamento familiar',
                'Familiar',
                'Família'
            ]
        },
        'AR_CPLP': {
            'nome': 'Acordo de Residência CPLP',
            'categoria': 'Internacional',
            'variantes': [
                'AR - CPLP',
                'AR CPLP',
                'CPLP',
                'Acordo CPLP'
            ]
        },
        'OUTROS': {
            'nome': 'Outros Motivos',
            'categoria': 'Diversos',
            'variantes': [
                'Outros',
                'Investimento',
                'Aposentado',
                'Atividade religiosa',
                'Visto gold'
            ]
        }
    }
    
    # Grupos etários AIMA
    GRUPOS_ETARIOS_AIMA = [
        {'faixa': '0-14 anos', 'descricao': 'Crianças e adolescentes'},
        {'faixa': '15-24 anos', 'descricao': 'Jovens adultos'},
        {'faixa': '25-34 anos', 'descricao': 'Adultos jovens'},
        {'faixa': '35-44 anos', 'descricao': 'Adultos'},
        {'faixa': '45-54 anos', 'descricao': 'Adultos maduros'},
        {'faixa': '55-64 anos', 'descricao': 'Pré-reforma'},
        {'faixa': '65+ anos', 'descricao': 'Idosos'},
        {'faixa': 'Total', 'descricao': 'Todas as faixas'}
    ]
    
    # Codificação de dados
    ENCODING = 'utf-8'
    DECIMAL_SEPARATOR = ','
    THOUSANDS_SEPARATOR = '.'
    
    # Validação
    VALIDAR_FKS = True
    VALIDAR_TIPOS = True
    VALIDAR_RANGES = True
    VALIDAR_INTEGRAÇÃO = True  # Nova validação para integração com ETL_EDUCACAO
    
    # Formato de saída
    OUTPUT_ENCODING = 'utf-8'
    OUTPUT_SEPARATOR = ','
    OUTPUT_INDEX = False
    
    # Tabelas a serem geradas (12 tabelas AIMA)
    TABELAS_DIMENSOES = [
        'Dim_AnoRelatorio',
        'Dim_TipoRelatorio',
        'Dim_Despacho',
        'Dim_MotivoConcessao',
        'Dim_NacionalidadeAIMA'
    ]
    
    TABELAS_FATOS = [
        'Fact_ConcessoesPorNacionalidadeSexo',
        'Fact_ConcessoesPorDespacho',
        'Fact_ConcessoesPorMotivoNacionalidade',
        'Fact_PopulacaoEstrangeiraPorNacionalidadeSexo',
        'Fact_DistribuicaoEtariaConcessoes',
        'Fact_EvolucaoPopulacaoEstrangeira',
        'Fact_PopulacaoResidenteEtaria'
    ]
    
    @classmethod
    def get_todas_tabelas(cls):
        """Retorna lista de todas as tabelas"""
        return cls.TABELAS_DIMENSOES + cls.TABELAS_FATOS
    
    @classmethod
    def print_configuracoes(cls):
        """Imprime configurações no console"""
        print("=" * 60)
        print(f"{cls.PROJETO_NOME} - Versão {cls.VERSAO}")
        print("=" * 60)
        print(f"Fonte de Dados: {cls.FONTE_DADOS}")
        print(f"Anos de Referência: {cls.ANOS_REFERENCIA[0]}-{cls.ANOS_REFERENCIA[-1]}")
        print(f"Total de Tabelas: {len(cls.get_todas_tabelas())}")
        print(f"  - Dimensões: {len(cls.TABELAS_DIMENSOES)}")
        print(f"  - Fatos: {len(cls.TABELAS_FATOS)}")
        print(f"Validação FK: {cls.VALIDAR_FKS}")
        print(f"Validação Integração: {cls.VALIDAR_INTEGRAÇÃO}")
        print("=" * 60)


# ============================================================
# CONSTANTES E MAPEAMENTOS
# ============================================================

class Constantes:
    """Constantes utilizadas no pipeline AIMA"""
    
    # Mapeamento de variações de nomes de nacionalidades
    # Para padronização com ETL_EDUCACAO
    NACIONALIDADES_VARIANTES = {
        'Brasil': ['Brasil', 'Brazil', 'Brazilian'],
        'Angola': ['Angola', 'Angolan'],
        'Cabo Verde': ['Cabo Verde', 'Cape Verde'],
        'Guiné-Bissau': ['Guiné-Bissau', 'Guinea-Bissau', 'Guiné Bissau'],
        'São Tomé e Príncipe': ['São Tomé e Príncipe', 'Sao Tome e Principe', 'S. Tomé e Príncipe'],
        'Moçambique': ['Moçambique', 'Mozambique'],
        'Portugal': ['Portugal', 'Portuguese'],
        'Espanha': ['Espanha', 'Spain', 'Spanish'],
        'França': ['França', 'France', 'French'],
        'Reino Unido': ['Reino Unido', 'United Kingdom', 'UK', 'British', 'Reino Unido (British Subject)'],
        'Itália': ['Itália', 'Italy', 'Italian'],
        'Alemanha': ['Alemanha', 'Germany', 'German'],
        'Roménia': ['Roménia', 'Romania', 'Romenia', 'Romanian'],
        'Ucrânia': ['Ucrânia', 'Ukraine', 'Ukrainian'],
        'República da Moldávia': ['República da Moldávia', 'Moldova', 'Moldávia'],
        'Rússia': ['Rússia', 'Russia', 'Russian', 'Federação Russa'],
        'República Popular da China': ['República Popular da China', 'China', 'Chinese'],
        'Índia': ['Índia', 'India', 'Indian'],
        'Paquistão': ['Paquistão', 'Pakistan', 'Pakistani'],
        'Bangladesh': ['Bangladesh', 'Bangladeshi'],
        'Nepal': ['Nepal', 'Nepalese']
    }
    
    # Continentes
    CONTINENTES = {
        'PALOP': ['Angola', 'Cabo Verde', 'Guiné-Bissau', 'Moçambique', 'São Tomé e Príncipe'],
        'CPLP': ['Angola', 'Brasil', 'Cabo Verde', 'Guiné-Bissau', 'Moçambique', 
                 'São Tomé e Príncipe', 'Timor-Leste', 'Guiné Equatorial'],
        'UE': ['Alemanha', 'Espanha', 'França', 'Itália', 'Roménia', 'Portugal'],
        'LESTE_EUROPEU': ['Ucrânia', 'República da Moldávia', 'Rússia', 'Bulgária'],
        'ASIA': ['República Popular da China', 'Índia', 'Paquistão', 'Bangladesh', 'Nepal'],
        'AMERICA': ['Brasil', 'Venezuela', 'EUA', 'Canadá']
    }
    
    # Tipos de sexo (compatível com ETL_EDUCACAO)
    SEXOS = [
        {'id': 1, 'tipo': 'Masculino'},
        {'id': 2, 'tipo': 'Feminino'}
    ]
    
    # Métricas de evolução populacional
    METRICAS_EVOLUCAO = [
        'titulos_residencia',
        'concessao_ap',
        'prorrogacao_vld',
        'total',
        'variacao_percentual'
    ]


# ============================================================
# UTILITÁRIOS DE FORMATAÇÃO
# ============================================================

class Formatadores:
    """Funções utilitárias para formatação de dados AIMA"""
    
    @staticmethod
    def limpar_numero(valor):
        """Limpa e converte string numérica para número"""
        if pd.isna(valor) or valor == '':
            return None
        
        valor_str = str(valor)
        valor_str = valor_str.replace('%', '').replace(' ', '').strip()
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            return float(valor_str)
        except:
            return None
    
    @staticmethod
    def normalizar_nacionalidade(nome):
        """Normaliza nome de nacionalidade para padrão"""
        if pd.isna(nome):
            return None
        
        nome_limpo = str(nome).strip()
        
        # Buscar variante conhecida
        for padrao, variantes in Constantes.NACIONALIDADES_VARIANTES.items():
            if nome_limpo in variantes:
                return padrao
        
        # Retornar nome limpo se não encontrar
        return nome_limpo
    
    @staticmethod
    def normalizar_motivo(texto):
        """Normaliza motivo de concessão"""
        if pd.isna(texto):
            return 'OUTROS'
        
        texto_limpo = str(texto).strip().lower()
        
        # Buscar categoria correspondente
        for codigo, info in Config.MOTIVOS_CONCESSAO.items():
            variantes_lower = [v.lower() for v in info['variantes']]
            if texto_limpo in variantes_lower or any(v in texto_limpo for v in variantes_lower):
                return codigo
        
        return 'OUTROS'
    
    @staticmethod
    def extrair_faixa_etaria(texto):
        """Extrai faixa etária padronizada"""
        if pd.isna(texto):
            return None
        
        texto_str = str(texto).strip()
        
        # Mapeamento de variações
        mapeamentos = {
            '0-14': '0-14 anos',
            '15-24': '15-24 anos',
            '25-34': '25-34 anos',
            '35-44': '35-44 anos',
            '45-54': '45-54 anos',
            '55-64': '55-64 anos',
            '65+': '65+ anos',
            'Total': 'Total'
        }
        
        for chave, valor in mapeamentos.items():
            if chave in texto_str:
                return valor
        
        return texto_str
    
    @staticmethod
    def calcular_variacao_percentual(valor_atual, valor_anterior):
        """Calcula variação percentual entre dois valores"""
        if pd.isna(valor_anterior) or valor_anterior == 0:
            return None
        
        return ((valor_atual - valor_anterior) / valor_anterior) * 100
    
    @staticmethod
    def formatar_timestamp():
        """Retorna timestamp formatado"""
        return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


# ============================================================
# LOGGER PERSONALIZADO
# ============================================================

class Logger:
    """Sistema de logging para o pipeline AIMA"""
    
    def __init__(self, nome_modulo="ETL-AIMA"):
        self.nome_modulo = nome_modulo
        self.contador_erros = 0
        self.contador_avisos = 0
        self.contador_integracao = 0  # Novo: contador de problemas de integração
    
    def info(self, mensagem):
        """Log de informação"""
        print(f"[INFO] {self.nome_modulo}: {mensagem}")
    
    def sucesso(self, mensagem):
        """Log de sucesso"""
        print(f"✓ {mensagem}")
    
    def erro(self, mensagem):
        """Log de erro"""
        self.contador_erros += 1
        print(f"✗ [ERRO] {self.nome_modulo}: {mensagem}")
    
    def aviso(self, mensagem):
        """Log de aviso"""
        self.contador_avisos += 1
        print(f"⚠ [AVISO] {self.nome_modulo}: {mensagem}")
    
    def integracao(self, mensagem):
        """Log específico para integração"""
        self.contador_integracao += 1
        print(f"🔗 [INTEGRAÇÃO] {self.nome_modulo}: {mensagem}")
    
    def secao(self, titulo):
        """Imprime seção"""
        print("\n" + "=" * 60)
        print(titulo.center(60))
        print("=" * 60)
    
    def subsecao(self, titulo):
        """Imprime subseção"""
        print("\n" + "-" * 60)
        print(titulo)
        print("-" * 60)
    
    def progresso(self, atual, total, descricao=""):
        """Mostra progresso"""
        percentual = (atual / total) * 100
        barra = "█" * int(percentual / 2) + "░" * (50 - int(percentual / 2))
        print(f"\r[{barra}] {percentual:.1f}% - {descricao}", end='')
        if atual == total:
            print()
    
    def resumo_final(self):
        """Exibe resumo de erros e avisos"""
        print("\n" + "=" * 60)
        print("RESUMO DE EXECUÇÃO - AIMA")
        print("=" * 60)
        print(f"Total de Erros: {self.contador_erros}")
        print(f"Total de Avisos: {self.contador_avisos}")
        print(f"Questões de Integração: {self.contador_integracao}")
        
        if self.contador_erros == 0:
            print("✓ Processamento concluído sem erros!")
        else:
            print("✗ Processamento concluído com erros. Verifique os logs acima.")


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    # Teste de configurações
    Config.print_configuracoes()
    
    # Teste de formatadores
    print("\nTeste de Formatadores:")
    print(f"Nacionalidade: {Formatadores.normalizar_nacionalidade('Brasil')}")
    print(f"Motivo: {Formatadores.normalizar_motivo('Atividade profissional subordinada')}")
    print(f"Faixa etária: {Formatadores.extrair_faixa_etaria('25-34')}")
    print(f"Variação: {Formatadores.calcular_variacao_percentual(120, 100)}%")
    
    # Teste de logger
    logger = Logger("TESTE-AIMA")
    logger.secao("TESTE DE LOGGER AIMA")
    logger.info("Mensagem de informação")
    logger.sucesso("Operação bem-sucedida")
    logger.aviso("Mensagem de aviso")
    logger.integracao("Teste de integração")
    logger.progresso(50, 100, "Processando...")
    logger.resumo_final()
    
    print("\n✓ Módulo parte_01_imports_config.py (AIMA) carregado com sucesso!")
