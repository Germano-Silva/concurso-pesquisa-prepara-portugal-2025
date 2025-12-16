"""
============================================================
PARTE 1: IMPORTS E CONFIGURAÇÕES
Pipeline ETL - Educação (DP-01-A)
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
    """Configurações centralizadas do pipeline ETL"""
    
    # Informações do projeto
    PROJETO_NOME = "ETL INE 2011 - Educação"
    VERSAO = "1.0"
    ANO_REFERENCIA = 2011
    FONTE_DADOS = "INE Censos 2011"
    
    # Arquivos de entrada necessários
    ARQUIVOS_PAISES = [
        'Angola.csv',
        'Brasil.csv',
        'Cabo Verde.csv',
        'Espanha.csv',
        'França.csv',
        'Guiné-Bissau.csv',
        'Reino Unido.csv',
        'República da Moldávia.csv',
        'República Popular da China.csv',
        'Romenia.csv',
        'Sao tome e Principe.csv',
        'Ucrânia.csv'
    ]
    
    ARQUIVOS_AGREGADOS = [
        'Educação e Economia.csv',
        'Demografia e Geografia.csv'
    ]
    
    # Mapeamento de categorias para extração
    CATEGORIAS_EDUCACAO = [
        'NÍVEL DE ENSINO (15-64 anos)',
        'NÍVEL DE ENSINO (45-66 anos)'  # Reino Unido usa faixa diferente
    ]
    
    # Estrutura de níveis educacionais
    NIVEIS_EDUCACAO = {
        1: {
            'nome': 'Inferior ao básico 3º ciclo',
            'categoria': 'Baixa',
            'ordem': 1
        },
        2: {
            'nome': 'Básico 3º ciclo',
            'categoria': 'Média-Baixa',
            'ordem': 2
        },
        3: {
            'nome': 'Secundário e pós-secundário',
            'categoria': 'Média-Alta',
            'ordem': 3
        },
        4: {
            'nome': 'Superior',
            'categoria': 'Alta',
            'ordem': 4
        }
    }
    
    # Codificação de dados
    ENCODING = 'utf-8'
    DECIMAL_SEPARATOR = ','
    THOUSANDS_SEPARATOR = '.'
    
    # Validação
    VALIDAR_FKS = True
    VALIDAR_TIPOS = True
    VALIDAR_RANGES = True
    
    # Formato de saída
    OUTPUT_ENCODING = 'utf-8'
    OUTPUT_SEPARATOR = ','
    OUTPUT_INDEX = False
    
    # Tabelas a serem geradas (17 tabelas)
    TABELAS_DIMENSOES = [
        'Dim_PopulacaoResidente',
        'Dim_Nacionalidade',
        'Dim_Localidade',
        'Dim_Sexo',
        'Dim_GrupoEtario',
        'Dim_NivelEducacao',
        'Dim_MapeamentoNacionalidades'
    ]
    
    TABELAS_FATOS = [
        'Fact_PopulacaoPorNacionalidade',
        'Fact_PopulacaoPorNacionalidadeSexo',
        'Fact_PopulacaoPorLocalidade',
        'Fact_PopulacaoPorLocalidadeNacionalidade',
        'Fact_PopulacaoPorGrupoEtario',
        'Fact_EvolucaoTemporal',
        'Fact_NacionalidadePrincipal',
        'Fact_DistribuicaoGeografica',
        'Fact_PopulacaoEducacao',
        'Fact_EstatisticasEducacao'
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
        print(f"Ano de Referência: {cls.ANO_REFERENCIA}")
        print(f"Total de Tabelas: {len(cls.get_todas_tabelas())}")
        print(f"  - Dimensões: {len(cls.TABELAS_DIMENSOES)}")
        print(f"  - Fatos: {len(cls.TABELAS_FATOS)}")
        print(f"Validação FK: {cls.VALIDAR_FKS}")
        print("=" * 60)


# ============================================================
# CONSTANTES E MAPEAMENTOS
# ============================================================

class Constantes:
    """Constantes utilizadas no pipeline"""
    
    # Anos para análise temporal
    ANOS = {
        'ANO_2011': 2011,
        'ANO_2001': 2001
    }
    
    # Continentes
    CONTINENTES = {
        'Angola': 'África',
        'Brasil': 'América do Sul',
        'Cabo Verde': 'África',
        'Espanha': 'Europa',
        'França': 'Europa',
        'Guiné-Bissau': 'África',
        'Reino Unido': 'Europa',
        'República da Moldávia': 'Europa',
        'República Popular da China': 'Ásia',
        'Roménia': 'Europa',
        'São Tomé e Príncipe': 'África',
        'Ucrânia': 'Europa'
    }
    
    # Códigos ISO de países (ISO 3166-1 alpha-3)
    CODIGOS_PAIS = {
        'Angola': 'AGO',
        'Brasil': 'BRA',
        'Cabo Verde': 'CPV',
        'Espanha': 'ESP',
        'França': 'FRA',
        'Guiné-Bissau': 'GNB',
        'Reino Unido': 'GBR',
        'República da Moldávia': 'MDA',
        'República Popular da China': 'CHN',
        'Roménia': 'ROU',
        'São Tomé e Príncipe': 'STP',
        'Ucrânia': 'UKR'
    }
    
    # Grupos etários padrão
    GRUPOS_ETARIOS = [
        {'id': 1, 'faixa': '0-14 anos', 'descricao': 'Crianças e adolescentes'},
        {'id': 2, 'faixa': '15-64 anos', 'descricao': 'População ativa'},
        {'id': 3, 'faixa': '65+ anos', 'descricao': 'Idosos'},
        {'id': 4, 'faixa': '45-66 anos', 'descricao': 'Meia-idade (específico Reino Unido)'}
    ]
    
    # Tipos de sexo
    SEXOS = [
        {'id': 1, 'tipo': 'Masculino'},
        {'id': 2, 'tipo': 'Feminino'}
    ]


# ============================================================
# UTILITÁRIOS DE FORMATAÇÃO
# ============================================================

class Formatadores:
    """Funções utilitárias para formatação de dados"""
    
    @staticmethod
    def limpar_numero(valor):
        """
        Limpa e converte string numérica para número
        Exemplos: '1.234,56' -> 1234.56, '3,7%' -> 3.7
        """
        if pd.isna(valor) or valor == '':
            return None
        
        # Converter para string
        valor_str = str(valor)
        
        # Remover símbolos
        valor_str = valor_str.replace('%', '').replace(' ', '').strip()
        
        # Trocar separadores
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            return float(valor_str)
        except:
            return None
    
    @staticmethod
    def extrair_ano(texto):
        """Extrai ano de texto como '34,9 anos' -> 34.9"""
        if pd.isna(texto):
            return None
        
        texto_str = str(texto).replace('anos', '').strip()
        return Formatadores.limpar_numero(texto_str)
    
    @staticmethod
    def normalizar_nome_nacionalidade(nome):
        """Normaliza nome de nacionalidade"""
        if pd.isna(nome):
            return None
        
        # Mapeamento de variações
        mapeamento = {
            'Romenia': 'Roménia',
            'Romania': 'Roménia',
            'Sao tome e Principe': 'São Tomé e Príncipe',
            'Sao Tome e Principe': 'São Tomé e Príncipe',
            'Republica da Moldavia': 'República da Moldávia',
            'Republica Popular da China': 'República Popular da China'
        }
        
        nome_limpo = str(nome).strip()
        return mapeamento.get(nome_limpo, nome_limpo)
    
    @staticmethod
    def formatar_timestamp():
        """Retorna timestamp formatado"""
        return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    @staticmethod
    def formatar_percentual(valor, casas_decimais=2):
        """Formata valor como percentual"""
        if pd.isna(valor):
            return None
        return round(float(valor), casas_decimais)


# ============================================================
# LOGGER PERSONALIZADO
# ============================================================

class Logger:
    """Sistema de logging para o pipeline"""
    
    def __init__(self, nome_modulo="ETL"):
        self.nome_modulo = nome_modulo
        self.contador_erros = 0
        self.contador_avisos = 0
    
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
            print()  # Nova linha no final
    
    def resumo_final(self):
        """Exibe resumo de erros e avisos"""
        print("\n" + "=" * 60)
        print("RESUMO DE EXECUÇÃO")
        print("=" * 60)
        print(f"Total de Erros: {self.contador_erros}")
        print(f"Total de Avisos: {self.contador_avisos}")
        
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
    print(f"Número: {Formatadores.limpar_numero('1.234,56')}")
    print(f"Percentual: {Formatadores.limpar_numero('3,7%')}")
    print(f"Ano: {Formatadores.extrair_ano('34,9 anos')}")
    
    # Teste de logger
    logger = Logger("TESTE")
    logger.secao("TESTE DE LOGGER")
    logger.info("Mensagem de informação")
    logger.sucesso("Operação bem-sucedida")
    logger.aviso("Mensagem de aviso")
    logger.erro("Mensagem de erro")
    logger.progresso(50, 100, "Processando...")
    logger.resumo_final()
    
    print("\n✓ Módulo parte_01_imports_config.py carregado com sucesso!")
