"""
============================================================
PARTE 1: IMPORTS E CONFIGURAÇÕES
Pipeline ETL - Laboral (DP-01-B)
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
    """Configurações centralizadas do pipeline ETL Laboral"""
    
    # Informações do projeto
    PROJETO_NOME = "ETL INE 2011 - Laboral"
    VERSAO = "1.0"
    ANO_REFERENCIA = 2011
    FONTE_DADOS = "INE Censos 2011"
    
    # Arquivos de entrada necessários (mesmos do ETL Educação)
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
    
    # Categorias de extração laboral
    CATEGORIAS_CONDICAO_ECONOMICA = [
        'CONDIÇÃO PERANTE O TRABALHO',
        'POPULAÇÃO EMPREGADA',
        'POPULAÇÃO DESEMPREGADA'
    ]
    
    CATEGORIAS_PROFISSAO = [
        'PROFISSÃO (Grande Grupo)',
        'PROFISSÃO (1º dígito)',
    ]
    
    CATEGORIAS_SETOR = [
        'ATIVIDADE ECONÓMICA (Secção)',
        'ATIVIDADE ECONÓMICA (CAE Rev.3)'
    ]
    
    # Mapeamento de grandes grupos profissionais (CNP)
    GRANDES_GRUPOS_PROFISSIONAIS = {
        0: 'Profissões das Forças Armadas',
        1: 'Representantes do poder legislativo e de órgãos executivos, dirigentes, diretores e gestores executivos',
        2: 'Especialistas das atividades intelectuais e científicas',
        3: 'Técnicos e profissões de nível intermédio',
        4: 'Pessoal administrativo',
        5: 'Trabalhadores dos serviços pessoais, de proteção e segurança e vendedores',
        6: 'Agricultores e trabalhadores qualificados da agricultura, da pesca e da floresta',
        7: 'Trabalhadores qualificados da indústria, construção e artífices',
        8: 'Operadores de instalações e máquinas e trabalhadores da montagem',
        9: 'Trabalhadores não qualificados'
    }
    
    # Setores econômicos (CAE Rev.3 - Secções)
    SETORES_ECONOMICOS = {
        'A': 'Agricultura, produção animal, caça, floresta e pesca',
        'B': 'Indústrias extrativas',
        'C': 'Indústrias transformadoras',
        'D': 'Eletricidade, gás, vapor, água quente e fria e ar frio',
        'E': 'Captação, tratamento e distribuição de água; saneamento, gestão de resíduos e despoluição',
        'F': 'Construção',
        'G': 'Comércio por grosso e a retalho; reparação de veículos automóveis e motociclos',
        'H': 'Transportes e armazenagem',
        'I': 'Alojamento, restauração e similares',
        'J': 'Atividades de informação e de comunicação',
        'K': 'Atividades financeiras e de seguros',
        'L': 'Atividades imobiliárias',
        'M': 'Atividades de consultoria, científicas, técnicas e similares',
        'N': 'Atividades administrativas e dos serviços de apoio',
        'O': 'Administração Pública e Defesa; Segurança Social Obrigatória',
        'P': 'Educação',
        'Q': 'Atividades de saúde humana e apoio social',
        'R': 'Atividades artísticas, de espetáculos, desportivas e recreativas',
        'S': 'Outras atividades de serviços',
        'T': 'Atividades das famílias empregadoras de pessoal doméstico',
        'U': 'Atividades dos organismos internacionais e outras instituições extra-territoriais'
    }
    
    # Condições perante o trabalho
    CONDICOES_TRABALHO = [
        'População ativa',
        'População empregada',
        'População desempregada',
        'Estudantes',
        'Reformados',
        'Domésticos',
        'Outros inativos'
    ]
    
    # Situações profissionais
    SITUACOES_PROFISSIONAIS = [
        'Patrão',
        'Trabalhador por conta própria',
        'Trabalhador por conta de outrem',
        'Membro ativo de cooperativa de produção',
        'Trabalhador familiar não remunerado',
        'Outra situação'
    ]
    
    # Fontes de rendimento
    FONTES_RENDIMENTO = [
        'Trabalho por conta de outrem',
        'Trabalho por conta própria',
        'Pensões (reforma, sobrevivência, viuvez)',
        'Subsídios (desemprego, doença)',
        'Rendimentos de propriedade ou empresa',
        'Transferências familiares',
        'Outros rendimentos'
    ]
    
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
    
    # Tabelas a serem geradas (15 tabelas laborais)
    TABELAS_DIMENSOES = [
        'Dim_CondicaoEconomica',
        'Dim_GrupoProfissional',
        'Dim_ProfissaoDigito1',
        'Dim_SetorEconomico',
        'Dim_SituacaoProfissional',
        'Dim_FonteRendimento',
        'Dim_RegiaoNUTS'
    ]
    
    TABELAS_FATOS = [
        'Fact_PopulacaoPorCondicao',
        'Fact_EmpregadosPorProfissao',
        'Fact_EmpregadosPorSetor',
        'Fact_EmpregadosPorSituacao',
        'Fact_EmpregadosProfSexo',
        'Fact_EmpregadosRegiaoSetor',
        'Fact_PopulacaoTrabalhoEscolaridade',
        'Fact_PopulacaoRendimentoRegiao'
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
    """Constantes utilizadas no pipeline laboral"""
    
    # Anos para análise temporal
    ANOS = {
        'ANO_2011': 2011,
        'ANO_2001': 2001
    }
    
    # Continentes (reutilizar do ETL Educação)
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
    
    # Códigos ISO de países
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
    
    # Regiões NUTS II de Portugal
    REGIOES_NUTS_II = {
        'PT11': {'nome': 'Norte', 'nivel': 'NUTS II'},
        'PT16': {'nome': 'Centro', 'nivel': 'NUTS II'},
        'PT17': {'nome': 'Área Metropolitana de Lisboa', 'nivel': 'NUTS II'},
        'PT18': {'nome': 'Alentejo', 'nivel': 'NUTS II'},
        'PT15': {'nome': 'Algarve', 'nivel': 'NUTS II'},
        'PT20': {'nome': 'Região Autónoma dos Açores', 'nivel': 'NUTS II'},
        'PT30': {'nome': 'Região Autónoma da Madeira', 'nivel': 'NUTS II'}
    }
    
    # Regiões NUTS III (simplificado)
    REGIOES_NUTS_III = {
        'PT111': 'Alto Minho',
        'PT112': 'Cávado',
        'PT119': 'Ave',
        'PT11A': 'Área Metropolitana do Porto',
        'PT11B': 'Alto Tâmega',
        'PT11C': 'Tâmega e Sousa',
        'PT11D': 'Douro',
        'PT11E': 'Terras de Trás-os-Montes',
        'PT16B': 'Oeste',
        'PT16D': 'Região de Aveiro',
        'PT16E': 'Região de Coimbra',
        'PT16F': 'Região de Leiria',
        'PT16G': 'Viseu Dão Lafões',
        'PT16H': 'Beira Baixa',
        'PT16I': 'Médio Tejo',
        'PT16J': 'Beiras e Serra da Estrela',
        'PT170': 'Área Metropolitana de Lisboa',
        'PT181': 'Alentejo Litoral',
        'PT184': 'Baixo Alentejo',
        'PT185': 'Lezíria do Tejo',
        'PT186': 'Alto Alentejo',
        'PT187': 'Alentejo Central',
        'PT150': 'Algarve',
        'PT200': 'Região Autónoma dos Açores',
        'PT300': 'Região Autónoma da Madeira'
    }
    
    # Taxa de desemprego de referência (%)
    TAXA_DESEMPREGO_REFERENCIA_2011 = 12.9
    
    # Tipos de sexo
    SEXOS = [
        {'id': 1, 'tipo': 'Masculino'},
        {'id': 2, 'tipo': 'Feminino'}
    ]


# ============================================================
# UTILITÁRIOS DE FORMATAÇÃO (Reutilizar do ETL Educação)
# ============================================================

class Formatadores:
    """Funções utilitárias para formatação de dados"""
    
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
    def extrair_percentual(texto):
        """Extrai percentual de texto como '12,5%' -> 12.5"""
        if pd.isna(texto):
            return None
        
        texto_str = str(texto).replace('%', '').strip()
        return Formatadores.limpar_numero(texto_str)
    
    @staticmethod
    def normalizar_nome_profissao(nome):
        """Normaliza nome de profissão"""
        if pd.isna(nome):
            return None
        
        # Remover espaços extras
        nome_limpo = ' '.join(str(nome).split())
        return nome_limpo.strip()
    
    @staticmethod
    def normalizar_codigo_cae(codigo):
        """Normaliza código CAE (Classificação de Atividades Econômicas)"""
        if pd.isna(codigo):
            return None
        
        codigo_str = str(codigo).strip().upper()
        return codigo_str
    
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
# LOGGER PERSONALIZADO (Reutilizar do ETL Educação)
# ============================================================

class Logger:
    """Sistema de logging para o pipeline"""
    
    def __init__(self, nome_modulo="ETL-LABORAL"):
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
            print()
    
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
    print(f"Percentual: {Formatadores.extrair_percentual('12,5%')}")
    
    # Teste de logger
    logger = Logger("TESTE-LABORAL")
    logger.secao("TESTE DE LOGGER")
    logger.info("Mensagem de informação")
    logger.sucesso("Operação bem-sucedida")
    logger.aviso("Mensagem de aviso")
    logger.progresso(50, 100, "Processando...")
    logger.resumo_final()
    
    print("\n✓ Módulo parte_01_imports_config.py carregado com sucesso!")
