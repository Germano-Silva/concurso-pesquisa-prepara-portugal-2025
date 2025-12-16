# -*- coding: utf-8 -*-
"""
ETL_LABORAL_CONSOLIDADO - v1.0
Pipeline ETL para dados laborais dos Censos 2021
Segue Diagrama ER Star Schema Unificado
Versao Hibrida e Portavel - Windows Compatible
Autor: Germano Silva | Data: Dezembro 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import io
import sys
import warnings
import zipfile
warnings.filterwarnings('ignore')

# Configurar output UTF-8 para Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Deteccao automatica de ambiente
try:
    from google.colab import files
    AMBIENTE = 'COLAB'
    print("[COLAB] Ambiente detectado: Google Colab")
except ImportError:
    AMBIENTE = 'LOCAL'
    print("[LOCAL] Ambiente detectado: Execucao Local")


class ConfigAmbiente:
    """Configuracoes que se adaptam ao ambiente de execucao"""
    
    @staticmethod
    def get_script_dir():
        """Retorna o diretorio onde o script esta localizado"""
        return Path(__file__).parent.absolute()
    
    @staticmethod
    def get_pasta_dados_laborais():
        """Retorna pasta com dados laborais processados DP-01-B1"""
        if AMBIENTE == 'COLAB':
            return Path('/content/DP-01-B1')
        else:
            return ConfigAmbiente.get_script_dir().parent / 'data' / 'processed' / 'DP-01-B' / 'DP-01-B1'
    
    @staticmethod
    def get_pasta_dados_base():
        """Retorna pasta com dados base (Nacionalidade, Sexo, etc)"""
        if AMBIENTE == 'COLAB':
            return Path('/content/DP-01-A')
        else:
            return ConfigAmbiente.get_script_dir().parent / 'data' / 'processed' / 'DP-01-A'
    
    @staticmethod
    def get_pasta_output():
        if AMBIENTE == 'COLAB':
            return Path('/content/output')
        else:
            return ConfigAmbiente.get_script_dir() / 'output'
    
    @staticmethod
    def salvar_arquivo(filename, dataframe):
        pasta_output = ConfigAmbiente.get_pasta_output()
        pasta_output.mkdir(parents=True, exist_ok=True)
        filepath = pasta_output / filename
        dataframe.to_csv(filepath, index=False, encoding='utf-8')
        print(f"[SALVO] {filename} -> {filepath}")
        return filepath


class Config:
    PROJETO_NOME = "ETL Laboral Consolidado"
    VERSAO = "1.0-LABORAL"
    ANO_REFERENCIA = 2021
    FONTE_DADOS = "INE Censos 2021 - Populacao Estrangeira"
    
    # Arquivos necessarios de DP-01-B1 (com ou sem prefixo Dim_/Fact_)
    ARQUIVOS_LABORAIS_NECESSARIOS = {
        'CondicaoEconomica': ['Dim_CondicaoEconomica.csv', 'CondicaoEconomica.csv'],
        'GrupoProfissional': ['Dim_GrupoProfissional.csv', 'GrupoProfissional.csv'],
        'SetorEconomico': ['Dim_SetorEconomico.csv', 'SetorEconomico.csv'],
        'SituacaoProfissional': ['Dim_SituacaoProfissional.csv', 'SituacaoProfissional.csv'],
        'PopulacaoPorCondicao': ['Fact_PopulacaoPorCondicao.csv', 'PopulacaoPorCondicao.csv'],
        'EmpregadosPorProfissao': ['Fact_EmpregadosPorProfissao.csv', 'EmpregadosPorProfissao.csv'],
        'EmpregadosPorSetor': ['Fact_EmpregadosPorSetor.csv', 'EmpregadosPorSetor.csv'],
        'EmpregadosPorSituacao': ['Fact_EmpregadosPorSituacao.csv', 'EmpregadosPorSituacao.csv']
    }


class Logger:
    def __init__(self, nome="ETL"):
        self.nome = nome
    
    def info(self, msg):
        print(f"[INFO] {msg}")
    
    def sucesso(self, msg):
        print(f"[OK] {msg}")
    
    def erro(self, msg):
        print(f"[ERRO] {msg}")
    
    def separador(self):
        print("\n" + "="*70)


class ExtratorLaboral:
    """Importa dados laborais processados de DP-01-B1"""
    
    def __init__(self):
        self.logger = Logger("ExtratorLaboral")
        self.tabelas_laborais = {}
        self.tabelas_base = {}
    
    def carregar_tabelas_base(self):
        """Carrega dimensoes base compartilhadas (Nacionalidade, Sexo)"""
        self.logger.info("Importando tabelas base (Nacionalidade, Sexo, PopulacaoResidente)...")
        pasta_base = ConfigAmbiente.get_pasta_dados_base()
        
        if not pasta_base.exists():
            self.logger.erro(f"Pasta {pasta_base} nao encontrada!")
            return False
        
        arquivos_base = [
            'Nacionalidade.csv',
            'Sexo.csv',
            'PopulacaoResidente.csv'
        ]
        
        for arquivo in arquivos_base:
            filepath = pasta_base / arquivo
            if filepath.exists():
                try:
                    df = pd.read_csv(filepath, encoding='utf-8')
                    tabela_nome = arquivo.replace('.csv', '')
                    self.tabelas_base[tabela_nome] = df
                    self.logger.sucesso(f"{arquivo} importado ({len(df)} registros)")
                except Exception as e:
                    self.logger.erro(f"Erro ao importar {arquivo}: {e}")
            else:
                self.logger.erro(f"Arquivo {arquivo} nao encontrado!")
        
        return len(self.tabelas_base) > 0
    
    def carregar_tabelas_laborais(self):
        """Carrega tabelas laborais de DP-01-B1"""
        self.logger.info("Importando tabelas laborais de DP-01-B1...")
        pasta_laboral = ConfigAmbiente.get_pasta_dados_laborais()
        
        if not pasta_laboral.exists():
            self.logger.erro(f"Pasta {pasta_laboral} nao encontrada!")
            return False
        
        # Tentar localizar subpasta 'resultados_etl_laboral' ou 'Resultados_DP-01-B'
        possiveis_pastas = [
            pasta_laboral / 'resultados_etl_laboral',
            pasta_laboral / 'Resultados_DP-01-B',
            pasta_laboral  # Tentar na raiz tambem
        ]
        
        pasta_dados = None
        for pasta in possiveis_pastas:
            if pasta.exists():
                pasta_dados = pasta
                self.logger.info(f"Encontrada pasta de dados: {pasta}")
                break
        
        if not pasta_dados:
            self.logger.erro("Nenhuma pasta de resultados encontrada!")
            return False
        
        contador = 0
        for tabela_key, opcoes_arquivo in Config.ARQUIVOS_LABORAIS_NECESSARIOS.items():
            arquivo_encontrado = None
            
            # Tentar cada opção de nome de arquivo
            for opcao in opcoes_arquivo:
                filepath = pasta_dados / opcao
                if filepath.exists():
                    arquivo_encontrado = filepath
                    break
            
            if arquivo_encontrado:
                try:
                    df = pd.read_csv(arquivo_encontrado, encoding='utf-8')
                    
                    # Determinar nome padronizado com prefixo
                    if tabela_key in ['CondicaoEconomica', 'GrupoProfissional', 'SetorEconomico', 'SituacaoProfissional']:
                        tabela_nome = f'Dim_{tabela_key}'
                    else:
                        tabela_nome = f'Fact_{tabela_key}'
                    
                    self.tabelas_laborais[tabela_nome] = df
                    contador += 1
                    self.logger.sucesso(f"{arquivo_encontrado.name} importado como {tabela_nome} ({len(df)} registros)")
                except Exception as e:
                    self.logger.erro(f"Erro ao importar {arquivo_encontrado.name}: {e}")
            else:
                self.logger.info(f"Opcional: {tabela_key} nao encontrado (tentativas: {', '.join(opcoes_arquivo)})")
        
        self.logger.sucesso(f"Total de {contador} tabelas laborais carregadas")
        return contador > 0


class ValidadorIntegridade:
    """Valida integridade referencial entre tabelas"""
    
    def __init__(self):
        self.logger = Logger("Validador")
    
    def validar_fks_nacionalidade(self, fatos, dim_nacionalidade):
        """Valida FKs de nacionalidade_id"""
        self.logger.info("Validando integridade referencial de nacionalidade_id...")
        
        nac_ids_validos = set(dim_nacionalidade['nacionalidade_id'].values)
        
        tabelas_com_nac_id = [
            'Fact_PopulacaoPorCondicao',
            'Fact_EmpregadosPorProfissao',
            'Fact_EmpregadosPorSetor',
            'Fact_EmpregadosPorSituacao'
        ]
        
        for tabela_nome in tabelas_com_nac_id:
            if tabela_nome in fatos:
                df = fatos[tabela_nome]
                if 'nacionalidade_id' in df.columns:
                    invalidos = df[~df['nacionalidade_id'].isin(nac_ids_validos)]
                    if len(invalidos) > 0:
                        self.logger.erro(f"{tabela_nome}: {len(invalidos)} FKs invalidas de nacionalidade_id")
                    else:
                        self.logger.sucesso(f"{tabela_nome}: Todas FKs de nacionalidade_id validas")


class ConsolidadorLaboral:
    """Consolida e adapta dados laborais ao Star Schema"""
    
    def __init__(self):
        self.logger = Logger("Consolidador")
        self.dimensoes = {}
        self.fatos = {}
    
    def consolidar_dimensoes(self, tabelas_base, tabelas_laborais):
        """Consolida dimensoes com prefixo Dim_"""
        self.logger.info("Consolidando dimensoes...")
        
        # Dimensoes base
        if 'Nacionalidade' in tabelas_base:
            self.dimensoes['Dim_Nacionalidade'] = tabelas_base['Nacionalidade'].copy()
            self.logger.sucesso(f"Dim_Nacionalidade: {len(self.dimensoes['Dim_Nacionalidade'])} registros")
        
        if 'Sexo' in tabelas_base:
            self.dimensoes['Dim_Sexo'] = tabelas_base['Sexo'].copy()
            self.logger.sucesso(f"Dim_Sexo: {len(self.dimensoes['Dim_Sexo'])} registros")
        
        if 'PopulacaoResidente' in tabelas_base:
            self.dimensoes['Dim_PopulacaoResidente'] = tabelas_base['PopulacaoResidente'].copy()
            self.logger.sucesso(f"Dim_PopulacaoResidente: {len(self.dimensoes['Dim_PopulacaoResidente'])} registros")
        
        # Dimensoes laborais
        dims_laborais = [
            'Dim_CondicaoEconomica',
            'Dim_GrupoProfissional',
            'Dim_SetorEconomico',
            'Dim_SituacaoProfissional'
        ]
        
        for dim_nome in dims_laborais:
            if dim_nome in tabelas_laborais:
                self.dimensoes[dim_nome] = tabelas_laborais[dim_nome].copy()
                self.logger.sucesso(f"{dim_nome}: {len(self.dimensoes[dim_nome])} registros")
        
        return self.dimensoes
    
    def consolidar_fatos(self, tabelas_laborais):
        """Consolida fatos com prefixo Fact_"""
        self.logger.info("Consolidando fatos...")
        
        fatos_laborais = [
            'Fact_PopulacaoPorCondicao',
            'Fact_EmpregadosPorProfissao',
            'Fact_EmpregadosPorSetor',
            'Fact_EmpregadosPorSituacao'
        ]
        
        for fato_nome in fatos_laborais:
            if fato_nome in tabelas_laborais:
                df = tabelas_laborais[fato_nome].copy()
                
                # Garantir que PKs sejam sequenciais
                if len(df) > 0:
                    pk_col = df.columns[0]  # Assumir primeira coluna e PK
                    df[pk_col] = range(1, len(df) + 1)
                
                self.fatos[fato_nome] = df
                self.logger.sucesso(f"{fato_nome}: {len(df)} registros")
        
        return self.fatos


class Exportador:
    """Exporta dados consolidados"""
    
    def __init__(self):
        self.logger = Logger("Exportador")
    
    def exportar_zip_consolidado(self, dimensoes, fatos):
        self.logger.info("Criando arquivo ZIP consolidado...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'ETL_LABORAL_CONSOLIDADO_2021_{timestamp}.zip'
        
        pasta_output = ConfigAmbiente.get_pasta_output()
        pasta_output.mkdir(parents=True, exist_ok=True)
        zip_path = pasta_output / zip_filename
        
        todas_tabelas = {**dimensoes, **fatos}
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for nome_tabela, df in todas_tabelas.items():
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                zip_file.writestr(f'{nome_tabela}.csv', csv_buffer.getvalue())
            
            # Adicionar README
            readme = self._gerar_readme(dimensoes, fatos)
            zip_file.writestr('README_LABORAL.txt', readme)
        
        self.logger.sucesso(f"ZIP criado: {zip_path}")
        return zip_path
    
    def _gerar_readme(self, dimensoes, fatos):
        readme = f"""
========================================================================
ETL LABORAL CONSOLIDADO - DATASET CENSOS 2021
========================================================================
Data de Geracao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Versao: 1.0-LABORAL
Fonte: INE Censos 2021 - Populacao Estrangeira
Modelo: Star Schema Unificado

========================================================================
DIMENSOES ({len(dimensoes)} tabelas)
========================================================================
"""
        for nome, df in dimensoes.items():
            readme += f"\n{nome}.csv - {len(df)} registros"
        
        readme += f"""

========================================================================
FATOS ({len(fatos)} tabelas)
========================================================================
"""
        for nome, df in fatos.items():
            readme += f"\n{nome}.csv - {len(df)} registros"
        
        readme += """

========================================================================
ESTRUTURA DOS DADOS
========================================================================

Fact_PopulacaoPorCondicao:
- População por nacionalidade e condição econômica (Ativa/Inativa)
- Vincula: Dim_Nacionalidade, Dim_CondicaoEconomica, Dim_PopulacaoResidente

Fact_EmpregadosPorProfissao:
- Empregados por nacionalidade e grupo profissional
- Vincula: Dim_Nacionalidade, Dim_GrupoProfissional

Fact_EmpregadosPorSetor:
- Empregados por nacionalidade e setor econômico (CAE)
- Vincula: Dim_Nacionalidade, Dim_SetorEconomico

Fact_EmpregadosPorSituacao:
- Empregados por nacionalidade e situação profissional
- Vincula: Dim_Nacionalidade, Dim_SituacaoProfissional

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Integra com dados de Educacao (DP-01-A)
- Integra com dados AIMA (DP-02-A)
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
"""
        return readme


class OrquestradorLaboral:
    """Orquestra todo o processo de ETL Laboral"""
    
    def __init__(self):
        self.logger = Logger("Orquestrador")
        self.extrator = ExtratorLaboral()
        self.consolidador = ConsolidadorLaboral()
        self.validador = ValidadorIntegridade()
        self.exportador = Exportador()
    
    def executar_pipeline_completo(self):
        self.logger.separador()
        print(f"{Config.PROJETO_NOME} - Versao {Config.VERSAO}")
        print(f"Ambiente: {AMBIENTE}")
        print(f"Fonte: {Config.FONTE_DADOS}")
        self.logger.separador()
        
        # FASE 1: Carregar tabelas base
        self.logger.info("\n>>> FASE 1: Importacao de Tabelas Base")
        if not self.extrator.carregar_tabelas_base():
            self.logger.erro("Falha ao carregar tabelas base")
            return False
        
        # FASE 2: Carregar tabelas laborais
        self.logger.info("\n>>> FASE 2: Importacao de Tabelas Laborais")
        if not self.extrator.carregar_tabelas_laborais():
            self.logger.erro("Falha ao carregar tabelas laborais")
            return False
        
        # FASE 3: Consolidacao
        self.logger.info("\n>>> FASE 3: Consolidacao de Dimensoes e Fatos")
        dimensoes = self.consolidador.consolidar_dimensoes(
            self.extrator.tabelas_base,
            self.extrator.tabelas_laborais
        )
        
        fatos = self.consolidador.consolidar_fatos(
            self.extrator.tabelas_laborais
        )
        
        # FASE 4: Validacao
        self.logger.info("\n>>> FASE 4: Validacao de Integridade")
        if 'Dim_Nacionalidade' in dimensoes:
            self.validador.validar_fks_nacionalidade(fatos, dimensoes['Dim_Nacionalidade'])
        
        # FASE 5: Exportacao
        self.logger.info("\n>>> FASE 5: Exportacao")
        zip_path = self.exportador.exportar_zip_consolidado(dimensoes, fatos)
        
        # Relatorio final
        self.logger.separador()
        print("CONSOLIDACAO LABORAL CONCLUIDA COM SUCESSO!")
        self.logger.separador()
        print(f"[OK] Dimensoes: {len(dimensoes)}")
        print(f"[OK] Fatos: {len(fatos)}")
        print(f"[OK] Arquivo: {zip_path}")
        self.logger.separador()
        
        return True


def executar_consolidacao_laboral():
    """Funcao principal para executar consolidacao laboral"""
    orquestrador = OrquestradorLaboral()
    return orquestrador.executar_pipeline_completo()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("  ETL LABORAL CONSOLIDADO v1.0 - CENSOS 2021")
    print("="*70)
    print("\n[INICIO] Iniciando pipeline de consolidacao laboral...\n")
    executar_consolidacao_laboral()
