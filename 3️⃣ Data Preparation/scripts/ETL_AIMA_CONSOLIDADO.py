# -*- coding: utf-8 -*-
"""
ETL_AIMA_CONSOLIDADO - v1.0
Pipeline ETL para dados AIMA/SEF (2020-2024)
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
    def get_pasta_dados_aima():
        """Retorna pasta com dados AIMA processados DP-02-A2"""
        if AMBIENTE == 'COLAB':
            return Path('/content/DP-02-A2/data')
        else:
            return ConfigAmbiente.get_script_dir().parent / 'data' / 'processed' / 'DP-02-A' / 'DP-02-A2' / 'data'
    
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
    PROJETO_NOME = "ETL AIMA Consolidado"
    VERSAO = "1.0-AIMA"
    ANO_REFERENCIA_INICIAL = 2020
    ANO_REFERENCIA_FINAL = 2024
    FONTE_DADOS = "AIMA/SEF - RIFA 2020-2022, RMA 2023-2024"
    
    # Arquivos necessarios de DP-02-A2/data/
    ARQUIVOS_AIMA_NECESSARIOS = {
        # Dimensoes
        'AnoRelatorio': 'AnoRelatorio.csv',
        'TipoRelatorio': 'TipoRelatorio.csv',
        'Despacho': 'Despacho.csv',
        'MotivoConcessao': 'MotivoConcessao.csv',
        'NacionalidadeAIMA': 'NacionalidadeAIMA.csv',
        'Sexo': 'Sexo.csv',
        
        # Fatos
        'ConcessoesPorNacionalidadeSexo': 'ConcessoesPorNacionalidadeSexo.csv',
        'ConcessoesPorDespacho': 'ConcessoesPorDespacho.csv',
        'ConcessoesPorMotivoNacionalidade': 'ConcessoesPorMotivoNacionalidade.csv',
        'PopulacaoEstrangeiraPorNacionalidadeSexo': 'PopulacaoEstrangeiraPorNacionalidadeSexo.csv',
        'DistribuicaoEtariaConcessoes': 'DistribuicaoEtariaConcessoes.csv',
        'PopulacaoResidenteEtaria': 'PopulacaoResidenteEtaria.csv'
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
    
    def aviso(self, msg):
        print(f"[AVISO] {msg}")
    
    def separador(self):
        print("\n" + "="*70)


class ExtratorAIMA:
    """Importa dados AIMA processados de DP-02-A2"""
    
    def __init__(self):
        self.logger = Logger("ExtratorAIMA")
        self.tabelas_aima = {}
        self.tabelas_base = {}
    
    def carregar_tabelas_base(self):
        """Carrega dimensoes base compartilhadas (Nacionalidade)"""
        self.logger.info("Importando tabelas base (Nacionalidade, Sexo)...")
        pasta_base = ConfigAmbiente.get_pasta_dados_base()
        
        if not pasta_base.exists():
            self.logger.aviso(f"Pasta {pasta_base} nao encontrada!")
            self.logger.info("Continuando sem dimensoes base...")
            return True  # Nao e critico para AIMA
        
        arquivos_base = [
            'Nacionalidade.csv',
            'Sexo.csv'
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
                self.logger.aviso(f"Arquivo {arquivo} nao encontrado (opcional)")
        
        return True
    
    def carregar_tabelas_aima(self):
        """Carrega tabelas AIMA de DP-02-A2/data/"""
        self.logger.info("Importando tabelas AIMA de DP-02-A2...")
        pasta_aima = ConfigAmbiente.get_pasta_dados_aima()
        
        if not pasta_aima.exists():
            self.logger.erro(f"Pasta {pasta_aima} nao encontrada!")
            return False
        
        contador = 0
        for tabela_key, arquivo_nome in Config.ARQUIVOS_AIMA_NECESSARIOS.items():
            filepath = pasta_aima / arquivo_nome
            
            if filepath.exists():
                try:
                    df = pd.read_csv(filepath, encoding='utf-8')
                    
                    # Determinar nome padronizado com prefixo
                    if tabela_key in ['AnoRelatorio', 'TipoRelatorio', 'Despacho', 
                                     'MotivoConcessao', 'NacionalidadeAIMA', 'Sexo']:
                        tabela_nome = f'Dim_{tabela_key}'
                    else:
                        tabela_nome = f'Fact_{tabela_key}'
                    
                    self.tabelas_aima[tabela_nome] = df
                    contador += 1
                    self.logger.sucesso(f"{arquivo_nome} importado como {tabela_nome} ({len(df)} registros)")
                except Exception as e:
                    self.logger.erro(f"Erro ao importar {arquivo_nome}: {e}")
            else:
                self.logger.aviso(f"Opcional: {arquivo_nome} nao encontrado")
        
        self.logger.sucesso(f"Total de {contador} tabelas AIMA carregadas")
        return contador > 0


class ValidadorIntegridadeAIMA:
    """Valida integridade referencial entre tabelas AIMA"""
    
    def __init__(self):
        self.logger = Logger("ValidadorAIMA")
    
    def validar_anos(self, fatos):
        """Valida cobertura temporal"""
        self.logger.info("Validando cobertura temporal (2020-2024)...")
        
        anos_esperados = set(range(2020, 2025))
        anos_encontrados = set()
        
        for tabela_nome, df in fatos.items():
            if 'ano' in df.columns:
                anos_encontrados.update(df['ano'].unique())
        
        anos_faltantes = anos_esperados - anos_encontrados
        
        if anos_faltantes:
            self.logger.aviso(f"Anos faltantes: {sorted(anos_faltantes)}")
        else:
            self.logger.sucesso("Cobertura completa 2020-2024")
    
    def validar_fontes(self, fatos):
        """Valida fontes de dados (RIFA vs RMA)"""
        self.logger.info("Validando fontes de dados...")
        
        fontes_esperadas = {'RIFA', 'RMA'}
        fontes_encontradas = set()
        
        for tabela_nome, df in fatos.items():
            if 'fonte' in df.columns:
                fontes_encontradas.update(df['fonte'].unique())
        
        if fontes_esperadas.issubset(fontes_encontradas):
            self.logger.sucesso(f"Fontes validadas: {fontes_encontradas}")
        else:
            self.logger.aviso(f"Fontes encontradas: {fontes_encontradas}")


class ConsolidadorAIMA:
    """Consolida e adapta dados AIMA ao Star Schema"""
    
    def __init__(self):
        self.logger = Logger("ConsolidadorAIMA")
        self.dimensoes = {}
        self.fatos = {}
    
    def consolidar_dimensoes(self, tabelas_base, tabelas_aima):
        """Consolida dimensoes com prefixo Dim_"""
        self.logger.info("Consolidando dimensoes...")
        
        # Dimensoes base (se disponiveis)
        if 'Nacionalidade' in tabelas_base:
            self.dimensoes['Dim_Nacionalidade'] = tabelas_base['Nacionalidade'].copy()
            self.logger.sucesso(f"Dim_Nacionalidade: {len(self.dimensoes['Dim_Nacionalidade'])} registros")
        
        if 'Sexo' in tabelas_base:
            # Preferir Sexo de base, senao usar de AIMA
            self.dimensoes['Dim_Sexo'] = tabelas_base['Sexo'].copy()
            self.logger.sucesso(f"Dim_Sexo (base): {len(self.dimensoes['Dim_Sexo'])} registros")
        elif 'Dim_Sexo' in tabelas_aima:
            self.dimensoes['Dim_Sexo'] = tabelas_aima['Dim_Sexo'].copy()
            self.logger.sucesso(f"Dim_Sexo (AIMA): {len(self.dimensoes['Dim_Sexo'])} registros")
        
        # Dimensoes AIMA especificas
        dims_aima = [
            'Dim_AnoRelatorio',
            'Dim_TipoRelatorio',
            'Dim_Despacho',
            'Dim_MotivoConcessao',
            'Dim_NacionalidadeAIMA'
        ]
        
        for dim_nome in dims_aima:
            if dim_nome in tabelas_aima:
                self.dimensoes[dim_nome] = tabelas_aima[dim_nome].copy()
                self.logger.sucesso(f"{dim_nome}: {len(self.dimensoes[dim_nome])} registros")
        
        return self.dimensoes
    
    def consolidar_fatos(self, tabelas_aima):
        """Consolida fatos com prefixo Fact_"""
        self.logger.info("Consolidando fatos...")
        
        fatos_aima = [
            'Fact_ConcessoesPorNacionalidadeSexo',
            'Fact_ConcessoesPorDespacho',
            'Fact_ConcessoesPorMotivoNacionalidade',
            'Fact_PopulacaoEstrangeiraPorNacionalidadeSexo',
            'Fact_DistribuicaoEtariaConcessoes',
            'Fact_PopulacaoResidenteEtaria'
        ]
        
        for fato_nome in fatos_aima:
            if fato_nome in tabelas_aima:
                df = tabelas_aima[fato_nome].copy()
                
                # Garantir PKs sequenciais se existir coluna ID
                if len(df) > 0 and df.columns[0].endswith('_id'):
                    pk_col = df.columns[0]
                    df[pk_col] = range(1, len(df) + 1)
                
                self.fatos[fato_nome] = df
                self.logger.sucesso(f"{fato_nome}: {len(df)} registros")
        
        return self.fatos
    
    def criar_fato_evolucao_populacional(self, fatos):
        """Cria tabela fato de evolucao populacional agregada"""
        self.logger.info("Criando Fact_EvolucaoPopulacaoEstrangeira...")
        
        if 'Fact_PopulacaoEstrangeiraPorNacionalidadeSexo' not in fatos:
            self.logger.aviso("Dados insuficientes para criar evolucao populacional")
            return
        
        df_base = fatos['Fact_PopulacaoEstrangeiraPorNacionalidadeSexo'].copy()
        
        # Agregar por ano e fonte
        df_evolucao = df_base.groupby(['ano', 'fonte']).agg({
            'quantidade': 'sum'
        }).reset_index()
        
        df_evolucao.columns = ['ano', 'fonte', 'total_populacao']
        df_evolucao['evolucao_id'] = range(1, len(df_evolucao) + 1)
        
        # Reordenar colunas
        df_evolucao = df_evolucao[['evolucao_id', 'ano', 'fonte', 'total_populacao']]
        
        self.fatos['Fact_EvolucaoPopulacaoEstrangeira'] = df_evolucao
        self.logger.sucesso(f"Fact_EvolucaoPopulacaoEstrangeira: {len(df_evolucao)} registros")


class ExportadorAIMA:
    """Exporta dados consolidados AIMA"""
    
    def __init__(self):
        self.logger = Logger("ExportadorAIMA")
    
    def exportar_zip_consolidado(self, dimensoes, fatos):
        self.logger.info("Criando arquivo ZIP consolidado...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'ETL_AIMA_CONSOLIDADO_2020-2024_{timestamp}.zip'
        
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
            zip_file.writestr('README_AIMA.txt', readme)
        
        self.logger.sucesso(f"ZIP criado: {zip_path}")
        return zip_path
    
    def _gerar_readme(self, dimensoes, fatos):
        readme = f"""
========================================================================
ETL AIMA CONSOLIDADO - DATASET AIMA/SEF 2020-2024
========================================================================
Data de Geracao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Versao: 1.0-AIMA
Fonte: AIMA/SEF - RIFA 2020-2022, RMA 2023-2024
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

Fact_ConcessoesPorNacionalidadeSexo:
- Concessões de títulos de residência por nacionalidade e sexo (2020-2024)
- Vincula: Dim_AnoRelatorio, Dim_NacionalidadeAIMA, Dim_Sexo

Fact_ConcessoesPorDespacho:
- Concessões por tipo de despacho administrativo
- Vincula: Dim_AnoRelatorio, Dim_Despacho

Fact_ConcessoesPorMotivoNacionalidade:
- Concessões por motivo (trabalho, família, estudo, etc) e nacionalidade
- Vincula: Dim_AnoRelatorio, Dim_MotivoConcessao, Dim_NacionalidadeAIMA

Fact_PopulacaoEstrangeiraPorNacionalidadeSexo:
- População estrangeira residente por nacionalidade e sexo
- Vincula: Dim_AnoRelatorio, Dim_NacionalidadeAIMA, Dim_Sexo

Fact_DistribuicaoEtariaConcessoes:
- Distribuição etária das concessões de residência
- Vincula: Dim_AnoRelatorio, Dim_Sexo

Fact_PopulacaoResidenteEtaria:
- População residente por faixa etária
- Vincula: Dim_AnoRelatorio

Fact_EvolucaoPopulacaoEstrangeira:
- Evolução temporal agregada da população estrangeira
- Vincula: Dim_AnoRelatorio

========================================================================
COBERTURA TEMPORAL
========================================================================
2020: RIFA 2020 (SEF)
2021: RIFA 2021 (SEF)
2022: RIFA 2022 (SEF)
2023: RMA 2023 (AIMA)
2024: RMA 2024 (AIMA)

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Integra com dados de Educacao (DP-01-A)
- Integra com dados Laborais (DP-01-B)
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
"""
        return readme


class OrquestradorAIMA:
    """Orquestra todo o processo de ETL AIMA"""
    
    def __init__(self):
        self.logger = Logger("OrquestradorAIMA")
        self.extrator = ExtratorAIMA()
        self.consolidador = ConsolidadorAIMA()
        self.validador = ValidadorIntegridadeAIMA()
        self.exportador = ExportadorAIMA()
    
    def executar_pipeline_completo(self):
        self.logger.separador()
        print(f"{Config.PROJETO_NOME} - Versao {Config.VERSAO}")
        print(f"Ambiente: {AMBIENTE}")
        print(f"Fonte: {Config.FONTE_DADOS}")
        self.logger.separador()
        
        # FASE 1: Carregar tabelas base (opcional)
        self.logger.info("\n>>> FASE 1: Importacao de Tabelas Base (Opcional)")
        self.extrator.carregar_tabelas_base()
        
        # FASE 2: Carregar tabelas AIMA
        self.logger.info("\n>>> FASE 2: Importacao de Tabelas AIMA")
        if not self.extrator.carregar_tabelas_aima():
            self.logger.erro("Falha ao carregar tabelas AIMA")
            return False
        
        # FASE 3: Consolidacao
        self.logger.info("\n>>> FASE 3: Consolidacao de Dimensoes e Fatos")
        dimensoes = self.consolidador.consolidar_dimensoes(
            self.extrator.tabelas_base,
            self.extrator.tabelas_aima
        )
        
        fatos = self.consolidador.consolidar_fatos(
            self.extrator.tabelas_aima
        )
        
        # Criar fatos derivados
        self.consolidador.criar_fato_evolucao_populacional(fatos)
        
        # FASE 4: Validacao
        self.logger.info("\n>>> FASE 4: Validacao de Integridade")
        self.validador.validar_anos(fatos)
        self.validador.validar_fontes(fatos)
        
        # FASE 5: Exportacao
        self.logger.info("\n>>> FASE 5: Exportacao")
        zip_path = self.exportador.exportar_zip_consolidado(dimensoes, fatos)
        
        # Relatorio final
        self.logger.separador()
        print("CONSOLIDACAO AIMA CONCLUIDA COM SUCESSO!")
        self.logger.separador()
        print(f"[OK] Dimensoes: {len(dimensoes)}")
        print(f"[OK] Fatos: {len(fatos)}")
        print(f"[OK] Arquivo: {zip_path}")
        print(f"[OK] Periodo: 2020-2024")
        self.logger.separador()
        
        return True


def executar_consolidacao_aima():
    """Funcao principal para executar consolidacao AIMA"""
    orquestrador = OrquestradorAIMA()
    return orquestrador.executar_pipeline_completo()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("  ETL AIMA CONSOLIDADO v1.0 - AIMA/SEF 2020-2024")
    print("="*70)
    print("\n[INICIO] Iniciando pipeline de consolidacao AIMA...\n")
    executar_consolidacao_aima()
