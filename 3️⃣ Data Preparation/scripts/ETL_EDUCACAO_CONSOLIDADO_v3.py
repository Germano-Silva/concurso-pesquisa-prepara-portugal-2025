# -*- coding: utf-8 -*-
"""
ETL_EDUCACAO_CONSOLIDADO - v3.0 TEMPORAL (2011 + 2021)
Pipeline ETL completo para consolidacao temporal de dados educacionais
Segue Diagrama ER Star Schema Unificado
Versao Consolidada e Portavel - Windows Compatible
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
    def get_pasta_input():
        if AMBIENTE == 'COLAB':
            return Path('/content/input')
        else:
            return ConfigAmbiente.get_script_dir() / 'input'
    
    @staticmethod
    def get_pasta_dados_2021():
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
    PROJETO_NOME = "ETL Educacao Consolidado Temporal"
    VERSAO = "3.0-CONSOLIDADO-TEMPORAL"
    ANOS_REFERENCIA = [2011, 2021]
    FONTE_DADOS = "INE Censos 2011 + 2021"
    
    # Mapeamento de nacionalidades 2011 -> 2021
    MAPEAMENTO_NACIONALIDADES = {
        'Angola': 'Angola',
        'Brasil': 'Brasil',
        'Cabo Verde': 'Cabo Verde',
        'Espanha': 'Espanha',
        'Franca': 'França',
        'Guine-Bissau': 'Guiné-Bissau',
        'Reino Unido': 'Reino Unido',
        'Republica da Moldavia': 'República da Moldávia',
        'Republica Popular da China': 'China',
        'Romenia': 'Roménia',
        'Sao tome e Principe': 'São Tomé e Príncipe',
        'Ucrania': 'Ucrânia'
    }
    
    # Mapeamento de niveis educacionais
    NIVEIS_EDUCACAO_2011 = {
        1: 'Inferior ao basico 3 ciclo',
        2: 'Basico 3 ciclo',
        3: 'Secundario e pos-secundario',
        4: 'Superior'
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


class Extrator2011:
    """Extrai e processa dados brutos de 2011"""
    
    def __init__(self):
        self.logger = Logger("Extrator2011")
        self.dados_brutos = {}
    
    def carregar_dados_2011(self):
        self.logger.info("Carregando dados brutos 2011 da pasta input/")
        pasta_input = ConfigAmbiente.get_pasta_input()
        
        if not pasta_input.exists():
            self.logger.erro(f"Pasta {pasta_input} nao encontrada!")
            return False
        
        contador = 0
        for arquivo in pasta_input.glob('*.csv'):
            try:
                df = pd.read_csv(arquivo, encoding='utf-8', sep=';', decimal=',')
                nome_pais = arquivo.stem
                self.dados_brutos[nome_pais] = df
                contador += 1
                self.logger.sucesso(f"{arquivo.name} carregado ({len(df)} linhas)")
            except Exception as e:
                self.logger.erro(f"Erro ao carregar {arquivo.name}: {e}")
        
        self.logger.sucesso(f"Total de {contador} arquivos carregados (2011)")
        return contador > 0
    
    def processar_educacao_2011(self):
        """Processa dados educacionais de 2011"""
        self.logger.info("Processando dados educacionais de 2011...")
        
        registros_educacao = []
        arquivos_processados = []
        arquivos_sem_dados = []
        
        # Mapeamento de subcategorias para nivel_educacao_id
        mapa_niveis = {
            'Inferior ao básico 3º ciclo': 1,
            'Inferior ao b�sico 3� ciclo': 1,
            'Básico 3º ciclo': 2,
            'B�sico 3� ciclo': 2,
            'Secundário e pós-secundário': 3,
            'Secund�rio e p�s-secund�rio': 3,
            'Superior': 4
        }
        
        for nome_arquivo, df in self.dados_brutos.items():
            # Mapear nome do arquivo para nacionalidade padronizada
            nome_padronizado = Config.MAPEAMENTO_NACIONALIDADES.get(nome_arquivo, nome_arquivo)
            
            # Filtrar linhas de nivel de ensino (CORRIGIDO: aceita NÍVEL com ou sem acento)
            df_ensino = df[df['Categoria'].str.contains(
                r'N[ÍI]VEL\s+DE\s+ENSINO',
                case=False,
                na=False,
                regex=True
            )]
            
            if df_ensino.empty:
                arquivos_sem_dados.append(nome_arquivo)
                self.logger.info(f"⚠️  {nome_arquivo}: Sem dados de educação")
                continue
            
            registros_antes = len(registros_educacao)
            
            for idx, row in df_ensino.iterrows():
                try:
                    subcategoria = str(row.get('Subcategoria', '')).strip()
                    
                    # Pular linha de total
                    if 'Total' in subcategoria or 'Popula' in subcategoria:
                        continue
                    
                    # Mapear subcategoria para nivel_id
                    nivel_id = mapa_niveis.get(subcategoria)
                    if nivel_id is None:
                        continue
                    
                    # Extrair valor
                    valor = self._limpar_numero(row.get('Dados 2011', 0))
                    
                    if valor and valor > 0:
                        registros_educacao.append({
                            'nacionalidade': nome_padronizado,
                            'nivel_educacao_id': nivel_id,
                            'populacao_total': int(valor),
                            'ano_referencia': 2011
                        })
                except Exception as e:
                    self.logger.erro(f"Erro ao processar linha {idx} de {nome_arquivo}: {e}")
                    continue
            
            registros_adicionados = len(registros_educacao) - registros_antes
            if registros_adicionados > 0:
                arquivos_processados.append(f"{nome_arquivo} ({registros_adicionados} registros)")
        
        self.logger.sucesso(f"Arquivos processados ({len(arquivos_processados)}): {', '.join(arquivos_processados)}")
        if arquivos_sem_dados:
            self.logger.info(f"Arquivos sem dados ({len(arquivos_sem_dados)}): {', '.join(arquivos_sem_dados)}")
        self.logger.sucesso(f"Total: {len(registros_educacao)} registros educacionais de 2011")
        return pd.DataFrame(registros_educacao) if registros_educacao else pd.DataFrame()
    
    def _limpar_numero(self, valor):
        if pd.isna(valor) or valor == '':
            return None
        valor_str = str(valor).replace('%', '').replace(' ', '').strip()
        valor_str = valor_str.replace('.', '').replace(',', '.')
        try:
            return float(valor_str)
        except:
            return None


class Extrator2021:
    """Importa dados processados de 2021"""
    
    def __init__(self):
        self.logger = Logger("Extrator2021")
        self.tabelas_2021 = {}
    
    def carregar_tabelas_2021(self):
        self.logger.info("Importando TODAS as tabelas processadas de 2021...")
        pasta_dp01a = ConfigAmbiente.get_pasta_dados_2021()
        
        if not pasta_dp01a.exists():
            self.logger.erro(f"Pasta {pasta_dp01a} nao encontrada!")
            return False
        
        # CARREGAR TODAS AS TABELAS do DP-01-A
        arquivos_necessarios = [
            # Dimensoes Base
            'Nacionalidade.csv',
            'Sexo.csv',
            'Localidade.csv',
            'GrupoEtario.csv',
            'PopulacaoResidente.csv',
            # Dimensoes Educacao
            'NivelEducacao.csv',
            'MapeamentoNacionalidades.csv',
            # Fatos Base
            'PopulacaoPorNacionalidade.csv',
            'PopulacaoPorLocalidade.csv',
            'PopulacaoPorGrupoEtario.csv',
            'EvolucaoTemporal.csv',
            'NacionalidadePrincipal.csv',
            'DistribuicaoGeografica.csv',
            # Fatos Educacao
            'PopulacaoEducacao.csv',
            'EstatisticasEducacao.csv'
        ]
        
        for arquivo in arquivos_necessarios:
            filepath = pasta_dp01a / arquivo
            if filepath.exists():
                try:
                    df = pd.read_csv(filepath, encoding='utf-8')
                    tabela_nome = arquivo.replace('.csv', '')
                    self.tabelas_2021[tabela_nome] = df
                    self.logger.sucesso(f"{arquivo} importado ({len(df)} registros)")
                except Exception as e:
                    self.logger.erro(f"Erro ao importar {arquivo}: {e}")
            else:
                self.logger.info(f"Arquivo {arquivo} nao encontrado (opcional)")
        
        return len(self.tabelas_2021) > 0


class ConsolidadorTemporal:
    """Consolida dados de 2011 e 2021"""
    
    def __init__(self):
        self.logger = Logger("Consolidador")
        self.dimensoes = {}
        self.fatos = {}
    
    def consolidar_dimensoes_base(self, tabelas_2021):
        """Consolida TODAS as dimensoes base"""
        self.logger.info("Consolidando dimensoes base...")
        
        # Dimensoes Base
        if 'Nacionalidade' in tabelas_2021:
            self.dimensoes['Dim_Nacionalidade'] = tabelas_2021['Nacionalidade'].copy()
            self.logger.sucesso(f"Dim_Nacionalidade: {len(self.dimensoes['Dim_Nacionalidade'])} registros")
        
        if 'Sexo' in tabelas_2021:
            self.dimensoes['Dim_Sexo'] = tabelas_2021['Sexo'].copy()
            self.logger.sucesso(f"Dim_Sexo: {len(self.dimensoes['Dim_Sexo'])} registros")
        
        if 'Localidade' in tabelas_2021:
            self.dimensoes['Dim_Localidade'] = tabelas_2021['Localidade'].copy()
            self.logger.sucesso(f"Dim_Localidade: {len(self.dimensoes['Dim_Localidade'])} registros")
        
        if 'GrupoEtario' in tabelas_2021:
            self.dimensoes['Dim_GrupoEtario'] = tabelas_2021['GrupoEtario'].copy()
            self.logger.sucesso(f"Dim_GrupoEtario: {len(self.dimensoes['Dim_GrupoEtario'])} registros")
        
        if 'PopulacaoResidente' in tabelas_2021:
            self.dimensoes['Dim_PopulacaoResidente'] = tabelas_2021['PopulacaoResidente'].copy()
            self.logger.sucesso(f"Dim_PopulacaoResidente: {len(self.dimensoes['Dim_PopulacaoResidente'])} registros")
        
        # Dimensoes Educacao
        if 'NivelEducacao' in tabelas_2021:
            self.dimensoes['Dim_NivelEducacao'] = tabelas_2021['NivelEducacao'].copy()
            self.logger.sucesso(f"Dim_NivelEducacao: {len(self.dimensoes['Dim_NivelEducacao'])} registros")
        
        if 'MapeamentoNacionalidades' in tabelas_2021:
            self.dimensoes['Dim_MapeamentoNacionalidades'] = tabelas_2021['MapeamentoNacionalidades'].copy()
            self.logger.sucesso(f"Dim_MapeamentoNacionalidades: {len(self.dimensoes['Dim_MapeamentoNacionalidades'])} registros")
        
        return self.dimensoes
    
    def consolidar_fatos_base(self, tabelas_2021):
        """Consolida TODOS os fatos base"""
        self.logger.info("Consolidando fatos base...")
        
        fatos_base = [
            'PopulacaoPorNacionalidade',
            'PopulacaoPorLocalidade',
            'PopulacaoPorGrupoEtario',
            'EvolucaoTemporal',
            'NacionalidadePrincipal',
            'DistribuicaoGeografica'
        ]
        
        for fato_nome in fatos_base:
            if fato_nome in tabelas_2021:
                self.fatos[f'Fact_{fato_nome}'] = tabelas_2021[fato_nome].copy()
                self.logger.sucesso(f"Fact_{fato_nome}: {len(self.fatos[f'Fact_{fato_nome}'])} registros")
        
        return self.fatos
    
    def consolidar_populacao_educacao(self, dados_2011_df, dados_2021_df, mapa_nacs):
        """Consolida Fact_PopulacaoEducacao 2011 + 2021"""
        self.logger.info("Consolidando Fact_PopulacaoEducacao...")
        
        # Processar dados 2011
        if not dados_2011_df.empty:
            # Mapear nacionalidades para IDs
            dados_2011_df['nacionalidade_id'] = dados_2011_df['nacionalidade'].map(
                lambda x: self._obter_nacionalidade_id(x, mapa_nacs)
            )
            
            # Remover registros sem nacionalidade_id
            dados_2011_df = dados_2011_df[dados_2011_df['nacionalidade_id'].notna()]
            
            # Padronizar estrutura
            dados_2011_processados = dados_2011_df[[
                'nacionalidade_id', 'nivel_educacao_id', 
                'populacao_total', 'ano_referencia'
            ]].copy()
            dados_2011_processados['faixa_etaria'] = '15-64 anos'
            dados_2011_processados['percentual_nivel'] = 0.0  # Calcular depois
        else:
            dados_2011_processados = pd.DataFrame()
        
        # Processar dados 2021
        dados_2021_processados = dados_2021_df[[
            'nacionalidade_id', 'nivel_educacao_id', 
            'populacao_total', 'ano_referencia', 
            'faixa_etaria', 'percentual_nivel'
        ]].copy()
        
        # Consolidar
        fact_pop_edu = pd.concat([dados_2011_processados, dados_2021_processados], 
                                  ignore_index=True)
        
        # Gerar IDs sequenciais
        fact_pop_edu.insert(0, 'populacao_educacao_id', range(1, len(fact_pop_edu) + 1))
        
        self.fatos['Fact_PopulacaoEducacao'] = fact_pop_edu
        self.logger.sucesso(f"Fact_PopulacaoEducacao: {len(fact_pop_edu)} registros " +
                           f"({len(dados_2011_processados)} de 2011 + {len(dados_2021_processados)} de 2021)")
        return fact_pop_edu
    
    def consolidar_estatisticas_educacao(self, dados_2011_df, dados_2021_df, mapa_nacs):
        """Consolida Fact_EstatisticasEducacao"""
        self.logger.info("Consolidando Fact_EstatisticasEducacao...")
        
        # Calcular estatisticas de 2011
        if not dados_2011_df.empty:
            stats_2011 = self._calcular_estatisticas_2011(dados_2011_df, mapa_nacs)
        else:
            stats_2011 = pd.DataFrame()
        
        # Dados 2021 ja estao prontos - remover ID se existir
        stats_2021 = dados_2021_df.copy()
        if 'estatistica_id' in stats_2021.columns:
            stats_2021 = stats_2021.drop(columns=['estatistica_id'])
        
        # Consolidar
        fact_stats = pd.concat([stats_2011, stats_2021], ignore_index=True)
        
        # Gerar IDs sequenciais
        fact_stats.insert(0, 'estatistica_id', range(1, len(fact_stats) + 1))
        
        self.fatos['Fact_EstatisticasEducacao'] = fact_stats
        self.logger.sucesso(f"Fact_EstatisticasEducacao: {len(fact_stats)} registros")
        return fact_stats
    
    def _calcular_estatisticas_2011(self, dados_2011_df, mapa_nacs):
        """Calcula estatisticas agregadas para 2011"""
        stats_list = []
        
        # Agrupar por nacionalidade
        for nac_nome in dados_2011_df['nacionalidade'].unique():
            nac_id = self._obter_nacionalidade_id(nac_nome, mapa_nacs)
            if pd.isna(nac_id):
                continue
            
            dados_nac = dados_2011_df[dados_2011_df['nacionalidade'] == nac_nome]
            
            # Calcular totais por nivel
            total_pop = dados_nac['populacao_total'].sum()
            
            sem_educacao = dados_nac[dados_nac['nivel_educacao_id'] == 1]['populacao_total'].sum()
            ensino_basico = dados_nac[dados_nac['nivel_educacao_id'] == 2]['populacao_total'].sum()
            ensino_secundario = dados_nac[dados_nac['nivel_educacao_id'] == 3]['populacao_total'].sum()
            ensino_superior = dados_nac[dados_nac['nivel_educacao_id'] == 4]['populacao_total'].sum()
            
            # Percentuais
            if total_pop > 0:
                perc_sem = (sem_educacao / total_pop) * 100
                perc_basico = (ensino_basico / total_pop) * 100
                perc_sec = (ensino_secundario / total_pop) * 100
                perc_sup = (ensino_superior / total_pop) * 100
                
                # Índice educacional padronizado (escala 0-10)
                # Fórmula: (% sem_edu * 0.0 + % básico * 2.5 + % secund * 6.0 + % superior * 10.0) / 100
                indice = (perc_sem * 0.0 + perc_basico * 2.5 + perc_sec * 6.0 + perc_sup * 10.0) / 100
            else:
                perc_sem = perc_basico = perc_sec = perc_sup = indice = 0.0
            
            stats_list.append({
                'nacionalidade_id': int(nac_id),
                'populacao_total_educacao': int(total_pop),
                'sem_educacao': int(sem_educacao),
                'ensino_basico': int(ensino_basico),
                'ensino_secundario': int(ensino_secundario),
                'ensino_superior': int(ensino_superior),
                'percentual_sem_educacao': round(perc_sem, 2),
                'percentual_ensino_basico': round(perc_basico, 2),
                'percentual_ensino_secundario': round(perc_sec, 2),
                'percentual_ensino_superior': round(perc_sup, 2),
                'indice_educacional': round(indice, 2),
                'ano_referencia': 2011
            })
        
        return pd.DataFrame(stats_list)
    
    def _obter_nacionalidade_id(self, nome_nac, mapa_nacs):
        """Mapeia nome de nacionalidade para ID"""
        try:
            nac_row = mapa_nacs[mapa_nacs['nome_nacionalidade'] == nome_nac]
            if not nac_row.empty:
                return nac_row.iloc[0]['nacionalidade_id']
        except:
            pass
        return None


class Exportador:
    """Exporta dados consolidados"""
    
    def __init__(self):
        self.logger = Logger("Exportador")
    
    def exportar_zip_consolidado(self, dimensoes, fatos):
        self.logger.info("Criando arquivo ZIP consolidado...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'ETL_EDUCACAO_CONSOLIDADO_2011_2021_{timestamp}.zip'
        
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
            zip_file.writestr('README_CONSOLIDACAO.txt', readme)
        
        self.logger.sucesso(f"ZIP criado: {zip_path}")
        return zip_path
    
    def _gerar_readme(self, dimensoes, fatos):
        readme = f"""
========================================================================
ETL EDUCACAO CONSOLIDADO - DATASET TEMPORAL 2011 + 2021
========================================================================
Data de Geracao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Versao: 3.0-CONSOLIDADO-TEMPORAL
Fonte: INE Censos 2011 + 2021
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

Fact_PopulacaoEducacao:
- Registros com ano_referencia = 2011 (dados historicos)
- Registros com ano_referencia = 2021 (dados atuais)
- Permite analise de evolucao temporal por nacionalidade e nivel

Fact_EstatisticasEducacao:
- Estatisticas agregadas por nacionalidade e ano
- Comparacao de indicadores educacionais 2011 vs 2021

========================================================================
COMPATIBILIDADE
========================================================================
- Segue Diagrama ER Star Schema Unificado
- Compativel com analises temporais
- Pronto para import em banco de dados relacional
- UTF-8 encoding

========================================================================
"""
        return readme


class OrquestradorConsolidacao:
    """Orquestra todo o processo de consolidacao"""
    
    def __init__(self):
        self.logger = Logger("Orquestrador")
        self.extrator_2011 = Extrator2011()
        self.extrator_2021 = Extrator2021()
        self.consolidador = ConsolidadorTemporal()
        self.exportador = Exportador()
    
    def executar_pipeline_completo(self):
        self.logger.separador()
        print(f"{Config.PROJETO_NOME} - Versao {Config.VERSAO}")
        print(f"Ambiente: {AMBIENTE}")
        print(f"Fonte: {Config.FONTE_DADOS}")
        self.logger.separador()
        
        # FASE 1: Extrair dados 2011
        self.logger.info("\n>>> FASE 1: Extracao de Dados 2011")
        if not self.extrator_2011.carregar_dados_2011():
            self.logger.erro("Falha ao carregar dados 2011")
            return False
        
        dados_edu_2011 = self.extrator_2011.processar_educacao_2011()
        
        # FASE 2: Importar tabelas 2021
        self.logger.info("\n>>> FASE 2: Importacao de Tabelas 2021")
        if not self.extrator_2021.carregar_tabelas_2021():
            self.logger.erro("Falha ao importar dados 2021")
            return False
        
        # FASE 3: Consolidacao
        self.logger.info("\n>>> FASE 3: Consolidacao de TODAS as Tabelas")
        
        # Consolidar TODAS as dimensoes
        dimensoes = self.consolidador.consolidar_dimensoes_base(
            self.extrator_2021.tabelas_2021
        )
        
        # Consolidar TODOS os fatos base
        fatos = self.consolidador.consolidar_fatos_base(
            self.extrator_2021.tabelas_2021
        )
        
        # Consolidar fatos educacionais (com dados 2011)
        if not dados_edu_2011.empty and 'Nacionalidade' in self.extrator_2021.tabelas_2021:
            fact_pop_edu = self.consolidador.consolidar_populacao_educacao(
                dados_edu_2011,
                self.extrator_2021.tabelas_2021['PopulacaoEducacao'],
                self.extrator_2021.tabelas_2021['Nacionalidade']
            )
            
            fact_stats = self.consolidador.consolidar_estatisticas_educacao(
                dados_edu_2011,
                self.extrator_2021.tabelas_2021['EstatisticasEducacao'],
                self.extrator_2021.tabelas_2021['Nacionalidade']
            )
        
        # FASE 4: Exportacao
        self.logger.info("\n>>> FASE 4: Exportacao")
        zip_path = self.exportador.exportar_zip_consolidado(
            self.consolidador.dimensoes,
            self.consolidador.fatos
        )
        
        # Relatorio final
        self.logger.separador()
        print("CONSOLIDACAO CONCLUIDA COM SUCESSO!")
        self.logger.separador()
        print(f"[OK] Dimensoes: {len(self.consolidador.dimensoes)}")
        print(f"[OK] Fatos: {len(self.consolidador.fatos)}")
        print(f"[OK] Arquivo: {zip_path}")
        self.logger.separador()
        
        return True


def executar_consolidacao_temporal():
    """Funcao principal para executar consolidacao"""
    orquestrador = OrquestradorConsolidacao()
    return orquestrador.executar_pipeline_completo()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("  ETL EDUCACAO CONSOLIDADO v3.0 - TEMPORAL (2011 + 2021)")
    print("="*70)
    print("\n[INICIO] Iniciando pipeline de consolidacao...\n")
    executar_consolidacao_temporal()
