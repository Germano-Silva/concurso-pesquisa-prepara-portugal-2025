#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
SCRIPT ETL COMPLETO - DADOS LABORAIS CENSOS 2021 PORTUGAL
=============================================================================

Autor: Sistema ETL Automatizado  
Data: Dezembro 2024
Fonte: INE - Censos 2021

OBJETIVO:
Processar 8 arquivos CSV dos Censos 2021 e criar modelo relacional normalizado
(3FN/BCNF) integrado com o modelo educacional existente (DP-01-A)

ARQUIVOS DE ENTRADA:
- Q3.1.csv: População por nacionalidade e condição econômica
- Q3.2.csv: População empregada por nacionalidade e profissão  
- Q3.3.csv: População empregada por nacionalidade e setor
- Q3.4.csv: População empregada por nacionalidade e situação
- Q20.csv: População empregada por profissão e sexo
- Q21.csv: População empregada por região e setor
- Q23.csv: População por escolaridade, trabalho e sexo
- Q24.csv: População por região e fonte de rendimento

SAÍDA:
- 7 tabelas dimensionais + 8 tabelas de fato em formato CSV
- Índice de tabelas geradas
- Relatório de estatísticas e validações
- Log detalhado do processamento
=============================================================================
"""

import pandas as pd
import numpy as np
import logging
import warnings
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_laboral_log.txt', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

warnings.filterwarnings('ignore')

# ============================================================================
# CLASSE PRINCIPAL ETL
# ============================================================================

class ETLLaboralProcessor:
    """Processador ETL para dados laborais dos Censos 2021"""
    
    def __init__(self):
        """Inicializa o processador ETL"""
        self.logger = logging.getLogger(__name__)
        self.raw_data = {}
        self.reference_tables = {}
        self.dimensional_tables = {}
        self.fact_tables = {}
        self.statistics = {
            'files_processed': 0,
            'records_input': 0,
            'records_output': 0,
            'validation_errors': [],
            'warnings': []
        }
        
        # Arquivos de entrada com variantes para Google Colab
        self.input_files = {
            'Q3.1': ['Q3.1.csv', 'Q3.1 (1).csv'],
            'Q3.2': ['Q3.2.csv', 'Q3.2 (1).csv'],
            'Q3.3': ['Q3.3.csv', 'Q3.3 (1).csv'],
            'Q3.4': ['Q3.4.csv', 'Q3.4 (1).csv'],
            'Q20': ['Q20.csv', 'Q20 (1).csv'],
            'Q21': ['Q21.csv', 'Q21 (1).csv'],
            'Q23': ['Q23.csv', 'Q23 (1).csv'],
            'Q24': ['Q24.csv', 'Q24 (1).csv']
        }
        
        self.logger.info("✅ ETL Processor inicializado")

    # ========================================================================
    # UTILITÁRIOS
    # ========================================================================

    def normalize_text(self, text: str) -> str:
        """Normaliza texto removendo caracteres especiais"""
        if pd.isna(text) or not isinstance(text, str):
            return text
            
        replacements = {
            'гo': 'ção', 'уo': 'ão', 'гг': 'çã', 'у': 'ã', 'й': 'é',
            'ь': 'í', 'з': 'ç', 'р': 'á', 'ш': 'õ', 'ж': 'ê',
            'щ': 'ú', 'ъ': 'ó', 'ю': 'û', 'я': 'ü'
        }
        
        normalized = text
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
            
        return normalized.strip()

    def find_available_file(self, file_variations: List[str]) -> Optional[str]:
        """Encontra qual variação do arquivo está disponível"""
        for file_name in file_variations:
            if Path(file_name).exists():
                return file_name
        return None

    def get_nacionalidade_id(self, nome: str) -> Optional[int]:
        """Mapeia nome de nacionalidade para ID"""
        if pd.isna(nome) or nome == '':
            return None
            
        nome_norm = self.normalize_text(str(nome).strip())
        
        mapping = {
            'População residente': 14, 'Nacionalidade portuguesa': 12,
            'Nacionalidade estrangeira': 11, 'Brasil': 4, 'Angola': 2,
            'Cabo Verde': 5, 'Reino Unido': 15, 'Ucrânia': 18, 'França': 8,
            'China': 6, 'Guiné-Bissau': 9, 'Índia': 19, 'Roménia': 16,
            'Itália': 10, 'Nepal': 13, 'Espanha': 7, 'Alemanha': 1,
            'São Tomé e Príncipe': 17, 'Apátridas': 3
        }
        
        return mapping.get(nome_norm)

    # ========================================================================
    # EXTRAÇÃO E LIMPEZA
    # ========================================================================

    def clean_dataframe(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Limpa e padroniza DataFrame"""
        self.logger.info(f"🧹 Limpando {file_name}...")
        
        df_clean = df.copy()
        
        # Remover linhas vazias
        df_clean = df_clean.dropna(how='all')
        
        # Filtrar linhas de metadados
        mask = df_clean.iloc[:, 0].astype(str).str.contains(
            'Quadro|Fonte:|Total|População empregada|Nível|NUTS', 
            case=False, na=False
        )
        df_clean = df_clean[~mask]
        
        # Filtrar primeira coluna inválida
        df_clean = df_clean[df_clean.iloc[:, 0].notna()]
        df_clean = df_clean[~df_clean.iloc[:, 0].astype(str).isin(['0', 'nan', ''])]
        
        # Normalizar primeira coluna
        df_clean.iloc[:, 0] = df_clean.iloc[:, 0].apply(self.normalize_text)
        
        # Converter colunas numéricas
        for col_idx in range(1, len(df_clean.columns)):
            if df_clean.iloc[:, col_idx].dtype == 'object':
                df_clean.iloc[:, col_idx] = (df_clean.iloc[:, col_idx]
                    .astype(str).str.replace(' ', '').str.replace(',', '.'))
                df_clean.iloc[:, col_idx] = pd.to_numeric(
                    df_clean.iloc[:, col_idx], errors='coerce').fillna(0)
        
        self.logger.info(f"✅ {file_name}: {len(df)} → {len(df_clean)} registros")
        return df_clean

    def load_reference_tables(self) -> None:
        """Carrega tabelas de referência existentes"""
        try:
            ref_files = {
                'Nacionalidade': 'Nacionalidade.csv',
                'Sexo': 'Sexo.csv',
                'PopulacaoResidente': 'PopulacaoResidente.csv',
                'NivelEducacao': 'NivelEducacao.csv'
            }
            
            for name, file in ref_files.items():
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                    self.reference_tables[name] = df
                    self.logger.info(f"✅ {name}: {len(df)} registros")
                except FileNotFoundError:
                    self.logger.warning(f"⚠️ {file} não encontrado - criando padrão")
                    self._create_minimal_reference(name)
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar referências: {e}")
            self._create_minimal_reference()

    def _create_minimal_reference(self, table_name: str = None) -> None:
        """Cria tabelas de referência mínimas"""
        if table_name == 'Nacionalidade' or table_name is None:
            self.reference_tables['Nacionalidade'] = pd.DataFrame({
                'nacionalidade_id': range(1, 20),
                'nome_nacionalidade': [
                    'Alemanha', 'Angola', 'Apátridas', 'Brasil', 'Cabo Verde',
                    'China', 'Espanha', 'França', 'Guiné-Bissau', 'Itália',
                    'Nacionalidade estrangeira', 'Nacionalidade portuguesa',
                    'Nepal', 'População residente', 'Reino Unido', 'Roménia',
                    'São Tomé e Príncipe', 'Ucrânia', 'Índia'
                ]
            })
            
        if table_name == 'Sexo' or table_name is None:
            self.reference_tables['Sexo'] = pd.DataFrame({
                'sexo_id': [1, 2, 3],
                'tipo_sexo': ['HM', 'H', 'M']
            })
            
        if table_name == 'PopulacaoResidente' or table_name is None:
            self.reference_tables['PopulacaoResidente'] = pd.DataFrame({
                'populacao_id': [1, 2],
                'total_populacao': [21130491, 21453772],
                'ano_referencia': [2021, 2011]
            })
            
        if table_name == 'NivelEducacao' or table_name is None:
            self.reference_tables['NivelEducacao'] = pd.DataFrame({
                'nivel_educacao_id': [1, 2, 3, 4, 5, 6, 7],
                'nome_nivel': [
                    'Sem nível de ensino', 'Ensino Básico 1º ciclo',
                    'Ensino Básico 2º ciclo', 'Ensino Básico 3º ciclo',
                    'Ensino Básico', 'Ensino Secundário/pós-secundário',
                    'Ensino Superior'
                ]
            })

    def extract_data(self) -> None:
        """Extrai dados de todos os arquivos CSV"""
        self.logger.info("=" * 60)
        self.logger.info("📂 FASE 1: EXTRAÇÃO DE DADOS")
        self.logger.info("=" * 60)
        
        for key, variations in self.input_files.items():
            try:
                found_file = self.find_available_file(variations)
                
                if not found_file:
                    self.logger.error(f"❌ {key}: Nenhuma variação encontrada")
                    continue
                
                self.logger.info(f"📄 Processando: {found_file}")
                
                # Tentar diferentes encodings
                df = None
                for encoding in ['utf-8', 'latin1', 'cp1252']:
                    try:
                        df = pd.read_csv(found_file, encoding=encoding, header=None)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    raise Exception(f"Erro de encoding em {found_file}")
                
                df_clean = self.clean_dataframe(df, found_file)
                
                if len(df_clean) == 0:
                    self.logger.warning(f"⚠️ {found_file}: 0 registros após limpeza!")
                    continue
                
                self.raw_data[key] = df_clean
                self.statistics['files_processed'] += 1
                self.statistics['records_input'] += len(df_clean)
                
            except Exception as e:
                error_msg = f"❌ Erro em {key}: {e}"
                self.logger.error(error_msg)
                self.statistics['validation_errors'].append(error_msg)

    # ========================================================================
    # CRIAÇÃO DE TABELAS DIMENSIONAIS
    # ========================================================================

    def create_dimensional_tables(self) -> None:
        """Cria todas as tabelas dimensionais"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 FASE 2: CRIAÇÃO DE TABELAS DIMENSIONAIS")
        self.logger.info("=" * 60)
        
        dims = [
            ('CondicaoEconomica', self._create_condicao_economica),
            ('GrupoProfissional', self._create_grupo_profissional),
            ('ProfissaoDigito1', self._create_profissao_digito1),
            ('SetorEconomico', self._create_setor_economico),
            ('SituacaoProfissional', self._create_situacao_profissional),
            ('FonteRendimento', self._create_fonte_rendimento),
            ('RegiaoNUTS', self._create_regiao_nuts)
        ]
        
        for name, func in dims:
            try:
                func()
                self.logger.info(f"✅ {name}: {len(self.dimensional_tables[name])} registros")
            except Exception as e:
                self.logger.error(f"❌ Erro em {name}: {e}")

    def _create_condicao_economica(self) -> None:
        """CondicaoEconomica"""
        self.dimensional_tables['CondicaoEconomica'] = pd.DataFrame([
            {'condicao_id': 1, 'nome_condicao': 'Total', 'categoria': 'Total'},
            {'condicao_id': 2, 'nome_condicao': 'Ativa', 'categoria': 'Ativa'},
            {'condicao_id': 3, 'nome_condicao': 'Empregada', 'categoria': 'Ativa'},
            {'condicao_id': 4, 'nome_condicao': 'Desempregada', 'categoria': 'Ativa'},
            {'condicao_id': 5, 'nome_condicao': 'Inativa', 'categoria': 'Inativa'},
            {'condicao_id': 6, 'nome_condicao': 'População com menos de 15 anos', 'categoria': 'Inativa'},
            {'condicao_id': 7, 'nome_condicao': 'Estudantes', 'categoria': 'Inativa'},
            {'condicao_id': 8, 'nome_condicao': 'Domésticos', 'categoria': 'Inativa'},
            {'condicao_id': 9, 'nome_condicao': 'Reformados', 'categoria': 'Inativa'},
            {'condicao_id': 10, 'nome_condicao': 'Incapacitados para o trabalho', 'categoria': 'Inativa'},
            {'condicao_id': 11, 'nome_condicao': 'Outra situação', 'categoria': 'Inativa'}
        ])

    def _create_grupo_profissional(self) -> None:
        """GrupoProfissional"""
        grupos = []
        descs = [
            'Profissões das Forças Armadas',
            'Representantes do poder legislativo e de órgãos executivos, dirigentes, directores e gestores executivos',
            'Especialistas das actividades intelectuais e científicas',
            'Técnicos e profissões de nível intermédio',
            'Pessoal administrativo',
            'Trabalhadores dos serviços pessoais, de protecção e segurança e vendedores',
            'Agricultores e trabalhadores qualificados da agricultura, da pesca e da floresta',
            'Trabalhadores qualificados da indústria, construção e artífices',
            'Operadores de instalações e máquinas e trabalhadores da montagem',
            'Trabalhadores não qualificados'
        ]
        for i, desc in enumerate(descs):
            grupos.append({'grupo_prof_id': i, 'codigo_grande_grupo': str(i), 'descricao': desc})
        
        self.dimensional_tables['GrupoProfissional'] = pd.DataFrame(grupos)

    def _create_profissao_digito1(self) -> None:
        """ProfissaoDigito1 (idêntico a GrupoProfissional)"""
        grupos = []
        descs = [
            'Profissões das Forças Armadas',
            'Representantes do poder legislativo e de órgãos executivos, dirigentes, directores e gestores executivos',
            'Especialistas das actividades intelectuais e científicas',
            'Técnicos e profissões de nível intermédio',
            'Pessoal administrativo',
            'Trabalhadores dos serviços pessoais, de protecção e segurança e vendedores',
            'Agricultores e trabalhadores qualificados da agricultura, da pesca e da floresta',
            'Trabalhadores qualificados da indústria, construção e artífices',
            'Operadores de instalações e máquinas e trabalhadores da montagem',
            'Trabalhadores não qualificados'
        ]
        for i, desc in enumerate(descs):
            grupos.append({'prof_digito1_id': i, 'codigo_digito1': str(i), 'descricao': desc})
        
        self.dimensional_tables['ProfissaoDigito1'] = pd.DataFrame(grupos)

    def _create_setor_economico(self) -> None:
        """SetorEconomico"""
        setores = []
        cae_letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U']
        cae_descs = [
            'Agricultura, produção animal, caça, floresta e pesca',
            'Indústrias extractivas',
            'Indústrias transformadoras',
            'Electricidade, gás, vapor, água quente e fria e ar frio',
            'Captação, tratamento e distribuição de água; saneamento, gestão de resíduos e despoluição',
            'Construção',
            'Comércio por grosso e a retalho; reparação de veículos automóveis e motociclos',
            'Transportes e armazenagem',
            'Alojamento, restauração e similares',
            'Actividades de informação e de comunicação',
            'Actividades financeiras e de seguros',
            'Actividades imobiliárias',
            'Actividades de consultoria, científicas, técnicas e similares',
            'Actividades administrativas e dos serviços de apoio',
            'Administração Pública e Defesa; Segurança Social Obrigatória',
            'Educação',
            'Actividades de saúde humana e apoio social',
            'Actividades artísticas, de espectáculos, desportivas e recreativas',
            'Outras actividades de serviços',
            'Atividades das famílias empregadoras de pessoal doméstico',
            'Actividades dos organismos internacionais e outras instituições extra-territoriais'
        ]
        
        for i, (cod, desc) in enumerate(zip(cae_letters, cae_descs)):
            setores.append({'setor_id': i+1, 'codigo_cae': cod, 'descricao': desc, 'agregado': False})
        
        # Setores agregados
        setores.extend([
            {'setor_id': 22, 'codigo_cae': 'AGR', 'descricao': 'Agricultura (Secção A)', 'agregado': True},
            {'setor_id': 23, 'codigo_cae': 'IND', 'descricao': 'Indústria (Secção B-E)', 'agregado': True},
            {'setor_id': 24, 'codigo_cae': 'CON', 'descricao': 'Construção (Secção F)', 'agregado': True},
            {'setor_id': 25, 'codigo_cae': 'COM', 'descricao': 'Comércio (Secção G-J)', 'agregado': True},
            {'setor_id': 26, 'codigo_cae': 'FIN', 'descricao': 'Atividades financeiras e imobiliárias (Secção K-L)', 'agregado': True},
            {'setor_id': 27, 'codigo_cae': 'SER', 'descricao': 'Outras atividades de serviços (Secção M-U)', 'agregado': True}
        ])
        
        self.dimensional_tables['SetorEconomico'] = pd.DataFrame(setores)

    def _create_situacao_profissional(self) -> None:
        """SituacaoProfissional"""
        self.dimensional_tables['SituacaoProfissional'] = pd.DataFrame([
            {'situacao_id': 1, 'nome_situacao': 'Empregador/patrão'},
            {'situacao_id': 2, 'nome_situacao': 'Trabalhador por conta própria'},
            {'situacao_id': 3, 'nome_situacao': 'Trabalhador por conta de outrem'},
            {'situacao_id': 4, 'nome_situacao': 'Outra situação'}
        ])

    def _create_fonte_rendimento(self) -> None:
        """FonteRendimento"""
        self.dimensional_tables['FonteRendimento'] = pd.DataFrame([
            {'fonte_id': 1, 'nome_fonte': 'Rendimento do trabalho'},
            {'fonte_id': 2, 'nome_fonte': 'Pensão / Reforma'},
            {'fonte_id': 3, 'nome_fonte': 'Rendimento de propriedade /empresa'},
            {'fonte_id': 4, 'nome_fonte': 'Subsídios temporários (desemprego, RSI, ...)'},
            {'fonte_id': 5, 'nome_fonte': 'A cargo da família'},
            {'fonte_id': 6, 'nome_fonte': 'Outra'}
        ])

    def _create_regiao_nuts(self) -> None:
        """RegiaoNUTS"""
        self.dimensional_tables['RegiaoNUTS'] = pd.DataFrame([
            {'nuts_id': 1, 'codigo_nuts': 'PT', 'nome_regiao': 'Portugal'},
            {'nuts_id': 2, 'codigo_nuts': 'PT11', 'nome_regiao': 'Norte'},
            {'nuts_id': 3, 'codigo_nuts': 'PT16', 'nome_regiao': 'Centro'},
            {'nuts_id': 4, 'codigo_nuts': 'PT17', 'nome_regiao': 'AM Lisboa'},
            {'nuts_id': 5, 'codigo_nuts': 'PT18', 'nome_regiao': 'Alentejo'},
            {'nuts_id': 6, 'codigo_nuts': 'PT15', 'nome_regiao': 'Algarve'},
            {'nuts_id': 7, 'codigo_nuts': 'PT20', 'nome_regiao': 'RA Açores'},
            {'nuts_id': 8, 'codigo_nuts': 'PT30', 'nome_regiao': 'RA Madeira'}
        ])

    # ========================================================================
    # CRIAÇÃO DE TABELAS DE FATO
    # ========================================================================

    def create_fact_tables(self) -> None:
        """Cria todas as tabelas de fato"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📈 FASE 3: CRIAÇÃO DE TABELAS DE FATO")
        self.logger.info("=" * 60)
        
        facts = [
            ('PopulacaoPorCondicao', self._create_populacao_condicao, 'Q3.1'),
            ('EmpregadosPorProfissao', self._create_empregados_profissao, 'Q3.2'),
            ('EmpregadosPorSetor', self._create_empregados_setor, 'Q3.3'),
            ('EmpregadosPorSituacao', self._create_empregados_situacao, 'Q3.4'),
            ('EmpregadosProfSexo', self._create_empregados_prof_sexo, 'Q20'),
            ('EmpregadosRegiaoSetor', self._create_empregados_regiao_setor, 'Q21'),
            ('PopulacaoTrabalhoEscolaridade', self._create_pop_trabalho_esc, 'Q23'),
            ('PopulacaoRendimentoRegiao', self._create_pop_rendimento_regiao, 'Q24')
        ]
        
        for name, func, source in facts:
            try:
                if source in self.raw_data:
                    func()
                    recs = len(self.fact_tables.get(name, []))
                    self.logger.info(f"✅ {name}: {recs} registros")
                else:
                    self.logger.warning(f"⚠️ {name}: Dados {source} ausentes")
            except Exception as e:
                self.logger.error(f"❌ Erro em {name}: {e}")

    def _create_populacao_condicao(self) -> None:
        """PopulacaoPorCondicao de Q3.1"""
        df = self.raw_data['Q3.1']
        records = []
        record_id = 1
        
        for idx, row in df.iterrows():
            nac_id = self.get_nacionalidade_id(row.iloc[0])
            if not nac_id:
                continue
            
            for cond_id in range(1, 12):
                if cond_id < len(row):
                    qtd = row.iloc[cond_id] if not pd.isna(row.iloc[cond_id]) else 0
                    if qtd > 0:
                        records.append({
                            'populacao_cond_id': record_id,
                            'populacao_id': 1,
                            'nacionalidade_id': nac_id,
                            'condicao_id': cond_id,
                            'quantidade': int(qtd),
                            'percentual': 0.0
                        })
                        record_id += 1
        
        self.fact_tables['PopulacaoPorCondicao'] = pd.DataFrame(records)

    def _create_empregados_profissao(self) -> None:
        """EmpregadosPorProfissao de Q3.2"""
        df = self.raw_data['Q3.2']
        records = []
        record_id = 1
        
        for idx, row in df.iterrows():
            nac_id = self.get_nacionalidade_id(row.iloc[0])
            if not nac_id:
                continue
            
            for prof_id in range(10):
                col_idx = prof_id + 2
                if col_idx < len(row):
                    qtd = row.iloc[col_idx] if not pd.isna(row.iloc[col_idx]) else 0
                    if qtd > 0:
                        records.append({
                            'emp_prof_id': record_id,
                            'nacionalidade_id': nac_id,
                            'grupo_prof_id': prof_id,
                            'quantidade': int(qtd)
                        })
                        record_id += 1
        
        self.fact_tables['EmpregadosPorProfissao'] = pd.DataFrame(records)

    def _create_empregados_setor(self) -> None:
        """EmpregadosPorSetor de Q3.3"""
        df = self.raw_data['Q3.3']
        records = []
        record_id = 1
        
        for idx, row in df.iterrows():
            nac_id = self.get_nacionalidade_id(row.iloc[0])
            if not nac_id:
                continue
            
            for setor_id in range(1, 22):
                col_idx = setor_id + 1
                if col_idx < len(row):
                    qtd = row.iloc[col_idx] if not pd.isna(row.iloc[col_idx]) else 0
                    if qtd > 0:
                        records.append({
                            'emp_setor_id': record_id,
                            'nacionalidade_id': nac_id,
                            'setor_id': setor_id,
                            'quantidade': int(qtd)
                        })
                        record_id += 1
        
        self.fact_tables['EmpregadosPorSetor'] = pd.DataFrame(records)

    def _create_empregados_situacao(self) -> None:
        """EmpregadosPorSituacao de Q3.4"""
        df = self.raw_data['Q3.4']
        records = []
        record_id = 1
        
        for idx, row in df.iterrows():
            nac_id = self.get_nacionalidade_id(row.iloc[0])
            if not nac_id:
                continue
            
            for sit_id in range(1, 5):
                col_idx = sit_id + 1
                if col_idx < len(row):
                    qtd = row.iloc[col_idx] if not pd.isna(row.iloc[col_idx]) else 0
                    if qtd > 0:
                        records.append({
                            'emp_situacao_id': record_id,
                            'nacionalidade_id': nac_id,
                            'situacao_id': sit_id,
                            'quantidade': int(qtd)
                        })
                        record_id += 1
        
        self.fact_tables['EmpregadosPorSituacao'] = pd.DataFrame(records)

    def _create_empregados_prof_sexo(self) -> None:
        """EmpregadosProfSexo de Q20"""
        df = self.raw_data['Q20']
        records = []
        record_id = 1
        
        prof_keywords = [
            'Forças Armadas', 'poder legislativo', 'Especialistas', 'Técnicos',
            'administrativo', 'serviços pessoais', 'Agricultores',
            'indústria', 'Operadores', 'não qualificados'
        ]
        
        for idx, row in df.iterrows():
            prof_nome = str(row.iloc[0])
            prof_id = None
            
            for i, keyword in enumerate(prof_keywords):
                if keyword.lower() in prof_nome.lower():
                    prof_id = i
                    break
            
            if prof_id is None:
                continue
            
            qtd_hm = row.iloc[1] if len(row) > 1 and not pd.isna(row.iloc[1]) else 0
            qtd_h = row.iloc[2] if len(row) > 2 and not pd.isna(row.iloc[2]) else 0
            qtd_m = row.iloc[3] if len(row) > 3 and not pd.isna(row.iloc[3]) else 0
            
            if qtd_h > 0 or qtd_m > 0:
                records.append({
                    'emp_prof_sexo_id': record_id,
                    'prof_digito1_id': prof_id,
                    'sexo_id': 1,
                    'quantidade_homens': int(qtd_h),
                    'quantidade_mulheres': int(qtd_m)
                })
                record_id += 1
        
        self.fact_tables['EmpregadosProfSexo'] = pd.DataFrame(records)

    def _create_empregados_regiao_setor(self) -> None:
        """EmpregadosRegiaoSetor de Q21"""
        df = self.raw_data['Q21']
        records = []
        record_id = 1
        
        regiao_map = {
            'Portugal': 1, 'Norte': 2, 'Centro': 3, 'AM Lisboa': 4,
            'Alentejo': 5, 'Algarve': 6, 'RA Açores': 7, 'RA Madeira': 8
        }
        
        for idx, row in df.iterrows():
            nuts_id = regiao_map.get(str(row.iloc[0]))
            if not nuts_id:
                continue
            
            for setor_idx in range(6):
                col_idx = setor_idx + 2
                if col_idx < len(row):
                    qtd = row.iloc[col_idx] if not pd.isna(row.iloc[col_idx]) else 0
                    if qtd > 0:
                        records.append({
                            'emp_regiao_setor_id': record_id,
                            'nuts_id': nuts_id,
                            'setor_id': setor_idx + 22,
                            'quantidade': int(qtd)
                        })
                        record_id += 1
        
        self.fact_tables['EmpregadosRegiaoSetor'] = pd.DataFrame(records)

    def _create_pop_trabalho_esc(self) -> None:
        """PopulacaoTrabalhoEscolaridade de Q23"""
        df = self.raw_data['Q23']
        records = []
        record_id = 1
        
        nivel_map = {
            'Sem nível': 1, 'Básico 1º': 2, 'Básico 2º': 3,
            'Básico 3º': 4, 'Secundário': 6, 'Superior': 7
        }
        
        condicoes = ['Empregada', 'Desempregada', 'Não activa']
        
        for idx, row in df.iterrows():
            nivel_nome = str(row.iloc[0])
            nivel_id = None
            
            for key, nid in nivel_map.items():
                if key.lower() in nivel_nome.lower():
                    nivel_id = nid
                    break
            
            if not nivel_id:
                continue
            
            for cond_idx, condicao in enumerate(condicoes):
                base_col = cond_idx * 3 + 1
                
                if base_col + 2 < len(row):
                    qtd_hm = row.iloc[base_col] if not pd.isna(row.iloc[base_col]) else 0
                    qtd_h = row.iloc[base_col + 1] if not pd.isna(row.iloc[base_col + 1]) else 0
                    qtd_m = row.iloc[base_col + 2] if not pd.isna(row.iloc[base_col + 2]) else 0
                    
                    if qtd_hm > 0:
                        records.append({
                            'pop_trab_esc_id': record_id,
                            'nivel_educacao_id': nivel_id,
                            'sexo_id': 1,
                            'condicao_trabalho': condicao,
                            'quantidade_hm': int(qtd_hm),
                            'quantidade_h': int(qtd_h),
                            'quantidade_m': int(qtd_m)
                        })
                        record_id += 1
        
        self.fact_tables['PopulacaoTrabalhoEscolaridade'] = pd.DataFrame(records)

    def _create_pop_rendimento_regiao(self) -> None:
        """PopulacaoRendimentoRegiao de Q24"""
        df = self.raw_data['Q24']
        records = []
        record_id = 1
        
        regiao_map = {
            'Portugal': 1, 'Norte': 2, 'Centro': 3, 'AM Lisboa': 4,
            'Alentejo': 5, 'Algarve': 6, 'RA Açores': 7, 'RA Madeira': 8
        }
        
        for idx, row in df.iterrows():
            nuts_id = regiao_map.get(str(row.iloc[0]))
            if not nuts_id:
                continue
            
            for fonte_id in range(1, 7):
                col_idx = fonte_id + 1
                if col_idx < len(row):
                    qtd = row.iloc[col_idx] if not pd.isna(row.iloc[col_idx]) else 0
                    if qtd > 0:
                        records.append({
                            'pop_rend_reg_id': record_id,
                            'nuts_id': nuts_id,
                            'fonte_id': fonte_id,
                            'quantidade': int(qtd)
                        })
                        record_id += 1
        
        self.fact_tables['PopulacaoRendimentoRegiao'] = pd.DataFrame(records)

    # ========================================================================
    # VALIDAÇÃO
    # ========================================================================

    def validate_all(self) -> Dict:
        """Valida integridade referencial"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("✓ FASE 4: VALIDAÇÃO DE INTEGRIDADE")
        self.logger.info("=" * 60)
        
        results = {'errors': [], 'warnings': [], 'passed': []}
        
        # Validar nacionalidades
        nac_ref = set(self.reference_tables['Nacionalidade']['nacionalidade_id'])
        for name, df in self.fact_tables.items():
            if 'nacionalidade_id' in df.columns:
                invalid = set(df['nacionalidade_id']) - nac_ref
                if invalid:
                    results['errors'].append(f"{name}: IDs inválidos {invalid}")
                else:
                    results['passed'].append(f"{name}: Nacionalidades ✓")
        
        # Validar sexos
        sex_ref = set(self.reference_tables['Sexo']['sexo_id'])
        for name, df in self.fact_tables.items():
            if 'sexo_id' in df.columns:
                invalid = set(df['sexo_id']) - sex_ref
                if invalid:
                    results['errors'].append(f"{name}: Sexo IDs inválidos {invalid}")
                else:
                    results['passed'].append(f"{name}: Sexos ✓")
        
        # Validar não-negativos
        for name, df in {**self.dimensional_tables, **self.fact_tables}.items():
            for col in df.select_dtypes(include=[np.number]).columns:
                if not col.endswith('_id'):
                    if (df[col] < 0).sum() > 0:
                        results['errors'].append(f"{name}.{col}: Valores negativos")
                    else:
                        results['passed'].append(f"{name}.{col}: Não-negativos ✓")
        
        self.logger.info(f"✅ Passou: {len(results['passed'])}")
        self.logger.info(f"⚠️ Avisos: {len(results['warnings'])}")
        self.logger.info(f"❌ Erros: {len(results['errors'])}")
        
        return results

    # ========================================================================
    # SALVAMENTO
    # ========================================================================

    def save_all_tables(self) -> None:
        """Salva todas as tabelas"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("💾 FASE 5: SALVAMENTO DE TABELAS")
        self.logger.info("=" * 60)
        
        for name, df in self.dimensional_tables.items():
            filename = f"{name}.csv"
            df.to_csv(filename, index=False, encoding='utf-8')
            self.statistics['records_output'] += len(df)
            self.logger.info(f"💾 {filename}: {len(df)} registros")
        
        for name, df in self.fact_tables.items():
            filename = f"{name}.csv"
            df.to_csv(filename, index=False, encoding='utf-8')
            self.statistics['records_output'] += len(df)
            self.logger.info(f"💾 {filename}: {len(df)} registros")
        
        self._create_index()

    def _create_index(self) -> None:
        """Cria índice de tabelas"""
        index = []
        
        for name, df in self.dimensional_tables.items():
            index.append({
                'arquivo': f"{name}.csv",
                'tabela': name,
                'tipo': 'Dimensional',
                'registros': len(df),
                'colunas': len(df.columns)
            })
        
        for name, df in self.fact_tables.items():
            index.append({
                'arquivo': f"{name}.csv",
                'tabela': name,
                'tipo': 'Fato',
                'registros': len(df),
                'colunas': len(df.columns)
            })
        
        pd.DataFrame(index).to_csv('INDICE_TABELAS_LABORAIS.csv', index=False, encoding='utf-8')
        self.logger.info("📋 INDICE_TABELAS_LABORAIS.csv criado")

    def generate_report(self) -> str:
        """Gera relatório final"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 FASE 6: RELATÓRIO FINAL")
        self.logger.info("=" * 60)
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        lines = [
            "RELATÓRIO ETL - DADOS LABORAIS CENSOS 2021",
            f"Timestamp: {timestamp}",
            "=" * 60,
            "",
            "ESTATÍSTICAS:",
            f"Arquivos processados: {self.statistics['files_processed']}/8",
            f"Registros entrada: {self.statistics['records_input']:,}",
            f"Registros saída: {self.statistics['records_output']:,}",
            f"Tabelas dimensionais: {len(self.dimensional_tables)}",
            f"Tabelas de fato: {len(self.fact_tables)}",
            "",
            "ARQUIVOS GERADOS:"
        ]
        
        for name in self.dimensional_tables.keys():
            lines.append(f"  {name}.csv")
        for name in self.fact_tables.keys():
            lines.append(f"  {name}.csv")
        
        lines.extend(["", "✅ PROCESSAMENTO CONCLUÍDO"])
        
        report = "\n".join(lines)
        
        with open('RELATORIO_ESTATISTICAS.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        self.logger.info("📊 RELATORIO_ESTATISTICAS.txt criado")
        return report

    # ========================================================================
    # EXECUÇÃO PRINCIPAL
    # ========================================================================

    def run_etl(self) -> bool:
        """Executa ETL completo"""
        try:
            self.logger.info("\n" + "🚀" * 30)
            self.logger.info("PROCESSO ETL LABORAL - CENSOS 2021 PORTUGAL")
            self.logger.info("🚀" * 30 + "\n")
            
            self.load_reference_tables()
            self.extract_data()
            
            if not self.raw_data:
                self.logger.error("❌ Nenhum arquivo carregado!")
                return False
            
            self.create_dimensional_tables()
            self.create_fact_tables()
            
            validation = self.validate_all()
            self.statistics['validation_errors'].extend(validation['errors'])
            self.statistics['warnings'].extend(validation['warnings'])
            
            self.save_all_tables()
            self.generate_report()
            
            success = len(self.statistics['validation_errors']) == 0
            
            if success:
                self.logger.info("\n" + "🎉" * 30)
                self.logger.info("ETL CONCLUÍDO COM SUCESSO!")
                self.logger.info("🎉" * 30)
            else:
                self.logger.warning(f"\n⚠️ ETL concluído com {len(self.statistics['validation_errors'])} erros")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ ERRO CRÍTICO: {e}")
            return False

# ============================================================================
# FUNÇÕES GOOGLE COLAB
# ============================================================================

def setup_colab():
    """Setup para Google Colab"""
    print("🔧 Configurando ambiente...")
    try:
        import pandas as pd
        import numpy as np
        print("✅ Dependências OK")
    except:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "numpy"])
    print("🎯 Pronto!")

def upload_files():
    """Upload de arquivos no Colab"""
    try:
        from google.colab import files
        print("📁 Faça upload dos 8 arquivos CSV:")
        print("Q3.1, Q3.2, Q3.3, Q3.4, Q20, Q21, Q23, Q24")
        uploaded = files.upload()
        print(f"✅ {len(uploaded)} arquivos carregados")
        return list(uploaded.keys())
    except:
        print("⚠️ Execute no Google Colab")
        return []

def download_results():
    """Download dos resultados"""
    try:
        from google.colab import files
        import zipfile
        
        print("📦 Criando ZIP...")
        
        outputs = []
        for file in Path('.').glob('*.csv'):
            if any(file.name.startswith(x) for x in ['Condicao', 'Grupo', 'Profissao', 'Setor', 'Situacao', 'Fonte', 'Regiao', 'Populacao', 'Empregados', 'INDICE']):
                outputs.append(file.name)
        
        for file in ['RELATORIO_ESTATISTICAS.txt', 'etl_laboral_log.txt']:
            if Path(file).exists():
                outputs.append(file)
        
        with zipfile.ZipFile('resultados_etl_laboral.zip', 'w') as zipf:
            for f in outputs:
                zipf.write(f)
                print(f"  ✅ {f}")
        
        print(f"\n📥 Baixando {len(outputs)} arquivos...")
        files.download('resultados_etl_laboral.zip')
        print("🎉 Download concluído!")
        
    except:
        print("📂 Arquivos salvos no diretório atual")

def run_colab_etl():
    """Função principal para Colab"""
    print("🇵🇹 ETL DADOS LABORAIS CENSOS 2021 PORTUGAL")
    print("=" * 50)
    
    setup_colab()
    upload_files()
    
    print("\n🔄 Processando...")
    processor = ETLLaboralProcessor()
    success = processor.run_etl()
    
    if success:
        download_results()
    
    return success

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        import google.colab
        print("🔍 Google Colab detectado")
        success = run_colab_etl()
    except:
        print("🖥️ Execução local")
        processor = ETLLaboralProcessor()
        success = processor.run_etl()
    
    if success:
        print("\n" + "=" * 60)
        print("🎉 ETL CONCLUÍDO COM SUCESSO!")
        print("📊 Dataset laboral normalizado criado")
        print("🔗 Pronto para análises")
        print("=" * 60)
    else:
        print("\n❌ ETL FALHOU - Verifique os logs")
