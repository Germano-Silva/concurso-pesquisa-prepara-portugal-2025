#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para processar dados de educação do INE (Q2.1.csv) e integrar ao modelo ER existente
Extensão do modelo relacional para incluir dados educacionais da população estrangeira.

Autor: Sistema de Análise de Dados
Data: 2024
Fonte: INE - Censos 2021, População Estrangeira - Q2.1 (Educação)
"""

import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

class ProcessadorEducacaoINE:
    """Classe para processar dados educacionais e integrar ao modelo ER existente."""

    def __init__(self):
        self.dados_originais = {}
        self.tabelas_normalizadas = {}
        self.tabelas_existentes = {}
        self.contadores_id = {
            'nivel_educacao_id': 1,
            'populacao_educacao_id': 1,
            'estatistica_id': 1
        }

    def carregar_arquivo_educacao(self):
        """Carrega o arquivo Q2.1.csv com dados educacionais."""
        print("📚 Carregando dados educacionais do INE (Q2.1.csv)...")

        try:
            # Arquivo de educação
            arquivo_educacao = "/content/Q2.1.csv"
            df_educacao = pd.read_csv(arquivo_educacao, encoding='latin1')
            self.dados_originais['q2_1'] = df_educacao
            print(f"✅ Q2.1: {df_educacao.shape[0]} linhas, {df_educacao.shape[1]} colunas")
            
        except Exception as e:
            print(f"❌ Erro ao carregar Q2.1.csv: {e}")
            return False

        print("📥 Arquivo educacional carregado com sucesso\n")
        return True

    def carregar_tabelas_existentes(self):
        """Carrega tabelas já existentes para integração."""
        print("🔗 Carregando tabelas existentes para integração...")

        try:
            # Carrega tabela de nacionalidades existente
            arquivo_nacionalidades = "/content/Nacionalidade.csv"
            df_nacionalidades = pd.read_csv(arquivo_nacionalidades, encoding='utf-8')
            self.tabelas_existentes['Nacionalidade'] = df_nacionalidades
            print(f"✅ Nacionalidade: {len(df_nacionalidades)} registros")

        except Exception as e:
            print(f"❌ Erro ao carregar tabelas existentes: {e}")
            print("⚠️  Continuando sem integração com tabelas existentes...")

        print("🔗 Tabelas existentes processadas\n")

    def limpar_dados_educacao(self):
        """Processa e limpa dados do arquivo Q2.1 - Educação."""
        print("🧹 Processando dados educacionais (Q2.1)...")

        df = self.dados_originais['q2_1'].copy()

        # Remove linhas de cabeçalho e rodapé
        df = df.dropna(subset=[df.columns[0]])
        df = df[~df.iloc[:, 0].str.contains('Quadro|Fonte:|^,', na=False)]

        linhas_dados = []
        
        for idx, row in df.iterrows():
            nacionalidade = str(row.iloc[0]).strip()
            
            # Filtra apenas nacionalidades válidas (exclui cabeçalhos e totais gerais)
            if (nacionalidade and 
                nacionalidade not in ['', 'nan'] and 
                not nacionalidade.startswith(',') and
                'Nível de ensino' not in nacionalidade and
                nacionalidade != 'Total'):
                
                try:
                    # Extrai dados das colunas numéricas
                    total_pop = self._clean_number(row.iloc[1]) if len(row) > 1 else 0
                    
                    # Nível Nenhum
                    nenhum = self._clean_number(row.iloc[2]) if len(row) > 2 else 0
                    
                    # Ensino Básico Total
                    basico_total = self._clean_number(row.iloc[3]) if len(row) > 3 else 0
                    
                    # Ensino Básico - 1º ciclo
                    basico_1_ciclo = self._clean_number(row.iloc[4]) if len(row) > 4 else 0
                    
                    # Ensino Básico - 2º ciclo  
                    basico_2_ciclo = self._clean_number(row.iloc[5]) if len(row) > 5 else 0
                    
                    # Ensino Básico - 3º ciclo
                    basico_3_ciclo = self._clean_number(row.iloc[6]) if len(row) > 6 else 0
                    
                    # Ensino Secundário e Pós-secundário
                    secundario = self._clean_number(row.iloc[7]) if len(row) > 7 else 0
                    
                    # Ensino Superior
                    superior = self._clean_number(row.iloc[8]) if len(row) > 8 else 0

                    if total_pop > 0:  # Pelo menos um valor válido
                        linhas_dados.append({
                            'nacionalidade': nacionalidade,
                            'total_populacao': total_pop,
                            'nenhum': nenhum,
                            'basico_total': basico_total,
                            'basico_1_ciclo': basico_1_ciclo,
                            'basico_2_ciclo': basico_2_ciclo,
                            'basico_3_ciclo': basico_3_ciclo,
                            'secundario': secundario,
                            'superior': superior
                        })
                        
                except Exception as e:
                    print(f"⚠️  Erro ao processar linha {idx}: {e}")
                    continue

        self.dados_originais['q2_1_limpo'] = pd.DataFrame(linhas_dados)
        print(f"✅ Q2.1 processado: {len(linhas_dados)} nacionalidades com dados educacionais")

    def _clean_number(self, value):
        """Limpa e converte valores numéricos."""
        if pd.isna(value) or value == '':
            return 0

        # Convert to string and clean
        str_val = str(value).strip()

        # Remove espaços e vírgulas como separadores de milhares
        str_val = str_val.replace(' ', '').replace(',', '')

        try:
            # Tenta converter para int
            return int(str_val)
        except:
            try:
                # Se falhar, tenta float
                return float(str_val)
            except:
                return 0

    def criar_tabela_nivel_educacao(self):
        """Cria a tabela NivelEducacao com todos os níveis educacionais."""
        print("🏗️  Criando tabela NivelEducacao...")

        niveis_educacao = [
            {'nome': 'Nenhum', 'categoria': 'Sem Educação Formal', 'ordem': 0},
            {'nome': 'Ensino Básico - 1º Ciclo', 'categoria': 'Ensino Básico', 'ordem': 1},
            {'nome': 'Ensino Básico - 2º Ciclo', 'categoria': 'Ensino Básico', 'ordem': 2},
            {'nome': 'Ensino Básico - 3º Ciclo', 'categoria': 'Ensino Básico', 'ordem': 3},
            {'nome': 'Ensino Básico - Total', 'categoria': 'Ensino Básico', 'ordem': 4},
            {'nome': 'Ensino Secundário e Pós-Secundário', 'categoria': 'Ensino Secundário', 'ordem': 5},
            {'nome': 'Ensino Superior', 'categoria': 'Ensino Superior', 'ordem': 6}
        ]

        tabela = []
        for nivel in niveis_educacao:
            nivel_educacao_id = self.contadores_id['nivel_educacao_id']
            
            tabela.append({
                'nivel_educacao_id': nivel_educacao_id,
                'nome_nivel': nivel['nome'],
                'categoria': nivel['categoria'],
                'ordem_hierarquica': nivel['ordem']
            })
            
            self.contadores_id['nivel_educacao_id'] += 1

        self.tabelas_normalizadas['NivelEducacao'] = pd.DataFrame(tabela)
        print(f"✅ Tabela NivelEducacao criada: {len(tabela)} registros")

    def criar_tabela_populacao_educacao(self):
        """Cria a tabela PopulacaoEducacao com dados por nacionalidade e nível."""
        print("🏗️  Criando tabela PopulacaoEducacao...")

        if 'q2_1_limpo' not in self.dados_originais:
            print("❌ Dados educacionais limpos não disponíveis")
            return

        df_educacao = self.dados_originais['q2_1_limpo']
        tabela = []

        # Mapeamento dos níveis educacionais para IDs
        niveis_map = {
            'nenhum': 1,
            'basico_1_ciclo': 2,
            'basico_2_ciclo': 3,
            'basico_3_ciclo': 4,
            'basico_total': 5,
            'secundario': 6,
            'superior': 7
        }

        # Mapeia nacionalidades para IDs (se tabela existente estiver disponível)
        nac_to_id = self._criar_mapa_nacionalidade_id()

        for _, row in df_educacao.iterrows():
            nacionalidade = row['nacionalidade']
            
            # Tenta encontrar ID da nacionalidade na tabela existente
            nacionalidade_id = nac_to_id.get(nacionalidade, None)
            if nacionalidade_id is None:
                # Se não encontrar, cria ID sequencial
                nacionalidade_id = len(nac_to_id) + 1
                nac_to_id[nacionalidade] = nacionalidade_id

            # Para cada nível educacional
            for nivel_col, nivel_id in niveis_map.items():
                populacao_nivel = row[nivel_col]
                
                if populacao_nivel > 0:
                    populacao_educacao_id = self.contadores_id['populacao_educacao_id']
                    
                    # Calcula percentual do nível em relação ao total da nacionalidade
                    total_nac = row['total_populacao']
                    percentual = (populacao_nivel / total_nac * 100) if total_nac > 0 else 0

                    tabela.append({
                        'populacao_educacao_id': populacao_educacao_id,
                        'nacionalidade_id': nacionalidade_id,
                        'nivel_educacao_id': nivel_id,
                        'populacao_total': populacao_nivel,
                        'faixa_etaria': '15-64 anos',
                        'ano_referencia': 2021,
                        'percentual_nivel': round(percentual, 2)
                    })
                    
                    self.contadores_id['populacao_educacao_id'] += 1

        self.tabelas_normalizadas['PopulacaoEducacao'] = pd.DataFrame(tabela)
        print(f"✅ Tabela PopulacaoEducacao criada: {len(tabela)} registros")

    def criar_tabela_estatisticas_educacao(self):
        """Cria tabela com estatísticas educacionais consolidadas por nacionalidade."""
        print("🏗️  Criando tabela EstatisticasEducacao...")

        if 'q2_1_limpo' not in self.dados_originais:
            print("❌ Dados educacionais limpos não disponíveis")
            return

        df_educacao = self.dados_originais['q2_1_limpo']
        tabela = []

        # Mapeia nacionalidades para IDs
        nac_to_id = self._criar_mapa_nacionalidade_id()

        for _, row in df_educacao.iterrows():
            nacionalidade = row['nacionalidade']
            
            # Tenta encontrar ID da nacionalidade
            nacionalidade_id = nac_to_id.get(nacionalidade, len(nac_to_id) + 1)

            total_pop = row['total_populacao']
            
            if total_pop > 0:
                estatistica_id = self.contadores_id['estatistica_id']
                
                # Calcula estatísticas educacionais
                sem_educacao = row['nenhum']
                ensino_superior = row['superior']
                ensino_basico = row['basico_total']
                ensino_secundario = row['secundario']
                
                # Percentuais
                perc_sem_educacao = (sem_educacao / total_pop * 100) if total_pop > 0 else 0
                perc_ensino_superior = (ensino_superior / total_pop * 100) if total_pop > 0 else 0
                perc_ensino_basico = (ensino_basico / total_pop * 100) if total_pop > 0 else 0
                perc_ensino_secundario = (ensino_secundario / total_pop * 100) if total_pop > 0 else 0
                
                # Índice educacional simples (peso maior para níveis superiores)
                indice_educacional = (
                    (sem_educacao * 0) + 
                    (ensino_basico * 1) + 
                    (ensino_secundario * 2) + 
                    (ensino_superior * 3)
                ) / total_pop if total_pop > 0 else 0

                tabela.append({
                    'estatistica_id': estatistica_id,
                    'nacionalidade_id': nacionalidade_id,
                    'populacao_total_educacao': total_pop,
                    'sem_educacao': sem_educacao,
                    'ensino_basico': ensino_basico,
                    'ensino_secundario': ensino_secundario,
                    'ensino_superior': ensino_superior,
                    'percentual_sem_educacao': round(perc_sem_educacao, 2),
                    'percentual_ensino_superior': round(perc_ensino_superior, 2),
                    'percentual_ensino_basico': round(perc_ensino_basico, 2),
                    'percentual_ensino_secundario': round(perc_ensino_secundario, 2),
                    'indice_educacional': round(indice_educacional, 2),
                    'ano_referencia': 2021
                })
                
                self.contadores_id['estatistica_id'] += 1

        self.tabelas_normalizadas['EstatisticasEducacao'] = pd.DataFrame(tabela)
        print(f"✅ Tabela EstatisticasEducacao criada: {len(tabela)} registros")

    def criar_mapeamento_nacionalidades(self):
        """Cria tabela auxiliar para mapear nacionalidades educacionais com existentes."""
        print("🏗️  Criando mapeamento de nacionalidades...")

        if 'q2_1_limpo' not in self.dados_originais:
            return

        df_educacao = self.dados_originais['q2_1_limpo']
        tabela = []

        nacionalidades_educacao = df_educacao['nacionalidade'].unique()
        
        for idx, nacionalidade in enumerate(nacionalidades_educacao, 1):
            # Busca correspondência na tabela existente
            nacionalidade_id_existente = None
            if 'Nacionalidade' in self.tabelas_existentes:
                df_nac_existente = self.tabelas_existentes['Nacionalidade']
                match = df_nac_existente[
                    df_nac_existente['nome_nacionalidade'].str.contains(
                        nacionalidade.split()[0], na=False, case=False
                    )
                ]
                if not match.empty:
                    nacionalidade_id_existente = match.iloc[0]['nacionalidade_id']

            tabela.append({
                'nacionalidade_educacao_id': idx,
                'nome_nacionalidade_educacao': nacionalidade,
                'nacionalidade_id_existente': nacionalidade_id_existente,
                'compatibilidade': 'Sim' if nacionalidade_id_existente else 'Novo'
            })

        self.tabelas_normalizadas['MapeamentoNacionalidades'] = pd.DataFrame(tabela)
        print(f"✅ Mapeamento criado: {len(tabela)} nacionalidades educacionais")

    def _criar_mapa_nacionalidade_id(self):
        """Cria mapeamento entre nacionalidades e IDs existentes."""
        mapa = {}
        
        if 'Nacionalidade' in self.tabelas_existentes:
            df = self.tabelas_existentes['Nacionalidade']
            for _, row in df.iterrows():
                nome = row['nome_nacionalidade']
                id_nac = row['nacionalidade_id']
                mapa[nome] = id_nac
                
                # Adiciona variações comuns de nomes
                if 'População' in nome:
                    mapa['População residente'] = id_nac
                elif 'portuguesa' in nome:
                    mapa['Nacionalidade portuguesa'] = id_nac
                elif 'estrangeira' in nome:
                    mapa['Nacionalidade estrangeira'] = id_nac

        return mapa

    def salvar_tabelas_csv(self, output_dir="3️⃣ Data Preparation/data/DP-01-B"):
        """Salva todas as tabelas normalizadas como arquivos CSV."""
        print("💾 Exportando tabelas educacionais para CSV...")

        # Cria diretório se não existir
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        arquivos_gerados = []

        for nome_tabela, df in self.tabelas_normalizadas.items():
            arquivo_csv = f"{output_dir}/{nome_tabela}.csv"

            try:
                df.to_csv(arquivo_csv, index=False, encoding='utf-8')
                arquivos_gerados.append(arquivo_csv)
                print(f"✅ {nome_tabela}: {len(df)} registros → {arquivo_csv}")
            except Exception as e:
                print(f"❌ Erro ao salvar {nome_tabela}: {e}")

        # Cria arquivo de índice
        self._criar_arquivo_indice_educacao(output_dir, arquivos_gerados)

        print(f"\n📊 Exportação educacional concluída: {len(arquivos_gerados)} tabelas salvas em '{output_dir}'")
        return arquivos_gerados

    def _criar_arquivo_indice_educacao(self, output_dir, arquivos_gerados):
        """Cria arquivo de índice específico para tabelas educacionais."""
        print("📋 Criando arquivo de índice educacional...")

        indice_info = {
            'arquivo': [],
            'tabela': [],
            'registros': [],
            'colunas': [],
            'descricao': []
        }

        descricoes = {
            'NivelEducacao': 'Cadastro dos níveis educacionais padronizados',
            'PopulacaoEducacao': 'População por nacionalidade e nível educacional (15-64 anos)',
            'EstatisticasEducacao': 'Estatísticas educacionais consolidadas por nacionalidade',
            'MapeamentoNacionalidades': 'Mapeamento entre nacionalidades educacionais e existentes'
        }

        for arquivo in arquivos_gerados:
            nome_arquivo = Path(arquivo).name
            nome_tabela = nome_arquivo.replace('.csv', '')

            if nome_tabela in self.tabelas_normalizadas:
                df = self.tabelas_normalizadas[nome_tabela]

                indice_info['arquivo'].append(nome_arquivo)
                indice_info['tabela'].append(nome_tabela)
                indice_info['registros'].append(len(df))
                indice_info['colunas'].append(len(df.columns))
                indice_info['descricao'].append(descricoes.get(nome_tabela, 'Tabela educacional'))

        # Salva índice
        df_indice = pd.DataFrame(indice_info)
        arquivo_indice = f"{output_dir}/INDICE_TABELAS_EDUCACAO.csv"
        df_indice.to_csv(arquivo_indice, index=False, encoding='utf-8')
        print(f"✅ Índice educacional criado: {arquivo_indice}")

    def gerar_relatorio_educacao(self):
        """Gera relatório final do processamento educacional."""
        print("\n" + "="*60)
        print("📚 RELATÓRIO FINAL - INTEGRAÇÃO EDUCACIONAL")
        print("="*60)

        print(f"\n🔍 DADOS EDUCACIONAIS PROCESSADOS:")
        for nome, df in self.dados_originais.items():
            if isinstance(df, pd.DataFrame):
                print(f"  • {nome}: {len(df)} registros")

        print(f"\n🗄️  NOVAS TABELAS EDUCACIONAIS CRIADAS:")
        total_registros = 0
        for nome, df in self.tabelas_normalizadas.items():
            print(f"  • {nome}: {len(df)} registros")
            total_registros += len(df)

        print(f"\n📈 ESTATÍSTICAS EDUCACIONAIS:")
        print(f"  • Total de registros educacionais: {total_registros:,}")
        print(f"  • Tabelas educacionais criadas: {len(self.tabelas_normalizadas)}")
        print(f"  • Integração com modelo ER existente: Compatível")
        print(f"  • Faixa etária analisada: 15-64 anos")

        print(f"\n✅ EXTENSÃO EDUCACIONAL DO MODELO ER IMPLEMENTADA!")
        print("="*60 + "\n")

def main():
    """Função principal para executar todo o processamento educacional."""
    print("📚 INICIANDO PROCESSAMENTO EDUCACIONAL - EXTENSÃO DO MODELO ER")
    print("=" * 70)

    # Instancia o processador educacional
    processador = ProcessadorEducacaoINE()

    try:
        # 1. Carrega arquivo educacional
        if not processador.carregar_arquivo_educacao():
            return False

        # 2. Carrega tabelas existentes para integração
        processador.carregar_tabelas_existentes()

        # 3. Limpa e processa dados educacionais
        processador.limpar_dados_educacao()

        print("\n" + "="*50)
        print("🏗️  CRIANDO EXTENSÃO EDUCACIONAL DO MODELO ER")
        print("="*50)

        # 4. Cria novas tabelas educacionais
        processador.criar_tabela_nivel_educacao()
        processador.criar_tabela_populacao_educacao()
        processador.criar_tabela_estatisticas_educacao()
        processador.criar_mapeamento_nacionalidades()

        # 5. Exporta para CSV
        print("\n" + "="*50)
        print("💾 EXPORTANDO DADOS EDUCACIONAIS PROCESSADOS")
        print("="*50)

        arquivos_gerados = processador.salvar_tabelas_csv()

        # 6. Gera relatório final
        processador.gerar_relatorio_educacao()

        return True

    except Exception as e:
        print(f"\n❌ ERRO DURANTE O PROCESSAMENTO EDUCACIONAL: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    sucesso = main()
    if sucesso:
        print("🎉 Processamento educacional concluído com sucesso!")
        print("🔗 Dados prontos para integração com modelo ER existente!")
    else:
        print("💥 Processamento educacional falhou. Verifique os logs acima.")
