# -*- coding: utf-8 -*-
"""
ETL_EDUCACAO - SCRIPT HIBRIDO (Colab + Local)
Pipeline ETL completo para dados INE 2011 - Educacao
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
    def get_pasta_input():
        if AMBIENTE == 'COLAB':
            return Path('/content/input')
        else:
            return Path('input')
    
    @staticmethod
    def get_pasta_output():
        if AMBIENTE == 'COLAB':
            return Path('/content/output')
        else:
            return Path('output')
    
    @staticmethod
    def upload_arquivos():
        if AMBIENTE == 'COLAB':
            print("\n[UPLOAD] Faca upload dos arquivos CSV:")
            uploaded = files.upload()
            dados = {}
            for filename, content in uploaded.items():
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding='utf-8', sep=';', decimal=',')
                    dados[filename] = df
                    print(f"[OK] {filename} carregado ({len(df)} linhas)")
                except Exception as e:
                    print(f"[ERRO] Erro ao carregar {filename}: {e}")
            return dados
        else:
            print("\n[LEITURA] Lendo arquivos da pasta 'input/'...")
            pasta_input = ConfigAmbiente.get_pasta_input()
            
            if not pasta_input.exists():
                pasta_input.mkdir(parents=True, exist_ok=True)
                print(f"[AVISO] Pasta {pasta_input} criada. Coloque seus arquivos CSV la e execute novamente.")
                return {}
            
            dados = {}
            for arquivo in pasta_input.glob('*.csv'):
                try:
                    df = pd.read_csv(arquivo, encoding='utf-8', sep=';', decimal=',')
                    dados[arquivo.name] = df
                    print(f"[OK] {arquivo.name} carregado ({len(df)} linhas)")
                except Exception as e:
                    print(f"[ERRO] Erro ao carregar {arquivo.name}: {e}")
            
            if not dados:
                print(f"[AVISO] Nenhum arquivo CSV encontrado em {pasta_input}")
            
            return dados
    
    @staticmethod
    def download_arquivo(filename, dataframe):
        if AMBIENTE == 'COLAB':
            dataframe.to_csv(filename, index=False, encoding='utf-8')
            files.download(filename)
            print(f"[DOWNLOAD] {filename} disponivel para download")
        else:
            pasta_output = ConfigAmbiente.get_pasta_output()
            pasta_output.mkdir(parents=True, exist_ok=True)
            filepath = pasta_output / filename
            dataframe.to_csv(filepath, index=False, encoding='utf-8')
            print(f"[SALVO] {filename} salvo em {filepath}")
    
    @staticmethod
    def download_zip(zip_buffer, filename):
        if AMBIENTE == 'COLAB':
            files.download(filename)
        else:
            pasta_output = ConfigAmbiente.get_pasta_output()
            pasta_output.mkdir(parents=True, exist_ok=True)
            filepath = pasta_output / filename
            with open(filepath, 'wb') as f:
                f.write(zip_buffer.getvalue())
            print(f"[SALVO] {filename} salvo em {filepath}")


class Config:
    PROJETO_NOME = "ETL INE 2011 - Educacao"
    VERSAO = "2.1-HIBRIDO-WIN"
    ANO_REFERENCIA = 2011
    FONTE_DADOS = "INE Censos 2011"
    
    ARQUIVOS_PAISES = [
        'Angola.csv', 'Brasil.csv', 'Cabo Verde.csv', 'Espanha.csv',
        'Franca.csv', 'Guine-Bissau.csv', 'Reino Unido.csv',
        'Republica da Moldavia.csv', 'Republica Popular da China.csv',
        'Romenia.csv', 'Sao tome e Principe.csv', 'Ucrania.csv'
    ]
    
    NIVEIS_EDUCACAO = {
        1: {'nome': 'Inferior ao basico 3 ciclo', 'categoria': 'Baixa', 'ordem': 1},
        2: {'nome': 'Basico 3 ciclo', 'categoria': 'Media-Baixa', 'ordem': 2},
        3: {'nome': 'Secundario e pos-secundario', 'categoria': 'Media-Alta', 'ordem': 3},
        4: {'nome': 'Superior', 'categoria': 'Alta', 'ordem': 4}
    }


class Formatadores:
    @staticmethod
    def limpar_numero(valor):
        if pd.isna(valor) or valor == '':
            return None
        valor_str = str(valor).replace('%', '').replace(' ', '').strip()
        valor_str = valor_str.replace('.', '').replace(',', '.')
        try:
            return float(valor_str)
        except:
            return None


class Logger:
    def __init__(self, nome="ETL"):
        self.nome = nome
    
    def info(self, msg):
        print(f"[INFO] {msg}")
    
    def sucesso(self, msg):
        print(f"[OK] {msg}")
    
    def erro(self, msg):
        print(f"[ERRO] {msg}")


class ProcessadorEducacao:
    def __init__(self):
        self.logger = Logger("ProcessadorEducacao")
        self.dimensoes = {}
        self.fatos = {}
    
    def processar_dados(self, dados_brutos):
        self.logger.info("Iniciando processamento...")
        self._criar_dimensoes_base(dados_brutos)
        self._processar_educacao(dados_brutos)
        self.logger.sucesso(f"Processamento concluido: {len(self.dimensoes)} dimensoes, {len(self.fatos)} fatos")
        return self.dimensoes, self.fatos
    
    def _criar_dimensoes_base(self, dados):
        # Dim_Nacionalidade
        nacionalidades = []
        nac_id = 1
        for filename in dados.keys():
            nome_nac = filename.replace('.csv', '')
            nacionalidades.append({
                'nacionalidade_id': nac_id,
                'nome_nacionalidade': nome_nac,
                'codigo_pais': nome_nac[:3].upper()
            })
            nac_id += 1
        
        self.dimensoes['Dim_Nacionalidade'] = pd.DataFrame(nacionalidades)
        
        # Dim_Sexo
        self.dimensoes['Dim_Sexo'] = pd.DataFrame([
            {'sexo_id': 1, 'tipo_sexo': 'Masculino'},
            {'sexo_id': 2, 'tipo_sexo': 'Feminino'}
        ])
        
        # Dim_NivelEducacao
        niveis = []
        for niv_id, info in Config.NIVEIS_EDUCACAO.items():
            niveis.append({
                'nivel_educacao_id': niv_id,
                'nome_nivel': info['nome'],
                'categoria': info['categoria'],
                'ordem_hierarquica': info['ordem']
            })
        self.dimensoes['Dim_NivelEducacao'] = pd.DataFrame(niveis)
        
        self.logger.sucesso(f"Dimensoes base criadas: {len(self.dimensoes)}")
    
    def _processar_educacao(self, dados):
        registros_educacao = []
        
        for filename, df in dados.items():
            nome_nac = filename.replace('.csv', '')
            
            for idx, row in df.iterrows():
                categoria = str(row.get('Categoria', '')).strip()
                
                if 'NIVEL DE ENSINO' in categoria.upper():
                    for nivel_id in Config.NIVEIS_EDUCACAO.keys():
                        valor = row.get(f'Nivel_{nivel_id}', 0)
                        if pd.notna(valor):
                            registros_educacao.append({
                                'nacionalidade': nome_nac,
                                'nivel_educacao_id': nivel_id,
                                'populacao_total': Formatadores.limpar_numero(valor) or 0
                            })
        
        if registros_educacao:
            self.fatos['Fact_PopulacaoEducacao'] = pd.DataFrame(registros_educacao)
            self.logger.sucesso(f"Fact_PopulacaoEducacao criada: {len(registros_educacao)} registros")


class Exportador:
    def __init__(self):
        self.logger = Logger("Exportador")
    
    def exportar_individual(self, tabelas):
        self.logger.info(f"Exportando {len(tabelas)} tabelas...")
        for nome_tabela, df in tabelas.items():
            filename = f"{nome_tabela}.csv"
            ConfigAmbiente.download_arquivo(filename, df)
        self.logger.sucesso("Exportacao individual concluida")
    
    def exportar_zip(self, tabelas):
        import zipfile
        
        self.logger.info(f"Criando arquivo ZIP com {len(tabelas)} tabelas...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'ETL_EDUCACAO_StarSchema_{timestamp}.zip'
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for nome_tabela, df in tabelas.items():
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                zip_file.writestr(f'{nome_tabela}.csv', csv_buffer.getvalue())
        
        zip_buffer.seek(0)
        
        if AMBIENTE == 'COLAB':
            with open(zip_filename, 'wb') as f:
                f.write(zip_buffer.getvalue())
            files.download(zip_filename)
        else:
            ConfigAmbiente.download_zip(zip_buffer, zip_filename)
        
        self.logger.sucesso(f"ZIP criado: {zip_filename}")


class OrquestradorPipelineEducacao:
    def __init__(self):
        self.logger = Logger("Orquestrador")
        self.processador = ProcessadorEducacao()
        self.exportador = Exportador()
    
    def executar_pipeline(self, modo_exportacao='zip'):
        print("\n" + "=" * 60)
        print(f"{Config.PROJETO_NOME} - Versao {Config.VERSAO}")
        print("=" * 60)
        print(f"Ambiente: {AMBIENTE}")
        print(f"Fonte: {Config.FONTE_DADOS}")
        print("=" * 60 + "\n")
        
        # Fase 1: Upload de dados
        self.logger.info("FASE 1: Upload de Dados")
        dados_brutos = ConfigAmbiente.upload_arquivos()
        
        if not dados_brutos:
            self.logger.erro("Nenhum dado foi carregado. Encerrando.")
            return False
        
        # Fase 2: Processamento
        self.logger.info("\nFASE 2: Processamento e Transformacao")
        dimensoes, fatos = self.processador.processar_dados(dados_brutos)
        
        # Fase 3: Exportacao
        self.logger.info("\nFASE 3: Exportacao")
        todas_tabelas = {**dimensoes, **fatos}
        
        if modo_exportacao == 'zip':
            self.exportador.exportar_zip(todas_tabelas)
        else:
            self.exportador.exportar_individual(todas_tabelas)
        
        # Relatorio final
        print("\n" + "=" * 60)
        print("PIPELINE CONCLUIDO COM SUCESSO!")
        print("=" * 60)
        print(f"[OK] Dimensoes criadas: {len(dimensoes)}")
        print(f"[OK] Fatos criados: {len(fatos)}")
        print(f"[OK] Total de tabelas: {len(todas_tabelas)}")
        print("=" * 60 + "\n")
        
        return True


def executar_etl_educacao(modo='zip'):
    """
    Funcao principal para executar o ETL de Educacao
    Args: modo: 'zip' (padrao) ou 'individual'
    """
    orquestrador = OrquestradorPipelineEducacao()
    return orquestrador.executar_pipeline(modo_exportacao=modo)


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  ETL EDUCACAO - Script Hibrido (Colab + Local)")
    print("=" * 60)
    print("\n[INICIO] Iniciando pipeline automaticamente...\n")
    executar_etl_educacao()
