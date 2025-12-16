"""
============================================================
PARTE 7: CARREGAMENTO E EXPORTAÇÃO
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
Responsável pela fase de LOAD:
- Exportação de DataFrames para CSV
- Download via files.download() do Google Colab
- Geração de relatórios de qualidade
- Criação de metadados
- Validação final de integridade
"""

import pandas as pd
import numpy as np
from google.colab import files
from datetime import datetime
import io
import zipfile


# ============================================================
# CLASSE GERENCIADOR DE EXPORTAÇÃO
# ============================================================

class GerenciadorExportacao:
    """Gerencia exportação de tabelas para CSV e download"""
    
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.arquivos_gerados = {}
        self.estatisticas_exportacao = {}
    
    def exportar_tabela(self, nome_tabela, dataframe, prefixo_arquivo=""):
        """
        Exporta uma tabela para CSV em memória
        
        Parâmetros:
          nome_tabela: Nome da tabela
          dataframe: DataFrame a ser exportado
          prefixo_arquivo: Prefixo opcional para o nome do arquivo
        
        Retorna:
          Nome do arquivo gerado
        """
        if dataframe.empty:
            self.logger.aviso(f"Tabela {nome_tabela} está vazia. Não será exportada.")
            return None
        
        # Gerar nome do arquivo
        if prefixo_arquivo:
            nome_arquivo = f"{prefixo_arquivo}_{nome_tabela}.csv"
        else:
            nome_arquivo = f"{nome_tabela}.csv"
        
        # Converter DataFrame para CSV em memória
        csv_buffer = io.StringIO()
        dataframe.to_csv(
            csv_buffer,
            index=self.config.OUTPUT_INDEX,
            encoding=self.config.OUTPUT_ENCODING,
            sep=self.config.OUTPUT_SEPARATOR
        )
        
        # Armazenar conteúdo
        self.arquivos_gerados[nome_arquivo] = csv_buffer.getvalue()
        
        # Estatísticas
        self.estatisticas_exportacao[nome_tabela] = {
            'arquivo': nome_arquivo,
            'linhas': len(dataframe),
            'colunas': len(dataframe.columns),
            'tamanho_bytes': len(csv_buffer.getvalue()),
            'tamanho_kb': len(csv_buffer.getvalue()) / 1024
        }
        
        self.logger.sucesso(
            f"Exportada: {nome_arquivo} ({len(dataframe)} linhas, "
            f"{len(dataframe.columns)} colunas, "
            f"{self.estatisticas_exportacao[nome_tabela]['tamanho_kb']:.1f} KB)"
        )
        
        return nome_arquivo
    
    def exportar_todas_tabelas(self, dimensoes_dict, fatos_dict, prefixo="DP-01-A"):
        """
        Exporta todas as dimensões e fatos
        
        Parâmetros:
          dimensoes_dict: Dicionário {nome: DataFrame} de dimensões
          fatos_dict: Dicionário {nome: DataFrame} de fatos
          prefixo: Prefixo para os arquivos
        """
        self.logger.secao("EXPORTANDO TABELAS PARA CSV")
        
        # Exportar dimensões
        self.logger.subsecao("Dimensões")
        total_dim = 0
        for nome, df in dimensoes_dict.items():
            if self.exportar_tabela(nome, df, prefixo):
                total_dim += 1
        
        # Exportar fatos
        self.logger.subsecao("Fatos")
        total_fato = 0
        for nome, df in fatos_dict.items():
            if self.exportar_tabela(nome, df, prefixo):
                total_fato += 1
        
        self.logger.info(f"Total exportado: {total_dim} dimensões + {total_fato} fatos")
        
        return total_dim + total_fato
    
    def baixar_arquivo(self, nome_arquivo):
        """
        Baixa um arquivo individual via Google Colab files.download()
        
        Parâmetros:
          nome_arquivo: Nome do arquivo a baixar
        """
        if nome_arquivo not in self.arquivos_gerados:
            self.logger.erro(f"Arquivo {nome_arquivo} não encontrado para download")
            return False
        
        try:
            # Salvar temporariamente e baixar
            conteudo = self.arquivos_gerados[nome_arquivo]
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                f.write(conteudo)
            
            files.download(nome_arquivo)
            self.logger.sucesso(f"Download iniciado: {nome_arquivo}")
            return True
            
        except Exception as e:
            self.logger.erro(f"Erro ao baixar {nome_arquivo}: {str(e)}")
            return False
    
    def baixar_todos_arquivos(self):
        """Baixa todos os arquivos gerados individualmente"""
        self.logger.secao("INICIANDO DOWNLOADS")
        
        total_sucesso = 0
        total_erro = 0
        
        for i, nome_arquivo in enumerate(self.arquivos_gerados.keys(), 1):
            self.logger.progresso(i, len(self.arquivos_gerados), f"Baixando {nome_arquivo}")
            
            if self.baixar_arquivo(nome_arquivo):
                total_sucesso += 1
            else:
                total_erro += 1
        
        self.logger.info(f"Downloads concluídos: {total_sucesso} sucesso, {total_erro} erros")
    
    def criar_pacote_zip(self, nome_pacote="ETL_Educacao_Output.zip"):
        """
        Cria um arquivo ZIP com todos os CSVs gerados
        
        Parâmetros:
          nome_pacote: Nome do arquivo ZIP
        """
        self.logger.info(f"Criando pacote ZIP: {nome_pacote}")
        
        try:
            # Criar ZIP em memória
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for nome_arquivo, conteudo in self.arquivos_gerados.items():
                    zip_file.writestr(nome_arquivo, conteudo)
            
            # Salvar ZIP e baixar
            zip_data = zip_buffer.getvalue()
            with open(nome_pacote, 'wb') as f:
                f.write(zip_data)
            
            tamanho_mb = len(zip_data) / (1024 * 1024)
            self.logger.sucesso(
                f"Pacote ZIP criado: {nome_pacote} ({tamanho_mb:.2f} MB, "
                f"{len(self.arquivos_gerados)} arquivos)"
            )
            
            # Download
            files.download(nome_pacote)
            self.logger.sucesso(f"Download do pacote ZIP iniciado")
            
            return True
            
        except Exception as e:
            self.logger.erro(f"Erro ao criar pacote ZIP: {str(e)}")
            return False
    
    def gerar_relatorio_exportacao(self):
        """Gera relatório detalhado das exportações"""
        self.logger.secao("RELATÓRIO DE EXPORTAÇÃO")
        
        if not self.estatisticas_exportacao:
            self.logger.aviso("Nenhuma tabela foi exportada")
            return None
        
        # Criar DataFrame com estatísticas
        stats_list = []
        for tabela, stats in self.estatisticas_exportacao.items():
            stats_list.append({
                'Tabela': tabela,
                'Arquivo': stats['arquivo'],
                'Linhas': stats['linhas'],
                'Colunas': stats['colunas'],
                'Tamanho (KB)': round(stats['tamanho_kb'], 2)
            })
        
        df_stats = pd.DataFrame(stats_list)
        
        # Totais
        total_linhas = df_stats['Linhas'].sum()
        total_tamanho_kb = df_stats['Tamanho (KB)'].sum()
        total_tamanho_mb = total_tamanho_kb / 1024
        
        # Exibir relatório
        print("\n" + "=" * 80)
        print("ESTATÍSTICAS DE EXPORTAÇÃO")
        print("=" * 80)
        print(df_stats.to_string(index=False))
        print("-" * 80)
        print(f"TOTAL: {len(df_stats)} tabelas | {total_linhas:,} linhas | "
              f"{total_tamanho_mb:.2f} MB")
        print("=" * 80)
        
        return df_stats
    
    def obter_estatisticas(self):
        """Retorna estatísticas de exportação"""
        return self.estatisticas_exportacao


# ============================================================
# CLASSE VALIDADOR FINAL
# ============================================================

class ValidadorFinal:
    """Valida integridade dos dados antes da exportação final"""
    
    def __init__(self, logger, gerenciador_integridade):
        self.logger = logger
        self.gerenciador = gerenciador_integridade
        self.erros_criticos = []
        self.avisos = []
    
    def validar_pipeline_completo(self, dimensoes_dict, fatos_dict):
        """
        Executa validação completa de todas as tabelas
        
        Parâmetros:
          dimensoes_dict: Dicionário de dimensões
          fatos_dict: Dicionário de fatos
        
        Retorna:
          True se passou em todas as validações críticas
        """
        self.logger.secao("VALIDAÇÃO FINAL DO PIPELINE")
        
        self.erros_criticos = []
        self.avisos = []
        
        # 1. Validar existência de tabelas obrigatórias
        self._validar_tabelas_obrigatorias(dimensoes_dict, fatos_dict)
        
        # 2. Validar integridade referencial
        self._validar_integridade_referencial(dimensoes_dict, fatos_dict)
        
        # 3. Validar completude de dados
        self._validar_completude(dimensoes_dict, fatos_dict)
        
        # 4. Validar ranges de valores
        self._validar_ranges(fatos_dict)
        
        # Resumo
        self._exibir_resumo_validacao()
        
        return len(self.erros_criticos) == 0
    
    def _validar_tabelas_obrigatorias(self, dimensoes_dict, fatos_dict):
        """Verifica se todas as tabelas obrigatórias foram criadas"""
        self.logger.subsecao("Validando Tabelas Obrigatórias")
        
        tabelas_obrigatorias_dim = [
            'Dim_Nacionalidade',
            'Dim_PopulacaoResidente',
            'Dim_NivelEducacao',
            'Dim_Sexo',
            'Dim_GrupoEtario'
        ]
        
        tabelas_obrigatorias_fato = [
            'Fact_PopulacaoEducacao',
            'Fact_EstatisticasEducacao',
            'Fact_PopulacaoPorNacionalidade'
        ]
        
        # Validar dimensões
        for tabela in tabelas_obrigatorias_dim:
            if tabela not in dimensoes_dict or dimensoes_dict[tabela].empty:
                self.erros_criticos.append(f"CRÍTICO: Dimensão obrigatória ausente ou vazia: {tabela}")
            else:
                self.logger.sucesso(f"✓ {tabela}: {len(dimensoes_dict[tabela])} registros")
        
        # Validar fatos
        for tabela in tabelas_obrigatorias_fato:
            if tabela not in fatos_dict or fatos_dict[tabela].empty:
                self.erros_criticos.append(f"CRÍTICO: Fato obrigatório ausente ou vazio: {tabela}")
            else:
                self.logger.sucesso(f"✓ {tabela}: {len(fatos_dict[tabela])} registros")
    
    def _validar_integridade_referencial(self, dimensoes_dict, fatos_dict):
        """Valida FKs entre tabelas"""
        self.logger.subsecao("Validando Integridade Referencial")
        
        # Adicionar tabelas ao gerenciador
        for nome, df in dimensoes_dict.items():
            self.gerenciador.adicionar_tabela(nome, df)
        
        for nome, df in fatos_dict.items():
            self.gerenciador.adicionar_tabela(nome, df)
        
        # Definir relacionamentos críticos
        relacionamentos = [
            ('Fact_PopulacaoEducacao', 'nacionalidade_id', 'Dim_Nacionalidade', 'nacionalidade_id'),
            ('Fact_PopulacaoEducacao', 'nivel_educacao_id', 'Dim_NivelEducacao', 'nivel_educacao_id'),
            ('Fact_EstatisticasEducacao', 'nacionalidade_id', 'Dim_Nacionalidade', 'nacionalidade_id'),
            ('Fact_PopulacaoPorNacionalidade', 'nacionalidade_id', 'Dim_Nacionalidade', 'nacionalidade_id'),
        ]
        
        for rel in relacionamentos:
            self.gerenciador.adicionar_relacionamento(*rel)
        
        # Executar validações
        erros_integridade = self.gerenciador.validar_todos()
        
        if erros_integridade:
            self.erros_criticos.extend(erros_integridade)
            for erro in erros_integridade:
                self.logger.erro(erro)
        else:
            self.logger.sucesso("✓ Integridade referencial validada com sucesso")
    
    def _validar_completude(self, dimensoes_dict, fatos_dict):
        """Valida completude de dados (campos obrigatórios preenchidos)"""
        self.logger.subsecao("Validando Completude de Dados")
        
        validacoes = 0
        erros = 0
        
        # Validar dimensões
        for nome, df in dimensoes_dict.items():
            pk_col = df.columns[0]  # Assume primeira coluna é PK
            
            if df[pk_col].isna().any():
                self.erros_criticos.append(f"CRÍTICO: {nome} contém PKs nulas")
                erros += 1
            
            validacoes += 1
        
        # Validar fatos
        for nome, df in fatos_dict.items():
            pk_col = df.columns[0]
            
            if df[pk_col].isna().any():
                self.erros_criticos.append(f"CRÍTICO: {nome} contém PKs nulas")
                erros += 1
            
            validacoes += 1
        
        if erros == 0:
            self.logger.sucesso(f"✓ Completude validada: {validacoes} tabelas verificadas")
        else:
            self.logger.erro(f"✗ {erros} erros de completude encontrados")
    
    def _validar_ranges(self, fatos_dict):
        """Valida ranges de valores em campos numéricos"""
        self.logger.subsecao("Validando Ranges de Valores")
        
        # Validar percentuais (devem estar entre 0 e 100)
        colunas_percentuais = [
            'percentagem_total', 'percentagem_masculino', 'percentagem_feminino',
            'percentagem_grupo', 'percentual_nivel', 'concentracao_relativa',
            'percentual_sem_educacao', 'percentual_ensino_superior'
        ]
        
        problemas = 0
        
        for nome, df in fatos_dict.items():
            for col in colunas_percentuais:
                if col in df.columns:
                    valores_invalidos = df[(df[col] < 0) | (df[col] > 100)][col]
                    
                    if len(valores_invalidos) > 0:
                        self.avisos.append(
                            f"AVISO: {nome}.{col} contém {len(valores_invalidos)} valores fora do range [0, 100]"
                        )
                        problemas += 1
        
        if problemas == 0:
            self.logger.sucesso("✓ Ranges de valores validados")
        else:
            self.logger.aviso(f"⚠ {problemas} avisos de ranges encontrados")
    
    def _exibir_resumo_validacao(self):
        """Exibe resumo da validação"""
        print("\n" + "=" * 80)
        print("RESUMO DA VALIDAÇÃO FINAL")
        print("=" * 80)
        
        print(f"Erros Críticos: {len(self.erros_criticos)}")
        print(f"Avisos: {len(self.avisos)}")
        
        if self.erros_criticos:
            print("\n❌ ERROS CRÍTICOS:")
            for i, erro in enumerate(self.erros_criticos, 1):
                print(f"  {i}. {erro}")
        
        if self.avisos:
            print("\n⚠️  AVISOS:")
            for i, aviso in enumerate(self.avisos, 1):
                print(f"  {i}. {aviso}")
        
        if not self.erros_criticos and not self.avisos:
            print("\n✅ PIPELINE VALIDADO COM SUCESSO!")
            print("Todos os dados estão prontos para exportação.")
        elif not self.erros_criticos:
            print("\n✅ PIPELINE VALIDADO (com avisos)")
            print("Os dados podem ser exportados, mas revise os avisos acima.")
        else:
            print("\n❌ PIPELINE COM ERROS CRÍTICOS")
            print("Corrija os erros antes de exportar os dados.")
        
        print("=" * 80)


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Gerenciador de Exportação e Validador Final...")
    
    class LoggerTeste:
        def info(self, msg): print(f"INFO: {msg}")
        def sucesso(self, msg): print(f"✓ {msg}")
        def aviso(self, msg): print(f"⚠ {msg}")
        def erro(self, msg): print(f"✗ {msg}")
        def secao(self, titulo): print(f"\n{'='*60}\n{titulo}\n{'='*60}")
        def subsecao(self, titulo): print(f"\n{'-'*60}\n{titulo}\n{'-'*60}")
        def progresso(self, atual, total, desc): 
            print(f"[{atual}/{total}] {desc}")
    
    class ConfigTeste:
        OUTPUT_INDEX = False
        OUTPUT_ENCODING = 'utf-8'
        OUTPUT_SEPARATOR = ','
    
    # Criar dados de teste
    df_teste = pd.DataFrame({
        'id': [1, 2, 3],
        'nome': ['A', 'B', 'C'],
        'valor': [10, 20, 30]
    })
    
    # Testar exportação
    gerenciador = GerenciadorExportacao(LoggerTeste(), ConfigTeste())
    gerenciador.exportar_tabela('TesteTabela', df_teste, 'DP-01-A')
    
    # Testar relatório
    relatorio = gerenciador.gerar_relatorio_exportacao()
    print(f"\nRelatório gerado: {len(relatorio) if relatorio is not None else 0} linhas")
    
    print("\n✓ Módulo parte_07_carregamento_exportacao.py carregado com sucesso!")
