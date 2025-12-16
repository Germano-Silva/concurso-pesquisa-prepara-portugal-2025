"""
============================================================
PARTE 5: ORQUESTRADOR PRINCIPAL
Pipeline ETL - AIMA Integrado (DP-02-A)
Google Colab
============================================================
"""

import pandas as pd
import numpy as np
from google.colab import files
import io
import zipfile
from datetime import datetime

from parte_01_imports_config import Config, Constantes, Formatadores, Logger
from parte_02_classes_base_ref import GerenciadorIntegridade, ValidadorIntegracao
from parte_03_transformador_dimensoes_aima import TransformadorDimensoesAIMA, LookupDimensoesAIMA
from parte_04_transformador_fatos_aima import TransformadorFatosAIMA

# ============================================================
# ORQUESTRADOR DO PIPELINE AIMA
# ============================================================

class OrquestradorPipelineAIMA:
    """Orquestrador principal do pipeline ETL AIMA"""
    
    def __init__(self):
        self.logger = Logger("OrquestradorAIMA")
        self.dimensoes = {}
        self.fatos = {}
        self.dados_brutos = {}
        self.dimensoes_base = {}  # Dimensões do ETL_EDUCACAO/LABORAL
        self.lookup = None
        self.estatisticas = {}
    
    def fase_1_upload_dados(self):
        """
        FASE 1: Upload de dados via Google Colab
        Permite upload de múltiplos arquivos CSV por ano (2020-2024)
        """
        self.logger.secao("FASE 1: UPLOAD DE DADOS AIMA")
        
        print("\n📁 INSTRUÇÕES DE UPLOAD:")
        print("=" * 60)
        print("Por favor, faça upload dos arquivos CSV AIMA por ano:")
        print("  - Organize por ano: 2020, 2021, 2022, 2023, 2024")
        print("  - Arquivos esperados por ano:")
        for arquivo in Config.ARQUIVOS_POR_ANO:
            print(f"    • {arquivo}")
        print("\nVocê pode fazer upload de todos os arquivos de uma vez.")
        print("=" * 60)
        
        # Upload de arquivos
        uploaded = files.upload()
        
        if not uploaded:
            self.logger.erro("Nenhum arquivo foi carregado")
            return False
        
        self.logger.sucesso(f"{len(uploaded)} arquivo(s) carregado(s)")
        
        # Processar arquivos carregados
        for filename, content in uploaded.items():
            try:
                df = pd.read_csv(io.BytesIO(content), encoding='utf-8', sep=',')
                self.dados_brutos[filename] = df
                self.logger.info(f"Arquivo processado: {filename} ({len(df)} linhas)")
            except Exception as e:
                self.logger.erro(f"Erro ao processar {filename}: {e}")
        
        self.logger.sucesso(f"Total de arquivos processados: {len(self.dados_brutos)}")
        return True
    
    def fase_2_integracao_educacao_laboral(self, dimensoes_educacao_laboral=None):
        """
        FASE 2: Integração com dimensões do ETL_EDUCACAO e ETL_LABORAL
        
        Args:
            dimensoes_educacao_laboral: Dict com dimensões compartilhadas dos outros pipelines
                                       Ex: {'Dim_Nacionalidade': df, 'Dim_Sexo': df, ...}
        """
        self.logger.secao("FASE 2: INTEGRAÇÃO COM ETL_EDUCACAO E ETL_LABORAL")
        
        if dimensoes_educacao_laboral is None:
            self.logger.aviso("Nenhuma dimensão base fornecida. Pipeline executará em modo standalone.")
            self.logger.info("Para integração completa, forneça dimensões via parâmetro.")
            return True
        
        # Carregar dimensões compatibilizadas
        dimensoes_compativeis = ['Dim_Nacionalidade', 'Dim_Sexo', 'Dim_GrupoEtario', 'Dim_PopulacaoResidente']
        
        for dim_nome in dimensoes_compativeis:
            if dim_nome in dimensoes_educacao_laboral:
                self.dimensoes_base[dim_nome] = dimensoes_educacao_laboral[dim_nome]
                registros = len(dimensoes_educacao_laboral[dim_nome])
                self.logger.sucesso(f"{dim_nome} integrada: {registros} registros")
            else:
                self.logger.aviso(f"{dim_nome} não fornecida")
        
        total_integradas = len(self.dimensoes_base)
        self.logger.info(f"Total de dimensões integradas: {total_integradas}/{len(dimensoes_compativeis)}")
        
        return True
    
    def fase_3_extracao_consolidacao(self):
        """
        FASE 3: Extração e consolidação dos dados brutos AIMA
        Prepara dados de diferentes arquivos para transformação
        """
        self.logger.secao("FASE 3: EXTRAÇÃO E CONSOLIDAÇÃO")
        
        # Consolidar nacionalidades únicas de todos os arquivos
        nacionalidades_set = set()
        
        for filename, df in self.dados_brutos.items():
            # Procurar colunas de nacionalidade
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['nacionalidade', 'país', 'pais', 'country']):
                    nacs = df[col].dropna().unique()
                    nacionalidades_set.update(nacs)
                    self.logger.info(f"{filename}: {len(nacs)} nacionalidades encontradas")
        
        nacionalidades_list = sorted(list(nacionalidades_set))
        
        # Preparar dados consolidados
        dados_consolidados = {
            'nacionalidades': nacionalidades_list,
            'despachos': self._extrair_despachos(),
            'motivos': self._extrair_motivos(),
            'concessoes_nacionalidade': self._consolidar_concessoes_nacionalidade(),
            'concessoes_despacho': self._consolidar_concessoes_despacho(),
            'concessoes_motivo': self._consolidar_concessoes_motivo(),
            'populacao_estrangeira': self._consolidar_populacao_estrangeira(),
            'distribuicao_etaria': self._consolidar_distribuicao_etaria(),
            'evolucao_populacao': self._consolidar_evolucao_populacao(),
            'populacao_residente_etaria': self._consolidar_populacao_residente_etaria()
        }
        
        # Atualizar dados_brutos com dados consolidados
        self.dados_brutos.update(dados_consolidados)
        
        self.logger.sucesso(f"Dados consolidados: {len(dados_consolidados)} categorias")
        return True
    
    def _extrair_despachos(self):
        """Extrai informações de despachos dos dados brutos"""
        despachos_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'despacho' in filename.lower() and 'descricao' in filename.lower():
                for col in df.columns:
                    if 'despacho' in col.lower():
                        despachos_list.extend(df[col].dropna().unique())
        
        return pd.DataFrame({'codigo_despacho': list(set(despachos_list))}) if despachos_list else None
    
    def _extrair_motivos(self):
        """Extrai informações de motivos dos dados brutos"""
        motivos_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'motivo' in filename.lower():
                for col in df.columns:
                    if 'motivo' in col.lower():
                        motivos_list.extend(df[col].dropna().unique())
        
        return pd.DataFrame({'motivo': list(set(motivos_list))}) if motivos_list else None
    
    def _consolidar_concessoes_nacionalidade(self):
        """Consolida dados de concessões por nacionalidade"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'concessao' in filename.lower() and 'residencia' in filename.lower():
                # Adicionar coluna de ano se não existir
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    # Tentar extrair ano do nome do arquivo
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_concessoes_despacho(self):
        """Consolida dados de concessões por despacho"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'despacho' in filename.lower() and 'concessao' in filename.lower():
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_concessoes_motivo(self):
        """Consolida dados de concessões por motivo"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'motivo' in filename.lower():
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_populacao_estrangeira(self):
        """Consolida dados de população estrangeira"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'populacao' in filename.lower() and 'estrangeira' in filename.lower():
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_distribuicao_etaria(self):
        """Consolida dados de distribuição etária"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'etaria' in filename.lower() or 'etaria' in str(df.columns).lower():
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_evolucao_populacao(self):
        """Consolida dados de evolução da população"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'evolucao' in filename.lower():
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def _consolidar_populacao_residente_etaria(self):
        """Consolida dados de população residente por faixa etária"""
        df_list = []
        
        for filename, df in self.dados_brutos.items():
            if 'residente' in filename.lower() and 'distribuicao' in filename.lower():
                if 'ano' not in df.columns and 'Ano' not in df.columns:
                    for ano in Config.ANOS_REFERENCIA:
                        if str(ano) in filename:
                            df['Ano'] = ano
                            break
                
                df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else None
    
    def fase_4_transformacao_dimensoes(self):
        """
        FASE 4: Transformação - Criação de dimensões AIMA
        """
        self.logger.secao("FASE 4: TRANSFORMAÇÃO - DIMENSÕES")
        
        transformador = TransformadorDimensoesAIMA(self.logger)
        
        # Criar dimensões
        self.dimensoes = transformador.criar_todas_dimensoes(
            dados_brutos=self.dados_brutos,
            dimensoes_base=self.dimensoes_base
        )
        
        # Criar lookup
        self.lookup = LookupDimensoesAIMA(self.dimensoes)
        
        # Gerar relatório
        transformador.gerar_relatorio_dimensoes()
        
        # Estatísticas
        self.estatisticas['dimensoes'] = lookup.estatisticas_lookup()
        
        return True
    
    def fase_5_transformacao_fatos(self):
        """
        FASE 5: Transformação - Criação de tabelas fato AIMA
        """
        self.logger.secao("FASE 5: TRANSFORMAÇÃO - FATOS")
        
        if self.lookup is None:
            self.logger.erro("Lookup não inicializado. Execute fase_4_transformacao_dimensoes primeiro.")
            return False
        
        transformador = TransformadorFatosAIMA(self.dimensoes, self.lookup, self.logger)
        
        # Criar fatos
        self.fatos = transformador.criar_todos_fatos(
            dados_brutos=self.dados_brutos,
            dimensoes_base=self.dimensoes_base
        )
        
        # Gerar relatório
        transformador.gerar_relatorio_fatos()
        
        # Estatísticas
        self.estatisticas['fatos'] = {
            'total_tabelas': len(self.fatos),
            'total_registros': sum(len(df) for df in self.fatos.values())
        }
        
        return True
    
    def fase_6_validacao(self):
        """
        FASE 6: Validação de integridade referencial e qualidade
        """
        self.logger.secao("FASE 6: VALIDAÇÃO")
        
        # Validação de integridade referencial
        gerenciador = GerenciadorIntegridade()
        
        # Adicionar dimensões
        for nome, df in self.dimensoes.items():
            gerenciador.adicionar_tabela(nome, df)
        
        # Adicionar fatos
        for nome, df in self.fatos.items():
            gerenciador.adicionar_tabela(nome, df)
        
        # Definir relacionamentos principais
        self._definir_relacionamentos_fk(gerenciador)
        
        # Executar validação
        erros = gerenciador.validar_todos()
        
        # Gerar relatório
        gerenciador.gerar_relatorio()
        
        # Validação de integração (se houver dimensões base)
        if self.dimensoes_base:
            self.logger.subsecao("Validação de Integração")
            validador_int = ValidadorIntegracao()
            validador_int.carregar_dimensoes_base(
                dim_nacionalidade=self.dimensoes_base.get('Dim_Nacionalidade'),
                dim_sexo=self.dimensoes_base.get('Dim_Sexo'),
                dim_grupoetario=self.dimensoes_base.get('Dim_GrupoEtario')
            )
            
            erros_integracao = validador_int.validar_todos(self.dimensoes)
            
            if erros_integracao:
                for erro in erros_integracao:
                    self.logger.aviso(erro)
        
        # Estatísticas
        self.estatisticas['validacao'] = {
            'erros_integridade': len(erros),
            'relacionamentos_validados': len(gerenciador.relacionamentos)
        }
        
        return len(erros) == 0
    
    def _definir_relacionamentos_fk(self, gerenciador):
        """Define relacionamentos FK -> PK para validação"""
        
        # Fact_ConcessoesPorNacionalidadeSexo
        if 'Fact_ConcessoesPorNacionalidadeSexo' in self.fatos:
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorNacionalidadeSexo', 'ano_id', 'Dim_AnoRelatorio', 'ano_id')
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorNacionalidadeSexo', 'tipo_id', 'Dim_TipoRelatorio', 'tipo_id')
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorNacionalidadeSexo', 'nacionalidade_aima_id', 'Dim_NacionalidadeAIMA', 'nacionalidade_aima_id')
        
        # Fact_ConcessoesPorDespacho
        if 'Fact_ConcessoesPorDespacho' in self.fatos:
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorDespacho', 'ano_id', 'Dim_AnoRelatorio', 'ano_id')
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorDespacho', 'despacho_id', 'Dim_Despacho', 'despacho_id')
        
        # Fact_ConcessoesPorMotivoNacionalidade
        if 'Fact_ConcessoesPorMotivoNacionalidade' in self.fatos:
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorMotivoNacionalidade', 'ano_id', 'Dim_AnoRelatorio', 'ano_id')
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorMotivoNacionalidade', 'motivo_id', 'Dim_MotivoConcessao', 'motivo_id')
            gerenciador.adicionar_relacionamento('Fact_ConcessoesPorMotivoNacionalidade', 'nacionalidade_aima_id', 'Dim_NacionalidadeAIMA', 'nacionalidade_aima_id')
        
        # Outros fatos...
        self.logger.info(f"Definidos {len(gerenciador.relacionamentos)} relacionamentos para validação")
    
    def fase_7_exportacao(self, modo='zip'):
        """
        FASE 7: Exportação dos dados transformados
        
        Args:
            modo: 'zip' (padrão) ou 'individual'
        """
        self.logger.secao("FASE 7: EXPORTAÇÃO")
        
        todas_tabelas = {}
        todas_tabelas.update(self.dimensoes)
        todas_tabelas.update(self.fatos)
        
        if modo == 'zip':
            return self._exportar_zip(todas_tabelas)
        else:
            return self._exportar_individual(todas_tabelas)
    
    def _exportar_zip(self, tabelas):
        """Exporta todas as tabelas em um arquivo ZIP"""
        self.logger.subsecao("Exportando como ZIP")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'ETL_AIMA_StarSchema_{timestamp}.zip'
        
        # Criar arquivo ZIP em memória
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for nome_tabela, df in tabelas.items():
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                zip_file.writestr(f'{nome_tabela}.csv', csv_buffer.getvalue())
                self.logger.info(f"Adicionado: {nome_tabela}.csv ({len(df)} registros)")
        
        # Download do ZIP
        zip_buffer.seek(0)
        files.download(zip_filename, zip_buffer.getvalue())
        
        self.logger.sucesso(f"Exportação concluída: {zip_filename}")
        self.logger.info(f"Total de tabelas: {len(tabelas)}")
        
        return True
    
    def _exportar_individual(self, tabelas):
        """Exporta cada tabela como arquivo CSV individual"""
        self.logger.subsecao("Exportando arquivos individuais")
        
        for nome_tabela, df in tabelas.items():
            csv_content = df.to_csv(index=False, encoding='utf-8')
            files.download(f'{nome_tabela}.csv', csv_content)
            self.logger.info(f"Exportado: {nome_tabela}.csv ({len(df)} registros)")
        
        self.logger.sucesso(f"Exportação concluída: {len(tabelas)} arquivos")
        return True
    
    def executar_pipeline_completo(self, dimensoes_educacao_laboral=None, modo_exportacao='zip'):
        """
        Executa o pipeline ETL AIMA completo
        
        Args:
            dimensoes_educacao_laboral: Dict com dimensões dos outros pipelines (opcional)
            modo_exportacao: 'zip' ou 'individual'
        
        Returns:
            bool: True se sucesso, False se erro
        """
        self.logger.secao("INICIANDO PIPELINE ETL AIMA COMPLETO")
        Config.print_configuracoes()
        
        inicio = datetime.now()
        
        try:
            # Fase 1: Upload
            if not self.fase_1_upload_dados():
                return False
            
            # Fase 2: Integração
            if not self.fase_2_integracao_educacao_laboral(dimensoes_educacao_laboral):
                return False
            
            # Fase 3: Extração
            if not self.fase_3_extracao_consolidacao():
                return False
            
            # Fase 4: Dimensões
            if not self.fase_4_transformacao_dimensoes():
                return False
            
            # Fase 5: Fatos
            if not self.fase_5_transformacao_fatos():
                return False
            
            # Fase 6: Validação
            if not self.fase_6_validacao():
                self.logger.aviso("Validação encontrou problemas, mas pipeline continuará")
            
            # Fase 7: Exportação
            if not self.fase_7_exportacao(modo=modo_exportacao):
                return False
            
            # Relatório final
            fim = datetime.now()
            duracao = (fim - inicio).total_seconds()
            
            self.gerar_relatorio_final(duracao)
            
            return True
            
        except Exception as e:
            self.logger.erro(f"Erro fatal no pipeline: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def gerar_relatorio_final(self, duracao):
        """Gera relatório final da execução"""
        self.logger.secao("RELATÓRIO FINAL - ETL AIMA")
        
        print(f"\n⏱️  Duração total: {duracao:.2f} segundos")
        print(f"\n📊 ESTATÍSTICAS:")
        print(f"  - Dimensões criadas: {len(self.dimensoes)}")
        print(f"  - Fatos criados: {len(self.fatos)}")
        print(f"  - Total de tabelas: {len(self.dimensoes) + len(self.fatos)}")
        
        if 'fatos' in self.estatisticas:
            print(f"  - Total de registros (fatos): {self.estatisticas['fatos']['total_registros']}")
        
        print(f"\n✅ Pipeline ETL AIMA concluído com sucesso!")
        print(f"📅 Data/Hora: {Formatadores.formatar_timestamp()}")
        print("=" * 60)


# ============================================================
# FUNÇÃO PRINCIPAL PARA EXECUÇÃO NO COLAB
# ============================================================

def executar_pipeline_aima(dimensoes_base=None, modo_download='zip'):
    """
    Função wrapper para executar pipeline AIMA no Google Colab
    
    Args:
        dimensoes_base: Dict com dimensões do ETL_EDUCACAO/LABORAL (opcional)
        modo_download: 'zip' ou 'individual'
    
    Returns:
        OrquestradorPipelineAIMA: Instância do orquestrador com resultados
    
    Exemplo de uso no Colab:
        >>> orquestrador = executar_pipeline_aima()
        >>> # Com integração:
        >>> orquestrador = executar_pipeline_aima(dimensoes_base={
        >>>     'Dim_Nacionalidade': df_nacionalidade,
        >>>     'Dim_Sexo': df_sexo,
        >>>     'Dim_GrupoEtario': df_grupoetario
        >>> })
    """
    orquestrador = OrquestradorPipelineAIMA()
    sucesso = orquestrador.executar_pipeline_completo(
        dimensoes_educacao_laboral=dimensoes_base,
        modo_exportacao=modo_download
    )
    
    if sucesso:
        print("\n🎉 Pipeline AIMA executado com sucesso!")
        print("📦 Arquivos disponíveis para download.")
    else:
        print("\n❌ Pipeline AIMA falhou. Verifique os logs acima.")
    
    return orquestrador


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    logger = Logger("TESTE-ORQUESTRADOR-AIMA")
    logger.secao("TESTE - Orquestrador AIMA")
    
    # Criar orquestrador
    orq = OrquestradorPipelineAIMA()
    
    logger.info("Orquestrador criado com sucesso")
    logger.info(f"Configuração: {Config.PROJETO_NOME} v{Config.VERSAO}")
    
    print("\n" + "=" * 60)
    print("INSTRUÇÕES DE USO NO GOOGLE COLAB:")
    print("=" * 60)
    print("\n1. Copie todos os arquivos parte_*.py para o Colab")
    print("2. Execute:")
    print("   from parte_05_orquestrador_aima import executar_pipeline_aima")
    print("   orquestrador = executar_pipeline_aima()")
    print("\n3. Para integração com outros pipelines:")
    print("   orquestrador = executar_pipeline_aima(dimensoes_base={")
    print("       'Dim_Nacionalidade': df_nac,")
    print("       'Dim_Sexo': df_sexo")
    print("   })")
    print("=" * 60)
    
    logger.sucesso("Teste concluído com sucesso!")
    print("\n✓ Módulo parte_05_orquestrador_aima.py carregado com sucesso!")
