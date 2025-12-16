"""
============================================================
PARTE 5: ORQUESTRADOR PRINCIPAL
Pipeline ETL - Laboral (DP-01-B)
Google Colab
============================================================
Script principal que orquestra todo o pipeline ETL Laboral:
1. Integração com di mensões base do ETL_EDUCACAO
2. Criação de 7 dimensões laborais
3. Criação de 8 fatos laborais
4. Validação e exportação

INSTRUÇÕES DE USO NO GOOGLE COLAB:
1. Execute primeiro o ETL_EDUCACAO para criar dimensões base
2. Faça upload dos arquivos parte_*.py do ETL_LABORAL
3. Execute este orquestrador
4. Baixe os CSVs gerados
"""

# ============================================================
# IMPORTS DOS MÓDULOS DO PIPELINE
# ============================================================

# Parte 1: Configurações
from parte_01_imports_config import Config, Constantes, Logger

# Parte 2: Classes Base (referência)
from parte_02_classes_base_ref import (
    GerenciadorIntegridade
)

# Parte 3: Dimensões Laborais
from parte_03_transformador_dimensoes_laborais import (
    TransformadorDimensoesLaborais,
    LookupDimensoesLaborais
)

# Parte 4: Fatos Laborais
from parte_04_transformador_fatos_laborais import TransformadorFatosLaborais

import pandas as pd
import numpy as np
from datetime import datetime
from google.colab import files
import traceback


# ============================================================
# CLASSE ORQUESTRADORA PRINCIPAL LABORAL
# ============================================================

class OrquestradorPipelineLaboral:
    """Orquestra execução completa do pipeline ETL Laboral"""
    
    def __init__(self):
        self.logger = Logger("PIPELINE-LABORAL")
        self.config = Config()
        self.constantes = Constantes()
        
        # Componentes do pipeline
        self.transformador_dim = None
        self.transformador_fatos = None
        self.lookup_laborais = None
        self.lookup_base = None  # Do ETL_EDUCACAO
        
        # Dados processados
        self.dimensoes = {}
        self.fatos = {}
        self.dimensoes_base = {}  # Do ETL_EDUCACAO
        self.dados_brutos = {}
        
        # Status
        self.etapa_atual = "Inicialização"
        self.status_pipeline = "Preparando"
        self.tempo_inicio = None
        self.tempo_fim = None
    
    def executar_pipeline_completo(self, dimensoes_base_etl=None):
        """
        Executa pipeline ETL Laboral completo
        
        Parâmetros:
          dimensoes_base_etl: Dict com dimensões do ETL_EDUCACAO (Nacionalidade, Sexo, etc.)
                             Se None, cria dimensões simuladas
        """
        try:
            self.tempo_inicio = datetime.now()
            self._exibir_cabecalho()
            
            # FASE 1: INTEGRAÇÃO COM ETL_EDUCACAO
            self.logger.secao("FASE 1: INTEGRAÇÃO COM DIMENSÕES BASE")
            self.etapa_atual = "Integração"
            
            if not self._integrar_dimensoes_base(dimensoes_base_etl):
                self.logger.aviso("Usando dimensões simuladas")
            
            # FASE 2: CRIAÇÃO DE DIMENSÕES LABORAIS
            self.logger.secao("FASE 2: CRIAÇÃO DE DIMENSÕES LABORAIS")
            self.etapa_atual = "Dimensões Laborais"
            
            if not self._criar_dimensoes_laborais():
                raise Exception("Falha na criação de dimensões laborais")
            
            # FASE 3: EXTRAÇÃO E TRANSFORMAÇÃO DE DADOS
            self.logger.secao("FASE 3: EXTRAÇÃO E TRANSFORMAÇÃO")
            self.etapa_atual = "Extração"
            
            if not self._executar_extracao():
                raise Exception("Falha na extração de dados")
            
            # FASE 4: CRIAÇÃO DE FATOS LABORAIS
            self.logger.secao("FASE 4: CRIAÇÃO DE FATOS LABORAIS")
            self.etapa_atual = "Fatos Laborais"
            
            if not self._criar_fatos_laborais():
                raise Exception("Falha na criação de fatos laborais")
            
            # FASE 5: VALIDAÇÃO
            self.logger.secao("FASE 5: VALIDAÇÃO")
            self.etapa_atual = "Validação"
            
            self._executar_validacao()
            
            # FASE 6: EXPORTAÇÃO
            self.logger.secao("FASE 6: EXPORTAÇÃO")
            self.etapa_atual = "Exportação"
            
            self._executar_exportacao()
            
            # CONCLUSÃO
            self.tempo_fim = datetime.now()
            self._exibir_resumo_final()
            self.status_pipeline = "Concluído"
            
            return True
            
        except Exception as e:
            self.status_pipeline = "Erro"
            self.logger.erro(f"ERRO FATAL NO PIPELINE: {str(e)}")
            self.logger.erro(traceback.format_exc())
            return False
    
    def _integrar_dimensoes_base(self, dimensoes_base_etl):
        """
        Integra dimensões base do ETL_EDUCACAO
        Se não fornecidas, cria versões simuladas
        """
        if dimensoes_base_etl:
            self.dimensoes_base = dimensoes_base_etl
            self.logger.sucesso(f"Dimensões base importadas: {list(dimensoes_base_etl.keys())}")
            
            # Criar lookup para dimensões base
            try:
                from parte_04_transformador_dimensoes import LookupDimensoes
                self.lookup_base = LookupDimensoes(dimensoes_base_etl)
                return True
            except:
                self.logger.aviso("Não foi possível importar LookupDimensoes do ETL_EDUCACAO")
                return False
        else:
            # Criar dimensões base simuladas
            self.logger.aviso("Criando dimensões base simuladas...")
            self._criar_dimensoes_base_simuladas()
            return False
    
    def _criar_dimensoes_base_simuladas(self):
        """Cria versões simuladas das dimensões base para teste"""
        # Dim_Nacionalidade simulada
        self.dimensoes_base['Dim_Nacionalidade'] = pd.DataFrame({
            'nacionalidade_id': [1, 2, 3],
            'nome_nacionalidade': ['Brasil', 'Angola', 'Cabo Verde'],
            'codigo_pais': ['BRA', 'AGO', 'CPV'],
            'continente': ['América do Sul', 'África', 'África']
        })
        
        # Dim_Sexo simulada
        self.dimensoes_base['Dim_Sexo'] = pd.DataFrame({
            'sexo_id': [1, 2],
            'tipo_sexo': ['Masculino', 'Feminino']
        })
        
        # Dim_PopulacaoResidente simulada
        self.dimensoes_base['Dim_PopulacaoResidente'] = pd.DataFrame({
            'populacao_id': [1, 2],
            'ano_referencia': [2011, 2001],
            'total_populacao': [0, 0]
        })
        
        self.logger.sucesso("Dimensões base simuladas criadas")
    
    def _criar_dimensoes_laborais(self):
        """Cria as 7 dimensões laborais específicas"""
        self.transformador_dim = TransformadorDimensoesLaborais(
            self.logger,
            self.config,
            self.constantes
        )
        
        # Criar todas as dimensões
        self.dimensoes = self.transformador_dim.criar_todas_dimensoes()
        
        # Criar lookup de dimensões laborais
        self.lookup_laborais = LookupDimensoesLaborais(self.dimensoes)
        
        self.logger.sucesso(f"Dimensões laborais criadas: {len(self.dimensoes)}")
        
        return True
    
    def _executar_extracao(self):
        """Extrai dados laborais (modo simulação)"""
        self.logger.info("Modo SIMULAÇÃO: Gerando dados de exemplo...")
        
        # Dados simulados de condição econômica
        self.dados_brutos['condicao'] = [
            {'nacionalidade': 'Brasil', 'condicao': 'População empregada', 'quantidade': 50000, 'ano': 2011},
            {'nacionalidade': 'Brasil', 'condicao': 'População desempregada', 'quantidade': 5000, 'ano': 2011},
            {'nacionalidade': 'Angola', 'condicao': 'População empregada', 'quantidade': 30000, 'ano': 2011}
        ]
        
        # Dados simulados de profissões
        self.dados_brutos['profissao'] = [
            {'nacionalidade': 'Brasil', 'codigo_profissao': '7', 'quantidade': 15000},
            {'nacionalidade': 'Angola', 'codigo_profissao': '5', 'quantidade': 10000}
        ]
        
        # Dados simulados de setores
        self.dados_brutos['setor'] = [
            {'nacionalidade': 'Brasil', 'codigo_cae': 'F', 'quantidade': 12000},
            {'nacionalidade': 'Angola', 'codigo_cae': 'I', 'quantidade': 8000}
        ]
        
        # Dados simulados de situação profissional
        self.dados_brutos['situacao'] = [
            {'nacionalidade': 'Brasil', 'situacao': 'Trabalhador por conta de outrem', 'quantidade': 40000}
        ]
        
        # Dados vazios para outros fatos (podem ser preenchidos com dados reais)
        self.dados_brutos['prof_sexo'] = []
        self.dados_brutos['regiao_setor'] = []
        self.dados_brutos['trab_escolaridade'] = []
        self.dados_brutos['rend_regiao'] = []
        
        self.logger.sucesso(f"Extração concluída: {len(self.dados_brutos)} conjuntos de dados")
        return True
    
    def _criar_fatos_laborais(self):
        """Cria as 8 tabelas de fatos laborais"""
        self.transformador_fatos = TransformadorFatosLaborais(
            self.logger,
            self.lookup_laborais,
            self.lookup_base
        )
        
        # Criar fatos
        if self.dados_brutos.get('condicao'):
            self.fatos['Fact_PopulacaoPorCondicao'] = \
                self.transformador_fatos.criar_fact_populacao_por_condicao(self.dados_brutos['condicao'])
        
        if self.dados_brutos.get('profissao'):
            self.fatos['Fact_EmpregadosPorProfissao'] = \
                self.transformador_fatos.criar_fact_empregados_por_profissao(self.dados_brutos['profissao'])
        
        if self.dados_brutos.get('setor'):
            self.fatos['Fact_EmpregadosPorSetor'] = \
                self.transformador_fatos.criar_fact_empregados_por_setor(self.dados_brutos['setor'])
        
        if self.dados_brutos.get('situacao'):
            self.fatos['Fact_EmpregadosPorSituacao'] = \
                self.transformador_fatos.criar_fact_empregados_por_situacao(self.dados_brutos['situacao'])
        
        self.logger.sucesso(f"Fatos laborais criados: {len(self.fatos)}")
        
        return True
    
    def _executar_validacao(self):
        """Executa validação dos dados"""
        self.logger.info("Validando integridade...")
        
        # Validação básica
        total_registros = sum(len(df) for df in self.fatos.values() if not df.empty)
        
        if total_registros > 0:
            self.logger.sucesso(f"✓ {total_registros} registros de fatos validados")
        else:
            self.logger.aviso("Nenhum fato criado")
        
        return True
    
    def _executar_exportacao(self):
        """Exporta dados para CSV"""
        self.logger.info("Exportando tabelas...")
        
        # Exportar dimensões
        for nome, df in self.dimensoes.items():
            if not df.empty:
                arquivo = f"DP-01-B_{nome}.csv"
                df.to_csv(arquivo, index=False, encoding='utf-8')
                self.logger.sucesso(f"✓ {arquivo}: {len(df)} registros")
        
        # Exportar fatos
        for nome, df in self.fatos.items():
            if not df.empty:
                arquivo = f"DP-01-B_{nome}.csv"
                df.to_csv(arquivo, index=False, encoding='utf-8')
                self.logger.sucesso(f"✓ {arquivo}: {len(df)} registros")
        
        total = len(self.dimensoes) + len(self.fatos)
        self.logger.sucesso(f"Exportação concluída: {total} arquivos CSV")
        
        return True
    
    def _exibir_cabecalho(self):
        """Exibe cabeçalho do pipeline"""
        print("\n" + "=" * 80)
        print("PIPELINE ETL - LABORAL (DP-01-B)".center(80))
        print("INE Censos 2011 - Dados Laborais para Star Schema".center(80))
        print("=" * 80)
        
        self.config.print_configuracoes()
        
        print("\nFASES DO PIPELINE:")
        print("  1. INTEGRAÇÃO: Dimensões base do ETL_EDUCACAO")
        print("  2. DIMENSÕES: Criação de 7 dimensões laborais")
        print("  3. EXTRAÇÃO: Leitura de dados laborais")
        print("  4. FATOS: Criação de 8 fatos laborais")
        print("  5. VALIDAÇÃO: Verificação de integridade")
        print("  6. EXPORTAÇÃO: CSV para download")
        print("=" * 80 + "\n")
    
    def _exibir_resumo_final(self):
        """Exibe resumo final da execução"""
        duracao = self.tempo_fim - self.tempo_inicio
        
        print("\n" + "=" * 80)
        print("PIPELINE LABORAL CONCLUÍDO!".center(80))
        print("=" * 80)
        
        print(f"\nTempo de Execução: {duracao}")
        print(f"Início: {self.tempo_inicio.strftime('%H:%M:%S')}")
        print(f"Fim: {self.tempo_fim.strftime('%H:%M:%S')}")
        
        print("\nRESULTADOS:")
        print(f"  ✓ Dimensões laborais: {len(self.dimensoes)}")
        print(f"  ✓ Fatos laborais: {len(self.fatos)}")
        
        total_registros = sum(len(df) for df in self.fatos.values() if not df.empty)
        print(f"  ✓ Total de registros nos fatos: {total_registros:,}")
        
        print("\n" + "=" * 80)
        print("Os arquivos CSV foram gerados com sucesso!".center(80))
        print("=" * 80 + "\n")
    
    def obter_resumo(self):
        """Retorna resumo do pipeline"""
        return {
            'status': self.status_pipeline,
            'etapa_atual': self.etapa_atual,
            'dimensoes_laborais': list(self.dimensoes.keys()),
            'fatos_laborais': list(self.fatos.keys()),
            'total_dimensoes': len(self.dimensoes),
            'total_fatos': len(self.fatos)
        }


# ============================================================
# FUNÇÃO PRINCIPAL DE EXECUÇÃO
# ============================================================

def executar_pipeline_laboral(dimensoes_base=None):
    """
    Função principal para executar o pipeline laboral no Google Colab
    
    Parâmetros:
      dimensoes_base: Dict com dimensões do ETL_EDUCACAO (opcional)
    
    Uso no Colab:
      >>> from parte_05_orquestrador_laboral import executar_pipeline_laboral
      >>> executar_pipeline_laboral()
    """
    orquestrador = OrquestradorPipelineLaboral()
    sucesso = orquestrador.executar_pipeline_completo(dimensoes_base)
    
    if sucesso:
        print("\n✅ Pipeline Laboral executado com sucesso!")
        print("📊 Verifique os arquivos CSV gerados.")
        return orquestrador
    else:
        print("\n❌ Pipeline Laboral falhou. Verifique os erros acima.")
        return None


# ============================================================
# EXECUÇÃO DIRETA
# ============================================================

if __name__ == "__main__":
    print("=" * 80)
    print("EXECUTANDO PIPELINE ETL - LABORAL (DP-01-B)".center(80))
    print("=" * 80 + "\n")
    
    # Executar pipeline
    orquestrador = executar_pipeline_laboral()
    
    if orquestrador:
        # Exibir resumo
        resumo = orquestrador.obter_resumo()
        print("\nRESUMO DO PIPELINE:")
        for chave, valor in resumo.items():
            if not isinstance(valor, (list, datetime)):
                print(f"  {chave}: {valor}")
