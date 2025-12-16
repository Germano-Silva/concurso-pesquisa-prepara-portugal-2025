"""
============================================================
PARTE 4: TRANSFORMADOR DE FATOS AIMA
Pipeline ETL - AIMA Integrado (DP-02-A)
Google Colab
============================================================
"""

import pandas as pd
import numpy as np
from parte_01_imports_config import Config, Constantes, Formatadores, Logger
from parte_02_classes_base_ref import FatoBase
from parte_03_transformador_dimensoes_aima import LookupDimensoesAIMA

# ============================================================
# TRANSFORMADOR DE FATOS AIMA
# ============================================================

class TransformadorFatosAIMA:
    """Transformador responsável por criar todas as tabelas fato AIMA"""
    
    def __init__(self, dimensoes, lookup, logger=None):
        """
        Args:
            dimensoes: Dict com DataFrames das dimensões
            lookup: Objeto LookupDimensoesAIMA para resolução de IDs
            logger: Logger personalizado
        """
        self.dimensoes = dimensoes
        self.lookup = lookup
        self.logger = logger or Logger("TransformadorFatosAIMA")
        self.fatos = {}
        
        # Lookup de Sexo (assumindo compatibilidade com ETL_EDUCACAO)
        self.sexo_lookup = {'Masculino': 1, 'Feminino': 2, 'Homens': 1, 'Mulheres': 2}
    
    def criar_fact_concessoes_por_nacionalidade_sexo(self, dados_concessoes):
        """
        Cria Fact_ConcessoesPorNacionalidadeSexo
        Estrutura: concessao_nac_sexo_id, ano_id, tipo_id, nacionalidade_aima_id, 
                   sexo_id, total_homens_mulheres
        
        Args:
            dados_concessoes: DataFrame com dados de concessões por nacionalidade
                             Esperado: Ano, Nacionalidade, Homens, Mulheres
        """
        self.logger.subsecao("Criando Fact_ConcessoesPorNacionalidadeSexo")
        
        if dados_concessoes is None or len(dados_concessoes) == 0:
            self.logger.aviso("Nenhum dado de concessões fornecido")
            return pd.DataFrame()
        
        registros = []
        concessao_id = 1
        tipo_id = self.lookup.get_tipo_id('Concessão de Títulos')
        
        for _, row in dados_concessoes.iterrows():
            ano = row.get('Ano', row.get('ano'))
            nacionalidade = row.get('Nacionalidade', row.get('nacionalidade', row.get('País')))
            homens = Formatadores.limpar_numero(row.get('Homens', row.get('homens', 0)))
            mulheres = Formatadores.limpar_numero(row.get('Mulheres', row.get('mulheres', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            nacionalidade_aima_id = self.lookup.get_nacionalidade_aima_id(nacionalidade)
            
            if ano_id and nacionalidade_aima_id:
                # Registro para Homens
                if homens and homens > 0:
                    registros.append({
                        'concessao_nac_sexo_id': concessao_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'nacionalidade_aima_id': nacionalidade_aima_id,
                        'sexo_id': 1,  # Masculino
                        'total_homens_mulheres': int(homens)
                    })
                    concessao_id += 1
                
                # Registro para Mulheres
                if mulheres and mulheres > 0:
                    registros.append({
                        'concessao_nac_sexo_id': concessao_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'nacionalidade_aima_id': nacionalidade_aima_id,
                        'sexo_id': 2,  # Feminino
                        'total_homens_mulheres': int(mulheres)
                    })
                    concessao_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_ConcessoesPorNacionalidadeSexo criada: {len(df_fato)} registros")
        self.fatos['Fact_ConcessoesPorNacionalidadeSexo'] = df_fato
        
        return df_fato
    
    def criar_fact_concessoes_por_despacho(self, dados_despachos):
        """
        Cria Fact_ConcessoesPorDespacho
        Estrutura: concessao_despacho_id, ano_id, tipo_id, despacho_id, concessoes
        
        Args:
            dados_despachos: DataFrame com dados de concessões por despacho
                            Esperado: Ano, Despacho, Total
        """
        self.logger.subsecao("Criando Fact_ConcessoesPorDespacho")
        
        if dados_despachos is None or len(dados_despachos) == 0:
            self.logger.aviso("Nenhum dado de despachos fornecido")
            return pd.DataFrame()
        
        registros = []
        concessao_id = 1
        tipo_id = self.lookup.get_tipo_id('Concessão de Títulos')
        
        for _, row in dados_despachos.iterrows():
            ano = row.get('Ano', row.get('ano'))
            codigo_despacho = row.get('Despacho', row.get('despacho', row.get('codigo_despacho')))
            total = Formatadores.limpar_numero(row.get('Total', row.get('total', row.get('Concessoes', 0))))
            
            ano_id = self.lookup.get_ano_id(ano)
            despacho_id = self.lookup.get_despacho_id(codigo_despacho)
            
            if ano_id and despacho_id and total and total > 0:
                registros.append({
                    'concessao_despacho_id': concessao_id,
                    'ano_id': ano_id,
                    'tipo_id': tipo_id,
                    'despacho_id': despacho_id,
                    'concessoes': int(total)
                })
                concessao_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_ConcessoesPorDespacho criada: {len(df_fato)} registros")
        self.fatos['Fact_ConcessoesPorDespacho'] = df_fato
        
        return df_fato
    
    def criar_fact_concessoes_por_motivo_nacionalidade(self, dados_motivos):
        """
        Cria Fact_ConcessoesPorMotivoNacionalidade
        Estrutura: concessao_motivo_nac_id, ano_id, motivo_id, 
                   nacionalidade_aima_id, total_motivo
        
        Args:
            dados_motivos: DataFrame com dados de concessões por motivo
                          Esperado: Ano, Motivo, Nacionalidade, Total
        """
        self.logger.subsecao("Criando Fact_ConcessoesPorMotivoNacionalidade")
        
        if dados_motivos is None or len(dados_motivos) == 0:
            self.logger.aviso("Nenhum dado de motivos fornecido")
            return pd.DataFrame()
        
        registros = []
        concessao_id = 1
        
        for _, row in dados_motivos.iterrows():
            ano = row.get('Ano', row.get('ano'))
            motivo = row.get('Motivo', row.get('motivo'))
            nacionalidade = row.get('Nacionalidade', row.get('nacionalidade', row.get('País')))
            total = Formatadores.limpar_numero(row.get('Total', row.get('total', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            motivo_id = self.lookup.get_motivo_id(motivo)
            nacionalidade_aima_id = self.lookup.get_nacionalidade_aima_id(nacionalidade)
            
            if ano_id and motivo_id and nacionalidade_aima_id and total and total > 0:
                registros.append({
                    'concessao_motivo_nac_id': concessao_id,
                    'ano_id': ano_id,
                    'motivo_id': motivo_id,
                    'nacionalidade_aima_id': nacionalidade_aima_id,
                    'total_motivo': int(total)
                })
                concessao_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_ConcessoesPorMotivoNacionalidade criada: {len(df_fato)} registros")
        self.fatos['Fact_ConcessoesPorMotivoNacionalidade'] = df_fato
        
        return df_fato
    
    def criar_fact_populacao_estrangeira_por_nacionalidade_sexo(self, dados_pop_estrangeira):
        """
        Cria Fact_PopulacaoEstrangeiraPorNacionalidadeSexo
        Estrutura: pop_est_nac_sexo_id, ano_id, tipo_id, 
                   nacionalidade_aima_id, sexo_id, total_homens_mulheres
        
        Args:
            dados_pop_estrangeira: DataFrame com população estrangeira por nacionalidade
                                  Esperado: Ano, Nacionalidade, Homens, Mulheres
        """
        self.logger.subsecao("Criando Fact_PopulacaoEstrangeiraPorNacionalidadeSexo")
        
        if dados_pop_estrangeira is None or len(dados_pop_estrangeira) == 0:
            self.logger.aviso("Nenhum dado de população estrangeira fornecido")
            return pd.DataFrame()
        
        registros = []
        pop_id = 1
        tipo_id = self.lookup.get_tipo_id('População Estrangeira Residente')
        
        for _, row in dados_pop_estrangeira.iterrows():
            ano = row.get('Ano', row.get('ano'))
            nacionalidade = row.get('Nacionalidade', row.get('nacionalidade', row.get('País')))
            homens = Formatadores.limpar_numero(row.get('Homens', row.get('homens', 0)))
            mulheres = Formatadores.limpar_numero(row.get('Mulheres', row.get('mulheres', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            nacionalidade_aima_id = self.lookup.get_nacionalidade_aima_id(nacionalidade)
            
            if ano_id and nacionalidade_aima_id:
                # Registro para Homens
                if homens and homens > 0:
                    registros.append({
                        'pop_est_nac_sexo_id': pop_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'nacionalidade_aima_id': nacionalidade_aima_id,
                        'sexo_id': 1,  # Masculino
                        'total_homens_mulheres': int(homens)
                    })
                    pop_id += 1
                
                # Registro para Mulheres
                if mulheres and mulheres > 0:
                    registros.append({
                        'pop_est_nac_sexo_id': pop_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'nacionalidade_aima_id': nacionalidade_aima_id,
                        'sexo_id': 2,  # Feminino
                        'total_homens_mulheres': int(mulheres)
                    })
                    pop_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_PopulacaoEstrangeiraPorNacionalidadeSexo criada: {len(df_fato)} registros")
        self.fatos['Fact_PopulacaoEstrangeiraPorNacionalidadeSexo'] = df_fato
        
        return df_fato
    
    def criar_fact_distribuicao_etaria_concessoes(self, dados_etaria, dim_grupo_etario=None):
        """
        Cria Fact_DistribuicaoEtariaConcessoes
        Estrutura: dist_etaria_conc_id, ano_id, tipo_id, 
                   grupoetario_id, sexo_id, total_homens_mulheres
        
        Args:
            dados_etaria: DataFrame com distribuição etária de concessões
                         Esperado: Ano, FaixaEtaria, Homens, Mulheres
            dim_grupo_etario: Dimensão GrupoEtario do ETL_EDUCACAO (para FK)
        """
        self.logger.subsecao("Criando Fact_DistribuicaoEtariaConcessoes")
        
        if dados_etaria is None or len(dados_etaria) == 0:
            self.logger.aviso("Nenhum dado de distribuição etária fornecido")
            return pd.DataFrame()
        
        registros = []
        dist_id = 1
        tipo_id = self.lookup.get_tipo_id('Concessão de Títulos')
        
        # Criar lookup de grupos etários
        lookup_etario = {}
        if dim_grupo_etario is not None:
            for _, row in dim_grupo_etario.iterrows():
                faixa = row['faixa_etaria']
                grupo_id = row['grupoetario_id']
                lookup_etario[faixa] = grupo_id
        
        for _, row in dados_etaria.iterrows():
            ano = row.get('Ano', row.get('ano'))
            faixa_etaria = Formatadores.extrair_faixa_etaria(row.get('FaixaEtaria', row.get('faixa_etaria')))
            homens = Formatadores.limpar_numero(row.get('Homens', row.get('homens', 0)))
            mulheres = Formatadores.limpar_numero(row.get('Mulheres', row.get('mulheres', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            grupoetario_id = lookup_etario.get(faixa_etaria, None)
            
            if ano_id and grupoetario_id:
                # Registro para Homens
                if homens and homens > 0:
                    registros.append({
                        'dist_etaria_conc_id': dist_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'grupoetario_id': grupoetario_id,
                        'sexo_id': 1,  # Masculino
                        'total_homens_mulheres': int(homens)
                    })
                    dist_id += 1
                
                # Registro para Mulheres
                if mulheres and mulheres > 0:
                    registros.append({
                        'dist_etaria_conc_id': dist_id,
                        'ano_id': ano_id,
                        'tipo_id': tipo_id,
                        'grupoetario_id': grupoetario_id,
                        'sexo_id': 2,  # Feminino
                        'total_homens_mulheres': int(mulheres)
                    })
                    dist_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_DistribuicaoEtariaConcessoes criada: {len(df_fato)} registros")
        self.fatos['Fact_DistribuicaoEtariaConcessoes'] = df_fato
        
        return df_fato
    
    def criar_fact_evolucao_populacao_estrangeira(self, dados_evolucao):
        """
        Cria Fact_EvolucaoPopulacaoEstrangeira
        Estrutura: evolucao_pop_id, ano_id, titulos_residencia, 
                   concessao_ap, prorrogacao_vld, total, variacao_percent
        
        Args:
            dados_evolucao: DataFrame com evolução anual da população estrangeira
                           Esperado: Ano, TitulosResidencia, ConcessaoAP, 
                                    ProrrogacaoVLD, Total
        """
        self.logger.subsecao("Criando Fact_EvolucaoPopulacaoEstrangeira")
        
        if dados_evolucao is None or len(dados_evolucao) == 0:
            self.logger.aviso("Nenhum dado de evolução fornecido")
            return pd.DataFrame()
        
        # Ordenar por ano para calcular variação
        dados_evolucao = dados_evolucao.sort_values('Ano' if 'Ano' in dados_evolucao.columns else 'ano')
        
        registros = []
        evolucao_id = 1
        total_anterior = None
        
        for _, row in dados_evolucao.iterrows():
            ano = row.get('Ano', row.get('ano'))
            titulos = Formatadores.limpar_numero(row.get('TitulosResidencia', row.get('titulos_residencia', 0)))
            concessao_ap = Formatadores.limpar_numero(row.get('ConcessaoAP', row.get('concessao_ap', 0)))
            prorrogacao = Formatadores.limpar_numero(row.get('ProrrogacaoVLD', row.get('prorrogacao_vld', 0)))
            total = Formatadores.limpar_numero(row.get('Total', row.get('total', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            
            # Calcular variação percentual
            variacao_percent = None
            if total_anterior is not None and total is not None:
                variacao_percent = Formatadores.calcular_variacao_percentual(total, total_anterior)
            
            if ano_id:
                registros.append({
                    'evolucao_pop_id': evolucao_id,
                    'ano_id': ano_id,
                    'titulos_residencia': int(titulos) if titulos else 0,
                    'concessao_ap': int(concessao_ap) if concessao_ap else 0,
                    'prorrogacao_vld': int(prorrogacao) if prorrogacao else 0,
                    'total': int(total) if total else 0,
                    'variacao_percent': round(variacao_percent, 2) if variacao_percent is not None else None
                })
                evolucao_id += 1
                total_anterior = total
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_EvolucaoPopulacaoEstrangeira criada: {len(df_fato)} registros")
        self.fatos['Fact_EvolucaoPopulacaoEstrangeira'] = df_fato
        
        return df_fato
    
    def criar_fact_populacao_residente_etaria(self, dados_pop_residente, dim_grupo_etario=None):
        """
        Cria Fact_PopulacaoResidenteEtaria
        Estrutura: pop_res_etaria_id, ano_id, tipo_id, grupoetario_id, total
        
        Args:
            dados_pop_residente: DataFrame com população residente por faixa etária
                                Esperado: Ano, FaixaEtaria, Total
            dim_grupo_etario: Dimensão GrupoEtario do ETL_EDUCACAO (para FK)
        """
        self.logger.subsecao("Criando Fact_PopulacaoResidenteEtaria")
        
        if dados_pop_residente is None or len(dados_pop_residente) == 0:
            self.logger.aviso("Nenhum dado de população residente fornecido")
            return pd.DataFrame()
        
        registros = []
        pop_id = 1
        tipo_id = self.lookup.get_tipo_id('População Residente - Distribuição Etária')
        
        # Criar lookup de grupos etários
        lookup_etario = {}
        if dim_grupo_etario is not None:
            for _, row in dim_grupo_etario.iterrows():
                faixa = row['faixa_etaria']
                grupo_id = row['grupoetario_id']
                lookup_etario[faixa] = grupo_id
        
        for _, row in dados_pop_residente.iterrows():
            ano = row.get('Ano', row.get('ano'))
            faixa_etaria = Formatadores.extrair_faixa_etaria(row.get('FaixaEtaria', row.get('faixa_etaria')))
            total = Formatadores.limpar_numero(row.get('Total', row.get('total', 0)))
            
            ano_id = self.lookup.get_ano_id(ano)
            grupoetario_id = lookup_etario.get(faixa_etaria, None)
            
            if ano_id and grupoetario_id and total and total > 0:
                registros.append({
                    'pop_res_etaria_id': pop_id,
                    'ano_id': ano_id,
                    'tipo_id': tipo_id,
                    'grupoetario_id': grupoetario_id,
                    'total': int(total)
                })
                pop_id += 1
        
        df_fato = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Fact_PopulacaoResidenteEtaria criada: {len(df_fato)} registros")
        self.fatos['Fact_PopulacaoResidenteEtaria'] = df_fato
        
        return df_fato
    
    def criar_todos_fatos(self, dados_brutos, dimensoes_base=None):
        """
        Cria todas as tabelas fato AIMA
        
        Args:
            dados_brutos: Dict com DataFrames dos arquivos AIMA brutos
            dimensoes_base: Dict com dimensões do ETL_EDUCACAO (opcional)
        
        Returns:
            Dict com todas as tabelas fato criadas
        """
        self.logger.secao("CRIANDO FATOS AIMA")
        
        dimensoes_base = dimensoes_base or {}
        dim_grupo_etario = dimensoes_base.get('Dim_GrupoEtario', None)
        
        # 1. Fact_ConcessoesPorNacionalidadeSexo
        if 'concessoes_nacionalidade' in dados_brutos:
            self.criar_fact_concessoes_por_nacionalidade_sexo(dados_brutos['concessoes_nacionalidade'])
        
        # 2. Fact_ConcessoesPorDespacho
        if 'concessoes_despacho' in dados_brutos:
            self.criar_fact_concessoes_por_despacho(dados_brutos['concessoes_despacho'])
        
        # 3. Fact_ConcessoesPorMotivoNacionalidade
        if 'concessoes_motivo' in dados_brutos:
            self.criar_fact_concessoes_por_motivo_nacionalidade(dados_brutos['concessoes_motivo'])
        
        # 4. Fact_PopulacaoEstrangeiraPorNacionalidadeSexo
        if 'populacao_estrangeira' in dados_brutos:
            self.criar_fact_populacao_estrangeira_por_nacionalidade_sexo(dados_brutos['populacao_estrangeira'])
        
        # 5. Fact_DistribuicaoEtariaConcessoes
        if 'distribuicao_etaria' in dados_brutos:
            self.criar_fact_distribuicao_etaria_concessoes(dados_brutos['distribuicao_etaria'], dim_grupo_etario)
        
        # 6. Fact_EvolucaoPopulacaoEstrangeira
        if 'evolucao_populacao' in dados_brutos:
            self.criar_fact_evolucao_populacao_estrangeira(dados_brutos['evolucao_populacao'])
        
        # 7. Fact_PopulacaoResidenteEtaria
        if 'populacao_residente_etaria' in dados_brutos:
            self.criar_fact_populacao_residente_etaria(dados_brutos['populacao_residente_etaria'], dim_grupo_etario)
        
        self.logger.secao(f"FATOS CRIADOS: {len(self.fatos)}")
        
        return self.fatos
    
    def gerar_relatorio_fatos(self):
        """Gera relatório resumido das tabelas fato criadas"""
        print("\n" + "=" * 60)
        print("RELATÓRIO DE FATOS AIMA")
        print("=" * 60)
        
        total_registros = 0
        for nome, df in self.fatos.items():
            registros = len(df)
            total_registros += registros
            print(f"\n{nome}:")
            print(f"  - Registros: {registros}")
            print(f"  - Colunas: {', '.join(df.columns)}")
            print(f"  - Memória: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        print(f"\nTotal de registros (todos os fatos): {total_registros}")
        print("=" * 60)


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    from parte_03_transformador_dimensoes_aima import TransformadorDimensoesAIMA, LookupDimensoesAIMA
    
    logger = Logger("TESTE-FATOS-AIMA")
    logger.secao("TESTE - Transformador de Fatos AIMA")
    
    # Criar dimensões primeiro
    trans_dim = TransformadorDimensoesAIMA(logger)
    dados_brutos_dim = {
        'nacionalidades': ['Brasil', 'Angola'],
        'despachos': None,
        'motivos': None
    }
    dimensoes = trans_dim.criar_todas_dimensoes(dados_brutos_dim)
    
    # Criar lookup
    lookup = LookupDimensoesAIMA(dimensoes)
    
    # Criar transformador de fatos
    trans_fato = TransformadorFatosAIMA(dimensoes, lookup, logger)
    
    # Dados de teste
    dados_brutos_fatos = {
        'concessoes_nacionalidade': pd.DataFrame({
            'Ano': [2020, 2020],
            'Nacionalidade': ['Brasil', 'Angola'],
            'Homens': [1000, 500],
            'Mulheres': [900, 450]
        }),
        'evolucao_populacao': pd.DataFrame({
            'Ano': [2020, 2021],
            'TitulosResidencia': [10000, 12000],
            'ConcessaoAP': [5000, 6000],
            'ProrrogacaoVLD': [3000, 3500],
            'Total': [18000, 21500]
        })
    }
    
    # Criar fatos
    fatos = trans_fato.criar_todos_fatos(dados_brutos_fatos)
    
    # Gerar relatório
    trans_fato.gerar_relatorio_fatos()
    
    logger.sucesso("Teste concluído com sucesso!")
    print("\n✓ Módulo parte_04_transformador_fatos_aima.py carregado com sucesso!")
