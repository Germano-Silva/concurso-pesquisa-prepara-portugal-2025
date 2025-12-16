"""
============================================================
PARTE 3: TRANSFORMADOR DE DIMENSÕES AIMA
Pipeline ETL - AIMA Integrado (DP-02-A)
Google Colab
============================================================
"""

import pandas as pd
import numpy as np
from parte_01_imports_config import Config, Constantes, Formatadores, Logger
from parte_02_classes_base_ref import DimensaoBase

# ============================================================
# TRANSFORMADOR DE DIMENSÕES AIMA
# ============================================================

class TransformadorDimensoesAIMA:
    """Transformador responsável por criar todas as dimensões AIMA"""
    
    def __init__(self, logger=None):
        self.logger = logger or Logger("TransformadorDimensoesAIMA")
        self.dimensoes = {}
    
    def criar_dim_ano_relatorio(self):
        """
        Cria Dim_AnoRelatorio
        Estrutura: ano_id, ano, fonte
        """
        self.logger.subsecao("Criando Dim_AnoRelatorio")
        
        registros = []
        for ano in Config.ANOS_REFERENCIA:
            registros.append({
                'ano_id': ano,
                'ano': ano,
                'fonte': Config.FONTES_ANO[ano]
            })
        
        df_ano = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Dim_AnoRelatorio criada: {len(df_ano)} registros")
        self.dimensoes['Dim_AnoRelatorio'] = df_ano
        
        return df_ano
    
    def criar_dim_tipo_relatorio(self):
        """
        Cria Dim_TipoRelatorio
        Estrutura: tipo_id, tipo
        """
        self.logger.subsecao("Criando Dim_TipoRelatorio")
        
        registros = []
        for i, tipo in enumerate(Config.TIPOS_RELATORIO, 1):
            registros.append({
                'tipo_id': i,
                'tipo': tipo
            })
        
        df_tipo = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Dim_TipoRelatorio criada: {len(df_tipo)} registros")
        self.dimensoes['Dim_TipoRelatorio'] = df_tipo
        
        return df_tipo
    
    def criar_dim_despacho(self, dados_despachos=None):
        """
        Cria Dim_Despacho
        Estrutura: despacho_id, codigo_despacho, descricao
        
        Args:
            dados_despachos: DataFrame com despachos extraídos dos dados AIMA
        """
        self.logger.subsecao("Criando Dim_Despacho")
        
        registros = []
        despacho_id = 1
        
        # Adicionar despachos conhecidos
        for codigo, descricao in Config.DESPACHOS_CONHECIDOS.items():
            registros.append({
                'despacho_id': despacho_id,
                'codigo_despacho': codigo,
                'descricao': descricao
            })
            despacho_id += 1
        
        # Se houver dados adicionais, adicionar despachos não conhecidos
        if dados_despachos is not None:
            codigos_conhecidos = set(Config.DESPACHOS_CONHECIDOS.keys())
            
            for _, row in dados_despachos.iterrows():
                codigo = str(row.get('codigo_despacho', row.get('Despacho', ''))).strip()
                descricao = str(row.get('descricao', row.get('Descricao', codigo))).strip()
                
                if codigo and codigo not in codigos_conhecidos:
                    registros.append({
                        'despacho_id': despacho_id,
                        'codigo_despacho': codigo,
                        'descricao': descricao
                    })
                    codigos_conhecidos.add(codigo)
                    despacho_id += 1
        
        df_despacho = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Dim_Despacho criada: {len(df_despacho)} registros")
        self.dimensoes['Dim_Despacho'] = df_despacho
        
        return df_despacho
    
    def criar_dim_motivo_concessao(self, dados_motivos=None):
        """
        Cria Dim_MotivoConcessao
        Estrutura: motivo_id, nome_motivo, categoria
        
        Args:
            dados_motivos: DataFrame com motivos extraídos dos dados AIMA
        """
        self.logger.subsecao("Criando Dim_MotivoConcessao")
        
        registros = []
        motivo_id = 1
        
        # Adicionar motivos padronizados
        for codigo, info in Config.MOTIVOS_CONCESSAO.items():
            registros.append({
                'motivo_id': motivo_id,
                'nome_motivo': info['nome'],
                'categoria': info['categoria']
            })
            motivo_id += 1
        
        # Se houver dados adicionais, adicionar motivos não conhecidos
        if dados_motivos is not None:
            motivos_conhecidos = set([info['nome'] for info in Config.MOTIVOS_CONCESSAO.values()])
            
            for _, row in dados_motivos.iterrows():
                motivo = str(row.get('motivo', row.get('Motivo', ''))).strip()
                
                if motivo and motivo not in motivos_conhecidos:
                    # Normalizar e categorizar
                    codigo_normalizado = Formatadores.normalizar_motivo(motivo)
                    categoria = Config.MOTIVOS_CONCESSAO[codigo_normalizado]['categoria']
                    
                    if motivo not in [r['nome_motivo'] for r in registros]:
                        registros.append({
                            'motivo_id': motivo_id,
                            'nome_motivo': motivo,
                            'categoria': categoria
                        })
                        motivos_conhecidos.add(motivo)
                        motivo_id += 1
        
        df_motivo = pd.DataFrame(registros)
        
        self.logger.sucesso(f"Dim_MotivoConcessao criada: {len(df_motivo)} registros")
        self.dimensoes['Dim_MotivoConcessao'] = df_motivo
        
        return df_motivo
    
    def criar_dim_nacionalidade_aima(self, dados_nacionalidades, dim_nacionalidade_base=None):
        """
        Cria Dim_NacionalidadeAIMA com mapeamento para Dim_Nacionalidade
        Estrutura: nacionalidade_aima_id, nome_nacionalidade_aima, nacionalidade_id
        
        Args:
            dados_nacionalidades: DataFrame com nacionalidades dos dados AIMA
            dim_nacionalidade_base: DataFrame Dim_Nacionalidade do ETL_EDUCACAO (opcional)
        """
        self.logger.subsecao("Criando Dim_NacionalidadeAIMA")
        
        # Extrair nacionalidades únicas
        if isinstance(dados_nacionalidades, pd.DataFrame):
            # Tentar diferentes nomes de colunas
            coluna_nac = None
            for col in ['nacionalidade', 'Nacionalidade', 'pais', 'País']:
                if col in dados_nacionalidades.columns:
                    coluna_nac = col
                    break
            
            if coluna_nac:
                nomes_unicos = dados_nacionalidades[coluna_nac].dropna().unique()
            else:
                self.logger.aviso("Coluna de nacionalidade não encontrada nos dados")
                nomes_unicos = []
        else:
            nomes_unicos = dados_nacionalidades
        
        registros = []
        nacionalidade_aima_id = 1
        
        # Criar lookup de nacionalidades base se disponível
        lookup_base = {}
        if dim_nacionalidade_base is not None:
            for _, row in dim_nacionalidade_base.iterrows():
                nome_base = row['nome_nacionalidade']
                id_base = row['nacionalidade_id']
                lookup_base[nome_base] = id_base
        
        # Processar cada nacionalidade AIMA
        for nome_aima in sorted(nomes_unicos):
            if pd.isna(nome_aima) or nome_aima == '':
                continue
            
            nome_aima_clean = str(nome_aima).strip()
            
            # Normalizar para encontrar correspondência
            nome_normalizado = Formatadores.normalizar_nacionalidade(nome_aima_clean)
            
            # Tentar encontrar ID na base
            nacionalidade_id = lookup_base.get(nome_normalizado, None)
            
            if nacionalidade_id is None and len(lookup_base) > 0:
                self.logger.aviso(f"Nacionalidade '{nome_aima_clean}' sem correspondência em Dim_Nacionalidade")
            
            registros.append({
                'nacionalidade_aima_id': nacionalidade_aima_id,
                'nome_nacionalidade_aima': nome_aima_clean,
                'nacionalidade_id': nacionalidade_id
            })
            nacionalidade_aima_id += 1
        
        df_nacionalidade_aima = pd.DataFrame(registros)
        
        # Estatísticas de mapeamento
        total = len(df_nacionalidade_aima)
        mapeados = df_nacionalidade_aima['nacionalidade_id'].notna().sum()
        taxa_mapeamento = (mapeados / total * 100) if total > 0 else 0
        
        self.logger.sucesso(f"Dim_NacionalidadeAIMA criada: {total} registros")
        self.logger.info(f"Taxa de mapeamento: {taxa_mapeamento:.1f}% ({mapeados}/{total})")
        
        self.dimensoes['Dim_NacionalidadeAIMA'] = df_nacionalidade_aima
        
        return df_nacionalidade_aima
    
    def criar_todas_dimensoes(self, dados_brutos=None, dimensoes_base=None):
        """
        Cria todas as dimensões AIMA
        
        Args:
            dados_brutos: Dict com DataFrames dos arquivos AIMA brutos
            dimensoes_base: Dict com dimensões do ETL_EDUCACAO (Dim_Nacionalidade, etc.)
        
        Returns:
            Dict com todas as dimensões criadas
        """
        self.logger.secao("CRIANDO DIMENSÕES AIMA")
        
        dados_brutos = dados_brutos or {}
        dimensoes_base = dimensoes_base or {}
        
        # 1. Dim_AnoRelatorio
        self.criar_dim_ano_relatorio()
        
        # 2. Dim_TipoRelatorio
        self.criar_dim_tipo_relatorio()
        
        # 3. Dim_Despacho
        dados_despachos = dados_brutos.get('despachos', None)
        self.criar_dim_despacho(dados_despachos)
        
        # 4. Dim_MotivoConcessao
        dados_motivos = dados_brutos.get('motivos', None)
        self.criar_dim_motivo_concessao(dados_motivos)
        
        # 5. Dim_NacionalidadeAIMA
        dados_nacionalidades = dados_brutos.get('nacionalidades', [])
        dim_nacionalidade_base = dimensoes_base.get('Dim_Nacionalidade', None)
        self.criar_dim_nacionalidade_aima(dados_nacionalidades, dim_nacionalidade_base)
        
        self.logger.secao(f"DIMENSÕES CRIADAS: {len(self.dimensoes)}")
        
        return self.dimensoes
    
    def gerar_relatorio_dimensoes(self):
        """Gera relatório resumido das dimensões criadas"""
        print("\n" + "=" * 60)
        print("RELATÓRIO DE DIMENSÕES AIMA")
        print("=" * 60)
        
        for nome, df in self.dimensoes.items():
            print(f"\n{nome}:")
            print(f"  - Registros: {len(df)}")
            print(f"  - Colunas: {', '.join(df.columns)}")
            print(f"  - Memória: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        print("\n" + "=" * 60)


# ============================================================
# LOOKUP PARA RESOLUÇÃO RÁPIDA DE IDs
# ============================================================

class LookupDimensoesAIMA:
    """Classe para lookup rápido de IDs das dimensões AIMA"""
    
    def __init__(self, dimensoes):
        """
        Args:
            dimensoes: Dict com DataFrames das dimensões
        """
        self.dimensoes = dimensoes
        self._criar_lookups()
    
    def _criar_lookups(self):
        """Cria dicionários de lookup para cada dimensão"""
        
        # Lookup Ano
        if 'Dim_AnoRelatorio' in self.dimensoes:
            df_ano = self.dimensoes['Dim_AnoRelatorio']
            self.lookup_ano = dict(zip(df_ano['ano'], df_ano['ano_id']))
        else:
            self.lookup_ano = {}
        
        # Lookup Tipo
        if 'Dim_TipoRelatorio' in self.dimensoes:
            df_tipo = self.dimensoes['Dim_TipoRelatorio']
            self.lookup_tipo = dict(zip(df_tipo['tipo'], df_tipo['tipo_id']))
        else:
            self.lookup_tipo = {}
        
        # Lookup Despacho
        if 'Dim_Despacho' in self.dimensoes:
            df_despacho = self.dimensoes['Dim_Despacho']
            self.lookup_despacho = dict(zip(df_despacho['codigo_despacho'], df_despacho['despacho_id']))
        else:
            self.lookup_despacho = {}
        
        # Lookup Motivo
        if 'Dim_MotivoConcessao' in self.dimensoes:
            df_motivo = self.dimensoes['Dim_MotivoConcessao']
            self.lookup_motivo = dict(zip(df_motivo['nome_motivo'], df_motivo['motivo_id']))
        else:
            self.lookup_motivo = {}
        
        # Lookup Nacionalidade AIMA
        if 'Dim_NacionalidadeAIMA' in self.dimensoes:
            df_nac_aima = self.dimensoes['Dim_NacionalidadeAIMA']
            self.lookup_nac_aima = dict(zip(df_nac_aima['nome_nacionalidade_aima'], df_nac_aima['nacionalidade_aima_id']))
        else:
            self.lookup_nac_aima = {}
    
    def get_ano_id(self, ano):
        """Retorna ano_id para um ano"""
        return self.lookup_ano.get(ano, None)
    
    def get_tipo_id(self, tipo):
        """Retorna tipo_id para um tipo de relatório"""
        return self.lookup_tipo.get(tipo, None)
    
    def get_despacho_id(self, codigo_despacho):
        """Retorna despacho_id para um código de despacho"""
        return self.lookup_despacho.get(codigo_despacho, None)
    
    def get_motivo_id(self, nome_motivo):
        """Retorna motivo_id para um motivo de concessão"""
        return self.lookup_motivo.get(nome_motivo, None)
    
    def get_nacionalidade_aima_id(self, nome_nacionalidade):
        """Retorna nacionalidade_aima_id para uma nacionalidade"""
        return self.lookup_nac_aima.get(nome_nacionalidade, None)
    
    def get_motivo_id_por_codigo(self, codigo):
        """
        Retorna motivo_id baseado no código normalizado
        Ex: 'ATIVIDADE_PROFISSIONAL' -> ID correspondente
        """
        if codigo in Config.MOTIVOS_CONCESSAO:
            nome_motivo = Config.MOTIVOS_CONCESSAO[codigo]['nome']
            return self.get_motivo_id(nome_motivo)
        return None
    
    def estatisticas_lookup(self):
        """Retorna estatísticas dos lookups"""
        return {
            'anos': len(self.lookup_ano),
            'tipos': len(self.lookup_tipo),
            'despachos': len(self.lookup_despacho),
            'motivos': len(self.lookup_motivo),
            'nacionalidades_aima': len(self.lookup_nac_aima)
        }


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    logger = Logger("TESTE-DIM-AIMA")
    logger.secao("TESTE - Transformador de Dimensões AIMA")
    
    # Criar transformador
    transformador = TransformadorDimensoesAIMA(logger)
    
    # Dados de teste
    dados_brutos = {
        'nacionalidades': ['Brasil', 'Angola', 'Cabo Verde', 'Ucrânia'],
        'despachos': None,
        'motivos': None
    }
    
    # Criar dimensões
    dimensoes_base = None  # Simular sem integração
    dimensoes = transformador.criar_todas_dimensoes(dados_brutos, dimensoes_base)
    
    # Gerar relatório
    transformador.gerar_relatorio_dimensoes()
    
    # Testar lookup
    logger.subsecao("Testando Lookup")
    lookup = LookupDimensoesAIMA(dimensoes)
    
    print(f"\nAno 2020 -> ID: {lookup.get_ano_id(2020)}")
    print(f"Tipo 'Concessão de Títulos' -> ID: {lookup.get_tipo_id('Concessão de Títulos')}")
    print(f"Brasil -> ID: {lookup.get_nacionalidade_aima_id('Brasil')}")
    
    print(f"\nEstatísticas Lookup: {lookup.estatisticas_lookup()}")
    
    logger.sucesso("Teste concluído com sucesso!")
    print("\n✓ Módulo parte_03_transformador_dimensoes_aima.py carregado com sucesso!")
