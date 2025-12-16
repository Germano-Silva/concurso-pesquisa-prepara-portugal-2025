"""
============================================================
PARTE 3: TRANSFORMADOR DE DIMENSÕES LABORAIS
Pipeline ETL - Laboral (DP-01-B)
Google Colab
============================================================
Cria 7 dimensões laborais:
- Dim_CondicaoEconomica
- Dim_GrupoProfissional
- Dim_ProfissaoDigito1
- Dim_SetorEconomico
- Dim_SituacaoProfissional
- Dim_FonteRendimento
- Dim_RegiaoNUTS
"""

import pandas as pd
import numpy as np


# ============================================================
# CLASSE TRANSFORMADOR DE DIMENSÕES LABORAIS
# ============================================================

class TransformadorDimensoesLaborais:
    """Transforma dados brutos em dimensões laborais do modelo Star Schema"""
    
    def __init__(self, logger, config, constantes):
        self.logger = logger
        self.config = config
        self.constantes = constantes
        self.dimensoes = {}
    
    def criar_dim_condicao_economica(self):
        """
        Cria Dim_CondicaoEconomica
        Estrutura:
          - condicao_id (PK)
          - nome_condicao
          - categoria (Ativo/Inativo)
        """
        self.logger.info("Criando Dim_CondicaoEconomica...")
        
        registros = [
            {'condicao_id': 1, 'nome_condicao': 'População ativa', 'categoria': 'Ativo'},
            {'condicao_id': 2, 'nome_condicao': 'População empregada', 'categoria': 'Ativo'},
            {'condicao_id': 3, 'nome_condicao': 'População desempregada', 'categoria': 'Ativo'},
            {'condicao_id': 4, 'nome_condicao': 'Estudantes', 'categoria': 'Inativo'},
            {'condicao_id': 5, 'nome_condicao': 'Reformados', 'categoria': 'Inativo'},
            {'condicao_id': 6, 'nome_condicao': 'Domésticos', 'categoria': 'Inativo'},
            {'condicao_id': 7, 'nome_condicao': 'Outros inativos', 'categoria': 'Inativo'}
        ]
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_CondicaoEconomica'] = df
        
        self.logger.sucesso(f"Dim_CondicaoEconomica criada: {len(df)} condições")
        return df
    
    def criar_dim_grupo_profissional(self):
        """
        Cria Dim_GrupoProfissional (CNP - Grandes Grupos)
        Estrutura:
          - grupo_prof_id (PK)
          - codigo_grande_grupo (0-9)
          - descricao
        """
        self.logger.info("Criando Dim_GrupoProfissional...")
        
        registros = []
        for codigo, descricao in self.config.GRANDES_GRUPOS_PROFISSIONAIS.items():
            registros.append({
                'grupo_prof_id': codigo + 1,  # PK começa em 1
                'codigo_grande_grupo': str(codigo),
                'descricao': descricao
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_GrupoProfissional'] = df
        
        self.logger.sucesso(f"Dim_GrupoProfissional criada: {len(df)} grupos")
        return df
    
    def criar_dim_profissao_digito1(self):
        """
        Cria Dim_ProfissaoDigito1 (1º dígito da profissão)
        Estrutura:
          - prof_digito1_id (PK)
          - codigo_digito1 (0-9)
          - descricao (mesmo do grande grupo)
        """
        self.logger.info("Criando Dim_ProfissaoDigito1...")
        
        # Reutilizar os mesmos grupos profissionais
        registros = []
        for codigo, descricao in self.config.GRANDES_GRUPOS_PROFISSIONAIS.items():
            registros.append({
                'prof_digito1_id': codigo + 1,
                'codigo_digito1': str(codigo),
                'descricao': descricao
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_ProfissaoDigito1'] = df
        
        self.logger.sucesso(f"Dim_ProfissaoDigito1 criada: {len(df)} categorias")
        return df
    
    def criar_dim_setor_economico(self):
        """
        Cria Dim_SetorEconomico (CAE Rev.3)
        Estrutura:
          - setor_id (PK)
          - codigo_cae (Secção A-U)
          - descricao
          - agregado (Primário/Secundário/Terciário)
        """
        self.logger.info("Criando Dim_SetorEconomico...")
        
        # Classificação em setores agregados
        agregados = {
            'A': 'Primário',
            'B': 'Secundário',
            'C': 'Secundário',
            'D': 'Secundário',
            'E': 'Secundário',
            'F': 'Secundário',
           'G': 'Terciário',
            'H': 'Terciário',
            'I': 'Terciário',
            'J': 'Terciário',
            'K': 'Terciário',
            'L': 'Terciário',
            'M': 'Terciário',
            'N': 'Terciário',
            'O': 'Terciário',
            'P': 'Terciário',
            'Q': 'Terciário',
            'R': 'Terciário',
            'S': 'Terciário',
            'T': 'Terciário',
            'U': 'Terciário'
        }
        
        registros = []
        for i, (codigo, descricao) in enumerate(self.config.SETORES_ECONOMICOS.items(), 1):
            registros.append({
                'setor_id': i,
                'codigo_cae': codigo,
                'descricao': descricao,
                'agregado': agregados.get(codigo, 'Terciário')
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_SetorEconomico'] = df
        
        self.logger.sucesso(f"Dim_SetorEconomico criada: {len(df)} setores")
        return df
    
    def criar_dim_situacao_profissional(self):
        """
        Cria Dim_SituacaoProfissional
        Estrutura:
          - situacao_id (PK)
          - nome_situacao
        """
        self.logger.info("Criando Dim_SituacaoProfissional...")
        
        registros = []
        for i, situacao in enumerate(self.config.SITUACOES_PROFISSIONAIS, 1):
            registros.append({
                'situacao_id': i,
                'nome_situacao': situacao
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_SituacaoProfissional'] = df
        
        self.logger.sucesso(f"Dim_SituacaoProfissional criada: {len(df)} situações")
        return df
    
    def criar_dim_fonte_rendimento(self):
        """
        Cria Dim_FonteRendimento
        Estrutura:
          - fonte_id (PK)
          - nome_fonte
        """
        self.logger.info("Criando Dim_FonteRendimento...")
        
        registros = []
        for i, fonte in enumerate(self.config.FONTES_RENDIMENTO, 1):
            registros.append({
                'fonte_id': i,
                'nome_fonte': fonte
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_FonteRendimento'] = df
        
        self.logger.sucesso(f"Dim_FonteRendimento criada: {len(df)} fontes")
        return df
    
    def criar_dim_regiao_nuts(self, dados_localidades=None):
        """
        Cria Dim_RegiaoNUTS (NUTS II e NUTS III)
        Estrutura:
          - nuts_id (PK)
          - codigo_nuts (PTXXX)
          - nome_regiao
          - localidade_id (FK - pode ser NULL)
        
        Parâmetros:
          dados_localidades: DataFrame Dim_Localidade do ETL base (opcional)
        """
        self.logger.info("Criando Dim_RegiaoNUTS...")
        
        registros = []
        pk_counter = 1
        
        # Adicionar NUTS II
        for codigo, info in self.constantes.REGIOES_NUTS_II.items():
            registros.append({
                'nuts_id': pk_counter,
                'codigo_nuts': codigo,
                'nome_regiao': info['nome'],
                'localidade_id': None  # Pode ser linkado depois
            })
            pk_counter += 1
        
        # Adicionar NUTS III (simplificado)
        for codigo, nome in self.constantes.REGIOES_NUTS_III.items():
            registros.append({
                'nuts_id': pk_counter,
                'codigo_nuts': codigo,
                'nome_regiao': nome,
                'localidade_id': None
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_RegiaoNUTS'] = df
        
        self.logger.sucesso(
            f"Dim_RegiaoNUTS criada: {len(df)} regiões "
            f"({len(self.constantes.REGIOES_NUTS_II)} NUTS II + "
            f"{len(self.constantes.REGIOES_NUTS_III)} NUTS III)"
        )
        
        return df
    
    def criar_todas_dimensoes(self):
        """Cria todas as 7 dimensões laborais"""
        self.logger.secao("CRIANDO DIMENSÕES LABORAIS")
        
        self.criar_dim_condicao_economica()
        self.criar_dim_grupo_profissional()
        self.criar_dim_profissao_digito1()
        self.criar_dim_setor_economico()
        self.criar_dim_situacao_profissional()
        self.criar_dim_fonte_rendimento()
        self.criar_dim_regiao_nuts()
        
        self.logger.sucesso(f"Total: {len(self.dimensoes)} dimensões laborais criadas")
        
        return self.dimensoes
    
    def obter_todas_dimensoes(self):
        """Retorna dict com todas as dimensões criadas"""
        return self.dimensoes
    
    def obter_dimensao(self, nome):
        """Retorna uma dimensão específica"""
        return self.dimensoes.get(nome)


# ============================================================
# CLASSE AUXILIAR DE LOOKUP DE IDs LABORAIS
# ============================================================

class LookupDimensoesLaborais:
    """Fornece métodos rápidos para lookup de IDs nas dimensões laborais"""
    
    def __init__(self, dimensoes_dict):
        self.dimensoes = dimensoes_dict
        self._indices = {}
        self._criar_indices()
    
    def _criar_indices(self):
        """Cria índices para lookup rápido"""
        # Índice para Condição Econômica
        if 'Dim_CondicaoEconomica' in self.dimensoes:
            df = self.dimensoes['Dim_CondicaoEconomica']
            self._indices['condicao'] = df.set_index('nome_condicao')['condicao_id'].to_dict()
        
        # Índice para Grupo Profissional
        if 'Dim_GrupoProfissional' in self.dimensoes:
            df = self.dimensoes['Dim_GrupoProfissional']
            self._indices['grupo_prof'] = df.set_index('codigo_grande_grupo')['grupo_prof_id'].to_dict()
        
        # Índice para Setor Econômico
        if 'Dim_SetorEconomico' in self.dimensoes:
            df = self.dimensoes['Dim_SetorEconomico']
            self._indices['setor'] = df.set_index('codigo_cae')['setor_id'].to_dict()
        
        # Índice para Situação Profissional
        if 'Dim_SituacaoProfissional' in self.dimensoes:
            df = self.dimensoes['Dim_SituacaoProfissional']
            self._indices['situacao'] = df.set_index('nome_situacao')['situacao_id'].to_dict()
        
        # Índice para Fonte de Rendimento
        if 'Dim_FonteRendimento' in self.dimensoes:
            df = self.dimensoes['Dim_FonteRendimento']
            self._indices['fonte'] = df.set_index('nome_fonte')['fonte_id'].to_dict()
        
        # Índice para Região NUTS
        if 'Dim_RegiaoNUTS' in self.dimensoes:
            df = self.dimensoes['Dim_RegiaoNUTS']
            self._indices['nuts'] = df.set_index('codigo_nuts')['nuts_id'].to_dict()
    
    def get_condicao_id(self, nome_condicao):
        """Retorna ID da condição econômica pelo nome"""
        return self._indices.get('condicao', {}).get(nome_condicao)
    
    def get_grupo_prof_id(self, codigo_grupo):
        """Retorna ID do grupo profissional pelo código"""
        return self._indices.get('grupo_prof', {}).get(str(codigo_grupo))
    
    def get_setor_id(self, codigo_cae):
        """Retorna ID do setor econômico pelo código CAE"""
        return self._indices.get('setor', {}).get(codigo_cae)
    
    def get_situacao_id(self, nome_situacao):
        """Retorna ID da situação profissional pelo nome"""
        return self._indices.get('situacao', {}).get(nome_situacao)
    
    def get_fonte_id(self, nome_fonte):
        """Retorna ID da fonte de rendimento pelo nome"""
        return self._indices.get('fonte', {}).get(nome_fonte)
    
    def get_nuts_id(self, codigo_nuts):
        """Retorna ID da região NUTS pelo código"""
        return self._indices.get('nuts', {}).get(codigo_nuts)


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Transformador de Dimensões Laborais...")
    
    class LoggerTeste:
        def info(self, msg): print(f"INFO: {msg}")
        def sucesso(self, msg): print(f"✓ {msg}")
        def secao(self, titulo): print(f"\n{'='*60}\n{titulo}\n{'='*60}")
    
    from parte_01_imports_config import Config, Constantes
    
    # Criar transformador
    transformador = TransformadorDimensoesLaborais(LoggerTeste(), Config(), Constantes())
    
    # Criar dimensões
    dimensoes = transformador.criar_todas_dimensoes()
    
    print(f"\nDimensões criadas: {list(dimensoes.keys())}")
    
    # Testar lookup
    lookup = LookupDimensoesLaborais(dimensoes)
    condicao_id = lookup.get_condicao_id('População empregada')
    print(f"ID de 'População empregada': {condicao_id}")
    
    setor_id = lookup.get_setor_id('C')
    print(f"ID do setor 'C' (Indústrias transformadoras): {setor_id}")
    
    print("\n✓ Módulo parte_03_transformador_dimensoes_laborais.py carregado com sucesso!")
