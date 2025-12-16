"""
============================================================
PARTE 2: CLASSES BASE E VALIDADORES
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
"""

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


# ============================================================
# CLASSE BASE PARA TABELAS
# ============================================================

class TabelaBase(ABC):
    """Classe abstrata base para todas as tabelas do modelo"""
    
    def __init__(self, nome_tabela, pk_nome='id'):
        self.nome_tabela = nome_tabela
        self.pk_nome = pk_nome
        self.df = pd.DataFrame()
        self.metadados = {}
        self.estatisticas = {}
    
    @abstractmethod
    def criar_estrutura(self):
        """Define a estrutura (colunas e tipos) da tabela"""
        pass
    
    @abstractmethod
    def validar(self):
        """Valida os dados da tabela"""
        pass
    
    def adicionar_registros(self, dados):
        """Adiciona registros à tabela"""
        if isinstance(dados, dict):
            self.df = pd.concat([self.df, pd.DataFrame([dados])], ignore_index=True)
        elif isinstance(dados, list):
            self.df = pd.concat([self.df, pd.DataFrame(dados)], ignore_index=True)
        elif isinstance(dados, pd.DataFrame):
            self.df = pd.concat([self.df, dados], ignore_index=True)
    
    def gerar_pk(self):
        """Gera valores para chave primária"""
        if self.pk_nome in self.df.columns:
            self.df[self.pk_nome] = range(1, len(self.df) + 1)
    
    def get_dataframe(self):
        """Retorna o DataFrame"""
        return self.df
    
    def contar_registros(self):
        """Retorna número de registros"""
        return len(self.df)
    
    def resumir(self):
        """Retorna resumo da tabela"""
        return {
            'nome': self.nome_tabela,
            'registros': self.contar_registros(),
            'colunas': list(self.df.columns),
            'memoria_mb': self.df.memory_usage(deep=True).sum() / 1024 / 1024
        }


# ============================================================
# CLASSE BASE PARA DIMENSÕES
# ============================================================

class DimensaoBase(TabelaBase):
    """Classe base para tabelas de dimensão"""
    
    def __init__(self, nome_tabela, pk_nome='id'):
        super().__init__(nome_tabela, pk_nome)
        self.tipo = 'DIMENSAO'
    
    def validar(self):
        """Validação padrão para dimensões"""
        erros = []
        
        # Verificar se há registros
        if len(self.df) == 0:
            erros.append(f"Dimensão {self.nome_tabela} está vazia")
        
        # Verificar duplicatas na PK
        if self.pk_nome in self.df.columns:
            if self.df[self.pk_nome].duplicated().any():
                erros.append(f"Chave primária {self.pk_nome} contém duplicatas")
        
        # Verificar valores nulos na PK
        if self.pk_nome in self.df.columns:
            if self.df[self.pk_nome].isna().any():
                erros.append(f"Chave primária {self.pk_nome} contém valores nulos")
        
        return erros


# ============================================================
# CLASSE BASE PARA FATOS
# ============================================================

class FatoBase(TabelaBase):
    """Classe base para tabelas de fato"""
    
    def __init__(self, nome_tabela, pk_nome='id', fks=None):
        super().__init__(nome_tabela, pk_nome)
        self.tipo = 'FATO'
        self.fks = fks or []  # Lista de tuplas (coluna_fk, tabela_dimensao)
    
    def validar(self):
        """Validação padrão para fatos"""
        erros = []
        
        # Verificar se há registros
        if len(self.df) == 0:
            erros.append(f"Fato {self.nome_tabela} está vazio")
        
        # Verificar duplicatas na PK
        if self.pk_nome in self.df.columns:
            if self.df[self.pk_nome].duplicated().any():
                erros.append(f"Chave primária {self.pk_nome} contém duplicatas")
        
        return erros
    
    def validar_fks(self, dimensoes_dict):
        """
        Valida integridade referencial das foreign keys
        dimensoes_dict: dicionário {nome_tabela: dataframe}
        """
        erros = []
        
        for coluna_fk, tabela_dim in self.fks:
            if coluna_fk not in self.df.columns:
                erros.append(f"FK {coluna_fk} não existe na tabela")
                continue
            
            if tabela_dim not in dimensoes_dict:
                erros.append(f"Dimensão {tabela_dim} não encontrada para validação")
                continue
            
            df_dim = dimensoes_dict[tabela_dim]
            pk_dim = df_dim.columns[0]  # Assume primeira coluna é PK
            
            # Valores inválidos (não existem na dimensão)
            valores_fk = self.df[coluna_fk].dropna().unique()
            valores_pk = df_dim[pk_dim].unique()
            valores_invalidos = set(valores_fk) - set(valores_pk)
            
            if valores_invalidos:
                erros.append(
                    f"FK {coluna_fk} contém {len(valores_invalidos)} valores "
                    f"que não existem em {tabela_dim}.{pk_dim}"
                )
        
        return erros


# ============================================================
# VALIDADORES DE DADOS
# ============================================================

class ValidadorDados:
    """Classe para validação de qualidade de dados"""
    
    @staticmethod
    def validar_tipos(df, schema):
        """
        Valida tipos de dados das colunas
        schema: dict {coluna: tipo_esperado}
        """
        erros = []
        
        for coluna, tipo_esperado in schema.items():
            if coluna not in df.columns:
                erros.append(f"Coluna {coluna} não existe")
                continue
            
            tipo_atual = df[coluna].dtype
            
            # Mapeamento de tipos pandas para tipos esperados
            mapeamento = {
                'int': ['int64', 'int32', 'int16', 'int8'],
                'float': ['float64', 'float32'],
                'str': ['object', 'string'],
                'datetime': ['datetime64[ns]']
            }
            
            tipos_validos = mapeamento.get(tipo_esperado, [tipo_esperado])
            
            if str(tipo_atual) not in tipos_validos:
                erros.append(
                    f"Coluna {coluna}: tipo {tipo_atual} diferente do esperado {tipo_esperado}"
                )
        
        return erros
    
    @staticmethod
    def validar_range(df, coluna, minimo=None, maximo=None):
        """Valida se valores estão dentro de um range"""
        erros = []
        
        if coluna not in df.columns:
            return [f"Coluna {coluna} não existe"]
        
        valores = df[coluna].dropna()
        
        if minimo is not None:
            valores_abaixo = (valores < minimo).sum()
            if valores_abaixo > 0:
                erros.append(
                    f"Coluna {coluna}: {valores_abaixo} valores abaixo do mínimo ({minimo})"
                )
        
        if maximo is not None:
            valores_acima = (valores > maximo).sum()
            if valores_acima > 0:
                erros.append(
                    f"Coluna {coluna}: {valores_acima} valores acima do máximo ({maximo})"
                )
        
        return erros
    
    @staticmethod
    def validar_uniqueness(df, colunas):
        """Valida se combinação de colunas é única"""
        if isinstance(colunas, str):
            colunas = [colunas]
        
        duplicatas = df.duplicated(subset=colunas, keep=False)
        num_duplicatas = duplicatas.sum()
        
        if num_duplicatas > 0:
            return [f"Encontradas {num_duplicatas} linhas duplicadas em {colunas}"]
        
        return []
    
    @staticmethod
    def validar_completude(df, coluna, minimo_preenchimento=0.95):
        """Valida se coluna tem preenchimento mínimo"""
        if coluna not in df.columns:
            return [f"Coluna {coluna} não existe"]
        
        total = len(df)
        preenchidos = df[coluna].notna().sum()
        taxa_preenchimento = preenchidos / total if total > 0 else 0
        
        if taxa_preenchimento < minimo_preenchimento:
            return [
                f"Coluna {coluna}: apenas {taxa_preenchimento:.1%} preenchido "
                f"(mínimo: {minimo_preenchimento:.1%})"
            ]
        
        return []
    
    @staticmethod
    def validar_valores_permitidos(df, coluna, valores_permitidos):
        """Valida se apenas valores permitidos estão presentes"""
        if coluna not in df.columns:
            return [f"Coluna {coluna} não existe"]
        
        valores_unicos = df[coluna].dropna().unique()
        valores_invalidos = set(valores_unicos) - set(valores_permitidos)
        
        if valores_invalidos:
            return [
                f"Coluna {coluna}: valores inválidos encontrados: {valores_invalidos}"
            ]
        
        return []


# ============================================================
# GERENCIADOR DE INTEGRIDADE REFERENCIAL
# ============================================================

class GerenciadorIntegridade:
    """Gerencia validações de integridade referencial entre tabelas"""
    
    def __init__(self):
        self.tabelas = {}
        self.relacionamentos = []
        self.erros = []
    
    def adicionar_tabela(self, nome, dataframe):
        """Adiciona tabela para validação"""
        self.tabelas[nome] = dataframe
    
    def adicionar_relacionamento(self, tabela_fato, coluna_fk, tabela_dim, coluna_pk):
        """Define relacionamento FK -> PK"""
        self.relacionamentos.append({
            'fato': tabela_fato,
            'fk': coluna_fk,
            'dimensao': tabela_dim,
            'pk': coluna_pk
        })
    
    def validar_todos(self):
        """Executa todas as validações de integridade"""
        self.erros = []
        
        for rel in self.relacionamentos:
            erros_rel = self._validar_relacionamento(rel)
            self.erros.extend(erros_rel)
        
        return self.erros
    
    def _validar_relacionamento(self, rel):
        """Valida um relacionamento específico"""
        erros = []
        
        # Verificar se tabelas existem
        if rel['fato'] not in self.tabelas:
            return [f"Tabela de fato {rel['fato']} não encontrada"]
        
        if rel['dimensao'] not in self.tabelas:
            return [f"Tabela de dimensão {rel['dimensao']} não encontrada"]
        
        df_fato = self.tabelas[rel['fato']]
        df_dim = self.tabelas[rel['dimensao']]
        
        # Verificar se colunas existem
        if rel['fk'] not in df_fato.columns:
            return [f"FK {rel['fk']} não existe em {rel['fato']}"]
        
        if rel['pk'] not in df_dim.columns:
            return [f"PK {rel['pk']} não existe em {rel['dimensao']}"]
        
        # Validar integridade referencial
        valores_fk = df_fato[rel['fk']].dropna().unique()
        valores_pk = df_dim[rel['pk']].unique()
        valores_orfaos = set(valores_fk) - set(valores_pk)
        
        if valores_orfaos:
            erros.append(
                f"ERRO DE INTEGRIDADE: {rel['fato']}.{rel['fk']} -> "
                f"{rel['dimensao']}.{rel['pk']}: "
                f"{len(valores_orfaos)} valores órfãos encontrados"
            )
        
        return erros
    
    def gerar_relatorio(self):
        """Gera relatório de validação"""
        print("\n" + "=" * 60)
        print("RELATÓRIO DE INTEGRIDADE REFERENCIAL")
        print("=" * 60)
        
        print(f"\nTabelas carregadas: {len(self.tabelas)}")
        print(f"Relacionamentos definidos: {len(self.relacionamentos)}")
        print(f"Erros encontrados: {len(self.erros)}")
        
        if self.erros:
            print("\nERROS DETALHADOS:")
            for i, erro in enumerate(self.erros, 1):
                print(f"  {i}. {erro}")
        else:
            print("\n✓ Todas as validações de integridade passaram!")
        
        print("=" * 60)


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando classes base e validadores...")
    
    # Teste de validador de dados
    df_teste = pd.DataFrame({
        'id': [1, 2, 3],
        'nome': ['A', 'B', 'C'],
        'valor': [10, 20, 30]
    })
    
    validador = ValidadorDados()
    erros = validador.validar_range(df_teste, 'valor', minimo=0, maximo=100)
    print(f"Validação de range: {len(erros)} erros")
    
    # Teste de gerenciador de integridade
    gerenciador = GerenciadorIntegridade()
    gerenciador.adicionar_tabela('dimensao', pd.DataFrame({'id': [1, 2, 3]}))
    gerenciador.adicionar_tabela('fato', pd.DataFrame({'dim_id': [1, 2, 3]}))
    gerenciador.adicionar_relacionamento('fato', 'dim_id', 'dimensao', 'id')
    
    erros_integridade = gerenciador.validar_todos()
    print(f"Validação de integridade: {len(erros_integridade)} erros")
    
    print("\n✓ Módulo parte_02_classes_base.py carregado com sucesso!")
