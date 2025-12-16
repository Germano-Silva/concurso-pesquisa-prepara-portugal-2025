"""
============================================================
PARTE 2: CLASSES BASE (REFERÊNCIA)
Pipeline ETL - Laboral (DP-01-B)
Google Colab
============================================================
Este módulo reutiliza as classes base do ETL_EDUCACAO.
Para usar, certifique-se de ter o módulo ETL_EDUCACAO disponível.
============================================================
"""

# Importar classes base do ETL Educação
# No Google Colab, garanta que parte_02_classes_base.py do ETL_EDUCACAO esteja disponível

try:
    import sys
    sys.path.append('../ETL_EDUCACAO')
    
    from parte_02_classes_base import (
        TabelaBase,
        DimensaoBase,
        FatoBase,
        ValidadorDados,
        GerenciadorIntegridade
    )
    
    print("✓ Classes base importadas do ETL_EDUCACAO com sucesso!")
    
except ImportError:
    print("⚠ AVISO: Não foi possível importar do ETL_EDUCACAO")
    print("Copiando definições das classes base localmente...")
    
    # Fallback: Redefinir classes base localmente
    import pandas as pd
    from abc import ABC, abstractmethod
    
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
    
    
    class DimensaoBase(TabelaBase):
        """Classe base para tabelas de dimensão"""
        
        def __init__(self, nome_tabela, pk_nome='id'):
            super().__init__(nome_tabela, pk_nome)
            self.tipo = 'DIMENSAO'
        
        def validar(self):
            """Validação padrão para dimensões"""
            erros = []
            
            if len(self.df) == 0:
                erros.append(f"Dimensão {self.nome_tabela} está vazia")
            
            if self.pk_nome in self.df.columns:
                if self.df[self.pk_nome].duplicated().any():
                    erros.append(f"Chave primária {self.pk_nome} contém duplicatas")
            
            if self.pk_nome in self.df.columns:
                if self.df[self.pk_nome].isna().any():
                    erros.append(f"Chave primária {self.pk_nome} contém valores nulos")
            
            return erros
    
    
    class FatoBase(TabelaBase):
        """Classe base para tabelas de fato"""
        
        def __init__(self, nome_tabela, pk_nome='id', fks=None):
            super().__init__(nome_tabela, pk_nome)
            self.tipo = 'FATO'
            self.fks = fks or []
        
        def validar(self):
            """Validação padrão para fatos"""
            erros = []
            
            if len(self.df) == 0:
                erros.append(f"Fato {self.nome_tabela} está vazio")
            
            if self.pk_nome in self.df.columns:
                if self.df[self.pk_nome].duplicated().any():
                    erros.append(f"Chave primária {self.pk_nome} contém duplicatas")
            
            return erros
    
    
    class ValidadorDados:
        """Classe para validação de qualidade de dados"""
        
        @staticmethod
        def validar_tipos(df, schema):
            """Valida tipos de dados das colunas"""
            erros = []
            
            mapeamento = {
                'int': ['int64', 'int32', 'int16', 'int8'],
                'float': ['float64', 'float32'],
                'str': ['object', 'string'],
                'datetime': ['datetime64[ns]']
            }
            
            for coluna, tipo_esperado in schema.items():
                if coluna not in df.columns:
                    erros.append(f"Coluna {coluna} não existe")
                    continue
                
                tipo_atual = df[coluna].dtype
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
            
            if rel['fato'] not in self.tabelas:
                return [f"Tabela de fato {rel['fato']} não encontrada"]
            
            if rel['dimensao'] not in self.tabelas:
                return [f"Tabela de dimensão {rel['dimensao']} não encontrada"]
            
            df_fato = self.tabelas[rel['fato']]
            df_dim = self.tabelas[rel['dimensao']]
            
            if rel['fk'] not in df_fato.columns:
                return [f"FK {rel['fk']} não existe em {rel['fato']}"]
            
            if rel['pk'] not in df_dim.columns:
                return [f"PK {rel['pk']} não existe em {rel['dimensao']}"]
            
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


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando classes base para ETL Laboral...")
    
    # Testar criação de dimensão
    class TesteDimensao(DimensaoBase):
        def criar_estrutura(self):
            self.df = pd.DataFrame({'id': [1, 2], 'nome': ['A', 'B']})
        
        def validar(self):
            return super().validar()
    
    dim = TesteDimensao('Dim_Teste')
    dim.criar_estrutura()
    erros = dim.validar()
    print(f"Dimensão criada: {dim.contar_registros()} registros, {len(erros)} erros")
    
    print("\n✓ Módulo parte_02_classes_base_ref.py carregado com sucesso!")
