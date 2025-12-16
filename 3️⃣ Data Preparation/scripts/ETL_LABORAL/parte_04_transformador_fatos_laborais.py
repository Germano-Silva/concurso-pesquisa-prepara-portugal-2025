"""
============================================================
PARTE 4: TRANSFORMADOR DE FATOS LABORAIS
Pipeline ETL - Laboral (DP-01-B)
Google Colab
============================================================
Cria 8 tabelas de fatos laborais:
- Fact_PopulacaoPorCondicao
- Fact_EmpregadosPorProfissao
- Fact_EmpregadosPorSetor
- Fact_EmpregadosPorSituacao
- Fact_EmpregadosProfSexo
- Fact_EmpregadosRegiaoSetor
- Fact_PopulacaoTrabalhoEscolaridade
- Fact_PopulacaoRendimentoRegiao
"""

import pandas as pd
import numpy as np


# ============================================================
# CLASSE TRANSFORMADOR DE FATOS LABORAIS
# ============================================================

class TransformadorFatosLaborais:
    """Transforma dados brutos em tabelas de fatos laborais"""
    
    def __init__(self, logger, lookup_laborais, lookup_base=None):
        self.logger = logger
        self.lookup = lookup_laborais
        self.lookup_base = lookup_base  # Lookup do ETL_EDUCACAO (Nacionalidade, Sexo, etc.)
        self.fatos = {}
    
    def criar_fact_populacao_por_condicao(self, dados_condicao):
        """
        Cria Fact_PopulacaoPorCondicao
        População por condição perante o trabalho
        
        Estrutura:
          - populacao_cond_id (PK)
          - populacao_id (FK - ano)
          - nacionalidade_id (FK)
          - condicao_id (FK)
          - quantidade
          - percentual
        """
        self.logger.info("Criando Fact_PopulacaoPorCondicao...")
        
        registros = []
        pk_counter = 1
        
        # Agrupar por nacionalidade para calcular percentuais
        grupos_nac = {}
        for dados in dados_condicao:
            nac = dados.get('nacionalidade')
            if nac not in grupos_nac:
                grupos_nac[nac] = []
            grupos_nac[nac].append(dados)
        
        for nacionalidade, items in grupos_nac.items():
            total_nac = sum(i.get('quantidade', 0) for i in items)
            
            nac_id = self.lookup_base.get_nacionalidade_id(nacionalidade) if self.lookup_base else None
            
            for item in items:
                condicao = item.get('condicao', 'População ativa')
                quantidade = item.get('quantidade', 0)
                ano = item.get('ano', 2011)
                
                condicao_id = self.lookup.get_condicao_id(condicao)
                ano_id = self.lookup_base.get_ano_id(ano) if self.lookup_base else 1
                
                percentual = (quantidade / total_nac * 100) if total_nac > 0 else 0
                
                registros.append({
                    'populacao_cond_id': pk_counter,
                    'populacao_id': ano_id,
                    'nacionalidade_id': nac_id,
                    'condicao_id': condicao_id,
                    'quantidade': quantidade,
                    'percentual': round(percentual, 2)
                })
                pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoPorCondicao'] = df
        
        self.logger.sucesso(f"Fact_PopulacaoPorCondicao criada: {len(df)} registros")
        return df
    
    def criar_fact_empregados_por_profissao(self, dados_profissao):
        """
        Cria Fact_EmpregadosPorProfissao
        Empregados por grande grupo profissional
        
        Estrutura:
          - emp_prof_id (PK)
          - nacionalidade_id (FK)
          - grupo_prof_id (FK)
          - quantidade
        """
        self.logger.info("Criando Fact_EmpregadosPorProfissao...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_profissao:
            nacionalidade = dados.get('nacionalidade')
            codigo_profissao = dados.get('codigo_profissao', '0')
            quantidade = dados.get('quantidade', 0)
            
            nac_id = self.lookup_base.get_nacionalidade_id(nacionalidade) if self.lookup_base else None
            grupo_prof_id = self.lookup.get_grupo_prof_id(codigo_profissao)
            
            registros.append({
                'emp_prof_id': pk_counter,
                'nacionalidade_id': nac_id,
                'grupo_prof_id': grupo_prof_id,
                'quantidade': quantidade
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EmpregadosPorProfissao'] = df
        
        self.logger.sucesso(f"Fact_EmpregadosPorProfissao criada: {len(df)} registros")
        return df
    
    def criar_fact_empregados_por_setor(self, dados_setor):
        """
        Cria Fact_EmpregadosPorSetor
        Empregados por setor econômico (CAE Rev.3)
        
        Estrutura:
          - emp_setor_id (PK)
          - nacionalidade_id (FK)
          - setor_id (FK)
          - quantidade
        """
        self.logger.info("Criando Fact_EmpregadosPorSetor...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_setor:
            nacionalidade = dados.get('nacionalidade')
            codigo_cae = dados.get('codigo_cae', 'C')
            quantidade = dados.get('quantidade', 0)
            
            nac_id = self.lookup_base.get_nacionalidade_id(nacionalidade) if self.lookup_base else None
            setor_id = self.lookup.get_setor_id(codigo_cae)
            
            registros.append({
                'emp_setor_id': pk_counter,
                'nacionalidade_id': nac_id,
                'setor_id': setor_id,
                'quantidade': quantidade
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EmpregadosPorSetor'] = df
        
        self.logger.sucesso(f"Fact_EmpregadosPorSetor criada: {len(df)} registros")
        return df
    
    def criar_fact_empregados_por_situacao(self, dados_situacao):
        """
        Cria Fact_EmpregadosPorSituacao
        Empregados por situação profissional
        
        Estrutura:
          - emp_situacao_id (PK)
          - nacionalidade_id (FK)
          - situacao_id (FK)
          - quantidade
        """
        self.logger.info("Criando Fact_EmpregadosPorSituacao...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_situacao:
            nacionalidade = dados.get('nacionalidade')
            situacao = dados.get('situacao', 'Trabalhador por conta de outrem')
            quantidade = dados.get('quantidade', 0)
            
            nac_id = self.lookup_base.get_nacionalidade_id(nacionalidade) if self.lookup_base else None
            situacao_id = self.lookup.get_situacao_id(situacao)
            
            registros.append({
                'emp_situacao_id': pk_counter,
                'nacionalidade_id': nac_id,
                'situacao_id': situacao_id,
                'quantidade': quantidade
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EmpregadosPorSituacao'] = df
        
        self.logger.sucesso(f"Fact_EmpregadosPorSituacao criada: {len(df)} registros")
        return df
    
    def criar_fact_empregados_prof_sexo(self, dados_prof_sexo):
        """
        Cria Fact_EmpregadosProfSexo
        Empregados por profissão e sexo
        
        Estrutura:
          - emp_prof_sexo_id (PK)
          - prof_digito1_id (FK)
          - sexo_id (FK)
          - quantidade_homens
          - quantidade_mulheres
        """
        self.logger.info("Criando Fact_EmpregadosProfSexo...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_prof_sexo:
            codigo_prof = dados.get('codigo_profissao', '0')
            quantidade_h = dados.get('quantidade_homens', 0)
            quantidade_m = dados.get('quantidade_mulheres', 0)
            
            prof_id = self.lookup.get_grupo_prof_id(codigo_prof)
            
            registros.append({
                'emp_prof_sexo_id': pk_counter,
                'prof_digito1_id': prof_id,
                'sexo_id': 1,  # Masculino
                'quantidade_homens': quantidade_h,
                'quantidade_mulheres': 0
            })
            pk_counter += 1
            
            registros.append({
                'emp_prof_sexo_id': pk_counter,
                'prof_digito1_id': prof_id,
                'sexo_id': 2,  # Feminino
                'quantidade_homens': 0,
                'quantidade_mulheres': quantidade_m
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EmpregadosProfSexo'] = df
        
        self.logger.sucesso(f"Fact_EmpregadosProfSexo criada: {len(df)} registros")
        return df
    
    def criar_fact_empregados_regiao_setor(self, dados_regiao_setor):
        """
        Cria Fact_EmpregadosRegiaoSetor
        Empregados por região NUTS e setor econômico
        
        Estrutura:
          - emp_regiao_setor_id (PK)
          - nuts_id (FK)
          - setor_id (FK)
          - quantidade
        """
        self.logger.info("Criando Fact_EmpregadosRegiaoSetor...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_regiao_setor:
            codigo_nuts = dados.get('codigo_nuts', 'PT17')
            codigo_cae = dados.get('codigo_cae', 'C')
            quantidade = dados.get('quantidade', 0)
            
            nuts_id = self.lookup.get_nuts_id(codigo_nuts)
            setor_id = self.lookup.get_setor_id(codigo_cae)
            
            registros.append({
                'emp_regiao_setor_id': pk_counter,
                'nuts_id': nuts_id,
                'setor_id': setor_id,
                'quantidade': quantidade
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EmpregadosRegiaoSetor'] = df
        
        self.logger.sucesso(f"Fact_EmpregadosRegiaoSetor criada: {len(df)} registros")
        return df
    
    def criar_fact_populacao_trabalho_escolaridade(self, dados_trab_esc):
        """
        Cria Fact_PopulacaoTrabalhoEscolaridade
        População por condição de trabalho e nível de escolaridade
        
        Estrutura:
          - pop_trab_esc_id (PK)
          - nivel_educacao_id (FK)
          - sexo_id (FK)
          - condicao_trabalho
          - quantidade_hm
          - quantidade_h
          - quantidade_m
        """
        self.logger.info("Criando Fact_PopulacaoTrabalhoEscolaridade...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_trab_esc:
            nivel_educacao = dados.get('nivel_educacao', 'Superior')
            condicao = dados.get('condicao_trabalho', 'Empregado')
            qtd_hm = dados.get('quantidade_hm', 0)
            qtd_h = dados.get('quantidade_h', 0)
            qtd_m = dados.get('quantidade_m', 0)
            
            # Mapear nível de educação (simplificado)
            nivel_map = {
                'Inferior ao básico 3º ciclo': 1,
                'Básico 3º ciclo': 2,
                'Secundário e pós-secundário': 3,
                'Superior': 4
            }
            nivel_id = nivel_map.get(nivel_educacao, 4)
            
            registros.append({
                'pop_trab_esc_id': pk_counter,
                'nivel_educacao_id': nivel_id,
                'sexo_id': 1,  # Ambos
                'condicao_trabalho': condicao,
                'quantidade_hm': qtd_hm,
                'quantidade_h': qtd_h,
                'quantidade_m': qtd_m
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoTrabalhoEscolaridade'] = df
        
        self.logger.sucesso(f"Fact_PopulacaoTrabalhoEscolaridade criada: {len(df)} registros")
        return df
    
    def criar_fact_populacao_rendimento_regiao(self, dados_rend_regiao):
        """
        Cria Fact_PopulacaoRendimentoRegiao
        População por fonte de rendimento e região NUTS
        
        Estrutura:
          - pop_rend_reg_id (PK)
          - nuts_id (FK)
          - fonte_id (FK)
          - quantidade
        """
        self.logger.info("Criando Fact_PopulacaoRendimentoRegiao...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_rend_regiao:
            codigo_nuts = dados.get('codigo_nuts', 'PT17')
            fonte_rendimento = dados.get('fonte_rendimento', 'Trabalho por conta de outrem')
            quantidade = dados.get('quantidade', 0)
            
            nuts_id = self.lookup.get_nuts_id(codigo_nuts)
            fonte_id = self.lookup.get_fonte_id(fonte_rendimento)
            
            registros.append({
                'pop_rend_reg_id': pk_counter,
                'nuts_id': nuts_id,
                'fonte_id': fonte_id,
                'quantidade': quantidade
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoRendimentoRegiao'] = df
        
        self.logger.sucesso(f"Fact_PopulacaoRendimentoRegiao criada: {len(df)} registros")
        return df
    
    def obter_todos_fatos(self):
        """Retorna dict com todos os fatos criados"""
        return self.fatos
    
    def obter_fato(self, nome):
        """Retorna um fato específico"""
        return self.fatos.get(nome)


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Transformador de Fatos Laborais...")
    
    class LoggerTeste:
        def info(self, msg): print(f"INFO: {msg}")
        def sucesso(self, msg): print(f"✓ {msg}")
    
    from parte_03_transformador_dimensoes_laborais import LookupDimensoesLaborais
    
    # Dados simulados
    dimensoes_teste = {
        'Dim_CondicaoEconomica': pd.DataFrame({
            'condicao_id': [1, 2],
            'nome_condicao': ['População ativa', 'População empregada']
        }),
        'Dim_GrupoProfissional': pd.DataFrame({
            'grupo_prof_id': [1],
            'codigo_grande_grupo': ['0']
        })
    }
    
    lookup = LookupDimensoesLaborais(dimensoes_teste)
    transformador = TransformadorFatosLaborais(LoggerTeste(), lookup)
    
    # Testar criação de um fato
    dados_teste = [
        {'nacionalidade': 'Brasil', 'condicao': 'População empregada', 'quantidade': 1000, 'ano': 2011}
    ]
    
    fact = transformador.criar_fact_populacao_por_condicao(dados_teste)
    print(f"\nFact_PopulacaoPorCondicao: {len(fact)} registros")
    
    print("\n✓ Módulo parte_04_transformador_fatos_laborais.py carregado com sucesso!")
