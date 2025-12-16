"""
============================================================
PARTE 6: TRANSFORMADOR DE FATOS BASE
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
Cria tabelas de fatos relacionadas à população base:
- Fact_PopulacaoPorNacionalidadeSexo
- Fact_PopulacaoPorGrupoEtario
- Fact_PopulacaoPorLocalidade
- Fact_PopulacaoPorLocalidadeNacionalidade
- Fact_NacionalidadePrincipal
- Fact_DistribuicaoGeografica
"""

import pandas as pd
import numpy as np


# ============================================================
# CLASSE TRANSFORMADOR DE FATOS BASE
# ============================================================

class TransformadorFatosBase:
    """Transforma dados brutos em tabelas de fatos base"""
    
    def __init__(self, logger, lookup_dimensoes):
        self.logger = logger
        self.lookup = lookup_dimensoes
        self.fatos = {}
    
    def criar_fact_populacao_por_nacionalidade_sexo(self, dados_agregados):
        """
        Cria Fact_PopulacaoPorNacionalidadeSexo
        
        Estrutura:
          - populacao_nacional_sexo_id (PK)
          - nacionalidade_id (FK)
          - sexo_id (FK)
          - populacao_id (FK - ano)
          - populacao_masculino
          - populacao_feminino
          - percentagem_masculino
          - percentagem_feminino
        
        Parâmetros:
          dados_agregados: lista de dicts com {nacionalidade, sexo, populacao, ano}
        """
        self.logger.info("Criando Fact_PopulacaoPorNacionalidadeSexo...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_agregados:
            nacionalidade = dados.get('nacionalidade')
            ano = dados.get('ano', 2011)
            pop_masculino = dados.get('populacao_masculino', 0)
            pop_feminino = dados.get('populacao_feminino', 0)
            
            # Lookup de IDs
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            ano_id = self.lookup.get_ano_id(ano)
            
            if not nac_id:
                self.logger.aviso(f"Nacionalidade não encontrada: {nacionalidade}")
                continue
            
            # Cálculos percentuais
            total_pop = pop_masculino + pop_feminino
            perc_masculino = (pop_masculino / total_pop * 100) if total_pop > 0 else 0
            perc_feminino = (pop_feminino / total_pop * 100) if total_pop > 0 else 0
            
            # Criar registros separados por sexo
            # Registro Masculino
            registros.append({
                'populacao_nacional_sexo_id': pk_counter,
                'nacionalidade_id': nac_id,
                'sexo_id': 1,  # Masculino
                'populacao_id': ano_id,
                'populacao_masculino': pop_masculino,
                'populacao_feminino': 0,
                'percentagem_masculino': round(perc_masculino, 2),
                'percentagem_feminino': 0.0
            })
            pk_counter += 1
            
            # Registro Feminino
            registros.append({
                'populacao_nacional_sexo_id': pk_counter,
                'nacionalidade_id': nac_id,
                'sexo_id': 2,  # Feminino
                'populacao_id': ano_id,
                'populacao_masculino': 0,
                'populacao_feminino': pop_feminino,
                'percentagem_masculino': 0.0,
                'percentagem_feminino': round(perc_feminino, 2)
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoPorNacionalidadeSexo'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoPorNacionalidadeSexo criada: {len(df)} registros"
        )
        
        return df
    
    def criar_fact_populacao_por_grupo_etario(self, dados_grupos_etarios):
        """
        Cria Fact_PopulacaoPorGrupoEtario
        
        Estrutura:
          - populacao_grupoetario_id (PK)
          - populacao_id (FK - ano)
          - grupoetario_id (FK)
          - nacionalidade_id (FK)
          - populacao_grupo
          - percentagem_grupo
          - idade_media
        
        Parâmetros:
          dados_grupos_etarios: lista de dicts com {nacionalidade, grupo_etario, populacao, idade_media}
        """
        self.logger.info("Criando Fact_PopulacaoPorGrupoEtario...")
        
        registros = []
        pk_counter = 1
        
        # Agrupar por nacionalidade para calcular percentuais
        grupos_por_nacionalidade = {}
        for dados in dados_grupos_etarios:
            nac = dados.get('nacionalidade')
            if nac not in grupos_por_nacionalidade:
                grupos_por_nacionalidade[nac] = []
            grupos_por_nacionalidade[nac].append(dados)
        
        for nacionalidade, grupos in grupos_por_nacionalidade.items():
            # Calcular total da nacionalidade
            total_nacionalidade = sum(g.get('populacao', 0) for g in grupos)
            
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            if not nac_id:
                self.logger.aviso(f"Nacionalidade não encontrada: {nacionalidade}")
                continue
            
            for grupo in grupos:
                faixa_etaria = grupo.get('faixa_etaria', '15-64 anos')
                populacao = grupo.get('populacao', 0)
                idade_media = grupo.get('idade_media', None)
                ano = grupo.get('ano', 2011)
                
                # Mapear faixa etária para grupo_etario_id
                grupo_id = self._mapear_grupo_etario(faixa_etaria)
                ano_id = self.lookup.get_ano_id(ano)
                
                # Calcular percentual
                percentagem = (populacao / total_nacionalidade * 100) if total_nacionalidade > 0 else 0
                
                registros.append({
                    'populacao_grupoetario_id': pk_counter,
                    'populacao_id': ano_id,
                    'grupoetario_id': grupo_id,
                    'nacionalidade_id': nac_id,
                    'populacao_grupo': populacao,
                    'percentagem_grupo': round(percentagem, 2),
                    'idade_media': round(idade_media, 1) if idade_media else None
                })
                pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoPorGrupoEtario'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoPorGrupoEtario criada: {len(df)} registros"
        )
        
        return df
    
    def criar_fact_populacao_por_localidade(self, dados_municipios):
        """
        Cria Fact_PopulacaoPorLocalidade
        
        Estrutura:
          - populacao_local_id (PK)
          - localidade_id (FK)
          - populacao_id (FK - ano)
          - populacao_total
          - populacao_portuguesa
          - populacao_estrangeira
          - apatridas
        
        Parâmetros:
          dados_municipios: lista de dicts com {municipio, populacao_total, portuguesa, estrangeira}
        """
        self.logger.info("Criando Fact_PopulacaoPorLocalidade...")
        
        registros = []
        pk_counter = 1
        
        # Agrupar por município
        municipios_agrupados = {}
        for dados in dados_municipios:
            municipio = dados.get('municipio')
            if not municipio:
                continue
            
            if municipio not in municipios_agrupados:
                municipios_agrupados[municipio] = {
                    'total': 0,
                    'portuguesa': 0,
                    'estrangeira': 0,
                    'apatridas': 0,
                    'ano': dados.get('ano', 2011)
                }
            
            # Acumular dados
            municipios_agrupados[municipio]['total'] += dados.get('populacao_total', 0)
            municipios_agrupados[municipio]['portuguesa'] += dados.get('populacao_portuguesa', 0)
            municipios_agrupados[municipio]['estrangeira'] += dados.get('populacao_estrangeira', 0)
            municipios_agrupados[municipio]['apatridas'] += dados.get('apatridas', 0)
        
        # Criar registros
        for municipio, totais in municipios_agrupados.items():
            localidade_id = self.lookup.get_localidade_id(municipio)
            if not localidade_id:
                self.logger.aviso(f"Localidade não encontrada: {municipio}")
                continue
            
            ano_id = self.lookup.get_ano_id(totais['ano'])
            
            registros.append({
                'populacao_local_id': pk_counter,
                'localidade_id': localidade_id,
                'populacao_id': ano_id,
                'populacao_total': totais['total'],
                'populacao_portuguesa': totais['portuguesa'],
                'populacao_estrangeira': totais['estrangeira'],
                'apatridas': totais['apatridas']
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoPorLocalidade'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoPorLocalidade criada: {len(df)} municípios"
        )
        
        return df
    
    def criar_fact_populacao_por_localidade_nacionalidade(
        self, 
        dados_loc_nac,
        fact_localidade_df
    ):
        """
        Cria Fact_PopulacaoPorLocalidadeNacionalidade
        
        Estrutura:
          - populacao_local_nacional_id (PK)
          - populacao_local_id (FK para Fact_PopulacaoPorLocalidade)
          - nacionalidade_id (FK)
          - populacao_nacional
        
        Parâmetros:
          dados_loc_nac: lista de dicts com {municipio, nacionalidade, populacao}
          fact_localidade_df: DataFrame da Fact_PopulacaoPorLocalidade para lookup
        """
        self.logger.info("Criando Fact_PopulacaoPorLocalidadeNacionalidade...")
        
        registros = []
        pk_counter = 1
        
        # Criar índice de localidade_id -> populacao_local_id
        localidade_to_pop_local = {}
        if not fact_localidade_df.empty:
            for _, row in fact_localidade_df.iterrows():
                localidade_to_pop_local[row['localidade_id']] = row['populacao_local_id']
        
        for dados in dados_loc_nac:
            municipio = dados.get('municipio')
            nacionalidade = dados.get('nacionalidade')
            populacao = dados.get('populacao', 0)
            
            # Lookup de IDs
            localidade_id = self.lookup.get_localidade_id(municipio)
            nacionalidade_id = self.lookup.get_nacionalidade_id(nacionalidade)
            
            if not localidade_id or not nacionalidade_id:
                continue
            
            # Obter populacao_local_id
            populacao_local_id = localidade_to_pop_local.get(localidade_id)
            if not populacao_local_id:
                continue
            
            registros.append({
                'populacao_local_nacional_id': pk_counter,
                'populacao_local_id': populacao_local_id,
                'nacionalidade_id': nacionalidade_id,
                'populacao_nacional': populacao
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoPorLocalidadeNacionalidade'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoPorLocalidadeNacionalidade criada: {len(df)} registros"
        )
        
        return df
    
    def criar_fact_nacionalidade_principal(self, dados_ranking):
        """
        Cria Fact_NacionalidadePrincipal
        Ranking das principais nacionalidades estrangeiras
        
        Estrutura:
          - nacionalidade_principal_id (PK)
          - nacionalidade_id (FK)
          - posicao_ranking
          - populacao_2021
          - populacao_2011
          - percentagem_variacao
        
        Parâmetros:
          dados_ranking: lista de dicts com {nacionalidade, populacao_2011, populacao_2021, ranking}
        """
        self.logger.info("Criando Fact_NacionalidadePrincipal...")
        
        registros = []
        pk_counter = 1
        
        # Ordenar por posição no ranking
        dados_ordenados = sorted(dados_ranking, key=lambda x: x.get('ranking', 999))
        
        for dados in dados_ordenados:
            nacionalidade = dados.get('nacionalidade')
            pop_2011 = dados.get('populacao_2011', 0)
            pop_2021 = dados.get('populacao_2021', 0)
            ranking = dados.get('ranking', pk_counter)
            
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            if not nac_id:
                self.logger.aviso(f"Nacionalidade não encontrada: {nacionalidade}")
                continue
            
            # Calcular variação percentual
            if pop_2011 > 0:
                variacao = ((pop_2021 - pop_2011) / pop_2011) * 100
            else:
                variacao = 0.0
            
            registros.append({
                'nacionalidade_principal_id': pk_counter,
                'nacionalidade_id': nac_id,
                'posicao_ranking': ranking,
                'populacao_2021': pop_2021,
                'populacao_2011': pop_2011,
                'percentagem_variacao': round(variacao, 2)
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_NacionalidadePrincipal'] = df
        
        self.logger.sucesso(
            f"Fact_NacionalidadePrincipal criada: Top {len(df)} nacionalidades"
        )
        
        return df
    
    def criar_fact_distribuicao_geografica(self, dados_distribuicao):
        """
        Cria Fact_DistribuicaoGeografica
        Distribuição geográfica de nacionalidades por localidade
        
        Estrutura:
          - distribuicao_geo_id (PK)
          - localidade_id (FK)
          - nacionalidade_id (FK)
          - populacao_nacional_local
          - concentracao_relativa (% da nacionalidade naquela localidade)
          - dominio_regional (Alta/Média/Baixa concentração)
        
        Parâmetros:
          dados_distribuicao: lista de dicts com {municipio, nacionalidade, populacao}
        """
        self.logger.info("Criando Fact_DistribuicaoGeografica...")
        
        registros = []
        pk_counter = 1
        
        # Calcular total por nacionalidade para concentração relativa
        totais_por_nacionalidade = {}
        for dados in dados_distribuicao:
            nac = dados.get('nacionalidade')
            pop = dados.get('populacao', 0)
            totais_por_nacionalidade[nac] = totais_por_nacionalidade.get(nac, 0) + pop
        
        # Criar registros com cálculo de concentração
        for dados in dados_distribuicao:
            municipio = dados.get('municipio')
            nacionalidade = dados.get('nacionalidade')
            populacao = dados.get('populacao', 0)
            
            # Lookup de IDs
            localidade_id = self.lookup.get_localidade_id(municipio)
            nacionalidade_id = self.lookup.get_nacionalidade_id(nacionalidade)
            
            if not localidade_id or not nacionalidade_id:
                continue
            
            # Calcular concentração relativa
            total_nac = totais_por_nacionalidade.get(nacionalidade, 1)
            concentracao = (populacao / total_nac * 100) if total_nac > 0 else 0
            
            # Classificar domínio regional
            if concentracao >= 20:
                dominio = 'Alta'
            elif concentracao >= 5:
                dominio = 'Média'
            else:
                dominio = 'Baixa'
            
            registros.append({
                'distribuicao_geo_id': pk_counter,
                'localidade_id': localidade_id,
                'nacionalidade_id': nacionalidade_id,
                'populacao_nacional_local': populacao,
                'concentracao_relativa': round(concentracao, 2),
                'dominio_regional': dominio
            })
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_DistribuicaoGeografica'] = df
        
        self.logger.sucesso(
            f"Fact_DistribuicaoGeografica criada: {len(df)} distribuições geográficas"
        )
        
        return df
    
    def obter_todos_fatos(self):
        """Retorna dict com todos os fatos criados"""
        return self.fatos
    
    def obter_fato(self, nome):
        """Retorna um fato específico"""
        return self.fatos.get(nome)
    
    # ============================================================
    # MÉTODOS AUXILIARES
    # ============================================================
    
    @staticmethod
    def _mapear_grupo_etario(faixa_etaria):
        """Mapeia descrição de faixa etária para grupo_etario_id"""
        mapeamento = {
            '0-14 anos': 1,
            '15-64 anos': 2,
            '65+ anos': 3,
            '65 anos ou +': 3,
            '45-66 anos': 4
        }
        
        return mapeamento.get(faixa_etaria, 2)  # Default: população ativa


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Transformador de Fatos Base...")
    
    class LoggerTeste:
        def info(self, msg): print(f"INFO: {msg}")
        def sucesso(self, msg): print(f"✓ {msg}")
        def aviso(self, msg): print(f"⚠ {msg}")
    
    class LookupTeste:
        def get_nacionalidade_id(self, nome):
            return 1 if nome == 'Brasil' else None
        
        def get_localidade_id(self, nome):
            return 1 if nome == 'Lisboa' else None
        
        def get_ano_id(self, ano):
            return 1 if ano == 2011 else 2
    
    # Criar transformador
    transformador = TransformadorFatosBase(LoggerTeste(), LookupTeste())
    
    # Testar criação de fatos
    dados_sexo_teste = [
        {'nacionalidade': 'Brasil', 'ano': 2011, 'populacao_masculino': 100, 'populacao_feminino': 120}
    ]
    fact_sexo = transformador.criar_fact_populacao_por_nacionalidade_sexo(dados_sexo_teste)
    print(f"Fact_PopulacaoPorNacionalidadeSexo: {len(fact_sexo)} registros")
    
    dados_grupo_teste = [
        {'nacionalidade': 'Brasil', 'faixa_etaria': '15-64 anos', 'populacao': 150, 'idade_media': 35.5, 'ano': 2011}
    ]
    fact_grupo = transformador.criar_fact_populacao_por_grupo_etario(dados_grupo_teste)
    print(f"Fact_PopulacaoPorGrupoEtario: {len(fact_grupo)} registros")
    
    dados_ranking_teste = [
        {'nacionalidade': 'Brasil', 'populacao_2011': 100000, 'populacao_2021': 150000, 'ranking': 1}
    ]
    fact_ranking = transformador.criar_fact_nacionalidade_principal(dados_ranking_teste)
    print(f"Fact_NacionalidadePrincipal: {len(fact_ranking)} registros")
    
    print("\n✓ Módulo parte_06_transformador_fatos_base.py carregado com sucesso!")
