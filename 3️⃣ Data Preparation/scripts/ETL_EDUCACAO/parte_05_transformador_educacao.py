"""
============================================================
PARTE 5: TRANSFORMADOR DE DADOS EDUCACIONAIS
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
"""

import pandas as pd


# ============================================================
# CLASSE TRANSFORMADOR DE DADOS EDUCACIONAIS
# ============================================================

class TransformadorEducacao:
    """Transforma dados educacionais brutos em tabelas de fato"""
    
    def __init__(self, logger, lookup):
        self.logger = logger
        self.lookup = lookup
        self.fatos = {}
    
    def criar_fact_populacao_educacao(self, dados_educacao_lista):
        """
        Cria Fact_PopulacaoEducacao
        Estrutura:
          - populacao_educacao_id (PK)
          - nacionalidade_id (FK)
          - nivel_educacao_id (FK)
          - populacao_total
          - faixa_etaria
          - ano_referencia
          - percentual_nivel
        
        dados_educacao_lista: lista de dicts do parser
        """
        self.logger.info("Criando Fact_PopulacaoEducacao...")
        
        registros = []
        pk_counter = 1
        
        for dados_pais in dados_educacao_lista:
            nacionalidade = dados_pais.get('nacionalidade')
            faixa_etaria = dados_pais.get('faixa_etaria', '15-64 anos')
            total_pop = dados_pais.get('total_populacao', 0)
            
            # Obter ID da nacionalidade
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            if nac_id is None:
                self.logger.aviso(
                    f"Nacionalidade {nacionalidade} não encontrada no lookup"
                )
                continue
            
            # Processar cada nível educacional
            for nivel in dados_pais.get('niveis', []):
                nivel_nome = nivel['nivel']
                quantidade = nivel.get('quantidade', 0)
                percentual = nivel.get('percentual', 0)
                
                # Obter ID do nível
                nivel_id = self.lookup.get_nivel_educacao_id(nivel_nome)
                
                if nivel_id is None:
                    self.logger.aviso(
                        f"Nível educacional '{nivel_nome}' não encontrado"
                    )
                    continue
                
                registros.append({
                    'populacao_educacao_id': pk_counter,
                    'nacionalidade_id': nac_id,
                    'nivel_educacao_id': nivel_id,
                    'populacao_total': int(quantidade) if quantidade else 0,
                    'faixa_etaria': faixa_etaria,
                    'ano_referencia': 2011,
                    'percentual_nivel': float(percentual) if percentual else 0.0
                })
                
                pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_PopulacaoEducacao'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoEducacao criada: {len(df)} registros "
            f"({len(dados_educacao_lista)} países × ~4 níveis)"
        )
        
        return df
    
    def criar_fact_estatisticas_educacao(self, dados_educacao_lista):
        """
        Cria Fact_EstatisticasEducacao
        Estrutura:
          - estatistica_id (PK)
          - nacionalidade_id (FK)
          - populacao_total_educacao
          - sem_educacao
          - ensino_basico
          - ensino_secundario
          - ensino_superior
          - percentual_sem_educacao
          - percentual_ensino_superior
          - percentual_ensino_basico
          - percentual_ensino_secundario
          - indice_educacional
          - ano_referencia
        """
        self.logger.info("Criando Fact_EstatisticasEducacao...")
        
        registros = []
        pk_counter = 1
        
        for dados_pais in dados_educacao_lista:
            nacionalidade = dados_pais.get('nacionalidade')
            total_pop = dados_pais.get('total_populacao', 0)
            
            # Obter ID da nacionalidade
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            if nac_id is None:
                continue
            
            # Agregar dados por categoria
            sem_educacao = 0
            ensino_basico = 0
            ensino_secundario = 0
            ensino_superior = 0
            
            perc_sem_educacao = 0.0
            perc_ensino_basico = 0.0
            perc_ensino_secundario = 0.0
            perc_ensino_superior = 0.0
            
            for nivel in dados_pais.get('niveis', []):
                nivel_nome = nivel['nivel']
                quantidade = nivel.get('quantidade', 0)
                percentual = nivel.get('percentual', 0)
                
                # Classificar nível
                if 'Inferior' in nivel_nome:
                    sem_educacao = quantidade
                    perc_sem_educacao = percentual
                elif 'Básico' in nivel_nome:
                    ensino_basico = quantidade
                    perc_ensino_basico = percentual
                elif 'Secundário' in nivel_nome:
                    ensino_secundario = quantidade
                    perc_ensino_secundario = percentual
                elif 'Superior' in nivel_nome:
                    ensino_superior = quantidade
                    perc_ensino_superior = percentual
            
            # Calcular índice educacional (ponderado)
            # Pesos: Superior=4, Secundário=3, Básico=2, Sem=1
            if total_pop > 0:
                indice = (
                    (sem_educacao * 1 + 
                     ensino_basico * 2 +
                     ensino_secundario * 3 +
                     ensino_superior * 4) / total_pop
                )
            else:
                indice = 0.0
            
            registros.append({
                'estatistica_id': pk_counter,
                'nacionalidade_id': nac_id,
                'populacao_total_educacao': int(total_pop) if total_pop else 0,
                'sem_educacao': int(sem_educacao) if sem_educacao else 0,
                'ensino_basico': int(ensino_basico) if ensino_basico else 0,
                'ensino_secundario': int(ensino_secundario) if ensino_secundario else 0,
                'ensino_superior': int(ensino_superior) if ensino_superior else 0,
                'percentual_sem_educacao': round(float(perc_sem_educacao), 2),
                'percentual_ensino_basico': round(float(perc_ensino_basico), 2),
                'percentual_ensino_secundario': round(float(perc_ensino_secundario), 2),
                'percentual_ensino_superior': round(float(perc_ensino_superior), 2),
                'indice_educacional': round(indice, 2),
                'ano_referencia': 2011
            })
            
            pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EstatisticasEducacao'] = df
        
        self.logger.sucesso(
            f"Fact_EstatisticasEducacao criada: {len(df)} registros "
            f"(1 por nacionalidade)"
        )
        
        # Exibir top 5 por ensino superior
        if len(df) > 0:
            top5 = df.nlargest(5, 'percentual_ensino_superior')
            self.logger.info("Top 5 nacionalidades por ensino superior:")
            for idx, row in top5.iterrows():
                nac_nome = self._get_nome_nacionalidade(row['nacionalidade_id'])
                print(f"  {nac_nome}: {row['percentual_ensino_superior']:.2f}%")
        
        return df
    
    def criar_fact_populacao_por_nacionalidade(self, dados_populacao_lista):
        """
        Cria Fact_PopulacaoPorNacionalidade
        Estrutura:
          - populacao_nacional_id (PK)
          - nacionalidade_id (FK)
          - populacao_id (FK - ano)
          - populacao_total
          - masculino
          - feminino
          - percentagem_total
        """
        self.logger.info("Criando Fact_PopulacaoPorNacionalidade...")
        
        registros = []
        pk_counter = 1
        
        for dados in dados_populacao_lista:
            nacionalidade = dados.get('nacionalidade')
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            
            if nac_id is None:
                continue
            
            # Dados de 2011
            if dados.get('total_2011'):
                ano_id = self.lookup.get_ano_id(2011)
                registros.append({
                    'populacao_nacional_id': pk_counter,
                    'nacionalidade_id': nac_id,
                    'populacao_id': ano_id,
                    'populacao_total': int(dados['total_2011']),
                    'masculino': int(dados.get('homens_2011', 0)),
                    'feminino': int(dados.get('mulheres_2011', 0)),
                    'percentagem_total': 0.0  # Será calculado depois
                })
                pk_counter += 1
            
            # Dados de 2001
            if dados.get('total_2001'):
                ano_id = self.lookup.get_ano_id(2001)
                registros.append({
                    'populacao_nacional_id': pk_counter,
                    'nacionalidade_id': nac_id,
                    'populacao_id': ano_id,
                    'populacao_total': int(dados['total_2001']),
                    'masculino': int(dados.get('homens_2001', 0)),
                    'feminino': int(dados.get('mulheres_2001', 0)),
                    'percentagem_total': 0.0
                })
                pk_counter += 1
        
        df = pd.DataFrame(registros)
        
        # Calcular percentagens
        if len(df) > 0:
            for ano_id in df['populacao_id'].unique():
                total_ano = df[df['populacao_id'] == ano_id]['populacao_total'].sum()
                if total_ano > 0:
                    mask = df['populacao_id'] == ano_id
                    df.loc[mask, 'percentagem_total'] = (
                        (df.loc[mask, 'populacao_total'] / total_ano * 100).round(2)
                    )
        
        self.fatos['Fact_PopulacaoPorNacionalidade'] = df
        
        self.logger.sucesso(
            f"Fact_PopulacaoPorNacionalidade criada: {len(df)} registros"
        )
        
        return df
    
    def criar_fact_evolucao_temporal(self, dados_populacao_lista):
        """
        Cria Fact_EvolucaoTemporal
        Estrutura:
          - evolucao_id (PK)
          - nacionalidade_id (FK)
          - populacao_id (FK - ano 2011)
          - ano_inicio (2001)
          - populacao_inicio
          - variacao_absoluta
          - variacao_percentual
          - taxa_crescimento
        """
        self.logger.info("Criando Fact_EvolucaoTemporal...")
        
        registros = []
        pk_counter = 1
        
        ano_id_2011 = self.lookup.get_ano_id(2011)
        
        for dados in dados_populacao_lista:
            nacionalidade = dados.get('nacionalidade')
            nac_id = self.lookup.get_nacionalidade_id(nacionalidade)
            
            if nac_id is None:
                continue
            
            pop_2011 = dados.get('total_2011', 0)
            pop_2001 = dados.get('total_2001', 0)
            
            if pop_2011 and pop_2001:
                variacao_abs = pop_2011 - pop_2001
                
                if pop_2001 > 0:
                    variacao_perc = ((pop_2011 - pop_2001) / pop_2001) * 100
                    # Taxa de crescimento anual composta (CAGR)
                    anos = 10
                    taxa_cresc = (((pop_2011 / pop_2001) ** (1/anos)) - 1) * 100
                else:
                    variacao_perc = 0.0
                    taxa_cresc = 0.0
                
                registros.append({
                    'evolucao_id': pk_counter,
                    'nacionalidade_id': nac_id,
                    'populacao_id': ano_id_2011,
                    'ano_inicio': 2001,
                    'populacao_inicio': int(pop_2001),
                    'variacao_absoluta': int(variacao_abs),
                    'variacao_percentual': round(variacao_perc, 2),
                    'taxa_crescimento': round(taxa_cresc, 2)
                })
                
                pk_counter += 1
        
        df = pd.DataFrame(registros)
        self.fatos['Fact_EvolucaoTemporal'] = df
        
        self.logger.sucesso(
            f"Fact_EvolucaoTemporal criada: {len(df)} registros"
        )
        
        # Exibir top 3 maiores crescimentos
        if len(df) > 0:
            top3 = df.nlargest(3, 'variacao_percentual')
            self.logger.info("Top 3 maiores crescimentos (2001-2011):")
            for idx, row in top3.iterrows():
                nac_nome = self._get_nome_nacionalidade(row['nacionalidade_id'])
                print(f"  {nac_nome}: +{row['variacao_percentual']:.1f}%")
        
        return df
    
    def obter_todos_fatos(self):
        """Retorna dict com todas as tabelas de fato criadas"""
        return self.fatos
    
    def obter_fato(self, nome):
        """Retorna uma tabela de fato específica"""
        return self.fatos.get(nome)
    
    # ============================================================
    # MÉTODOS AUXILIARES
    # ============================================================
    
    def _get_nome_nacionalidade(self, nac_id):
        """Obtém nome da nacionalidade pelo ID"""
        if 'Dim_Nacionalidade' not in self.lookup.dimensoes:
            return f"ID_{nac_id}"
        
        df_nac = self.lookup.dimensoes['Dim_Nacionalidade']
        resultado = df_nac[df_nac['nacionalidade_id'] == nac_id]
        
        if not resultado.empty:
            return resultado['nome_nacionalidade'].iloc[0]
        
        return f"ID_{nac_id}"


# ============================================================
# CLASSE DE ESTATÍSTICAS E ANÁLISES
# ============================================================

class AnalisadorEducacao:
    """Fornece análises estatísticas sobre dados educacionais"""
    
    def __init__(self, fact_educacao, fact_estatisticas):
        self.fact_educacao = fact_educacao
        self.fact_estatisticas = fact_estatisticas
    
    def calcular_coeficiente_gini_educacao(self):
        """
        Calcula coeficiente de Gini da distribuição educacional
        (desigualdade educacional entre nacionalidades)
        """
        if self.fact_estatisticas is None or len(self.fact_estatisticas) == 0:
            return None
        
        # Usar percentual de ensino superior como métrica
        valores = sorted(self.fact_estatisticas['percentual_ensino_superior'].values)
        n = len(valores)
        
        if n == 0:
            return None
        
        # Cálculo do índice de Gini
        cumsum = 0
        for i, val in enumerate(valores, 1):
            cumsum += (n - i + 0.5) * val
        
        gini = (n + 1 - 2 * cumsum / sum(valores)) / n if sum(valores) > 0 else 0
        
        return round(gini, 3)
    
    def obter_resumo_estatistico(self):
        """Retorna resumo estatístico dos dados educacionais"""
        if self.fact_estatisticas is None or len(self.fact_estatisticas) == 0:
            return {}
        
        return {
            'media_ensino_superior': round(
                self.fact_estatisticas['percentual_ensino_superior'].mean(), 2
            ),
            'mediana_ensino_superior': round(
                self.fact_estatisticas['percentual_ensino_superior'].median(), 2
            ),
            'desvio_padrao': round(
                self.fact_estatisticas['percentual_ensino_superior'].std(), 2
            ),
            'minimo': round(
                self.fact_estatisticas['percentual_ensino_superior'].min(), 2
            ),
            'maximo': round(
                self.fact_estatisticas['percentual_ensino_superior'].max(), 2
            ),
            'coef_gini': self.calcular_coeficiente_gini_educacao()
        }


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Transformador de Educação...")
    
    # Dados de teste simulados
    dados_teste = [
        {
            'nacionalidade': 'Brasil',
            'total_populacao': 93545,
            'faixa_etaria': '15-64 anos',
            'niveis': [
                {'nivel': 'Inferior ao básico 3º ciclo', 'quantidade': 24498, 'percentual': 26.19},
                {'nivel': 'Básico 3º ciclo', 'quantidade': 21132, 'percentual': 22.59},
                {'nivel': 'Secundário e pós-secundário', 'quantidade': 38411, 'percentual': 41.05},
                {'nivel': 'Superior', 'quantidade': 9504, 'percentual': 10.16}
            ]
        }
    ]
    
    print(f"Dados de teste: {len(dados_teste)} países")
    print("Níveis por país: 4")
    
    print("\n✓ Módulo parte_05_transformador_educacao.py carregado com sucesso!")
