"""
============================================================
PARTE 4: TRANSFORMADOR DE DIMENSÕES BASE
Pipeline ETL - Educação (DP-01-A)
Google Colab
============================================================
"""

import pandas as pd


# ============================================================
# CLASSE TRANSFORMADOR DE DIMENSÕES BASE
# ============================================================

class TransformadorDimensoesBase:
    """Transforma dados brutos em dimensões do modelo Star Schema"""
    
    def __init__(self, logger, constantes):
        self.logger = logger
        self.constantes = constantes
        self.dimensoes = {}
    
    def criar_dim_populacao_residente(self, anos=[2011, 2001]):
        """
        Cria dimensão Dim_PopulacaoResidente
        Estrutura:
          - populacao_id (PK)
          - total_populacao
          - ano_referencia
        """
        self.logger.info("Criando Dim_PopulacaoResidente...")
        
        registros = []
        for i, ano in enumerate(anos, 1):
            registros.append({
                'populacao_id': i,
                'total_populacao': 0,  # Será atualizado com dados reais
                'ano_referencia': ano
            })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_PopulacaoResidente'] = df
        
        self.logger.sucesso(
            f"Dim_PopulacaoResidente criada: {len(df)} anos"
        )
        
        return df
    
    def criar_dim_nacionalidade(self, paises_dict):
        """
        Cria dimensão Dim_Nacionalidade
        Estrutura:
          - nacionalidade_id (PK)
          - nome_nacionalidade
          - codigo_pais (ISO 3166-1 alpha-3)
          - continente
        """
        self.logger.info("Criando Dim_Nacionalidade...")
        
        registros = []
        
        # Adicionar nacionalidades dos países processados
        for i, nome_pais in enumerate(paises_dict.keys(), 1):
            nome_normalizado = self._normalizar_nome_pais(nome_pais)
            
            registros.append({
                'nacionalidade_id': i,
                'nome_nacionalidade': nome_normalizado,
                'codigo_pais': self.constantes.CODIGOS_PAIS.get(nome_normalizado, 'XXX'),
                'continente': self.constantes.CONTINENTES.get(nome_normalizado, 'Desconhecido')
            })
        
        # Adicionar nacionalidades agregadas
        id_atual = len(registros) + 1
        
        # Nacionalidade Portuguesa
        registros.append({
            'nacionalidade_id': id_atual,
            'nome_nacionalidade': 'Nacionalidade portuguesa',
            'codigo_pais': 'PRT',
            'continente': 'Europa'
        })
        id_atual += 1
        
        # Nacionalidade Estrangeira (agregado)
        registros.append({
            'nacionalidade_id': id_atual,
            'nome_nacionalidade': 'Nacionalidade estrangeira',
            'codigo_pais': 'FOR',
            'continente': 'Múltiplos'
        })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_Nacionalidade'] = df
        
        self.logger.sucesso(
            f"Dim_Nacionalidade criada: {len(df)} nacionalidades"
        )
        
        return df
    
    def criar_dim_sexo(self):
        """
        Cria dimensão Dim_Sexo
        Estrutura:
          - sexo_id (PK)
          - tipo_sexo
        """
        self.logger.info("Criando Dim_Sexo...")
        
        registros = [
            {'sexo_id': 1, 'tipo_sexo': 'Masculino'},
            {'sexo_id': 2, 'tipo_sexo': 'Feminino'}
        ]
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_Sexo'] = df
        
        self.logger.sucesso(f"Dim_Sexo criada: {len(df)} tipos")
        
        return df
    
    def criar_dim_grupo_etario(self):
        """
        Cria dimensão Dim_GrupoEtario
        Estrutura:
          - grupoetario_id (PK)
          - faixa_etaria
          - descricao
        """
        self.logger.info("Criando Dim_GrupoEtario...")
        
        registros = [
            {
                'grupoetario_id': 1,
                'faixa_etaria': '0-14 anos',
                'descricao': 'Crianças e adolescentes'
            },
            {
                'grupoetario_id': 2,
                'faixa_etaria': '15-64 anos',
                'descricao': 'População ativa'
            },
            {
                'grupoetario_id': 3,
                'faixa_etaria': '65+ anos',
                'descricao': 'Idosos'
            },
            {
                'grupoetario_id': 4,
                'faixa_etaria': '45-66 anos',
                'descricao': 'Meia-idade (específico Reino Unido)'
            }
        ]
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_GrupoEtario'] = df
        
        self.logger.sucesso(f"Dim_GrupoEtario criada: {len(df)} grupos")
        
        return df
    
    def criar_dim_localidade(self, dados_municipios):
        """
        Cria dimensão Dim_Localidade a partir de dados de municípios
        Estrutura:
          - localidade_id (PK)
          - nome_localidade
          - nivel_administrativo (Município, NUTS III, NUTS II, NUTS I)
          - codigo_regiao
        
        dados_municipios: lista de dicts com {municipio, nacionalidade, ...}
        """
        self.logger.info("Criando Dim_Localidade...")
        
        # Extrair municípios únicos
        municipios_unicos = set()
        for item in dados_municipios:
            if 'municipio' in item and item['municipio']:
                municipios_unicos.add(item['municipio'])
        
        registros = []
        for i, municipio in enumerate(sorted(municipios_unicos), 1):
            registros.append({
                'localidade_id': i,
                'nome_localidade': municipio,
                'nivel_administrativo': 'Município',
                'codigo_regiao': self._gerar_codigo_regiao(municipio)
            })
        
        # Adicionar regiões NUTS (simplificado)
        id_atual = len(registros) + 1
        
        regioes_nuts = [
            {'nome': 'Norte', 'nivel': 'NUTS II', 'codigo': 'PT11'},
            {'nome': 'Centro', 'nivel': 'NUTS II', 'codigo': 'PT16'},
            {'nome': 'Lisboa', 'nivel': 'NUTS II', 'codigo': 'PT17'},
            {'nome': 'Alentejo', 'nivel': 'NUTS II', 'codigo': 'PT18'},
            {'nome': 'Algarve', 'nivel': 'NUTS II', 'codigo': 'PT15'},
            {'nome': 'Região Autónoma dos Açores', 'nivel': 'NUTS II', 'codigo': 'PT20'},
            {'nome': 'Região Autónoma da Madeira', 'nivel': 'NUTS II', 'codigo': 'PT30'}
        ]
        
        for regiao in regioes_nuts:
            registros.append({
                'localidade_id': id_atual,
                'nome_localidade': regiao['nome'],
                'nivel_administrativo': regiao['nivel'],
                'codigo_regiao': regiao['codigo']
            })
            id_atual += 1
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_Localidade'] = df
        
        self.logger.sucesso(
            f"Dim_Localidade criada: {len(df)} localidades "
            f"({len(municipios_unicos)} municípios + {len(regioes_nuts)} regiões)"
        )
        
        return df
    
    def criar_dim_nivel_educacao(self, niveis_config):
        """
        Cria dimensão Dim_NivelEducacao
        Estrutura:
          - nivel_educacao_id (PK)
          - nome_nivel
          - categoria
          - ordem_hierarquica
        
        niveis_config: dict do Config.NIVEIS_EDUCACAO
        """
        self.logger.info("Criando Dim_NivelEducacao...")
        
        registros = []
        for nivel_id, info in niveis_config.items():
            registros.append({
                'nivel_educacao_id': nivel_id,
                'nome_nivel': info['nome'],
                'categoria': info['categoria'],
                'ordem_hierarquica': info['ordem']
            })
        
        df = pd.DataFrame(registros)
        df = df.sort_values('ordem_hierarquica')
        self.dimensoes['Dim_NivelEducacao'] = df
        
        self.logger.sucesso(f"Dim_NivelEducacao criada: {len(df)} níveis")
        
        return df
    
    def criar_dim_mapeamento_nacionalidades(self, nacionalidades_df):
        """
        Cria dimensão Dim_MapeamentoNacionalidades
        Mapeia variações de nomes de nacionalidades
        Estrutura:
          - nacionalidade_educacao_id (PK)
          - nome_nacionalidade_educacao
          - nacionalidade_id_existente (FK)
          - compatibilidade
        """
        self.logger.info("Criando Dim_MapeamentoNacionalidades...")
        
        registros = []
        
        # Para cada nacionalidade, criar mapeamento de variações
        for _, row in nacionalidades_df.iterrows():
            nac_id = row['nacionalidade_id']
            nome = row['nome_nacionalidade']
            
            # Adicionar nome principal
            registros.append({
                'nacionalidade_educacao_id': len(registros) + 1,
                'nome_nacionalidade_educacao': nome,
                'nacionalidade_id_existente': nac_id,
                'compatibilidade': 'Exata'
            })
            
            # Adicionar variações conhecidas
            variacoes = self._obter_variacoes_nome(nome)
            for variacao in variacoes:
                registros.append({
                    'nacionalidade_educacao_id': len(registros) + 1,
                    'nome_nacionalidade_educacao': variacao,
                    'nacionalidade_id_existente': nac_id,
                    'compatibilidade': 'Variação'
                })
        
        df = pd.DataFrame(registros)
        self.dimensoes['Dim_MapeamentoNacionalidades'] = df
        
        self.logger.sucesso(
            f"Dim_MapeamentoNacionalidades criada: {len(df)} mapeamentos"
        )
        
        return df
    
    def obter_todas_dimensoes(self):
        """Retorna dict com todas as dimensões criadas"""
        return self.dimensoes
    
    def obter_dimensao(self, nome):
        """Retorna uma dimensão específica"""
        return self.dimensoes.get(nome)
    
    # ============================================================
    # MÉTODOS AUXILIARES
    # ============================================================
    
    @staticmethod
    def _normalizar_nome_pais(nome):
        """Normaliza nome de país para padrão"""
        mapeamento = {
            'Romenia': 'Roménia',
            'Romania': 'Roménia',
            'Sao tome e Principe': 'São Tomé e Príncipe',
            'Sao Tome e Principe': 'São Tomé e Príncipe',
            'Republica da Moldavia': 'República da Moldávia',
            'República da Moldávia': 'República da Moldávia',
            'Republica Popular da China': 'República Popular da China',
            'República Popular da China': 'República Popular da China',
            'Guine-Bissau': 'Guiné-Bissau',
            'Guiné-Bissau': 'Guiné-Bissau'
        }
        
        nome_limpo = nome.strip()
        return mapeamento.get(nome_limpo, nome_limpo)
    
    @staticmethod
    def _gerar_codigo_regiao(nome_localidade):
        """Gera código simplificado para região (placeholder)"""
        # Em produção, usar mapeamento real de códigos NUTS III
        return f"PT{hash(nome_localidade) % 1000:03d}"
    
    @staticmethod
    def _obter_variacoes_nome(nome):
        """Retorna variações conhecidas de um nome de nacionalidade"""
        variacoes_conhecidas = {
            'Roménia': ['Romania', 'Romenia'],
            'São Tomé e Príncipe': ['Sao Tome e Principe', 'Sao tome e Principe'],
            'República da Moldávia': ['Republica da Moldavia', 'Moldavia'],
            'República Popular da China': ['Republica Popular da China', 'China'],
            'Guiné-Bissau': ['Guine-Bissau', 'Guine Bissau']
        }
        
        return variacoes_conhecidas.get(nome, [])


# ============================================================
# CLASSE AUXILIAR DE LOOKUP DE IDs
# ============================================================

class LookupDimensoes:
    """Fornece métodos rápidos para lookup de IDs nas dimensões"""
    
    def __init__(self, dimensoes_dict):
        self.dimensoes = dimensoes_dict
        self._indices = {}
        self._criar_indices()
    
    def _criar_indices(self):
        """Cria índices para lookup rápido"""
        # Índice para Nacionalidade
        if 'Dim_Nacionalidade' in self.dimensoes:
            df = self.dimensoes['Dim_Nacionalidade']
            self._indices['nacionalidade'] = df.set_index('nome_nacionalidade')['nacionalidade_id'].to_dict()
        
        # Índice para Localidade
        if 'Dim_Localidade' in self.dimensoes:
            df = self.dimensoes['Dim_Localidade']
            self._indices['localidade'] = df.set_index('nome_localidade')['localidade_id'].to_dict()
        
        # Índice para Nível Educação
        if 'Dim_NivelEducacao' in self.dimensoes:
            df = self.dimensoes['Dim_NivelEducacao']
            self._indices['nivel_educacao'] = df.set_index('nome_nivel')['nivel_educacao_id'].to_dict()
    
    def get_nacionalidade_id(self, nome):
        """Retorna ID da nacionalidade pelo nome"""
        return self._indices.get('nacionalidade', {}).get(nome)
    
    def get_localidade_id(self, nome):
        """Retorna ID da localidade pelo nome"""
        return self._indices.get('localidade', {}).get(nome)
    
    def get_nivel_educacao_id(self, nome):
        """Retorna ID do nível educação pelo nome"""
        return self._indices.get('nivel_educacao', {}).get(nome)
    
    def get_sexo_id(self, tipo):
        """Retorna ID do sexo (1=Masculino, 2=Feminino)"""
        return 1 if tipo.lower() in ['masculino', 'homens', 'm'] else 2
    
    def get_ano_id(self, ano):
        """Retorna ID do ano de população residente"""
        if 'Dim_PopulacaoResidente' in self.dimensoes:
            df = self.dimensoes['Dim_PopulacaoResidente']
            resultado = df[df['ano_referencia'] == ano]
            if not resultado.empty:
                return resultado['populacao_id'].iloc[0]
        return None


# ============================================================
# TESTE DO MÓDULO
# ============================================================

if __name__ == "__main__":
    print("Testando Transformador de Dimensões Base...")
    
    class LoggerTeste:
        def info(self, msg): print(f"INFO: {msg}")
        def sucesso(self, msg): print(f"✓ {msg}")
    
    class ConstantesTeste:
        CODIGOS_PAIS = {'Brasil': 'BRA', 'Angola': 'AGO'}
        CONTINENTES = {'Brasil': 'América do Sul', 'Angola': 'África'}
    
    # Criar transformador
    transformador = TransformadorDimensoesBase(LoggerTeste(), ConstantesTeste())
    
    # Testar criação de dimensões
    dim_pop = transformador.criar_dim_populacao_residente()
    print(f"Dim_PopulacaoResidente: {len(dim_pop)} registros")
    
    dim_nac = transformador.criar_dim_nacionalidade({'Brasil': {}, 'Angola': {}})
    print(f"Dim_Nacionalidade: {len(dim_nac)} registros")
    
    dim_sexo = transformador.criar_dim_sexo()
    print(f"Dim_Sexo: {len(dim_sexo)} registros")
    
    # Testar lookup
    lookup = LookupDimensoes(transformador.obter_todas_dimensoes())
    nac_id = lookup.get_nacionalidade_id('Brasil')
    print(f"ID de Brasil: {nac_id}")
    
    print("\n✓ Módulo parte_04_transformador_dimensoes.py carregado com sucesso!")
