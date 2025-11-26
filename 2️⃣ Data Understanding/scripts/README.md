# Script de Exploração de Microdados do INE - Censos 2021

## Descrição

Este script Python (`exploracao_microdados_ine.py`) foi convertido do Jupyter Notebook original e realiza uma análise exploratória completa dos dados do INE dos Censos 2021, focando no perfil sócio-profissional da população imigrante em Portugal.

## Objetivo da Conversão

**Transformou-se um notebook interativo em um script executável para:**
- Facilitar execução em ambientes de produção
- Permitir automação e integração em pipelines
- Melhorar a portabilidade e reutilização do código
- Documentação detalhada em português para outros desenvolvedores
- Tratamento robusto de erros e validações

## Funcionalidades Principais

### Análises Implementadas
- **Carregamento de Dados**: Sistema inteligente com múltiplos encodings
- **Análise Estrutural**: Dimensões, tipos de dados, memória utilizada
- **Dados Ausentes**: Identificação e quantificação de missing data
- **Nacionalidades**: Extração e análise de populações estrangeiras
- **Setores Económicos**: Mapeamento completo dos códigos CAE
- **Contexto Temporal**: Adequação dos dados para análise longitudinal
- **Relatório Completo**: Síntese de todas as análises realizadas

### Datasets Analisados
1. `Q2.1.csv` - Habilitações Literárias por Nacionalidade
2. `Q3.3.csv` - Setor de Atividade por Nacionalidade
3. `Q1.1.csv` - Demografia por Nacionalidade (2011-2021)
4. `Q13.csv` - Educação Geral (comparação nacional)
5. `Q21.csv` - Setores de Atividade Geral (comparação nacional)

## Estrutura de Dados Esperada

```
data/raw/ine/
├── Censos2021_csv/
│   ├── Q13.csv
│   └── Q21.csv
└── Censos2021_População estrangeira/
    ├── Q1.1.csv
    ├── Q2.1.csv
    └── Q3.3.csv
```

## Como Executar

### Pré-requisitos

```bash
# Instalar dependências necessárias
pip install pandas numpy matplotlib seaborn pathlib
```

### Formas de Execução

#### 1. Execução Básica
```bash
python exploracao_microdados_ine.py
```

#### 2. Especificando Diretório de Dados
```bash
python exploracao_microdados_ine.py --diretorio /caminho/para/dados
```

#### 3. Obtendo Ajuda
```bash
python exploracao_microdados_ine.py --help
python exploracao_microdados_ine.py --ajuda
```

### Parâmetros de Linha de Comando

| Parâmetro | Atalho | Padrão | Descrição |
|-----------|--------|--------|-----------|
| `--diretorio` | `-d` | `data/raw/ine` | Diretório base dos dados do INE |
| `--ajuda` | `--help` | - | Mostra informações detalhadas de uso |

## Saídas do Script

### Verificação de Dados
- Status de disponibilidade de cada arquivo ([OK]/[ERRO])
- Resumo quantitativo dos dados encontrados

### Análise Estrutural
- Dimensões (linhas × colunas)
- Uso de memória
- Tipos de dados por coluna
- Dados ausentes por variável

### Análise de Conteúdo
- Nacionalidades identificadas em cada dataset
- Setores de atividade económica (códigos CAE)
- Contexto temporal e geográfico
- Adequação para perguntas de pesquisa

## Características Técnicas

### Tratamento de Encoding
```python
# O script tenta automaticamente:
1. UTF-8 (padrão)
2. ISO-8859-1 (fallback para dados portugueses)
```

### Gestão de Erros
- Validação de arquivos existentes
- Tratamento de erros de encoding
- Mensagens de erro claras em português
- Recuperação graceful de falhas

### Arquitetura do Código
- **Classe Principal**: `AnalisadorMicrodadosINE`
- **Funções Utilitárias**: Modularização para reutilização
- **Configurações Centralizadas**: `ConfiguracaoCaminhos`
- **Type Hints**: Tipagem para melhor manutenibilidade

## Exemplo de Saída

```
INICIANDO ANÁLISE EXPLORATÓRIA DOS MICRODADOS DO INE
======================================================================
Projeto: Concurso de Pesquisa Prepara Portugal 2025
Foco: O Perfil Sócio-profissional do Imigrante em Portugal
======================================================================

Analisador de Microdados do INE inicializado
Diretório base: data/raw/ine

VERIFICAÇÃO DE DISPONIBILIDADE DOS DADOS
==================================================
[OK] educacao_nacionalidade: Q2.1.csv
[OK] setores_nacionalidade: Q3.3.csv
[OK] demografia_nacionalidade: Q1.1.csv
[ERRO] educacao_geral: Q13.csv
[ERRO] setores_geral: Q21.csv

Resumo: 3/5 arquivos disponíveis
```

## Diferenças em Relação ao Notebook

### Melhorias Implementadas
1. **Consolidação de Imports**: Todas as dependências no topo
2. **Documentação Detalhada**: Comentários explicativos em português
3. **Tratamento de Erros**: Sistema robusto de validação
4. **Argumentos CLI**: Flexibilidade na execução
5. **Modularização**: Código organizado em classes e funções
6. **Type Hints**: Melhor legibilidade e manutenção

### Funcionalidades Adicionadas
- Sistema de ajuda integrado
- Validação automática de arquivos
- Múltiplos encodings suportados
- Logs informativos detalhados
- Estrutura para extensão futura

## Desenvolvimento e Manutenção

### Como Modificar o Script

#### Adicionar Nova Análise
```python
def nova_analise(self, df: pd.DataFrame) -> None:
    """
    Adiciona uma nova análise ao fluxo principal.
    
    Args:
        df: DataFrame para análise
    """
    # Implementar lógica da análise
    pass

# Adicionar chamada em gerar_relatorio_completo()
```

#### Modificar Caminhos de Dados
```python
# Editar a classe ConfiguracaoCaminhos
self.DATASETS['novo_dataset'] = self.BASE_PATH / "novo_arquivo.csv"
```

### Extensões Possíveis
- Exportação de resultados para Excel/PDF
- Visualizações automáticas dos dados
- Comparação temporal automática
- Integração com APIs de dados

## Contexto do Projeto

**Projeto**: Concurso de Pesquisa Prepara Portugal 2025  
**Tema**: O Perfil Sócio-profissional do Imigrante em Portugal  
**Foco**: Relação entre Nível Educacional e Inserção no Mercado de Trabalho  
**Dados**: INE - Instituto Nacional de Estatística (Censos 2021)

## Suporte

Para dúvidas ou problemas:
1. Execute o script com `--ajuda` para ajuda detalhada
2. Verifique se a estrutura de dados está correta
3. Confirme que as dependências estão instaladas
4. Teste com dados de exemplo primeiro

---

**Nota**: Este script mantém toda a funcionalidade do notebook original, mas oferece maior flexibilidade e robustez para uso em ambientes de produção.
