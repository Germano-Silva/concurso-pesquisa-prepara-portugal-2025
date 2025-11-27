import os
import pandas as pd

caminho = "../data/processed/dados_setoriais_imigrantes.csv"

try:
    df = pd.read_csv(caminho)
except:
    df = None

def ok(msg):
    print(f"✔ {msg}")

def fail(msg):
    print(f"✘ {msg}")

print("\n==== VALIDAÇÃO ====\n")

# 1. Dataset criado
if df is not None:
    ok("Dataset consolidado criado")
else:
    fail("Dataset NÃO foi criado")

# 2. 22 setores CAE Rev.3
if df is not None and df["Setor_Atividade"].nunique() == 22:
    ok("Inclui os 22 setores CAE Rev.3")
else:
    fail("Não tem os 22 setores CAE Rev.3")

# 3. Comparativo imigrantes vs nacionais
if df is not None and {"Total_imigrantes", "Total_nacionais"}.issubset(df.columns):
    ok("Comparativo imigrantes vs nacionais presente")
else:
    fail("Comparativo NÃO encontrado")

# 4. Percentagens
if df is not None and "Percentagem" in df.columns:
    soma_setores = df.groupby("Setor_Atividade")["Percentagem"].sum()
    if soma_setores.between(99.5, 100.5).all():
        ok("Percentagens corretas (somam ~100% por setor)")
    else:
        fail(f"Percentagens incorretas:\n{soma_setores}")
else:
    fail("Coluna Percentagem não encontrada")

# 5. Arquivo salvo
if os.path.exists(caminho):
    ok(f"Arquivo encontrado em: {caminho}")
else:
    fail(f"Arquivo NÃO encontrado em: {caminho}")

print("\n==== FIM ====\n")
