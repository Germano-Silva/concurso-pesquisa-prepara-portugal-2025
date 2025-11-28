import pandas as pd

# Carregar dataset unificado
df = pd.read_csv(
    "./data/Q3.3.csv",
    encoding="latin1",
    header=2,  # linha com Total, A, B, C...
    engine="python"
)

# Remover colunas vazias e linhas inúteis
df = df.dropna(how="all")

# renomear primeira coluna
df.rename(columns={df.columns[0]: "Nacionalidade"}, inplace=True)

# Definir lista de setores CAE Rev.3 (22 setores)
setores = df.columns[2:].tolist()  # do A ao U

# Filtrar nacionais e imigrantes
df_nacionais = df[df["Nacionalidade"] == "População residente"]
df_imigrantes = df[df["Nacionalidade"] != "População residente"]

# Converter números com espaços para int
def limpar_valor(x):
    if pd.isna(x):
        return 0
    x = str(x).replace(" ", "").replace("\n", "").replace("\t", "")
    # Se não for número, retorna 0
    if not x.isdigit():
        return 0
    return int(x)


for col in setores:
    df_imigrantes[col] = df_imigrantes[col].apply(limpar_valor)
    df_nacionais[col] = df_nacionais[col].apply(limpar_valor)

# Criar dataset long (setor → valor)
df_imigrantes_long = df_imigrantes.melt(
    id_vars=["Nacionalidade"],
    value_vars=setores,
    var_name="Setor_Atividade",
    value_name="Total_imigrantes"
)

df_nacionais_long = df_nacionais.melt(
    id_vars=["Nacionalidade"],
    value_vars=setores,
    var_name="Setor_Atividade",
    value_name="Total_nacionais"
)

# Calcular percentagem dentro de cada setor para imigrantes
df_imigrantes_long["Percentagem"] = df_imigrantes_long.groupby("Setor_Atividade")["Total_imigrantes"].transform(
    lambda x: x / x.sum() * 100
)

# Merge para comparar setor × nacionalidade
df_comparativo = pd.merge(
    df_imigrantes_long,
    df_nacionais_long[["Setor_Atividade", "Total_nacionais"]],
    on="Setor_Atividade",
    how="left"
)

# Salvar
df_comparativo.to_csv("../data/processed/dados_setoriais_imigrantes.csv", index=False, encoding="utf-8")

print("Arquivo salvo em /data/processed/")
