import pandas as pd
import numpy as np

# Configuração para reprodutibilidade
np.random.seed(42)

# Carregando os datasets
print("Carregando datasets originais...")
df_store = pd.read_csv('brutos/store.csv')
df_train = pd.read_csv('brutos/train.csv')

# Função para amostrar n linhas por loja
def amostrar_por_loja(df, n_amostras=50):
    return df.groupby('Store').apply(
        lambda x: x.sample(n=min(len(x), n_amostras), random_state=42)
    ).reset_index(drop=True)

# Amostrando dados do dataset de vendas
print("Amostrando dados...")
df_train_reduzido = amostrar_por_loja(df_train, n_amostras=50)

# Pegando apenas as lojas que estão no dataset de vendas reduzido
lojas_selecionadas = df_train_reduzido['Store'].unique()
df_store_reduzido = df_store[df_store['Store'].isin(lojas_selecionadas)]

# Salvando os datasets reduzidos
print("Salvando datasets reduzidos...")
df_store_reduzido.to_csv('reduzidos/store_reduzido.csv', index=False)
df_train_reduzido.to_csv('reduzidos/train_reduzido.csv', index=False)

# Imprimindo estatísticas
print("\nEstatísticas dos datasets:")
print(f"Dataset de lojas original: {len(df_store)} registros")
print(f"Dataset de lojas reduzido: {len(df_store_reduzido)} registros")
print(f"Dataset de vendas original: {len(df_train)} registros")
print(f"Dataset de vendas reduzido: {len(df_train_reduzido)} registros")

# Verificando número de registros por loja no dataset reduzido
registros_por_loja = df_train_reduzido['Store'].value_counts()
print("\nNúmero de registros por loja (primeiras 5 lojas):")
print(registros_por_loja.head()) 