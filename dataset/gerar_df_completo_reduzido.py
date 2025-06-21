import pandas as pd
import numpy as np

print("Carregando datasets reduzidos...")
df_vendas = pd.read_csv('reduzidos/train_reduzido.csv', dtype={'StateHoliday': str})
df_lojas = pd.read_csv('reduzidos/store_reduzido.csv')

print("\nTratando dados...")
# Convertendo a coluna Date para datetime
df_vendas['Date'] = pd.to_datetime(df_vendas['Date'])

# Tratamento do dataset de lojas
# Preenchendo valores ausentes
df_lojas['PromoInterval'] = df_lojas['PromoInterval'].fillna("Nenhum")

colunas_preencher_zero = ['CompetitionOpenSinceMonth', 'CompetitionOpenSinceYear', 'Promo2SinceWeek', 'Promo2SinceYear']
for col in colunas_preencher_zero:
    df_lojas[col] = df_lojas[col].fillna(0)

# CompetitionDistance: Preencher com a MÉDIA
df_lojas['CompetitionDistance'] = df_lojas['CompetitionDistance'].fillna(df_lojas['CompetitionDistance'].mean())

print("\nRealizando merge dos datasets...")
# Merge dos dataframes
df_completo = pd.merge(df_vendas, df_lojas, on='Store', how='left')

# Filtrando apenas lojas abertas
df_completo = df_completo[df_completo['Open'] == 1].copy()
df_completo.drop(['Open'], axis=1, inplace=True)

print("\nSalvando novo df_completo...")
# Salvando o novo df_completo
df_completo.to_csv('processados/df_completo_reduzido.csv', index=False)

print("\nEstatísticas do novo df_completo:")
print(f"Total de registros: {len(df_completo)}")
print(f"Número de lojas únicas: {df_completo['Store'].nunique()}")
print(f"Período: de {df_completo['Date'].min().strftime('%d/%m/%Y')} até {df_completo['Date'].max().strftime('%d/%m/%Y')}") 