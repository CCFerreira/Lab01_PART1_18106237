import pandas as pd
import os

os.makedirs("data/silver", exist_ok=True)

df = pd.read_csv("data/raw/orders.csv")

# Padronização
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Conversão de datas
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

# Remover nulos
df = df.dropna()

# Remover duplicados
df = df.drop_duplicates()

# Salvar parquet
df.to_parquet("data/silver/orders.parquet")

print("Dados tratados e salvos na SILVER")