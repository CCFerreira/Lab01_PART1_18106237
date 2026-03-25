import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

senha = quote_plus("postgres123")
engine = create_engine(f"postgresql://postgres:{senha}@localhost:5432/lab01")

df = pd.read_parquet("data/silver/orders.parquet")

# ========================
# DIM TEMPO
# ========================
dim_tempo = df[['order_purchase_timestamp']].drop_duplicates().copy()

dim_tempo['ano'] = dim_tempo['order_purchase_timestamp'].dt.year
dim_tempo['mes'] = dim_tempo['order_purchase_timestamp'].dt.month
dim_tempo['dia'] = dim_tempo['order_purchase_timestamp'].dt.day
dim_tempo['hora'] = dim_tempo['order_purchase_timestamp'].dt.hour

dim_tempo.rename(columns={'order_purchase_timestamp': 'data'}, inplace=True)

dim_tempo.to_sql("dim_tempo", engine, if_exists="replace", index=False)

# ========================
# DIM STATUS
# ========================
dim_status = df[['order_status']].drop_duplicates()
dim_status.columns = ['status']

dim_status.to_sql("dim_status", engine, if_exists="replace", index=False)

# ========================
# FATO
# ========================
fato = df[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp']]

fato.columns = ['order_id', 'customer_id', 'status', 'data']

fato.to_sql("fato_pedidos", engine, if_exists="replace", index=False)

print("Modelo STAR criado com sucesso")
