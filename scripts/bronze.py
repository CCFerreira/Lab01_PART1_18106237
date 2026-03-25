import pandas as pd
import os

os.makedirs("data/raw", exist_ok=True)

url = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv"

df = pd.read_csv(url)

df.to_csv("data/raw/orders.csv", index=False)

print("Dados salvos na camada RAW")