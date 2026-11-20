import uuid, random, pandas as pd
df = pd.read_parquet("data/silver/orders.parquet")
new = df.sample(500, random_state=1).copy()
new["order_id"] = [str(uuid.uuid4()) for _ in range(500)]
new["order_ts"] = pd.Timestamp.now()
pd.concat([df, new]).to_parquet("data/silver/orders.parquet", index=False)
print("500 commandes ajoutées")
