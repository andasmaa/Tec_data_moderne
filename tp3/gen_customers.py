import random, pandas as pd
from faker import Faker

fake = Faker("fr_FR"); random.seed(7); N = 50_000
villes = ["Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Agadir"]
pd.DataFrame({
    "customer_id": range(1, N + 1),
    "full_name":   [fake.name() for _ in range(N)],
    "city":        random.choices(villes, k=N),
    "segment":     random.choices(["STANDARD", "PREMIUM", "VIP"], [80, 17, 3], k=N),
    "signup_date": [fake.date_between("-3y", "-90d") for _ in range(N)],
}).to_parquet("data/silver/customers.parquet", index=False)
print("50 000 clients générés")