import json, random, sys, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

# Optionnel : python producer.py 3000  -> s'arrête après 3000 événements
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else None

p = Producer({"bootstrap.servers": "localhost:9092",
              "enable.idempotence": True})   # pas de doublon en cas de renvoi

cities = ["Casablanca", "Rabat", "Marrakech", "Fès", "Tanger"]

def delivery(err, msg):
    if err:
        print("échec :", err)

n = 0
try:
    while LIMIT is None or n < LIMIT:
        evt = {
            "order_id": str(uuid.uuid4()),
            "customer_id": random.randint(1, 50_000),
            "order_ts": datetime.now(timezone.utc).isoformat(),
            "city": random.choice(cities),
            "amount": round(random.uniform(5, 900), 2),
            "status": "PAID",
        }
        # La clé détermine la partition : les événements d'une commande restent ordonnés
        p.produce("orders", key=evt["order_id"], value=json.dumps(evt), callback=delivery)
        p.poll(0)
        n += 1
        if n % 100 == 0:
            print(n, "événements envoyés")
        time.sleep(random.uniform(0.01, 0.2))
except KeyboardInterrupt:
    pass
finally:
    p.flush()
    print("total envoyé :", n)
