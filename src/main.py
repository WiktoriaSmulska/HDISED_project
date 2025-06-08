import json
import pymongo

# Połączenie z MongoDB lokalnie
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["hurtownia_danych"]
collection = db["zamowienia"]

# Wczytanie danych z pliku JSON
with open('../data/orders.json', encoding='utf-8') as f:
    data = json.load(f)

# Wstawienie danych do kolekcji
collection.insert_many(data)

print("Dane zostały załadowane do MongoDB.")

pipeline = [
    {"$group": {"_id": "$Region", "TotalSales": {"$sum": "$Sales"}}},
    {"$sort": {"TotalSales": -1}}
]

results = collection.aggregate(pipeline)

# Wyświetl wyniki
for r in results:
    print(r)


