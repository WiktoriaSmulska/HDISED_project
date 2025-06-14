import json
import pymongo
from datetime import datetime

# Połączenie z MongoDB lokalnie
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["hurtownia_danych"]
collection = db["zamowienia"]

# Wczytanie danych z pliku JSON
with open('../data/orders.json', encoding='utf-8') as f:
    data = json.load(f)

for d in data:
    d["Order Date"] = datetime.strptime(d["Order Date"], "%d/%m/%Y")
    d["Ship Date"] = datetime.strptime(d["Ship Date"], "%d/%m/%Y")

# Wstawienie danych do kolekcji
collection.delete_many({})
collection.insert_many(data)

print("Dane zostały załadowane do MongoDB.")


print("\n1. Sprzedaż według regionu: ")
pipeline_sales = [
    {"$group": {"_id": "$Region", "Total Sales": {"$sum": "$Sales"}}},
    {"$sort": {"Total Sales": -1}}
]
for r in collection.aggregate(pipeline_sales):
    print(r)

print("\n2. Top 5 produktów według sprzedaży: ")
pipeline_top = [
    {"$group": {"_id": "$Product Name", "Total Sales": {"$sum": "$Sales"}}},
    {"$sort": {"Total Sales": -1}},
    {"$limit": 5}
]
for r in collection.aggregate(pipeline_top):
    print(r)

print("\n3. Sprzedaż miesięczna: ")
pipline_month = pipeline_monthly = [
    {"$group": {"_id": {"year": {"$year": "$Order Date"}, "month":{"$month": "$Order Date"}}, "Total Sales": {"$sum": "$Sales"}}},
    {"$sort": {"_id.year": -1, "_id.month": 1}}
]
for r in collection.aggregate(pipline_month):
    print(r)

print("\n4. Liczba zamówień według segemntu klienta: ")
pipeline_segment = [
    {"$group": {"_id": "$Segment", "Orders Count": {"$sum": 1}}},
    {"$sort": {"Order Count": -1}}
]
for r in collection.aggregate(pipeline_segment):
    print(r)

print("\n5. Średnia wartość zamównienia: ")
pipeline_avg = [
    {"$group": {"_id": None, "AvgSale": {"$avg": "$Sales"}}}
]
for avg in collection.aggregate(pipeline_avg):
    print(avg)

print("\n6. Łączna sprzedaż w każdej kategori: ")
pipeline_category = [
    {"$group": {"_id": "$Category", "TotalSales": {"$sum": "$Sales"}}},
    {"$sort": {"TotalSales": -1}}
]
for r in collection.aggregate(pipeline_category):
    print(r)

print("\n 7. Top 3 miasta z najwyższą sprzedażą: ")
pipeline_top_city = [
    {"$group": {"_id": "$City", "TotalSales": {"$sum": "$Sales"}}},
    {"$sort": {"TotalSales": -1}},
    {"$limit": 3}
]
for r in collection.aggregate(pipeline_top_city):
    print(r)

print("\n8. Śreni czas realizacji zamówienia: ")
pipeline_avg_ship_date = [
    {"$project": {"Ship time": {"$divide": [{"$subtract": ["$Ship Date", "$Order Date"]},1000 * 60 * 60 * 24]}}},
    {"$group": {"_id": None,"Average Time": {"$avg": "$Ship time"}}}
]
for r in collection.aggregate(pipeline_avg_ship_date):
    print(r)

print("\n9. Top 5 klientów według wartości zamówień: ")
pipeline_top_klienci = [
    {"$group": {"_id": "$Customer Name", "TotalSales": {"$sum": "$Sales"}}},
    {"$sort": {"TotalSales": -1}},
    {"$limit": 5}
]
for r in collection.aggregate(pipeline_top_klienci):
    print(r)

print("\n 10. Liczba zamówień złożonych w każdym roku: ")
pipeline_zamowienia_na_rok = [
    {"$group": {"_id": {"year": {"$year": "$Order Date"}},"Orders Number": {"$sum": 1}}},
    {"$sort": {"_id.year": -1}}
]
for r in collection.aggregate(pipeline_zamowienia_na_rok):
    print(r)