from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["huwebshop"]
collection = db["products"]

# opdracht 1
print(collection.find_one())

# opdracht 2
for product in collection.find():
    if product["name"][0] == "R":
        print(product)
        break

# opdracht 3
total = 0
count = 0
for product in collection.find():
    try:
        total += product["price"]["mrsp"] / 100
        count += 1
    except KeyError:
        continue
print(total / count)
