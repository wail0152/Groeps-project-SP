from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["huwebshop"]
collection = db["profiles"]

key_lst = []
for profile in collection.find():
    for key, value in profile.items():
        if key not in key_lst:
            key_lst.append(key)

print(key_lst)
