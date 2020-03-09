import mysql.connector
from pymongo import MongoClient


limit = 100
client = MongoClient('localhost', 27017)
db = client["huwebshop"]
collection = db["products"]

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port='3307', database='huwebshop')
cursor = cnx.cursor()

cursor.execute("DROP TABLE IF EXISTS products;")

cursor.execute("CREATE TABLE products"
                "(id char(24) PRIMARY KEY,"
                "category char(24),"
                "sub_category char(24),"
                "sub_sub_category char(24),"
                "gender char(8),"
                "brand char(24),"
                "mrsp int(10))"
                "ENGINE=InnoDB")

command = ("INSERT INTO products (id, category, sub_category, sub_sub_category, gender, brand, mrsp) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s)")
values = []

counter = 0
for products in collection.find():
    values.append((str(products["_id"]), str(products["category"]), str(products["sub_category"]),
                   str(products["sub_sub_category"]), str(products["gender"]), str(products["brand"]),
                   str(products["price"]["mrsp"])))
    counter += 1
    if counter == limit:
        print(f"{counter} entries uploaded.")
        break

cursor.executemany(command, values)

cnx.commit()
cnx.close()
