import mysql.connector
from pymongo import MongoClient


limit = 100
client = MongoClient('localhost', 27017)
db = client["huwebshop"]
collection = db["profiles"]

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port='3307', database='huwebshop')
cursor = cnx.cursor()

cursor.execute("DROP TABLE IF EXISTS profiles;")

cursor.execute("CREATE TABLE profiles"
                "(int_id int(11) PRIMARY KEY,"
                "id char(24))"
                "ENGINE=InnoDB")

command = ("INSERT INTO profiles (int_id, id) VALUES (%s, %s)")
values = []

counter = 0
for profiles in collection.find():
    values.append((counter, str(profiles["_id"])))
    counter += 1
    if counter == limit:
        print(f"{counter} entries uploaded.")
        break

cursor.executemany(command, values)

cnx.commit()
cnx.close()
