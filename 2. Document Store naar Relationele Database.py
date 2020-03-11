import mysql.connector
from pymongo import MongoClient

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port='3307', database='huwebshop')
cursor = cnx.cursor()

limit = 100
client = MongoClient('localhost', 27017)
db = client["huwebshop"]


def make_normalized_table(table_name, db_name, search_value, id, collumn1):
    collection = db[db_name]
    cursor.execute(f"DELETE FROM {table_name};")

    counter = 0
    normalized_list = []
    upload_values = []
    for entry in collection.find():
        try:
            if entry[search_value] not in normalized_list:
                upload_values.append(tuple([counter, entry[search_value]]))
                normalized_list.append(entry[search_value])
                counter += 1
        except KeyError:
            continue

    cursor.executemany(f"INSERT INTO {table_name}({id}, {collumn1}) VALUES (%s, %s)", upload_values)
    cnx.commit()
    print(f"{table_name} table uploaded using {db_name} database.")


def create_table(table_name, db_name, values, headers):
    collection = db[db_name]
    cursor.execute(f"DELETE FROM {table_name};")

    counter = 0
    upload_values = []
    for entry in collection.find():
        try:
            upload = [counter]
            for value in values:
                if "-" in value:
                    vl = value.split("-")
                    upload.append(entry[vl[0]][vl[1]])
                else:
                    upload.append(entry[value])
            upload_values.append(tuple(upload))
            counter += 1
        except KeyError:
            continue

        if counter == limit:
            break

    parameters = ""
    value_string = ""
    for header in headers:
        parameters += header + ","
        value_string += "%s,"
    parameters = parameters[:-1]
    value_string = value_string[:-1]

    cursor.executemany(f"INSERT INTO {table_name}({parameters}) VALUES ({value_string})", upload_values)
    cnx.commit()
    print(f"{table_name} table uploaded using {db_name} database.")


make_normalized_table("gender", "products", "gender", "idgender", "gendernaam")
make_normalized_table("brand", "products", "brand", "idbrand", "brandnaam")
create_table("profile", "profiles", ["order-count", "recommendations-segment"], ["id", "`order.count`", "recmonendation_segment"])

cnx.close()
