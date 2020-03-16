import mysql.connector
from pymongo import MongoClient
from foreign_key_links import *

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port='3307', database='huwebshop')
cursor = cnx.cursor()

limit = 100
client = MongoClient('localhost', 27017)
db = client["huwebshop"]


def get_values(normalized, collection, values, get_fk):
    counter = 0
    normalized_list = []
    upload_values = []
    for entry in collection.find():
        try:
            upload = [counter]
            for value in values:
                if "-" in value:
                    vl = value.split("-")
                    upload.append(entry[vl[0]][vl[1]])
                elif value == "?":
                    upload.append(get_fk(entry))
                else:
                    upload.append(entry[value])

            if not [i for i in upload if i in normalized_list] and normalized or not normalized:
                upload_values.append(tuple(upload))
                normalized_list.extend(upload)
                counter += 1
        except KeyError:
            continue

        if counter == limit and not normalized:
            break

    return upload_values


def set_values(table_name, upload_values, headers):
    parameters = ""
    value_string = ""
    for header in headers:
        parameters += header + ","
        value_string += "%s,"
    parameters = parameters[:-1]
    value_string = value_string[:-1]

    cursor.executemany(f"INSERT INTO {table_name}({parameters}) VALUES ({value_string})", upload_values)
    cnx.commit()


def create_table(normalized, table_name, db_name, values, headers, get_fk=None):
    collection = db[db_name]
    cursor.execute(f"DELETE FROM {table_name};")

    upload_values = get_values(normalized, collection, values, get_fk)
    set_values(table_name, upload_values, headers)

    print(f"{table_name} table uploaded using {db_name} database.")


if __name__ == "__main__":
    get_buids()
    create_table(True, "gender", "products", ["gender"], ["idgender", "gendernaam"])
    create_table(True, "brand", "products", ["brand"], ["idbrand", "brandnaam"])
    create_table(False, "profile", "profiles", ["order-count", "recommendations-segment"], ["id", "`order.count`", "recmonendation_segment"])
    create_table(False, "session", "sessions", ["has_sale", "segment", "?"], ["id", "has_sale", "preferences", "profile_id"], link_profile_session)

    cnx.close()
