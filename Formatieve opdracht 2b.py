import mysql.connector
from pymongo import MongoClient

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port='3307', database='huwebshop')
cursor = cnx.cursor()

limit = 100
client = MongoClient('localhost', 27017)
db = client["huwebshop"]


def create_table(table_name, *args):
    collection = db[table_name]
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Making the create table part dynamically with the arguments that are provided.
    cursor_arg = f"CREATE TABLE {table_name}("
    for arg in args:
        cursor_arg += arg
    cursor_arg += "ENGINE=InnoDB"
    cursor.execute(cursor_arg)

    # Making the insert table part dynamically with the arguments that are provided.
    command_arg = f"INSERT INTO {table_name}("
    for arg in range(len(args)):
        sep = "," if arg < len(args) - 1 else ")"
        command_arg += args[arg].split()[0] + sep
    val_s = "VALUES (" + "%s," * len(args) + ")"
    val_s = val_s[:-2] + val_s[-1]  # Removing the last comma to make the statement working.
    command = (command_arg + val_s)

    values = []
    counter = 0
    for entry in collection.find():
        value_list = []
        for arg in args:
            value_list.append(entry[arg.split()[0]])
        values.append(tuple(value_list))

        counter += 1
        if counter == limit:
            print(f"{counter} entries of {table_name} uploaded.")
            break

    cursor.executemany(command, values)


create_table("products", "_id char(28) PRIMARY KEY,", "category char(43),", "sub_category char(25),", "sub_sub_category char(33),",
             "gender char(15),", "brand char(26))")

cnx.commit()
cnx.close()
