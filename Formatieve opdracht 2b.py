# Verkeerd!

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

    # Making the create table part dynamically with the arguments that are provided and excuting it.
    cursor_arg = f"CREATE TABLE {table_name}("
    for arg in args:
        cursor_arg += arg.replace("-", "_") if "-" in arg else arg
    cursor_arg += "ENGINE=InnoDB"
    cursor.execute(cursor_arg)

    # Making the insert table part dynamically with the arguments that are provided.
    command_arg = f"INSERT INTO {table_name}("
    for arg in range(len(args)):
        sep = "," if arg < len(args) - 1 else ")"
        correct_arg = args[arg].replace("-", "_") if "-" in args[arg] else args[arg]
        command_arg += correct_arg.split()[0] + sep
    val_s = "VALUES (" + "%s," * len(args) + ")"
    val_s = val_s[:-2] + val_s[-1]  # Removing the last comma to make the statement working.
    command = (command_arg + val_s)

    # Making the values part dynamically with the arguments that are provided.
    values = []
    counter = 0
    for entry in collection.find():
        value_list = []
        for arg in args:
            if "-" in arg:  # Checking if the argument is a double argument
                nested_arg = arg.split()[0].split("-")
                try:
                    value_list.append(entry[nested_arg[0]][nested_arg[1]])
                except KeyError:
                    value_list.append(None)
            else:
                try:
                    value_list.append(str(entry[arg.split()[0]]))
                except KeyError:
                    value_list.append(None)

        values.append(tuple(value_list))

        counter += 1
        if counter == limit:
            break

    for value in values:
        for ind in value:
            if type(ind) == list:
                print(value)
    cursor.executemany(command, values)
    print(f"{counter} entries of {table_name} uploaded.")


create_table("profiles", "_id VARCHAR(45) PRIMARY KEY,", "order-count VARCHAR(45))")

# create_table("sessions", "_id VARCHAR(45) PRIMARY KEY,", "has_sale VARCHAR(45),", "prefences VARCHAR(45))")

# create_table("products", "_id VARCHAR(45) PRIMARY KEY,", "category char(43),", "sub_category char(25),",
#             "sub_sub_category char(33),", "gender char(15),", "brand char(26),", "price-selling_price int)")

cnx.commit()
cnx.close()
