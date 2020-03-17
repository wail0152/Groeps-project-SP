import psycopg2
import random

c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor()

cur.execute("DROP TABLE IF EXISTS recommendations CASCADE")

cur.execute("""CREATE TABLE recommendations
                (id VARCHAR PRIMARY KEY,
                 product1 VARCHAR,
                 product2 VARCHAR,
                 product3 VARCHAR,
                 product4 VARCHAR);""")


def similar(product_id, attributes):
    # Making the attribute in a format for the select statements.
    attribute_string = ""
    for attribute in attributes:
        attribute_string += attribute + ","
    attribute_string = attribute_string[:-1]

    product_id = product_id.replace("'", "''")  # replacing single quote with double for the query.
    cur.execute(f"""select {attribute_string} from products where id = '{product_id}';""")
    product = list(cur.fetchall()[0])

    # Storing all the properties that has to be checked and matched.
    check_properties = []
    for property in product:
        check_properties.append(property)

    # Making the where conditions for the select statement.
    conditions = ""
    for index in range(len(check_properties)):
        try:
            check_properties[index] = check_properties[index].replace("'", "''")    # replacing single quote with double for the query.
            conditions += attributes[index] + " = " + "'" + check_properties[index] + "'"
            if index < len(check_properties) - 1:
                conditions += " and "
        except (TypeError, AttributeError):   # One or more of the attributes are None.
            return [product_id, product_id, product_id, product_id]

    # Fetching all the id's of 'similar' products (similar is easily definable by changing the attributes parameter).
    cur.execute(f"""select id from products where id != '{product_id}' and {conditions};""")
    similar_products = cur.fetchall()

    # Returning 4 random product id's (using sample instead of choices to avoid doubles).
    try:
        return [product[0] for product in random.sample(similar_products, k=4)]
    except ValueError:  # Not enough recommendations
        return [product_id, product_id, product_id, product_id]


def insert_recommendations():
    cur.execute("""select id from products;""")
    product_ids = cur.fetchall()

    # Getting all the ids of the similar products
    upload_values = []
    for id in product_ids:
        upload_values.append(list(id) + similar(id[0], ["subsubcategory", "targetaudience"]))

    cur.executemany(f"""INSERT INTO recommendations (id, product1, product2, 
                        product3, product4)
                        VALUES (%s, %s, %s, %s, %s)""", upload_values)


insert_recommendations()

c.commit()
cur.close()
c.close()
