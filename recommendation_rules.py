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


def similar_products(product_id, attributes):
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
    for product_id in product_ids:
        upload_values.append(list(product_id) + similar_products(product_id[0], ["subsubcategory", "targetaudience"]))

    cur.executemany(f"""INSERT INTO recommendations (id, product1, product2, 
                        product3, product4)
                        VALUES (%s, %s, %s, %s, %s)""", upload_values)


def similar_profile(profile_id, attributes):
    # Making the attribute in a format for the select statements.
    attribute_string = ""
    for attribute in attributes:
        attribute_string += attribute + ","
    attribute_string = attribute_string[:-1]

    # Check for similar profiles
    cur.execute(f"""select {attribute_string} from sessions where profid = '{profile_id}';""")
    info = list(cur.fetchall()[0])

    # Pick random profile
    conditions = ""
    for index in range(len(info)):
        conditions += attributes[index] + " = " + "'" + info[index] + "'"
        if index < len(info) - 1:
            conditions += " and "

    cur.execute(f"""select profid from sessions where profid != '{profile_id}' and {conditions} and sale = true;""")
    profiles = cur.fetchall()
    rand_profile = [profile[0] for profile in random.sample(profiles, k=1)]

    # Pick random product out of profile
    cur.execute(f"""select prodid from profiles_previously_viewed where profid = '{rand_profile[0]}';""")
    products_bought_ids = cur.fetchall()
    rand_product_id = [product_id[0] for product_id in random.sample(products_bought_ids, k=1)]

    # return db similar product id
    return similar_products(rand_product_id[0], ["subsubcategory", "targetaudience"])


def get_recommendation(profile_id):
    cur.execute(f"""select sale from sessions where profid = '{profile_id}' and sale = true;""")    # Check if profile id has a sale in the sessions.
    has_sale = cur.fetchall()
    print("has sale:", has_sale != [])

    if has_sale:    # if has_sale choose random product in the session and get a similar product via the database recommendation table.
        cur.execute(f"""select prodid from profiles_previously_viewed where profid = '{profile_id}';""")
        products_bought_ids = cur.fetchall()
        rand_product_id = [product_id[0] for product_id in random.sample(products_bought_ids, k=1)]
        return similar_products(rand_product_id[0], ["subsubcategory", "targetaudience"])
    else:   # else check for a similar profile.
        return similar_profile(profile_id, ["devicetype", "os"])


# insert_recommendations()  #TODO: let this run once and then comment it out.
print(get_recommendation("5a2df924a56ac6edb4feab38"))
# (has sale) 5a2df924a56ac6edb4feab38
# (doesn't have sale) 5ad7b06d89518600011831d0

c.commit()
cur.close()
c.close()