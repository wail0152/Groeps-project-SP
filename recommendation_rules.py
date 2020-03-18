import psycopg2
import random

c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor()

cur.execute("DROP TABLE IF EXISTS recommendations CASCADE")
cur.execute("""CREATE TABLE recommendations (id VARCHAR PRIMARY KEY, product1 VARCHAR, 
                product2 VARCHAR, product3 VARCHAR, product4 VARCHAR);""")


def insert_recommendations():
    cur.execute("""select id from products;""")
    pids = cur.fetchall()

    # Getting all the ids of the similar products
    upload_values = [list(pid) + similar_products(pid[0].replace("'", "''"), ["subsubcategory", "targetaudience"]) for pid in pids]
    cur.executemany(f"""INSERT INTO recommendations (id, product1, product2, product3, product4)
                        VALUES (%s, %s, %s, %s, %s)""", upload_values)


def similar_products(product_id, attributes):
    cur.execute(f"""select {get_attributes_query(attributes)} from products where id = '{product_id}';""")
    product = list(cur.fetchall()[0])

    # Fetching all the id's of 'similar' products (similar is easily definable by changing the attributes parameter).
    check_properties = [check_value for check_value in product]
    conditions = get_conditions_query(attributes, check_properties)
    if conditions is None:
        return [product_id, product_id, product_id, product_id]
    cur.execute(f"""select id from products where id != '{product_id}' and {conditions};""")
    matching_products = cur.fetchall()

    try:    # Returning 4 random product id's (using sample instead of choices to avoid doubles).
        return [product[0] for product in random.sample(matching_products, k=4)]
    except ValueError:  # Not enough recommendations
        return [product_id, product_id, product_id, product_id]


def similar_profile(profile_id, attributes):
    # Check for similar profiles
    cur.execute(f"""select {get_attributes_query(attributes)} from sessions where profid = '{profile_id}';""")
    info = list(cur.fetchall()[0])

    # Pick random profile
    conditions = get_conditions_query(attributes, info)
    cur.execute(f"""select profid from sessions where profid != '{profile_id}' and {conditions} and sale = true;""")
    profiles = cur.fetchall()
    rand_profile = [profile[0] for profile in random.sample(profiles, k=1)]

    return get_recommendation_products(rand_profile[0])


def get_attributes_query(attributes):
    # Making the attribute in a format for the select statements.
    attribute_string = ""
    for attribute in attributes:
        attribute_string += attribute + ","
    attribute_string = attribute_string[:-1]
    return attribute_string


def get_conditions_query(attributes, info):
    cond = ""
    for index in range(len(info)):
        try:
            info[index] = info[index].replace("'", "''")  # replacing single quote with double for the query.
            cond += attributes[index] + " = " + "'" + info[index] + "'"
            if index < len(info) - 1:
                cond += " and "
        except (TypeError, AttributeError): # One or more of the attributes are None.
            return None
    return cond


def get_recommendation_products(profile_id):
    cur.execute(f"""select prodid from profiles_previously_viewed where profid = '{profile_id}';""")
    bought_products_ids = cur.fetchall()
    rand_product_id = [product_id[0] for product_id in random.sample(bought_products_ids, k=1)]
    return similar_products(rand_product_id[0], ["subsubcategory", "targetaudience"])


def get_recommendation(profile_id):
    # Check if profile id has a sale in the sessions.
    cur.execute(f"""select sale from sessions where profid = '{profile_id}' and sale = true;""")
    has_sale = cur.fetchall()
    print("has sale:", has_sale != [])

    # if has_sale choose random product with profile id and get a similar product via the database recommendation table
    # else check for a similar profile.
    return get_recommendation_products(profile_id) if has_sale else similar_profile(profile_id, ["devicetype", "os"])


insert_recommendations()  #TODO: let this run once and then comment it out.
# [debug purpose] (has sale: 5a2df924a56ac6edb4feab38) (doesn't have sale: 5ad7b06d89518600011831d0).
# print(get_recommendation("5ad7b06d89518600011831d0"))

c.commit()
cur.close()
c.close()
