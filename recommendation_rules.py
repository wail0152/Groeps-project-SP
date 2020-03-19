import psycopg2
import random

c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor()
counter = 0
limit = 2100000
profile_to_sale = {}
products_to_profiles = {}


def create_recommendation_table(table_name):
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cur.execute(f"""CREATE TABLE {table_name} (id VARCHAR PRIMARY KEY, product1 VARCHAR,
                    product2 VARCHAR, product3 VARCHAR, product4 VARCHAR);""")


def get_profiles_sales():
    global profile_to_sale
    cur.execute(f"""select profid, sale from sessions;""")
    sales = cur.fetchall()
    for sale in sales:
        profile_to_sale[sale[0]] = sale[1]


def get_bought_products():
    global products_to_profiles
    cur.execute(f"""select profid, prodid from profiles_previously_viewed;""")
    bought_products_ids = cur.fetchall()
    for bought_products_id in bought_products_ids:
        products_to_profiles[bought_products_id[0]] = bought_products_id[1]


def insert_recommendations(select_table, insert_table, similar_def):
    global counter
    # create_recommendation_table(insert_table)
    cur.execute(f"""select id from {select_table};""")
    pids = cur.fetchall()

    # Getting all the ids of the similar products and uploading them.
    upload_values = []
    for pid in pids:
        print(counter)
        upload_values.append(list(pid) + similar_def(pid[0].replace("'", "''"), ["subsubcategory", "targetaudience"]))
        counter += 1
        if counter >= limit:
            break

    cur.executemany(f"""INSERT INTO {insert_table} (id, product1, product2, product3, product4)
                            VALUES (%s, %s, %s, %s, %s)""", upload_values)
    print(f"Inserted the data of the {insert_table} table.")


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
    temp = cur.fetchall()
    properties_to_match = list(temp[0])

    # Pick random profile
    conditions = get_conditions_query(attributes, properties_to_match)
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
    # This is to account for the imperfect data.
    if profile_id not in products_to_profiles:
        return similar_profile(profile_id, ["devicetype", "os"])

    bought_product_id = products_to_profiles[profile_id]
    return similar_products(bought_product_id, ["subsubcategory", "targetaudience"])


def get_recommendation(profile_id, placeholder):
    # Check if profile is empty and then return None.
    if profile_id not in profile_to_sale:
        return [profile_id, profile_id, profile_id, profile_id]

    # Check if profile id has a sale in the sessions.
    has_sale = profile_to_sale[profile_id]

    # if has_sale choose random product with profile id and get a similar product via the database recommendation table
    # else check for a similar profile.
    return get_recommendation_products(profile_id) if has_sale else similar_profile(profile_id, ["devicetype", "os"])


def get_recommendation_from_table(profile_id):
    cur.execute(f"""select * from profile_recommendations where id = '{profile_id}';""")
    recommendation = cur.fetchall()
    return list(recommendation[0][1:])


# TODO: let these four lines run once and then comment them out.
# insert_recommendations("products", "product_recommendations", similar_products)
get_profiles_sales()
get_bought_products()
insert_recommendations("profiles", "profile_recommendations", get_recommendation)
# [debug purpose] (has sale: 5a393d68ed295900010384ca) (doesn't have sale: 5a394bebed2959000103972f).
# print(get_recommendation_from_table("5a393d68ed295900010384ca"))


c.commit()
cur.close()
c.close()
