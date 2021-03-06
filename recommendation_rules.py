import psycopg2
import random

c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor()


def create_recommendation_table(table_name):
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cur.execute(f"""CREATE TABLE {table_name} (id VARCHAR PRIMARY KEY, product1 VARCHAR,
                    product2 VARCHAR, product3 VARCHAR, product4 VARCHAR);""")


def insert_recommendations(search_value, select_table, insert_table, func, max_amount):
    create_recommendation_table(insert_table)
    cur.execute(f"""select {search_value} from {select_table};""")
    pids = cur.fetchall()

    # Getting all the ids of the similar products and uploading them.
    upload_values = []
    counter = 0
    limit = max_amount
    for pid in pids:
        upload_values.append(list(pid) + func(pid[0].replace("'", "''"), ["subsubcategory", "targetaudience"]))
        counter += 1
        if counter >= limit != -1:
            break

    cur.executemany(f"""INSERT INTO {insert_table} (id, product1, product2, product3, product4)
                            VALUES (%s, %s, %s, %s, %s)""", upload_values)
    print(f"{counter} entries inserted into the {insert_table} table.")


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
        except (TypeError, AttributeError):  # One or more of the attributes are None.
            return None
    return cond


def get_recommendation_products(profile_id):
    cur.execute(f"""select prodid from profiles_previously_viewed where profid = '{profile_id}';""")
    bought_products_ids = cur.fetchall()

    # This is to account for the imperfect data.
    if not bought_products_ids:
        return similar_profile(profile_id, ["devicetype", "os"])

    rand_product_id = [product_id[0] for product_id in random.sample(bought_products_ids, k=1)]
    return similar_products(rand_product_id[0], ["subsubcategory", "targetaudience"])


def get_recommendation(profile_id, placeholder):
    # Check if profile id has a sale in the sessions.
    cur.execute(f"""select sale from sessions where profid = '{profile_id}' and sale = true;""")
    has_sale = cur.fetchall()

    # if has_sale choose random product with profile id and get a similar product via the database recommendation table
    # else check for a similar profile.
    return get_recommendation_products(profile_id) if has_sale else similar_profile(profile_id, ["devicetype", "os"])


def get_recommendation_from_table(profile_id):
    cur.execute(f"""select * from profile_recommendations where id = '{profile_id}';""")
    recommendation = cur.fetchall()
    return list(recommendation[0][1:])


# TODO: let these two lines run once and then comment them out.
insert_recommendations("id", "products", "product_recommendations", similar_products, -1)
insert_recommendations("profid", "sessions", "profile_recommendations", get_recommendation, 100)
# [debug purpose] (has sale: 59dce84fa56ac6edb4cd0e88) (doesn't have sale: 5a4a280ca8256100017f1243).
print(get_recommendation_from_table("59dce84fa56ac6edb4cd0e88"))  # or use get_recommendation() to get generate.


c.commit()
cur.close()
c.close()
