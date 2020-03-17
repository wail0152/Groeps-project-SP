import psycopg2

c = psycopg2.connect("dbname=postgres user=postgres password=postgres")
cur = c.cursor()

cur.execute("DROP TABLE IF EXISTS recommendations CASCADE")

cur.execute("""CREATE TABLE recommendations
                (id VARCHAR PRIMARY KEY,
                 product1 VARCHAR,
                 product2 VARCHAR,
                 product3 VARCHAR,
                 product4 VARCHAR);""")


def similar(attributes, sellect_statements):
    attribute_string = ""
    for attribute in attributes:
        attribute_string += attribute + ","
    attribute_string = attribute_string[:-1]

    sellect_string = ""
    for sellect_statement in sellect_statements:
        sellect_string += sellect_statement

    cur.execute(f"""select {attribute_string} from products where {sellect_statement};""")
    sellection = cur.fetchall()

    for item in sellection:
        print(item)


similar(["id", "name"], ["subsubcategory = 'Deodorant' and ", "targetaudience = 'Mannen'"])

c.commit()
cur.close()
c.close()
