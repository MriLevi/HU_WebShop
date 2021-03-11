import psycopg2
from psycopg2 import sql

#connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=Levidov123')

#cursor
cur = con.cursor()
#default index positions
categories_to_index_dict = {'id':0,'name':1,'brand':2,'category':3,'description':4,'fast_mover':5,'herhaalaankopen':6, 'selling_price':7,'doelgroep':8, 'sub_category':9, 'sub_sub_category':10}
categories_to_column_type = {'id':'VARCHAR(255)', 'name':'VARCHAR(255)','brand':'VARCHAR(255)','category':'VARCHAR(255)','description':'VARCHAR(10000)','fast_mover':'boolean','herhaalaankopen':'boolean', 'selling_price':'integer','doelgroep':'VARCHAR(255)', 'sub_category':'VARCHAR(255)', 'sub_sub_category':'VARCHAR(255)'}

def new_intelligence_table(filters=[]):
    '''This function creates a new table for the given filters
        As name it joins all the given filters together
        it adds the product_id as primary key, and creates recommendation1 through 4
        it also adds a foreign key constraint to product_id'''

    tablename = ''.join(x for x in filters) #create table name

    #this the create table statements with {} placeholders for sql.SQL, this works better than string formatting especially for table names
    create_statement = "CREATE TABLE {} ({} VARCHAR(255) NOT NULL PRIMARY KEY, recommendation1 VARCHAR(255), recommendation2 VARCHAR(255), " \
                       "recommendation3 VARCHAR(255), recommendation4 VARCHAR(255))"

    #this is the foreign key statements, again with {} placeholders sql.SQL
    foreign_key_statement = "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} (_id)"

    #try to create the table, print exception if anything goes wrong (table already exists most commonly)
    try:
        cur.execute(sql.SQL(create_statement).format(sql.Identifier(tablename), sql.Identifier("product_id")))
        con.commit()
        print(f'Succesfully created table with the name {tablename}')
    except Exception as e:
        print(e)
        con.rollback()
    try:
        cur.execute(
            sql.SQL(foreign_key_statement).format(sql.Identifier(tablename), sql.Identifier(f'FK{tablename[0:8]}1234'),
                                                  sql.Identifier("product_id"), sql.Identifier("product")))
        con.commit()
        print(f'Succesfully created foreign key')
    except Exception as e:
        print(e)
        con.rollback

def get_similar_product(productid, filters=[]):
    tablename = ''.join(x for x in filters)
    productid = str(productid)

    try:
        new_intelligence_table(filters)
    except:
        print('bestaat al')


    cur.execute("SELECT * FROM product WHERE _id = %s",(productid,))
    product_id_data = cur.fetchone()
    sqlquery = "SELECT * FROM product WHERE"
    filterlist = []

    for filter in filters:
        if product_id_data[categories_to_index_dict[filter]] != None:
            sqlquery += f' {filter} = %s AND'
            filterlist.append(product_id_data[categories_to_index_dict[filter]])
    sqlquery = sqlquery[:-4]
    print(sqlquery)
    filtertuple = tuple(filterlist)
    print(filtertuple)

    try:
        cur.execute(sqlquery,filtertuple)
    except Exception as e:
        print(e)
        con.rollback

    similar_products_data = cur.fetchall()
    insertstatement = "INSERT INTO {} (product_id, recommendation1, recommendation2, recommendation3, recommendation4) VALUES (%s, %s, %s, %s, %s)"

    if len(similar_products_data) > 3:
        try:
            cur.execute(sql.SQL(insertstatement).format(sql.Identifier(tablename)), (productid, similar_products_data[1][0], similar_products_data[2][0], similar_products_data[3][0], similar_products_data[4][0]))
            con.commit()
        except Exception as e:
            print(f'exception by inserten: {e}')
            con.rollback()
    cur.close()
get_similar_product(2554, ['category', 'brand', 'doelgroep'])