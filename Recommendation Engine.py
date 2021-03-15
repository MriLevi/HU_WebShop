import psycopg2
from psycopg2 import sql
from operator import itemgetter

#connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=Levidov123')

#cursor
cur = con.cursor()
#default index positions
categories_to_index_dict = {'id':0,'name':1,'brand':2,'category':3,'description':4,'fast_mover':5,'herhaalaankopen':6, 'selling_price':7,'doelgroep':8, 'sub_category':9, 'sub_sub_category':10}
#convert categories to their corresponding SQL column type
categories_to_column_type = {'id':'VARCHAR(255)', 'name':'VARCHAR(255)','brand':'VARCHAR(255)','category':'VARCHAR(255)','description':'VARCHAR(10000)','fast_mover':'boolean','herhaalaankopen':'boolean', 'selling_price':'integer','doelgroep':'VARCHAR(255)', 'sub_category':'VARCHAR(255)', 'sub_sub_category':'VARCHAR(255)'}

def new_intelligence_table(table, filters=[]):
    '''This function creates a new table for the given filters
        As name it joins all the given filters together
        it adds the product_id as primary key, and creates recommendation1 through 4
        it also adds a foreign key constraint to product_id'''
    tablename = ''.join(x for x in filters)

    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))
    if not cur.fetchone()[0]:
        # this the create table statements with {} placeholders for sql.SQL, this works better than string formatting especially for table names
        create_statement = "CREATE TABLE {} ({} VARCHAR(255) NOT NULL PRIMARY KEY, recommendation1 VARCHAR(255), recommendation2 VARCHAR(255), " \
                           "recommendation3 VARCHAR(255), recommendation4 VARCHAR(255))"

        # this is the foreign key statements, again with {} placeholders sql.SQL
        foreign_key_statement = "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} (_id)"

        # try to create the table, print exception if anything goes wrong (table already exists most commonly)
        try:
            cur.execute(sql.SQL(create_statement).format(sql.Identifier(tablename), sql.Identifier(table + "_id")))
            con.commit()
            print(f'Succesfully created table with the name {tablename}')
        except Exception as e:
            print(e)
            con.rollback()
        try:
            cur.execute(
                sql.SQL(foreign_key_statement).format(sql.Identifier(tablename),
                                                      sql.Identifier(f'FK{tablename[0:8]}1234'),
                                                      sql.Identifier(table + "_id"), sql.Identifier(table)))
            con.commit()
            print(f'Succesfully created foreign key')
        except Exception as e:
            print(e)
            con.rollback
    else:
        return print('tabel bestaat al')


def get_similar_product(productid, filters=[]):
    '''
    This function gets similar products based on a list of filters.
    The filters are a list of columns in the SQL table "product".
    when given the list ["category", "brand", "doelgroep"], this script will try to match as many fields as possible
    and fill a newly made table with 4 recommendations based on these three filters.
    If not all filters are present, it will still fill in the table but based on less information.
    '''
    tablename = ''.join(x for x in filters)
    productid = str(productid)

    #check of tabel al bestaat:
    new_intelligence_table("product", filters)

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

# cur.execute("SELECT * FROM product")
# allproducts = cur.fetchall()
# for i in allproducts:
#     get_similar_product(i[0], ['category', 'brand', 'doelgroep'])

def get_similar_profile_viewed_before(profile_id):
    cur.execute("SELECT * FROM viewed_before WHERE profile_id = %s", (profile_id,))
    results = cur.fetchall()
    productlist = []

    for i in results:
        productlist.append(i[1]) # append the viewed_before product to a list
    print(productlist)

    recommended_dict = {}
    for product in productlist:  # select profiles that have also viewed the product before
        cur.execute("SELECT profile_id FROM viewed_before WHERE product_id = %s", (product,))
        also_looked_at = cur.fetchall()
        count_product_dict = {}
        for profile in also_looked_at:
            cur.execute("SELECT product_id FROM viewed_before WHERE profile_id = %s", (profile,))
            viewed_before = cur.fetchall()
            for k in viewed_before:
                if k[0] in productlist:
                    continue
                else:
                    count_product_dict[k[0]] = count_product_dict.get (k[0], 0) + 1
        print(count_product_dict)
        for key, value in sorted(count_product_dict.items(), key=itemgetter(1), reverse=True):
            recommended_dict[key] = recommended_dict.get(key, 0) + value
    sorteddict = sorted(recommended_dict.items(), key=itemgetter(1), reverse=True)

    new_intelligence_table(profile, ["viewed_before", "recommendations"])


    cur.execute("INSERT INTO viewed_beforerecommendations (product_id, recommendation1, recommendation2, recommendation3, recommendation4) VALUES (%s, %s, %s, %s, %s)")





get_similar_product(9555, ['category', 'doelgroep'])

get_similar_profile_viewed_before("5a393d68ed295900010384ca")