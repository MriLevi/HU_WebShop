import psycopg2
from psycopg2 import sql
from operator import itemgetter

# connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=Levidov123')

# cursor
cur = con.cursor()

# default index positions
categories_to_index_dict = {'id': 0, 'name': 1, 'brand': 2, 'category': 3, 'description': 4, 'fast_mover': 5,
                            'herhaalaankopen': 6, 'selling_price': 7, 'doelgroep': 8, 'sub_category': 9,
                            'sub_sub_category': 10}


def new_intelligence_table(table, filters=[]):
    '''This function creates a new table for the given filters
        As name it joins all the given filters together
        it adds the product_id as primary key, and creates recommendation1 through 4
        it also adds a foreign key constraint to product_id'''
    tablename = ''.join(x for x in filters)

    # first check if the table already exists, if not, create it.
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tablename,))

    # cur.fetchone()[0] either returns True or False, based on if the table exists.
    if not cur.fetchone()[0]:
        # this the create table statements with {} placeholders for sql.SQL, this works better than string formatting especially for table names
        create_statement = "CREATE TABLE {} ({} VARCHAR(255) NOT NULL PRIMARY KEY, recommendation1 VARCHAR(255), recommendation2 VARCHAR(255), " \
                           "recommendation3 VARCHAR(255), recommendation4 VARCHAR(255))"

        # this is the foreign key statement, again with {} placeholders for sql.SQL
        foreign_key_statement = "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} (_id)"

        # try to create the table, print exception if anything goes wrong (table already exists most commonly)
        try:
            cur.execute(sql.SQL(create_statement).format(sql.Identifier(tablename), sql.Identifier(table + "_id")))
            con.commit()
            print(f'Succesfully created table with the name {tablename}')
        except Exception as e:
            print(e)
            con.rollback()

        # create the foreign key
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
    productid = str(productid)  # sommige productids zijn ints, convert ze

    # make new table
    new_intelligence_table("product", filters)

    # get all the data for the productid that was entered into get_similar_product
    cur.execute("SELECT * FROM product WHERE _id = %s", (productid,))
    product_id_data = cur.fetchone()
    sqlquery = "SELECT * FROM product WHERE"
    filterlist = []

    # check if the filter has a value, if so, add it to the sqlquery to select products with that filter
    for filter in filters:
        if product_id_data[categories_to_index_dict[filter]] != None:
            sqlquery += f' {filter} = %s AND'
            filterlist.append(product_id_data[categories_to_index_dict[filter]])

    # check if we have selected any filters and either cut off "WHERE" or cut off " AND" from sqlquery
    if len(sqlquery) == 27:
        sqlquery = sqlquery[:-6]
    else:
        sqlquery = sqlquery[:-4]

    # convert list of filters to tuple to pass to cur.execute()
    filtertuple = tuple(filterlist)

    # select all products in product with the newly made set of filters
    try:
        cur.execute(sqlquery, filtertuple)
    except Exception as e:
        print(e)
        con.rollback

    # save all the similar product data in a list
    similar_products_data = cur.fetchall()

    #make the insert statement
    insertstatement = "INSERT INTO {} (product_id, recommendation1, recommendation2, recommendation3, recommendation4) VALUES (%s, %s, %s, %s, %s)"

    #if we have more than 3 similar products, insert the first four into the table and commit
    if len(similar_products_data) > 3:
        try:
            cur.execute(sql.SQL(insertstatement).format(sql.Identifier(tablename)), (
                productid, similar_products_data[1][0], similar_products_data[2][0], similar_products_data[3][0],
                similar_products_data[4][0]))
            con.commit()
        except Exception as e:
            print(f'exception by inserten: {e}')
            con.rollback()


# here we use get_similar_products with a rule to filter on category, brand and doelgroep to populate recommendations
# the recommendations get inserted into the "categorybranddoelgroep" table that gets created upon running this function.

# cur.execute("SELECT * FROM product")
# allproducts = cur.fetchall()
# for i in allproducts:
#     get_similar_product(i[0], ['category', 'brand', 'doelgroep'])


def get_similar_profile_viewed_before(profile_id):
    '''This function looks at the table viewed_before for a specified profile_id
        it then takes the viewed_before products for the specified profile and tries to find those products
        in other profiles in viewed_before.
        It then counts other products viewed alongside the given products,
        and finally inserts the 4 most "also viewed" products into a new table called viewed_beforerecommendations'''

    # select the given profile and fetch the data, save the data in a list
    cur.execute("SELECT * FROM viewed_before WHERE profile_id = %s", (profile_id,))
    results = cur.fetchall()
    productlist = []

    # loop through the data and extract the product_ids
    for i in results:
        productlist.append(i[1])

    #this dict is used to save the recommendations in, later on
    recommended_dict = {}

    # select profiles that have also viewed the product before, do this for each product of the original profile
    for product in productlist:
        cur.execute("SELECT profile_id FROM viewed_before WHERE product_id = %s", (product,))
        also_looked_at = cur.fetchall()
        count_product_dict = {}
        #loop over the profiles we just found, fetch the products and put them in a list
        for profile in also_looked_at:
            cur.execute("SELECT product_id FROM viewed_before WHERE profile_id = %s", (profile,))
            viewed_before = cur.fetchall()
            #for each product in viewed before, count it
            for k in viewed_before:
                if k[0] in productlist:
                    continue
                else:
                    count_product_dict[k[0]] = count_product_dict.get(k[0], 0) + 1
        # create a new dictionary from each individual count dictionary with unified counts
        for key, value in sorted(count_product_dict.items(), key=itemgetter(1), reverse=True):
            recommended_dict[key] = recommended_dict.get(key, 0) + value
    #sort this dict
    sorteddict = sorted(recommended_dict.items(), key=itemgetter(1), reverse=True)

    #create a new table
    new_intelligence_table("profile", ["viewed_before", "recommendations"])

    #create the insert statement and insert the 4 product ids with the highest counts
    insertstatement = "INSERT INTO viewed_beforerecommendations (profile_id, recommendation1, recommendation2, recommendation3, recommendation4) VALUES (%s, %s, %s, %s, %s)"
    insertvalues = (profile_id, sorteddict[0][0], sorteddict[1][0], sorteddict[2][0], sorteddict[3][0])
    cur.execute(insertstatement, insertvalues)
    con.commit()


get_similar_profile_viewed_before("59dce49ca56ac6edb4c4fdf3")

cur.close()
con.close()




































'''
dogstring
                      ;\ 
                            |' \ 
         _                  ; : ; 
        / `-.              /: : | 
       |  ,-.`-.          ,': : | 
       \  :  `. `.       ,'-. : | 
        \ ;    ;  `-.__,'    `-.| 
         \ ;   ;  :::  ,::'`:.  `. 
          \ `-. :  `    :.    `.  \ 
           \   \    ,   ;   ,:    (\ 
            \   :., :.    ,'o)): ` `-. 
           ,/,' ;' ,::"'`.`---'   `.  `-._ 
         ,/  :  ; '"      `;'          ,--`. 
        ;/   :; ;             ,:'     (   ,:) 
          ,.,:.    ; ,:.,  ,-._ `.     \""'/ 
          '::'     `:'`  ,'(  \`._____.-'"' 
             ;,   ;  `.  `. `._`-.  \\ 
             ;:.  ;:       `-._`-.\  \`. 
              '`:. :        |' `. `\  ) \ 
      -hrr-      ` ;:       |    `--\__,' 
                   '`      ,' 
                        ,-'  '''
