# HU_WebShop
By Dennis Besselsen, Levi Verhoef and Ayoub Zouin


This is a project we did for Hogeschool Utrecht HBO-ICT. We had to convert a mongodb document store into an SQL relational database.
To do this, we designed a database model and then wrote python functions to fill tables. 
Make sure to edit your database connection details to fit your database.

When running this, keep in mind that because of the way the database is setup, you're gonna have to call functions in the right order as to not get foreign key violations.

We have created a few tables, and have programmed python functions to fill these:

table   - function

profile - profile_converter

product - product_converter

previously_recommended - previously_recommended_filler

viewed_before - viewed_before_filler

buid - buid_table_filler

session - session_filler

order - order_filler

product_order - product_order_filler

The recommended order of calling these functions is:

1. profile_converter (takes 10-20 minutes) 
2. product_converter (takes 1-2 minutes)
3. previously_recommended (takes about 30-50 minutes, ~8.4 million entries)
4. viewed_before (takes about 15-20 minutes)
5. buid_table_filler (takes about 20-40 minutes)
6. session_filler (takes about 15-20 minutes)
7. order_filler (takes about ??-?? minutes)
8. product_order_filler (takes about ??-?? minutes)
