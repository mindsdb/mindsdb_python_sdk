import mindsdb_sdk
import pandas as pd

con = mindsdb_sdk.connect()

# get user's database (connected to mindsdb as rental_db)
db = con.databases.rental_db

# get table
table1 = db.tables.house_sales


# ---- create new table ----

# copy create table house_sales and fill it with rows with type=house
table2 = db.tables.create('house_sales2', table1.filter(type='house'))

# create table from csv file
df = pd.read_csv('my_data.csv')
table3 = db.tables.create('my_table', df)


# ---- insert into table ----

# insert to table2 first 10 rows from table1
table2.insert(table1.limit(10))


# ---- update data in table ----

# get all rows with type=house from table1 and update values in table2 using key ('saledate', 'type', 'bedrooms')
table2.update(
    table1.filter(type='house'),
    on=['saledate', 'type', 'bedrooms']
)


# ---- delete rows from table ----

# delete all rows where bedrooms=2
table2.delete(bedrooms=2)


