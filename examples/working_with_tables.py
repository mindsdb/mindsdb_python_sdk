import mindsdb_sdk
import pandas as pd

con = mindsdb_sdk.connect()

# connect to mindsdb example database
example_db = con.databases.create(
    'example_db',
    engine='postgres',
    connection_args={
        "user": "demo_user",
        "password": "demo_password",
        "host": "3.220.66.106",
        "port": "5432",
        "database": "demo"
    }
)

# connect to the empty user database
my_db = con.databases.create(
    'my_db',
    engine='postgres',
    connection_args={
        "user": "postgres",
        "host": "localhost",
        "port": "5432",
        "database": "my_database"
    }
)

# get home_rentals table
table1 = example_db.tables.get('demo_data.home_rentals')

# ---- create new table ----

# create table home_rentals in user db and fill it with rows with location=great
table2 = my_db.tables.create('home_rentals', table1.filter(location='great'))


# create table from csv file

df = pd.read_csv('my_data.csv')
table3 = my_db.tables.create('my_table', df)


# ---- insert into table ----

# insert to table2 first 10 rows from table1
table2.insert(table1.limit(10))


# ---- update data in table ----

# get all rows with number_of_rooms=1 from table1 and update values in table2 using key ('location', 'neighborhood')
table2.update(
    table1.filter(number_of_rooms=1),
    on=['location', 'neighborhood']
)


# ---- delete rows from table ----

# delete all rows where bedrooms=2
table2.delete(number_of_rooms=1)


