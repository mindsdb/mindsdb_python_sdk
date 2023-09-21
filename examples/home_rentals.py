
import mindsdb_sdk

con = mindsdb_sdk.connect()

# connect to database
db = con.databases.create(
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

# get table
# because table with schema we are using .get
tbl = db.tables.get('demo_data.home_rentals')

# create model
model = con.models.create(
    'home_rentals_model',
    predict='rental_price',
    query=tbl
)

# wait till training complete
model.wait_complete()

# make prediction for first 3 rows
result = model.predict(tbl.limit(3))



