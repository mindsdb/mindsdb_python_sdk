
import mindsdb_sdk

con = mindsdb_sdk.connect()

openai_handler = con.ml_handlers.openai

# create ml engine
openai = con.ml_engines.create(
    'openai',
    handler=openai_handler,
    # handler='openai', # <-- another option to define handler
    connection_data={'api_key': ''}
)

# create model
model = con.models.create(
    'open1',
    predict='answer',
    engine=openai,  # created ml engine
    prompt_template='answer question: {{q}}'
)

# use model
model.predict({'q': 'size of the sun'})