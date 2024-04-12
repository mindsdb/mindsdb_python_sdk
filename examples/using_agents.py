import mindsdb_sdk

con = mindsdb_sdk.connect()

# We currently support Langchain as a backend.
print('Creating underlying langchain model for the agent to use...')
try:
    langchain_engine = con.ml_engines.get('langchain')
except Exception:
    # Create the engine if it doesn't exist.
    langchain_engine = con.ml_engines.create('langchain', handler='langchain')

# Actually create the underlying model the agent will use.
langchain_model = con.models.create(
    'agent_model',
    predict='answer',
    engine='langchain',
    prompt_template='You are a spicy, cheeky assistant. Add some personality and flare when responding to the user question: {{question}}',
    model_name='gpt-4-0125-preview' # This is the underlying LLM. Can use OpenAI, Claude, local Ollama, etc
    # Can optionally set LLM args here. For example:
    # temperature=0.0,
    # max_tokens=1000,
    # top_p=1.0,
    # top_k=0,
    # ...
)
print('Agent ready to use.')

# Now create an agent that will use the model we just created.
agent = con.agents.create('new_agent', langchain_model)
print('Ask a question: ')
question = input()
answer = agent.completion([{'question': question, 'answer': None}])
print(answer.content)
