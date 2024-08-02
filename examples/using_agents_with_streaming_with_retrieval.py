import mindsdb_sdk
from uuid import uuid4
import os

con = mindsdb_sdk.connect()

open_ai_key = os.getenv('OPENAI_API_KEY')
model_name = 'gpt-4o'

# Now create an agent that will use the model we just created.
agent = con.agents.create(name=f'mindsdb_retrieval_agent_{model_name}_{uuid4().hex}',
                          model=model_name,
                          params={'return_context': True})

agent.add_file('./data/tokaido-rulebook.pdf', 'rule book for the board game Tokaido')

question = "what are the rules for the game takaido?"

# Stream the completion
completion_stream = agent.completion_stream([{'question': question, 'answer': None}])

# Process the streaming response
full_response = ""
for chunk in completion_stream:
    print(chunk)  # Print the entire chunk for debugging
    if isinstance(chunk, dict):
        if 'output' in chunk:
            full_response += chunk['output']
    elif isinstance(chunk, str):
        full_response += chunk

print("\n\nFull response:")
print(full_response)