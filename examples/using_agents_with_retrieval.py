import mindsdb_sdk

con = mindsdb_sdk.connect()

# Now create an agent that will use the model we just created.
agent = con.agents.get('agent_with_retrieval')
agent.add_file('./data/tokaido-rulebook.pdf', 'rule book for the board game takaido')

print('Ask a question: ')
question = input()
answer = agent.completion([{'question': question, 'answer': None}])
print(answer.content)
