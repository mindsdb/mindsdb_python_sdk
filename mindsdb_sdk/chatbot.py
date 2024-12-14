from typing import List
from mindsdb_sdk.utils.objects_collection import CollectionBase


class Chatbot:
    """
    Chatbot object, used to manage or query chatbots.

    Create and interact with chatbots:

    >>> chatbot = project.chatbots.create('my_chatbot', model_name='gpt-4')
    >>> response = chatbot.ask('Hello! How are you?')

    """

    def __init__(self, api, project, data: dict):
        self.api = api
        self.project = project
        self.name = data.get('name')
        self.database_name = data.get('database')
        self.agent_name = data.get('agent')
        self.model_name = data.get('model_name')

    def __repr__(self):
        return f"{self.__class__.__name__}({self.project.name}.{self.name})"

    def ask(self, query: str, **options):
        """
        Ask the chatbot a question or send a message.

        >>> response = chatbot.ask('What is the weather today?')

        :param query: The input query or message for the chatbot.
        :param options: Additional options to customize the query.
        :return: Chatbot response.
        """
        payload = {
            'query': query,
            **options
        }
        return self.api.chatbot_interaction(self.project.name, self.name, payload)

    def update(self, name: str = None, agent_name: str = None, model_name: str = None, database_name: str = None, inplace: bool = False):
        """
        Update chatbot properties.

        >>> chatbot.update(model_name='gpt-4', database_name='slack_db')

        :param name: New name for the chatbot.
        :param model_name: New model to use for the chatbot.
        :param database_name: New database connection name.
        :param inplace: If True, updates the current object in-place.
        :return: Updated Chatbot object or None if inplace is True.
        """
        payload = {}

        if name:
            payload['name'] = name

        if database_name:
            payload['database_name'] = database_name

        if agent_name:
            payload['agent_name'] = agent_name

        if model_name:
            payload['model_name'] = model_name

        updated_chatbot = self.api.update_chatbot(
            project=self.project.name,
            chatbot_name=self.name,
            data=payload
        )

        if inplace:
            self.name = updated_chatbot.get('name', self.name)
            self.database_name = updated_chatbot.get('database', self.database_name)
            self.agent_name = updated_chatbot.get('agent', self.agent_name)
            self.model_name = updated_chatbot.get('model_name', self.model_name)
            return None

        return Chatbot(self.api, self.project, updated_chatbot)

    def delete(self):
        """
        Delete the chatbot.

        >>> chatbot.delete()

        """
        self.api.delete_chatbot(self.project.name, self.name)


class Chatbots(CollectionBase):
    """
    Chatbots

    Manage chatbots within a project.

    List chatbots:

    >>> chatbots = project.chatbots.list()

    Get chatbot by name:

    >>> chatbot = project.chatbots.get('my_chatbot')

    Create a chatbot:

    >>> chatbot = project.chatbots.create('my_chatbot', model_name='gpt-4')

    Delete a chatbot:

    >>> project.chatbots.drop('my_chatbot')

    """

    def __init__(self, project, api):
        self.project = project
        self.api = api

    def list(self) -> List[Chatbot]:
        """
        Get the list of chatbots in the project.

        >>> chatbots = project.chatbots.list()

        :return: List of chatbot objects.
        """
        return [
            Chatbot(self.api, self.project, item)
            for item in self.api.list_chatbots(self.project.name)
        ]

    def get(self, name: str) -> Chatbot:
        """
        Get a chatbot by name.

        >>> chatbot = project.chatbots.get('my_chatbot')

        :param name: Name of the chatbot.
        :return: Chatbot object.
        """
        data = self.api.get_chatbot(self.project.name, name)
        return Chatbot(self.api, self.project, data)

    def create(self, name: str, agent_name: str = None, model_name: str = None, database_name: str = None) -> Chatbot:
        """
        Create a new chatbot.

        >>> chatbot = project.chatbots.create(
        ...     'my_chatbot',
        ...     model_name='gpt-4',
        ...     database_name='slack_db'
        ... )

        :param name: Name of the chatbot.
        :param model_name: Name of the model or agent.
        :param database_name: Connection name for chat applications (e.g., Slack, Teams).
        :return: Created Chatbot object.
        """
        payload = {
            'name': name,
            'database_name': database_name
        }

        if agent_name:
            payload['agent_name'] = agent_name

        if model_name:
            payload['model_name'] = model_name

        self.api.create_chatbot(self.project.name, data=payload)

        return self.get(name)

    def drop(self, name: str):
        """
        Delete a chatbot by name.

        >>> project.chatbots.drop('my_chatbot')

        :param name: Name of the chatbot to delete.
        """
        self.api.delete_chatbot(self.project.name, name)
