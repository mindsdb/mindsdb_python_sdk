from typing import List
from mindsdb_sdk.utils.objects_collection import CollectionBase


class Chatbot:
    """
    Represents a chatbot that can be managed within a project.
    """

    def __init__(self, api, project, data: dict):
        self.api = api
        self.project = project
        self.name = data.get('name')
        self.database_name = data.get('database')
        self.agent_name = data.get('agent')
        self.model_name = data.get('model_name')
        self.is_running = data.get('is_running')

    def __repr__(self):
        return f"{self.__class__.__name__}({self.project.name}.{self.name})"

    def update(self, name: str = None, agent_name: str = None, model_name: str = None, database_name: str = None, inplace: bool = False):
        """
        Updates the chatbot's properties.

        Example usage:
            >>> chatbot.update(model_name='gpt-4', database_name='slack_db')

        :param name: (Optional) New name for the chatbot.
        :param agent_name: (Optional) New agent name to associate with the chatbot.
        :param model_name: (Optional) New model to use for the chatbot.
        :param database_name: (Optional) New database connection name.
        :param inplace: If True, updates the current object in-place; otherwise, returns a new Chatbot object.
        :return: Updated Chatbot object, or None if inplace is True.
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
        Deletes the chatbot from the project.

        Example usage:
            >>> chatbot.delete()
        """
        self.api.delete_chatbot(self.project.name, self.name)


class Chatbots(CollectionBase):
    """
    Manages chatbots within a project.

    Provides methods to list, retrieve, create, and delete chatbots.

    Example usage:

        List chatbots in a project:
            >>> chatbots = project.chatbots.list()

        Retrieve a chatbot by name:
            >>> chatbot = project.chatbots.get('my_chatbot')

        Create a new chatbot:
            >>> chatbot = project.chatbots.create(
                    'my_chatbot',
                    model_name='gpt-4',
                    database_name='slack_db',
                    is_running=True
                )

        Delete a chatbot by name:
            >>> project.chatbots.drop('my_chatbot')
    """

    def __init__(self, project, api):
        self.project = project
        self.api = api

    def list(self) -> List[Chatbot]:
        """
        Retrieves a list of all chatbots within the project.

        Example usage:
            >>> chatbots = project.chatbots.list()

        :return: List of Chatbot objects.
        """
        return [
            Chatbot(self.api, self.project, item)
            for item in self.api.list_chatbots(self.project.name)
        ]

    def get(self, name: str) -> Chatbot:
        """
        Retrieves a chatbot by its name.

        Example usage:
            >>> chatbot = project.chatbots.get('my_chatbot')

        :param name: The name of the chatbot to retrieve.
        :return: Chatbot object.
        """
        data = self.api.get_chatbot(self.project.name, name)
        return Chatbot(self.api, self.project, data)

    def create(self, name: str, agent_name: str = None, model_name: str = None, database_name: str = None, is_running: bool = False) -> Chatbot:
        """
        Creates a new chatbot within the project.

        Example usage:
            >>> chatbot = project.chatbots.create(
                    'my_chatbot',
                    model_name='gpt-4',
                    database_name='slack_db',
                    is_running=True
                )

        :param name: The name of the new chatbot.
        :param agent_name:  The agent name to associate with the chatbot.
        :param model_name:  The model to use for the chatbot.
        :param database_name: The database connection name for chat applications.
        :param is_running: (Optional) Indicates whether the chatbot should start in a running state. Default is False.
        :return: The created Chatbot object.
        """
        payload = {
            'name': name,
            'database_name': database_name,
            'is_running': is_running
        }

        if agent_name:
            payload['agent_name'] = agent_name

        if model_name:
            payload['model_name'] = model_name

        self.api.create_chatbot(self.project.name, data=payload)

        return self.get(name)

    def drop(self, name: str):
        """
        Deletes a chatbot by its name.

        Example usage:
            >>> project.chatbots.drop('my_chatbot')

        :param name: The name of the chatbot to delete.
        """
        self.api.delete_chatbot(self.project.name, name)
