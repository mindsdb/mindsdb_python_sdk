from requests.exceptions import HTTPError
from typing import List, Union
from uuid import uuid4
import datetime
import pandas as pd

from mindsdb_sdk.databases import Databases
from mindsdb_sdk.knowledge_bases import KnowledgeBases
from mindsdb_sdk.models import Model
from mindsdb_sdk.skills import Skill, Skills
from mindsdb_sdk.utils.objects_collection import CollectionBase


class AgentCompletion:
    """Represents a full MindsDB agent completion"""
    def __init__(self, content: str):
        self.content = content

    def __repr__(self):
        return self.content


class Agent:
    """Represents a MindsDB agent.
    
    Working with agents:

    Get an agent by name:

    >>> agent = agents.get('my_agent')

    Query an agent:

    >>> completion = agent.completion([{'question': 'What is your name?', 'answer': None}])
    >>> print(completion.content)

    List all agents:

    >>> agents = agents.list()

    Create a new agent:

    >>> model = models.get('my_model') # Or use models.create(...)
    >>> # Connect your agent to a MindsDB table.
    >>> text_to_sql_skill = skills.create('text_to_sql', 'sql', { 'tables': ['my_table'], 'database': 'my_database' })
    >>> agent = agents.create('my_agent', model, [text_to_sql_skill])

    Update an agent:

    >>> new_model = models.get('new_model')
    >>> agent.model_name = new_model.name
    >>> new_skill = skills.create('new_skill', 'sql', { 'tables': ['new_table'], 'database': 'new_database' })
    >>> updated_agent.skills.append(new_skill)
    >>> updated_agent = agents.update('my_agent', agent)

    Delete an agent by name:

    >>> agents.delete('my_agent')
    """
    def __init__(
            self,
            name: str,
            model_name: str,
            skills: List[Skill],
            params: dict,
            created_at: datetime.datetime,
            updated_at: datetime.datetime,
            collection: CollectionBase = None
            ):
        self.name = name
        self.model_name = model_name
        self.skills = skills
        self.params = params
        self.created_at = created_at
        self.updated_at = updated_at
        self.collection = collection

    def completion(self, messages: List[dict]) -> AgentCompletion:
        return self.collection.completion(self.name, messages)

    def add_file(self, file_path: str, description: str, knowledge_base: str = None):
        """
        Add a file to the agent for retrieval.

        :param file_path: Path to the file to be added.
        """
        self.collection.add_file(self.name, file_path, description, knowledge_base)

    def __repr__(self):
        return f'{self.__class__.__name__}(name: {self.name})'

    def __eq__(self, other):
        if self.name != other.name:
            return False
        if self.model_name != other.model_name:
            return False
        if self.skills != other.skills:
            return False
        if self.params != other.params:
            return False
        if self.created_at != other.created_at:
            return False
        return self.updated_at == other.updated_at

    @classmethod
    def from_json(cls, json: dict, collection: CollectionBase):
        return cls(
            json['name'],
            json['model_name'],
            [Skill.from_json(skill) for skill in json['skills']],
            json['params'],
            json['created_at'],
            json['updated_at'],
            collection
        )


class Agents(CollectionBase):
    """Collection for agents"""
    def __init__(self, api, project: str, knowledge_bases: KnowledgeBases, databases: Databases, skills: Skills = None):
        self.api = api
        self.project = project
        self.skills = skills or Skills(self.api, project)
        self.databases = databases
        self.knowledge_bases = knowledge_bases

    def list(self) -> List[Agent]:
        """
        List available agents.

        :return: list of agents
        """
        data = self.api.agents(self.project)
        return [Agent.from_json(agent, self) for agent in data]

    def get(self, name: str) -> Agent:
        """
        Gets an agent by name.

        :param name: Name of the agent

        :return: agent with given name
        """
        data = self.api.agent(self.project, name)
        return Agent.from_json(data, self)

    def completion(self, name: str, messages: List[dict]) -> AgentCompletion:
        """
        Queries the agent for a completion.

        :param name: Name of the agent
        :param messages: List of messages to be sent to the agent

        :return: completion from querying the agent
        """
        data = self.api.agent_completion(self.project, name, messages)
        return AgentCompletion(data['message']['content'])

    def add_file(self, name: str, file_path: str, description: str, knowledge_base: str = None):
        """
        Add a file to the agent for retrieval.

        :param name: Name of the agent
        :param file_path: Path to the file to be added, or name of existing file.
        :param description: Description of the file. Used by agent to know when to do retrieval
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        """
        filename = file_path.split('/')[-1]
        filename_no_extension = filename.split('.')[0]
        try:
            _ = self.api.get_file_metadata(filename_no_extension)
        except HTTPError as e:
            if e.response.status_code >= 400 and e.response.status_code != 404:
                raise e
            # Upload file if it doesn't exist.
            with open(file_path, 'rb') as file:
                content = file.read()
                df = pd.DataFrame.from_records([{'content': content}])
                self.api.upload_file(filename_no_extension, df)

        # Insert uploaded file into new knowledge base.
        if knowledge_base is not None:
            kb = self.knowledge_bases.get(knowledge_base)
        else:
            kb_name = f'{name}_{filename_no_extension}_kb'
            try:
                kb = self.knowledge_bases.get(kb_name)
            except AttributeError as e:
                # Create KB if it doesn't exist.
                kb = self.knowledge_bases.create(kb_name)
                # Wait for underlying embedding model to finish training.
                kb.model.wait_complete()

        # Insert the entire file.
        kb.insert_files([filename_no_extension])

        # Make sure skill name is unique.
        skill_name = f'{filename_no_extension}_retrieval_skill_{uuid4()}'
        retrieval_params = {
            'source': kb.name,
            'description': description,
        }
        file_retrieval_skill = self.skills.create(skill_name, 'retrieval', retrieval_params)
        agent = self.get(name)
        agent.skills.append(file_retrieval_skill)
        self.update(agent.name, agent)

    def create(
            self,
            name: str,
            model: Model,
            skills: List[Union[Skill, str]] = None,
            params: dict = None) -> Agent:
        """
        Create new agent and return it

        :param name: Name of the agent to be created
        :param model: Model to be used by the agent
        :param skills: List of skills to be used by the agent. Currently only 'sql' is supported.
        :param params: Parameters for the agent

        :return: created agent object
        """
        skills = skills or []
        skill_names = []
        for skill in skills:
            if isinstance(skill, str):
                # Check if skill exists.
                _ = self.skills.get(skill)
                skill_names.append(skill)
                continue
            # Create the skill if it doesn't exist.
            _ = self.skills.create(skill.name, skill.type, skill.params)
            skill_names.append(skill.name)

        data = self.api.create_agent(self.project, name, model.name, skill_names, params)
        return Agent.from_json(data, self)

    def update(self, name: str, updated_agent: Agent):
        """
        Update an agent by name.

        :param name: Name of the agent to be updated
        :param updated_agent: Agent with updated fields

        :return: updated agent object
        """
        updated_skills = set()
        for skill in updated_agent.skills:
            if isinstance(skill, str):
                # Skill must exist.
                _ = self.skills.get(skill)
                updated_skills.add(skill)
                continue
            try:
                # Create the skill if it doesn't exist.
                _ = self.skills.get(skill.name)
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e
                # Doesn't exist
                _ = self.skills.create(skill.name, skill.type, skill.params)
            updated_skills.add(skill.name)

        existing_agent = self.api.agent(self.project, name)
        existing_skills = set([s['name'] for s in existing_agent['skills']])
        skills_to_add = updated_skills.difference(existing_skills)
        skills_to_remove = existing_skills.difference(updated_skills)
        data = self.api.update_agent(
            self.project,
            name,
            updated_agent.name,
            updated_agent.model_name,
            list(skills_to_add),
            list(skills_to_remove),
            updated_agent.params
        )
        return Agent.from_json(data, self)

    def drop(self, name: str):
        """
        Drop an agent by name.

        :param name: Name of the agent to be dropped
        """
        _ = self.api.delete_agent(self.project, name)
