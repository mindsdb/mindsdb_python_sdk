from requests.exceptions import HTTPError
from typing import Iterable, List, Union
from urllib.parse import urlparse
from uuid import uuid4
import datetime
import json

from mindsdb_sdk.knowledge_bases import KnowledgeBase
from mindsdb_sdk.models import Model
from mindsdb_sdk.skills import Skill
from mindsdb_sdk.utils.objects_collection import CollectionBase


class AgentCompletion:
    """
    Represents a full MindsDB agent completion response.

    Attributes:
    content: The completion content.
    context: Only relevant for retrieval agents. Contains the context retrieved from the knowledge base.


    """

    def __init__(self, content: str, context: List[dict] = None):
        self.content = content
        self.context = context

    def __repr__(self):
        return f'{self.__class__.__name__}(content: {self.content}, context: {self.context})'


class Agent:
    """Represents a MindsDB agent.

    Working with agents:

    Get an agent by name:

    >>> agent = agents.get('my_agent')

    Query an agent:

    >>> completion = agent.completion([{'question': 'What is your name?', 'answer': None}])
    >>> print(completion.content)

    Query an agent with streaming:

    >>> completion = agent.completion_stream([{'question': 'What is your name?', 'answer': None}])
    >>> for chunk in completion:
            print(chunk.choices[0].delta.content)

    List all agents:

    >>> agents = agents.list()

    Create a new agent:

    >>> agent = agents.create(
        'my_agent',
        model={
            'model_name': 'gpt-3.5-turbo',
            'provider': 'openai',
            'api_key': 'your_openai_api_key_here'
        },
        data={'tables': ['my_database.my_table'], 'knowledge_base': 'my_kb'}
    )

    Update an agent:

    >>> agent.data['tables'].append('my_database.my_new_table')
    >>> updated_agent = agents.update('my_agent', agent)

    Delete an agent by name:

    >>> agents.drop('my_agent')
    """

    def __init__(
            self,
            name: str,
            created_at: datetime.datetime,
            updated_at: datetime.datetime,
            model: Union[Model, str, dict] = None,
            skills: List[Skill] = [],
            provider: str = None,
            data: dict = {},
            prompt_template: str = None,
            params: dict = {},
            skills_extra_parameters: dict = {},
            collection: CollectionBase = None
    ):
        self.name = name
        self.created_at = created_at
        self.updated_at = updated_at
        self.model = model
        self.skills = skills
        self.provider = provider
        self.data = data
        self.prompt_template = prompt_template
        self.params = params
        self.skills_extra_parameters = skills_extra_parameters
        self.collection = collection

    def completion(self, messages: List[dict]) -> AgentCompletion:
        return self.collection.completion(self.name, messages)

    def completion_v2(self, messages: List[dict]) -> AgentCompletion:
        return self.collection.completion_v2(self.name, messages)

    def completion_stream(self, messages: List[dict]) -> Iterable[object]:
        return self.collection.completion_stream(self.name, messages)

    def completion_stream_v2(self, messages: List[dict]) -> Iterable[object]:
        return self.collection.completion_stream_v2(self.name, messages)

    def add_files(self, file_paths: List[str], description: str, knowledge_base: str = None):
        """
        Add a list of files to the agent for retrieval.

        :param file_paths: List of paths to the files to be added.
        """
        self.collection.add_files(self.name, file_paths, description, knowledge_base)

    def add_file(self, file_path: str, description: str, knowledge_base: str = None):
        """
        Add a file to the agent for retrieval.

        :param file_path: Path to the file to be added.
        """
        self.collection.add_file(self.name, file_path, description, knowledge_base)

    def add_webpages(
            self,
            urls: List[str],
            description: str,
            knowledge_base: str = None,
            crawl_depth: int = 1,
            limit: int = None,
            filters: List[str] = None):
        """
        Add a crawled URL to the agent for retrieval.

        :param urls: URLs of pages to be crawled and added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only, -1 = default max
        :param limit: max count of pages to crawl
        :param filters: Include only URLs that match these regex patterns
        """
        self.collection.add_webpages(self.name, urls, description, knowledge_base=knowledge_base,
                                     crawl_depth=crawl_depth, limit=limit, filters=filters)

    def add_webpage(
            self,
            url: str,
            description: str,
            knowledge_base: str = None,
            crawl_depth: int = 1,
            limit: int = None,
            filters: List[str] = None):
        """
        Add a crawled URL to the agent for retrieval.

        :param url: URL of the page to be crawled and added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only, -1 = default max
        :param limit: max count of pages to crawl
        :param filters: Include only URLs that match these regex patterns
        """
        self.collection.add_webpage(self.name, url, description, knowledge_base=knowledge_base,
                                    crawl_depth=crawl_depth, limit=limit, filters=filters)

    def add_database(self, database: str, tables: List[str], description: str):
        """
        Add a database to the agent for retrieval.

        :param database: Name of the database to be added.
        :param tables: List of tables to be added.
        :param description: Description of the database tables. Used by the agent to know when to use SQL skill.
        """
        self.collection.add_database(self.name, database, tables, description)

    def __repr__(self):
        return f'{self.__class__.__name__}(name: {self.name})'

    def __eq__(self, other):
        if self.name != other.name:
            return False
        if self.model != other.model:
            return False
        if self.provider != other.provider:
            return False
        if self.data != other.data:
            return False
        if self.prompt_template != other.prompt_template:
            return False
        if self.skills != other.skills:
            return False
        if self.params != other.params:
            return False
        if self.skills_extra_parameters != other.skills_extra_parameters:
            return False
        if self.created_at != other.created_at:
            return False
        return self.updated_at == other.updated_at

    @classmethod
    def from_json(cls, json: dict, collection: CollectionBase):
        skills = []
        if json.get('skills'):
            skills = [Skill.from_json(skill) for skill in json['skills']]

        model = json.get('model') or json.get('model_name')

        return cls(
            json['name'],
            json['created_at'],
            json['updated_at'],
            model,
            skills,
            json.get('provider'),
            json.get('data', {}),
            json.get('prompt_template'),
            json.get('params', {}),
            json.get('skills_extra_parameters', {}),
            collection
        )


class Agents(CollectionBase):
    """Collection for agents"""

    def __init__(self, project, api):
        self.api = api
        self.project = project

        self.knowledge_bases = project.knowledge_bases
        self.models = project.models
        self.skills = project.skills

        self.databases = project.server.databases
        self.ml_engines = project.server.ml_engines

    def list(self) -> List[Agent]:
        """
        List available agents.

        :return: list of agents
        """
        data = self.api.agents(self.project.name)
        return [Agent.from_json(agent, self) for agent in data]

    def get(self, name: str) -> Agent:
        """
        Gets an agent by name.

        :param name: Name of the agent

        :return: agent with given name
        """
        data = self.api.agent(self.project.name, name)
        return Agent.from_json(data, self)

    def completion(self, name: str, messages: List[dict]) -> AgentCompletion:
        """
        Queries the agent for a completion.

        :param name: Name of the agent
        :param messages: List of messages to be sent to the agent

        :return: completion from querying the agent
        """
        data = self.api.agent_completion(self.project.name, name, messages)
        if 'context' in data['message']:
            return AgentCompletion(data['message']['content'], data['message'].get('context'))

        return AgentCompletion(data['message']['content'])

    def completion_v2(self, name: str, messages: List[dict]) -> AgentCompletion:
        """
        Queries the agent for a completion.

        :param name: Name of the agent
        :param messages: List of messages to be sent to the agent

        :return: completion from querying the agent
        """
        return self.api.agent_completion(self.project.name, name, messages)

    def completion_stream(self, name, messages: List[dict]) -> Iterable[object]:
        """
        Queries the agent for a completion and streams the response as an iterable object.

        :param name: Name of the agent
        :param messageS: List of messages to be sent to the agent

        :return: iterable of completion chunks from querying the agent.
        """
        return self.api.agent_completion_stream(self.project.name, name, messages)

    def completion_stream_v2(self, name, messages: List[dict]) -> Iterable[object]:
        """
        Queries the agent for a completion and streams the response as an iterable object.

        :param name: Name of the agent
        :param messages: List of messages to be sent to the agent

        :return: iterable of completion chunks from querying the agent.
        """
        return self.api.agent_completion_stream_v2(self.project.name, name, messages)

    def _create_default_knowledge_base(self, agent: Agent, name: str) -> KnowledgeBase:
        try:
            kb = self.knowledge_bases.create(name)
            return kb
        except Exception as e:
            raise ValueError(
                f"Failed to automatically create knowledge base for agent {agent.name}. "
                "Either provide an existing knowledge base name, "
                "or set your default embedding model via server.config.set_default_embedding_model(...) or through the MindsDB UI."
            )

    def add_files(self, name: str, file_paths: List[str], description: str = None):
        """
        Add a list of files to the agent for retrieval.

        :param name: Name of the agent
        :param file_paths: List of paths or URLs to the files to be added.
        :param description: Description of the file. Used by agent to know when to do retrieval
        """
        if not file_paths:
            return

        agent = self.get(name)
        filename_no_extension = ''
        for file_path in file_paths:
            filename = file_path.split('/')[-1].lower()
            filename_no_extension = filename.split('.')[0]
            try:
                _ = self.api.get_file_metadata(filename_no_extension)
            except HTTPError as e:
                if e.response.status_code >= 400 and e.response.status_code != 404:
                    raise e
                # upload file to mindsdb
                self.api.upload_file(filename, file_path)

            # Add file to agent's data if it hasn't been added already.
            if 'tables' not in agent.data or f'files.{filename_no_extension}' not in agent.data['tables']:
                agent.data.setdefault('tables', []).append(f'files.{filename_no_extension}')

        # Add the description provided to the agent's prompt template.
        if description:
            agent.prompt_template = (agent.prompt_template or '') + f'\n{description}'

        self.update(agent.name, agent)

    def add_file(self, name: str, file_path: str, description: str = None):
        """
        Add a file to the agent for retrieval.

        :param name: Name of the agent
        :param file_path: Path to the file to be added, or name of existing file.
        :param description: Description of the file. Used by agent to know when to do retrieval
        """
        self.add_files(name, [file_path], description)

    def add_webpages(
        self,
        name: str,
        urls: List[str],
        description: str = None,
        knowledge_base: str = None,
        crawl_depth: int = 1,
        limit: int = None,
        filters: List[str] = None
    ):
        """
        Add a list of webpages to the agent for retrieval.

        :param name: Name of the agent
        :param urls: List of URLs of the webpages to be added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
        :param limit: max count of pages to crawl
        :param filters: Include only URLs that match these regex patterns
        """
        if not urls:
            return
        agent = self.get(name)
        for url in urls:
            # Validate URLs.
            _ = urlparse(url)
        if knowledge_base is not None:
            kb = self.knowledge_bases.get(knowledge_base)
        else:
            kb_name = f'{name.lower()}_web_{uuid4().hex}_kb'
            kb = self._create_default_knowledge_base(agent, kb_name)

        # Insert crawled webpage.
        kb.insert_webpages(urls, crawl_depth=crawl_depth, filters=filters, limit=limit)

        # Add knowledge base to agent's data if it hasn't been added already.
        if 'knowledge_bases' not in agent.data or kb.name not in agent.data['knowledge_bases']:
            agent.data.setdefault('knowledge_bases', []).append(kb.name)

        # Add the description provided to the agent's prompt template.
        if description:
            agent.prompt_template = (agent.prompt_template or '') + f'\n{description}'

        self.update(agent.name, agent)

    def add_webpage(
        self,
        name: str,
        url: str,
        description: str = None,
        knowledge_base: str = None,
        crawl_depth: int = 1,
        limit: int = None,
        filters: List[str] = None
    ):
        """
        Add a webpage to the agent for retrieval.

        :param name: Name of the agent
        :param file_path: URL of the webpage to be added, or name of existing webpage.
        :param description: Description of the webpage. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
        :param limit: max count of pages to crawl
        :param filters: Include only URLs that match these regex patterns
        """
        self.add_webpages(name, [url], description, knowledge_base=knowledge_base,
                          crawl_depth=crawl_depth, limit=limit, filters=filters)

    def add_database(self, name: str, database: str, tables: List[str] = None, description: str = None):
        """
        Add a database to the agent for retrieval.

        :param name: Name of the agent
        :param database: Name of the database to be added.
        :param tables: List of tables to be added. If not provided, the entire database will be added.
        :param description: Description of the database. Used by agent to know when to do retrieval.
        """
        # Make sure database exists.
        db = self.databases.get(database)

        agent = self.get(name)

        if tables:
            # Ensure the tables exist.
            all_table_names = set([t.name for t in db.tables.list()])
            for t in tables:
                if t not in all_table_names:
                    raise ValueError(f'Table {t} does not exist in database {database}.')

            # Add table to agent's data if it hasn't been added already.
            if 'tables' not in agent.data or f'{database}.{t}' not in agent.data['tables']:
                agent.data.setdefault('tables', []).append(f'{database}.{t}')

        else:
            # If no tables are provided, add the database itself.
            if 'tables' not in agent.data or f'{database}.*' not in agent.data['tables']:
                agent.data.setdefault('tables', []).append(f'{database}.*')

        # Add the description provided to the agent's prompt template.
        if description:
            agent.prompt_template = (agent.prompt_template or '') + f'\n{description}'

        self.update(agent.name, agent)

    def create(
        self,
        name: str,
        model: Union[Model, str, dict] = None,
        provider: str = None,
        skills: List[Union[Skill, str]] = None,
        data: dict = None,
        prompt_template: str = None,
        params: dict = None,
        **kwargs
    ) -> Agent:
        """
        Create new agent and return it

        :param name: Name of the agent to be created
        :param model: Model to be used by the agent. This can be a Model object, a string with model name, or a dictionary with model parameters.
        :param skills: List of skills to be used by the agent. Currently only 'sql' is supported.
        :param provider: Provider of the model, e.g. 'mindsdb', 'openai', etc.
        :param data: Data to be used by the agent. This is usually a dictionary with 'tables' and/or 'knowledge_base' keys.
        :param params: Parameters for the agent

        :return: created agent object
        """
        skills = skills or []
        skill_names = []
        for skill in skills:
            if isinstance(skill, str):
                # Check if skill exists.
                # TODO what this line does?
                _ = self.skills.get(skill)
                skill_names.append(skill)
                continue
            # Create the skill if it doesn't exist.
            _ = self.skills.create(skill.name, skill.type, skill.params)
            skill_names.append(skill.name)

        if params is None:
            params = {}
        params.update(kwargs)

        model_name = None
        if isinstance(model_name, Model):
            model_name = model_name.name
            provider = 'mindsdb'
            model = None
        elif isinstance(model, str):
            model_name = model
            model = None

        agent = self.api.create_agent(
            self.project.name,
            name,
            model_name,
            provider,
            skill_names,
            data,
            model,
            prompt_template,
            params
        )
        return Agent.from_json(agent, self)

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

        existing_agent = self.api.agent(self.project.name, name)

        existing_skills = set([s['name'] for s in existing_agent.get('skills', [])])
        skills_to_add = updated_skills.difference(existing_skills)
        skills_to_remove = existing_skills.difference(updated_skills)

        updated_model_name = None
        updated_provider = updated_agent.provider
        updated_model = None
        if isinstance(updated_agent.model, Model):
            updated_model_name = updated_agent.model.name
            updated_provider = 'mindsdb'
        elif isinstance(updated_agent.model, str):
            updated_model_name = updated_agent.model
        elif isinstance(updated_agent.model, dict):
            updated_model = updated_agent.model

        agent = self.api.update_agent(
            self.project.name,
            name,
            updated_agent.name,
            updated_provider,
            updated_model_name,
            list(skills_to_add),
            list(skills_to_remove),
            updated_agent.data,
            updated_model,
            updated_agent.prompt_template,
            updated_agent.params
        )
        return Agent.from_json(agent, self)

    def drop(self, name: str):
        """
        Drop an agent by name.

        :param name: Name of the agent to be dropped
        """
        _ = self.api.delete_agent(self.project.name, name)
