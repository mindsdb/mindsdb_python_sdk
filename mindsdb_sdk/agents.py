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

_DEFAULT_LLM_MODEL = 'gpt-4o'
_DEFAULT_LLM_PROMPT = 'Answer the user"s question in a helpful way: {{question}}'


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

    >>> agents.drop('my_agent')
    """

    def __init__(
            self,
            name: str,
            model_name: str,
            skills: List[Skill],
            params: dict,
            created_at: datetime.datetime,
            updated_at: datetime.datetime,
            provider: str = None,
            collection: CollectionBase = None
    ):
        self.name = name
        self.model_name = model_name
        self.provider = provider
        self.skills = skills
        self.params = params
        self.created_at = created_at
        self.updated_at = updated_at
        self.collection = collection

    def completion(self, messages: List[dict]) -> AgentCompletion:
        return self.collection.completion(self.name, messages)

    def completion_stream(self, messages: List[dict]) -> Iterable[object]:
        return self.collection.completion_stream(self.name, messages)

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
            filters: List[str] = None):
        """
        Add a crawled URL to the agent for retrieval.

        :param urls: URLs of pages to be crawled and added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only, -1 = default max
        :param filters: Include only URLs that match these regex patterns
        """
        self.collection.add_webpages(self.name, urls, description, knowledge_base=knowledge_base, crawl_depth=crawl_depth, filters=filters)

    def add_webpage(
            self,
            url: str,
            description: str,
            knowledge_base: str = None,
            crawl_depth: int = 1,
            filters: List[str] = None):
        """
        Add a crawled URL to the agent for retrieval.

        :param url: URL of the page to be crawled and added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only, -1 = default max
        :param filters: Include only URLs that match these regex patterns
        """
        self.collection.add_webpage(self.name, url, description, knowledge_base=knowledge_base, crawl_depth=crawl_depth, filters=filters)

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
        if self.model_name != other.model_name:
            return False
        if self.provider != other.provider:
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
            json['provider'],
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

    def completion_stream(self, name, messages: List[dict]) -> Iterable[object]:
        """
        Queries the agent for a completion and streams the response as an iterable object.

        :param name: Name of the agent
        :param messageS: List of messages to be sent to the agent

        :return: iterable of completion chunks from querying the agent.
        """
        return self.api.agent_completion_stream(self.project.name, name, messages)

    def _create_default_knowledge_base(self, agent: Agent, name: str) -> KnowledgeBase:
        # Make sure default ML engine for embeddings exists.
        try:
            _ = self.ml_engines.get('langchain_embedding')
        except AttributeError:
            _ = self.ml_engines.create('langchain_embedding', 'langchain_embedding')
        # Include API keys in embeddings.
        if agent.provider == "mindsdb":
            agent_model = self.models.get(agent.model_name)
            training_options = json.loads(agent_model.data.get('training_options', '{}'))
            training_options_using = training_options.get('using', {})
            api_key_params = {k: v for k, v in training_options_using.items() if 'api_key' in k}
            kb = self.knowledge_bases.create(name, params=api_key_params)
        else:
            kb = self.knowledge_bases.create(name)
        # Wait for underlying embedding model to finish training.
        kb.model.wait_complete()
        return kb

    def add_files(self, name: str, file_paths: List[str], description: str, knowledge_base: str = None):
        """
        Add a list of files to the agent for retrieval.

        :param name: Name of the agent
        :param file_paths: List of paths or URLs to the files to be added.
        :param description: Description of the file. Used by agent to know when to do retrieval
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        """
        if not file_paths:
            return
        filename_no_extension = ''
        all_filenames = []
        for file_path in file_paths:
            filename = file_path.split('/')[-1].lower()
            filename_no_extension = filename.split('.')[0]
            all_filenames.append(filename_no_extension)
            try:
                _ = self.api.get_file_metadata(filename_no_extension)
            except HTTPError as e:
                if e.response.status_code >= 400 and e.response.status_code != 404:
                    raise e
                # upload file to mindsdb
                self.api.upload_file(filename, file_path)

        # Insert uploaded files into new knowledge base.
        agent = self.get(name)
        if knowledge_base is not None:
            kb = self.knowledge_bases.get(knowledge_base)
        else:
            kb_name = f'{name.lower()}_{filename_no_extension}_{uuid4().hex}_kb'
            kb = self._create_default_knowledge_base(agent, kb_name)

        # Insert the entire file.
        kb.insert_files(all_filenames)

        # Make sure skill name is unique.
        skill_name = f'{filename_no_extension}_retrieval_skill_{uuid4().hex}'
        retrieval_params = {
            'source': kb.name,
            'description': description,
        }
        file_retrieval_skill = self.skills.create(skill_name, 'retrieval', retrieval_params)
        agent.skills.append(file_retrieval_skill)
        self.update(agent.name, agent)

    def add_file(self, name: str, file_path: str, description: str, knowledge_base: str = None):
        """
        Add a file to the agent for retrieval.

        :param name: Name of the agent
        :param file_path: Path to the file to be added, or name of existing file.
        :param description: Description of the file. Used by agent to know when to do retrieval
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        """
        self.add_files(name, [file_path], description, knowledge_base)

    def add_webpages(
            self,
            name: str,
            urls: List[str],
            description: str,
            knowledge_base: str = None,
            crawl_depth: int = 1,
            filters: List[str] = None
            ):
        """
        Add a list of webpages to the agent for retrieval.

        :param name: Name of the agent
        :param urls: List of URLs of the webpages to be added.
        :param description: Description of the webpages. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
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
        kb.insert_webpages(urls, crawl_depth=crawl_depth, filters=filters)

        # Make sure skill name is unique.
        skill_name = f'web_retrieval_skill_{uuid4().hex}'
        retrieval_params = {
            'source': kb.name,
            'description': description,
        }
        webpage_retrieval_skill = self.skills.create(skill_name, 'retrieval', retrieval_params)
        agent.skills.append(webpage_retrieval_skill)
        self.update(agent.name, agent)

    def add_webpage(
            self,
            name: str,
            url: str,
            description: str,
            knowledge_base: str = None,
            crawl_depth: int = 1,
            filters: List[str] = None):
        """
        Add a webpage to the agent for retrieval.

        :param name: Name of the agent
        :param file_path: URL of the webpage to be added, or name of existing webpage.
        :param description: Description of the webpage. Used by agent to know when to do retrieval.
        :param knowledge_base: Name of an existing knowledge base to be used. Will create a default knowledge base if not given.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
        :param filters: Include only URLs that match these regex patterns
        """
        self.add_webpages(name, [url], description, knowledge_base=knowledge_base, crawl_depth=crawl_depth, filters=filters)

    def add_database(self, name: str, database: str, tables: List[str], description: str):
        """
        Add a database to the agent for retrieval.

        :param name: Name of the agent
        :param database: Name of the database to be added.
        :param tables: List of tables to be added.
        :param description: Description of the database. Used by agent to know when to do retrieval.
        """
        # Make sure database exists.
        db = self.databases.get(database)
        # Make sure tables exist.
        all_table_names = set([t.name for t in db.tables.list()])
        for t in tables:
            if t not in all_table_names:
                raise ValueError(f'Table {t} does not exist in database {database}.')

        # Make sure skill name is unique.
        skill_name = f'{database}_sql_skill_{uuid4().hex}'
        sql_params = {
            'database': database,
            'tables': tables,
            'description': description,
        }
        database_sql_skill = self.skills.create(skill_name, 'sql', sql_params)
        agent = self.get(name)

        if not agent.params:
            agent.params = {}
        if 'prompt_template' not in agent.params:
            # Set default prompt template. This is for langchain agent check.
            agent.params['prompt_template'] = 'using mindsdb sqltoolbox'

        agent.skills.append(database_sql_skill)
        self.update(agent.name, agent)

    def create(
            self,
            name: str,
            model: Union[Model, dict, str] = None,
            provider: str = None,
            skills: List[Union[Skill, str]] = None,
            params: dict = None,
            **kwargs) -> Agent:
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

        if 'prompt_template' not in params:
            params['prompt_template'] = _DEFAULT_LLM_PROMPT

        if model is None:
            model = _DEFAULT_LLM_MODEL
        elif isinstance(model, Model):
            model = model.name
            provider = 'mindsdb'

        data = self.api.create_agent(self.project.name, name, model, provider, skill_names, params)
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

        existing_agent = self.api.agent(self.project.name, name)
        existing_skills = set([s['name'] for s in existing_agent['skills']])
        skills_to_add = updated_skills.difference(existing_skills)
        skills_to_remove = existing_skills.difference(updated_skills)
        data = self.api.update_agent(
            self.project.name,
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
        _ = self.api.delete_agent(self.project.name, name)
