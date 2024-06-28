from typing import  List

from mindsdb_sql.parser.dialects.mindsdb import CreateDatabase, DropPredictor
from mindsdb_sql.parser.ast import DropDatabase
from mindsdb_sql.parser.ast import Identifier

from mindsdb_sdk.agents import Agents
from mindsdb_sdk.databases import Databases
from mindsdb_sdk.ml_engines import MLEngines
from mindsdb_sdk.skills import Skills
from mindsdb_sdk.utils.objects_collection import CollectionBase

from .models import Models
from .query import Query
from .views import Views
from .jobs import Jobs
from .knowledge_bases import KnowledgeBases


class Project:
    """
    Allows to work with project: to manage models and views inside of it or call raw queries inside of project

    Server instance allows to manipulate project and databases (integration) on mindsdb server

    Attributes for accessing to different objects:
        - models, see :func:`~mindsdb_sdk.models.Models`
        - views, see :func:`~mindsdb_sdk.views.Views`
        - jobs, see :func:`~mindsdb_sdk.jobs.Jobs`

    It is possible to cal queries from project context:

    Making prediciton using sql:

    >>> query = project.query('select * from database.table join model1')
    >>> df = query.fetch()

    Making time series prediction:

    >>> df = project.query('''
    ...      SELECT m.saledate as date, m.ma as forecast
    ...     FROM mindsdb.house_sales_model as m
    ...     JOIN example_db.demo_data.house_sales as t
    ...     WHERE t.saledate > LATEST AND t.type = 'house'
    ...     AND t.bedrooms=2
    ...     LIMIT 4;
    ...    ''').fetch()

    """

    def __init__(self, api, name, agents: Agents = None, skills: Skills = None, knowledge_bases: KnowledgeBases = None, databases: Databases = None, ml_engines: MLEngines = None):
        self.name = name
        self.api = api

        self.models = Models(self, api)

        # old api
        self.get_model = self.models.get
        self.list_models = self.models.list
        self.create_model = self.models.create
        self.drop_model = self.models.drop

        self.views = Views(self, api)

        # old api
        self.get_view = self.views.get
        self.list_views = self.views.list
        self.create_view = self.views.create
        self.drop_view = self.views.drop

        self.jobs = Jobs(self, api)

        # old api
        self.get_job = self.jobs.get
        self.list_jobs = self.jobs.list
        self.create_job = self.jobs.create
        self.drop_job = self.jobs.drop

        self.databases = databases or Databases(api)
        self.knowledge_bases = knowledge_bases or KnowledgeBases(self, api)

        self.skills = skills or Skills(api, name)
        self.agents = agents or Agents(api, name, self.knowledge_bases, self.databases, self.models, ml_engines, self.skills)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql: str) -> Query:
        """
        Execute raw query inside of project

        :param sql: sql query
        :return: Query object
        """
        return Query(self.api, sql, database=self.name)

    def drop_model_version(self, name: str, version: int):
        """
        Drop version of the model

        :param name: name of the model
        :param version: version to drop
        """
        ast_query = DropPredictor(Identifier(parts=[name, str(version)]))

        self.query(ast_query.to_string()).fetch()



class Projects(CollectionBase):
    """
    Projects
    ----------

    list of projects

    >>> projects.list()

    create

    >>> project = projects.create('proj')

    drop

    >>> projects.drop('proj')

    get existing

    >>> projects.get('proj')

    by attribute
    >>> projects.proj

    """

    def __init__(self, api):
        self.api = api

    def _list_projects(self):
        data = self.api.sql_query("select NAME from information_schema.databases where TYPE='project'")
        return list(data.NAME)

    def list(self) -> List[Project]:
        """
        Show list of project on server

        :return: list of Project objects
        """
        # select * from information_schema.databases where TYPE='project'
        return [Project(self.api, name) for name in self._list_projects()]

    def get(self, name: str = 'mindsdb') -> Project:
        """
        Get Project by name

        :param name: name of project
        :return: Project object
        """
        if name not in self._list_projects():
            raise AttributeError("Project doesn't exist")
        return Project(self.api, name)

    def create(self, name: str) -> Project:
        """
        Create new project and return it

        :param name: name of the project
        :return: Project object
        """

        ast_query = CreateDatabase(
            name=Identifier(name),
            engine='mindsdb',
            parameters={}
        )

        self.api.sql_query(ast_query.to_string())
        return Project(self.api, name)

    def drop(self, name: str):
        """
        Drop project from server

        :param name: name of the project
        """
        ast_query = DropDatabase(name=Identifier(name))
        self.api.sql_query(ast_query.to_string())