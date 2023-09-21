from .databases import Databases
from .projects import Project, Projects
from .ml_engines import MLEngines
from .handlers import Handlers


class Server(Project):
    """
    Server instance allows to manipulate project and databases (integration) on mindsdb server

    Attributes for accessing to different objects:
        - projects
        - databases
        - ml_engines

    Server is also root(mindsdb) project and has its attributes
        - models
        - views
        - jobs

    """

    def __init__(self, api):
        # server is also mindsdb project
        super().__init__(api, 'mindsdb')

        self.projects = Projects(api)

        # old api
        self.get_project = self.projects.get
        self.list_projects = self.projects.list
        self.create_project = self.projects.create
        self.drop_project = self.projects.drop

        self.databases = Databases(api)

        # old api
        self.get_database = self.databases.get
        self.list_databases = self.databases.list
        self.create_database = self.databases.create
        self.drop_database = self.databases.drop

        self.ml_engines = MLEngines(self.api)

        self.ml_handlers = Handlers(self.api, 'ml')
        self.data_handlers = Handlers(self.api, 'data')

    def __repr__(self):
        return f'{self.__class__.__name__}({self.api.url})'


