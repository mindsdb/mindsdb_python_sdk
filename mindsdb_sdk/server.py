from .databases import Databases
from .projects import Project, Projects
from .ml_engines import MLEngines
from .handlers import Handlers


class Server(Project):
    """
    Server instance allows to manipulate project and databases (integration) on mindsdb server

    Attributes for accessing to different objects:

        - projects, see :func:`~mindsdb_sdk.projects.Projects`
        - databases, see :func:`~mindsdb_sdk.databases.Databases`
        - ml_engines, see :func:`~mindsdb_sdk.ml_engines.MLEngines`
        - ml_handlers, see :func:`~mindsdb_sdk.handlers.MLHandlers`
        - data_handlers, see :func:`~mindsdb_sdk.handlers.DataHandlers`

    Server is also root(mindsdb) project and has attributes of project:
        - models, see :func:`~mindsdb_sdk.models.Models`
        - views, see :func:`~mindsdb_sdk.views.Views`
        - jobs, see :func:`~mindsdb_sdk.jobs.Jobs`

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

    def status(self) -> dict:
        """
        Get server information. It could content version
        Example of getting version for local:

        >>> print(server.status()['mindsdb_version'])

        :return: server status info
        """
        return self.api.status()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.api.url})'


