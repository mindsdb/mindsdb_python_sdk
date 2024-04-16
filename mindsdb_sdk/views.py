from typing import List, Union

from mindsdb_sql.parser.dialects.mindsdb import CreateView
from mindsdb_sql.parser.ast import DropView
from mindsdb_sql.parser.ast import Identifier

from mindsdb_sdk.utils.objects_collection import CollectionBase

from .query import Query
from .tables import Table


class View(Table):
    # The same as table
    pass


# TODO getting view sql from api not implemented yet
# class View(Table):
#     def __init__(self, api, data, project):
#         super().__init__(api, data['name'], project)
#         self.view_sql = data['sql']
#
#     def __repr__(self):
#         #
#         sql = self.view_sql.replace('\n', ' ')
#         if len(sql) > 40:
#             sql = sql[:37] + '...'
#
#         return f'{self.__class__.__name__}({self.name}{self._filters_repr()}, sql={sql})'

class Views(CollectionBase):
    """
     **Views**

    Get:

    >>> views = views.list()
    >>> view = views[0]

    By name:

    >>> view = views.get('view1')

    Create:

    >>> view = views.create(
    ...   'view1',
    ...   database='example_db',  # optional, can also be database object
    ...   query='select * from table1'
    ...)

    Create using query object:

    >>> view = views.create(
    ...   'view1',
    ...   query=database.query('select * from table1')
    ...)

    Getting data:

    >>> view = view.filter(a=1, b=2)
    >>> view = view.limit(100)
    >>> df = view.fetch()

    Drop view:

    >>> views.drop('view1')

    """

    def __init__(self, project, api):
        self.project = project
        self.api = api


    # The same as table
    def _list_views(self):
        df = self.api.objects_tree(self.project.name)
        df = df[df.type == 'view']

        return list(df['name'])

    def list(self) -> List[View]:
        """
        Show list of views in project

        :return: list of View objects
        """
        return [View(self.project, name) for name in self._list_views()]

    def create(self, name: str, sql: Union[str, Query], database: str = None) -> View:
        """
        Create new view in project and return it

        :param name: name of the view
        :param sql: sql query as string or query object
        :param database: datasource of the view (where input sql will be executed)
        :return: View object
        """
        if isinstance(sql, Query):
            database = sql.database
            sql = sql.sql
        elif not isinstance(sql, str):
            raise ValueError(sql)

        if database is not None:
            database = Identifier(database)
        ast_query = CreateView(
            name=Identifier(name),
            query_str=sql,
            from_table=database
        )

        self.project.query(ast_query.to_string()).fetch()
        return View(self.project, name)

    def drop(self, name: str):
        """
        Drop view from project

        :param name: name of the view
        """

        ast_query = DropView(names=[Identifier(name)])

        self.project.query(ast_query.to_string()).fetch()

    def get(self, name: str) -> View:
        """
        Get view by name from project

        :param name: name of the view
        :return: View object
        """

        if name not in self._list_views():
            raise AttributeError("View doesn't exist")
        return View(self.project, name)
