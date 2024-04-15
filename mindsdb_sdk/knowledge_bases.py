import copy
import json
from typing import Union, List

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreateKnowledgeBase, DropKnowledgeBase
from mindsdb_sql.parser.ast import Identifier, Star, Select, BinaryOperation, Constant, Insert

from mindsdb_sdk.utils.sql import dict_to_binary_op
from mindsdb_sdk.utils.objects_collection import CollectionBase

from .models import Model
from .tables import Table
from .query import Query
from .databases import Database


class KnowledgeBase(Query):
    def __init__(self, project, data):

        self.project = project
        self.name = data['name']

        self.storage = None
        if data['storage'] is not None:
            # if name contents '.' there could be errors

            parts = data['storage'].split('.')
            if len(parts) == 2:
                database_name, table_name = parts
                database = Database(project, database_name)
                table = Table(database, table_name)
                self.storage = table

        self.model = None
        if data['model'] is not None:
            self.model = Model(self.project, {'name': data['model']})

        params = data.get('params', {})
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                params = {}

        # columns
        self.metadata_columns = params.pop('metadata_columns', [])
        self.content_columns = params.pop('content_columns', [])
        self.id_column = params.pop('id_column', None)

        self.params = params

        # query behavior
        self._query = None
        self._limit = None

        database = project.name
        self._update_query()

        super().__init__(project.api, self.sql, database)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.project.name}.{self.name})'

    def find(self, query, limit=100):
        kb = copy.deepcopy(self)
        kb._query = query
        kb._limit = limit
        kb._update_query()

        return kb

    def _update_query(self):

        ast_query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[
                self.project.name, self.name
            ])
        )
        if self._query is not None:
            ast_query.where = BinaryOperation(op='=', args=[
                Identifier('content'),
                Constant(self._query)
            ])

        if self._limit is not None:
            ast_query.limit = Constant(self._limit)
        self.sql = ast_query.to_string()


    def insert(self, data: Union[pd.DataFrame, Query, dict]):
        if isinstance(data, dict):
            data = pd.DataFrame([data])

        if isinstance(data, pd.DataFrame):
            # insert data
            data_split = data.to_dict('split')

            ast_query = Insert(
                table=Identifier(self.name),
                columns=data_split['columns'],
                values=data_split['data']
            )

            sql = ast_query.to_string()
            self.api.sql_query(sql, self.database)
        else:
            # insert from select
            table = Identifier(parts=[self.database, self.name])
            self.api.sql_query(
                f'INSERT INTO {table.to_string()} ({data.sql})',
                database=data.database
            )


class KnowledgeBases(CollectionBase):
    """
    todo
    """

    def __init__(self, project, api):
        self.project = project
        self.api = api

    def _list(self, name: str = None) -> List[KnowledgeBase]:

        # TODO add filter by project. for now 'project' is empty
        ast_query = Select(targets=[Star()], from_table=Identifier(parts=['information_schema', 'knowledge_bases']))
        if name is not None:
            ast_query.where = dict_to_binary_op({'name': name})

        df = self.api.sql_query(ast_query.to_string(), database=self.project.name)

        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            KnowledgeBase(self.project, item)
            for item in df.to_dict('records')
        ]

    def list(self) -> List[KnowledgeBase]:

        return self._list()

    def get(self, name: str) -> KnowledgeBase:

        item = self._list(name)
        if len(item) == 1:
            return item[0]
        elif len(item) == 0:
            raise AttributeError("KnowledgeBase doesn't exist")
        else:
            raise RuntimeError("Several knowledgeBases with the same name")

    def create(
        self,
        name: str,
        model: Model = None,
        storage: Table = None,
        metadata_columns: list = None,
        content_columns: list = None,
        id_column: str = None,
        params: dict = None,
    ) -> KnowledgeBase:

        params_out = {}

        if metadata_columns is not None:
            params_out['metadata_columns'] = metadata_columns

        if content_columns is not None:
            params_out['content_columns'] = content_columns

        if id_column is not None:
            params_out['id_column'] = id_column

        if params is not None:
            params_out.update(params)

        if model is not None:
            model_name = Identifier(parts=[model.project.name, model.name])
        else:
            model_name = None

        if storage is not None:
            storage_name = Identifier(parts=[storage.db.name, storage.name])
        else:
            storage_name = None

        ast_query = CreateKnowledgeBase(
            Identifier(name),
            model=model_name,
            storage=storage_name,
            params=params_out
        )

        self.api.sql_query(ast_query.to_string(), database=self.project.name)

        return self.get(name)

    def drop(self, name: str):

        ast_query = DropKnowledgeBase(Identifier(name))

        self.api.sql_query(ast_query.to_string())
