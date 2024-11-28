import copy
import json
from typing import Union, List

import pandas as pd

from mindsdb_sql_parser.ast.mindsdb import CreateKnowledgeBase, DropKnowledgeBase
from mindsdb_sql_parser.ast import Identifier, Star, Select, BinaryOperation, Constant, Insert

from mindsdb_sdk.utils.sql import dict_to_binary_op, query_to_native_query
from mindsdb_sdk.utils.objects_collection import CollectionBase
from mindsdb_sdk.utils.context import is_saving

from .models import Model
from .tables import Table
from .query import Query
from .databases import Database


class KnowledgeBase(Query):
    """

    Knowledge base object, used to update or query knowledge base

    Add data to knowledge base:

    >>> kb.insert(pd.read_csv('house_sales.csv'))

    Query relevant results

    >>> df = kb.find('flats').fetch()

    """

    def __init__(self, api, project, data: dict):
        self.api = api
        self.project = project
        self.name = data['name']
        self.table_name = Identifier(parts=[self.project.name, self.name])

        self.storage = None
        if data.get('vector_database_table') is not None:
            database = Database(project, data['vector_database'])
            table = Table(database, data['vector_database_table'])
            self.storage = table

        self.model = None
        if data['embedding_model'] is not None:
            self.model = Model(self.project, {'name': data['embedding_model']})

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

        self._update_query()

        # empty database
        super().__init__(project.api, self.sql, None)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.project.name}.{self.name})'

    def find(self, query: str, limit: int = 100):
        """

        Query data from knowledge base.
        Knowledge base should return a most relevant results for the query

        >>> # query knowledge base
        >>> query = my_kb.find('dogs')
        >>> # fetch dataframe to client
        >>> print(query.fetch())

        :param query: text query
        :param limit: count of rows in result, default 100
        :return: Query object
        """

        kb = copy.deepcopy(self)
        kb._query = query
        kb._limit = limit
        kb._update_query()

        return kb

    def _update_query(self):

        ast_query = Select(
            targets=[Star()],
            from_table=self.table_name
        )
        if self._query is not None:
            ast_query.where = BinaryOperation(op='=', args=[
                Identifier('content'),
                Constant(self._query)
            ])

        if self._limit is not None:
            ast_query.limit = Constant(self._limit)
        self.sql = ast_query.to_string()

    def insert_files(self, file_paths: List[str]):
        """
        Insert data from file to knowledge base
        """
        self.api.insert_into_knowledge_base(
            self.project.name,
            self.name,
            data={'files': file_paths}
        )

    def insert_webpages(self, urls: List[str], crawl_depth: int = 1, filters: List[str] = None):
        """
        Insert data from crawled URLs to knowledge base.

        :param urls: URLs to be crawled and inserted.
        :param crawl_depth: How deep to crawl from each base URL. 0 = scrape given URLs only
        :param filters: Include only URLs that match these regex patterns
        """
        self.api.insert_into_knowledge_base(
            self.project.name,
            self.name,
            data={
                'urls': urls,
                'crawl_depth': crawl_depth,
                'filters': [] if filters is None else filters
            }
        )

    def insert(self, data: Union[pd.DataFrame, Query, dict]):
        """
        Insert data to knowledge base

        >>> # using dataframe
        >>> my_kb.insert(pd.read_csv('house_sales.csv'))
        >>> # using dict
        >>> my_kb.insert({'type': 'house', 'date': '2020-02-02'})

        If id is already exists in knowledge base:
        - it will be replaced
        - `id` column can be defined by id_column param, see create knowledge base

        :param data: Dataframe or Query object or dict.
        """

        if isinstance(data, Query):
            # for back compatibility
            return self.insert_query(data)

        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        else:
             raise ValueError("Unknown data type, accepted types: DataFrame, Query, dict")

        return self.api.insert_into_knowledge_base(
            self.project.name,
            self.name,
            data={'rows': data}
        )

    def insert_query(self, data: Query):
        """
        Insert data to knowledge base using query

        >>> my_kb.insert(server.databases.example_db.tables.houses_sales.filter(type='house'))

        Data will be if id (defined by id_column param, see create knowledge base) is already exists in knowledge base
        it will be replaced

        :param data: Dataframe or Query object or dict.
        """
        if is_saving():
            # generate insert from select query
            if data.database is not None:
                ast_query = Insert(
                    table=self.table_name,
                    from_select=query_to_native_query(data)
                )
                sql = ast_query.to_string()
            else:
                sql = f'INSERT INTO {self.table_name.to_string()} ({data.sql})'

            # don't execute it right now, return query object
            return Query(self, sql, self.database)

        # query have to be in context of mindsdb project
        self.api.insert_into_knowledge_base(
            self.project.name,
            self.name,
            data={'query': data.sql}
        )

    def completion(self, query, **data):
        data['query'] = query

        return self.api.knowledge_base_completion(self.project.name, self.name, data)


class KnowledgeBases(CollectionBase):
    """
    **Knowledge bases**

    Get list:

    >>> kb_list = server.knowledge_bases.list()
    >>> kb = kb_list[0]

    Get by name:

    >>> kb = server.knowledge_bases.get('my_kb')
    >>> # or :
    >>> kb = server.knowledge_bases.my_kb

    Create:

    >>> kb = server.knowledge_bases.create('my_kb')

    Drop:

    >>> server.knowledge_bases.drop('my_kb')

    """

    def __init__(self, project, api):
        self.project = project
        self.api = api

    def list(self) -> List[KnowledgeBase]:
        """

        Get list of knowledge bases inside of project:

        >>> kb_list = project.knowledge_bases.list()

        :return: list of knowledge bases
        """

        return [
            KnowledgeBase(self.api, self.project, item)
            for item in self.api.list_knowledge_bases(self.project.name)
        ]

    def get(self, name: str) -> KnowledgeBase:
        """
        Get knowledge base by name

        :param name: name of the knowledge base
        :return: KnowledgeBase object
        """

        data = self.api.get_knowledge_base(self.project.name, name)
        return KnowledgeBase(self.api, self.project, data)

    def create(
        self,
        name: str,
        model: Model = None,
        storage: Table = None,
        metadata_columns: list = None,
        content_columns: list = None,
        id_column: str = None,
        params: dict = None,
    ) -> Union[KnowledgeBase, Query]:
        """

        Create knowledge base:

        >>> kb = server.knowledge_bases.create(
        ...   'my_kb',
        ...   model=server.models.emb_model,
        ...   storage=server.databases.pvec.tables.tbl1,
        ...   metadata_columns=['date', 'author'],
        ...   content_columns=['review', 'description'],
        ...   id_column='number',
        ...   params={'a': 1}
        ...)

        :param name: name of the knowledge base
        :param model: embedding model, optional. Default: 'sentence_transformers' will be used (defined in mindsdb server)
        :param storage: vector storage, optional. Default: chromadb database will be created
        :param metadata_columns: columns to use as metadata, optional. Default: all columns which are not content and id
        :param content_columns: columns to use as content, optional. Default: all columns except id column
        :param id_column: the column to use as id, optinal. Default: 'id', if exists
        :param params: other parameters to knowledge base
        :return: created KnowledgeBase object
        """

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
            model = model.name

        payload = {
            'name': name,
            'model': model,
            'params': params_out
        }

        if storage is not None:
            payload['storage'] = {
                'database': storage.db.name,
                'table': storage.name
            }

        self.api.create_knowledge_base(self.project.name, data=payload)

        return self.get(name)

    def drop(self, name: str):
        """

        :param name:
        :return:
        """

        return self.api.delete_knowledge_base(self.project.name, name)
