from dataclasses import dataclass
from typing import List, Union

from mindsdb_sql.parser.ast import Show, Identifier
from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine, DropMLEngine

from mindsdb_sdk.utils.objects_collection import CollectionBase

from .handlers import Handler

@dataclass
class MLEngine:
    """
    :meta private:
    """
    name: str
    handler: str
    connection_data: dict


class MLEngines(CollectionBase):
    """

    **ML engines collection**

    Examples of usage:

    Get list

    >>> ml_engines = con.ml_engines.list()

    Get

    >>> openai_engine = con.ml_engines.openai1

    Create

    >>> con.ml_engines.create(
    ...    'openai1',
    ...    'openai',
    ...    connection_data={'api_key': '111'}
    ...)

    Drop

    >>>  con.ml_engines.drop('openai1')

    Upload BYOM model.
    After uploading a new ml engin will be availbe to create new model from it.

    >>> model_code = open('/path/to/model/code').read()
    >>> model_requirements = open('/path/to/model/requirements').read()
    >>> ml_engine = con.ml_engines.create_byom(
    ...    'my_byom_engine',
    ...    code=model_code,
    ...    requirements=model_requirements
    ...)

    """

    def __init__(self, api):
        self.api = api

    def list(self) -> List[MLEngine]:
        """
        Returns list of ml engines on server

        :return: list of ml engines
        """

        ast_query = Show(category='ml_engines')

        df = self.api.sql_query(ast_query.to_string())
        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            MLEngine(**item)
            for item in df.to_dict('records')
        ]

    def get(self, name: str) -> MLEngine:
        """
        Get ml engine by name

        :param name
        :return: ml engine object
        """
        name = name.lower()
        for item in self.list():
            if item.name == name:
                return item
        raise AttributeError(f"MLEngine doesn't exist {name}")

    def create(self, name: str, handler: Union[str, Handler], connection_data: dict = None) -> MLEngine:
        """
        Create new ml engine and return it

        :param name: ml engine name, string
        :param handler: handler name, string or Handler
        :param connection_data: parameters for ml engine, dict, optional
        :return: created ml engine object
        """

        if isinstance(handler, Handler):
            handler = handler.name

        ast_query = CreateMLEngine(Identifier(name), handler, params=connection_data)

        self.api.sql_query(ast_query.to_string())

        return MLEngine(name, handler, connection_data)

    def create_byom(self, name: str, code: str, requirements: Union[str, List[str]] = None):
        """
        Create new BYOM ML engine and return it

        :param code: model python code in string
        :param requirements: requirements for model. Optional if there is no special requirements.
           It can be content of 'requirement.txt' file or list of strings (item for every requirement).
        :return: created BYOM ml engine object
        """

        if requirements is None:
            requirements = ''
        elif isinstance(requirements, list):
            requirements = '\n'.join(requirements)

        self.api.upload_byom(name, code, requirements)

        return MLEngine(name, 'byom', {})

    def drop(self, name: str):
        """
        Drop ml engine by name

        :param name: name
        """
        ast_query = DropMLEngine(Identifier(name))

        self.api.sql_query(ast_query.to_string())

