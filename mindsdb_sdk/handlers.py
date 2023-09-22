from dataclasses import dataclass
import dataclasses
from typing import List

from mindsdb_sql.parser.ast import Show, Identifier, BinaryOperation, Constant

from mindsdb_sdk.utils.objects_collection import CollectionBase


@dataclass(init=False)
class Handler:
    """
    :meta private:
    """
    name: str
    title: str
    version: str
    description: str
    connection_args: dict
    import_success: bool
    import_error: str

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


class Handlers(CollectionBase):
    """
    :meta private:
    """

    def __init__(self, api, type):
        self.api = api
        self.type = type

    def list(self) -> List[Handler]:
        """
        Returns list of handlers on server depending on type
        :return: list of handlers
        """

        ast_query = Show(
            category='HANDLERS',
            where=BinaryOperation(
                op='=',
                args=[
                    Identifier('type'),
                    Constant(self.type)
                ]
            )
        )

        df = self.api.sql_query(ast_query.to_string())
        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            Handler(**item)
            for item in df.to_dict('records')
        ]

    def get(self, name: str) -> Handler:
        """
        Get handler by name

        :param name
        :return: handler object
        """
        name = name.lower()
        for item in self.list():
            if item.name == name:
                return item
        raise AttributeError(f"Handler doesn't exist: {name}")


class MLHandlers(Handlers):
    """
       **ML handlers colection**

       Examples of usage:

       Get list

       >>> con.ml_handlers.list()

       Get

       >>> openai_handler = con.ml_handlers.openai
       >>> openai_handler = con.ml_handlers.get('openai')

    """

    ...


class DataHandlers(Handlers):
    """
        **DATA handlers colection**

        Examples of usage:

        Get list

        >>> con.data_handlers.list()

        Get

        >>> pg_handler = con.data_handlers.postgres
        >>> pg_handler = con.data_handlers.get('postgres')

    """

    ...