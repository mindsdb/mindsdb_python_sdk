from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant, Select, Star, NativeQuery
from mindsdb_sdk.query import Query

def dict_to_binary_op(filters):
    where = None
    for name, value in filters.items():
        condition = BinaryOperation('=', args=[Identifier(name), Constant(value)])

        where = add_condition(where, condition)

    return where


def add_condition(where, condition):
    if where is None:
        return condition
    else:
        return BinaryOperation(
            'and',
            args=[where, condition]
        )


def query_to_native_query(query: Query):
    return Select(
        targets=[Star()],
        from_table= NativeQuery(
            integration=Identifier(query.database),
            query=query.sql
        )
    )