from mindsdb_sql.parser.ast import *


def dict_to_binary_op(filters):
    where = None
    for name, value in filters.items():
        where1 = BinaryOperation('=', args=[Identifier(name), Constant(value)])
        if where is None:
            where = where1
        else:
            where = BinaryOperation(
                'and',
                args=[where, where1]
            )
    return where




