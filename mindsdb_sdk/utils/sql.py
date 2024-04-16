from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant


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


