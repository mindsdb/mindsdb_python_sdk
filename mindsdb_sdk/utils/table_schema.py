from typing import List
from mindsdb_sdk.databases import Databases


def get_dataframe_schema(df):
    # Get the dtypes Series
    try:
        df = df.convert_dtypes()
    except Exception as e:
        raise f"Error converting dtypes: {e}"

    dtypes = df.dtypes

    # Convert the dtypes Series into a list of dictionaries
    schema = [{"name": column, "type": dtype.name} for column, dtype in dtypes.items()]

    return schema


def get_table_schemas(database: Databases, included_tables: List[str] = None):
    """
    Get table schemas from a database

    :param database: database object
    :param included_tables: list of table names to get schemas for
    :return: dictionary containing table schemas
    """

    tables = [table.name for table in database.tables.list()]

    if included_tables:
        tables = [table for table in tables if table in included_tables]

    table_schemas = {}
    for table in tables:
        table_df = database.get_table(table).fetch()
        # Convert schema to list of dictionaries
        table_schemas[table] = get_dataframe_schema(table_df)

    return table_schemas
