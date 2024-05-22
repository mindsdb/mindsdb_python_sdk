from typing import List

import pandas as pd

from mindsdb_sdk.databases import Databases


N_ROWS = 10


def get_dataframe_schema(df: pd.DataFrame):
    """
    Get the schema of a DataFrame

    :param df: DataFrame

    :return: list of dictionaries containing column names and types
    """
    # Get the dtypes Series
    try:
        df = df.convert_dtypes()
    except Exception as e:
        raise f"Error converting dtypes: {e}"

    dtypes = df.dtypes

    # Convert the dtypes Series into a list of dictionaries
    schema = [{"name": column, "type": dtype.name} for column, dtype in dtypes.items()]

    return schema


def get_table_schemas(database: Databases, included_tables: List[str] = None, n_rows: int = N_ROWS) -> dict:
    """
    Get table schemas from a database

    :param database: database object
    :param included_tables: list of table names to get schemas for
    :param n_rows: number of rows to fetch from each table

    :return: dictionary containing table schemas
    """

    tables = [table.name for table in database.tables.list()]

    if included_tables:
        tables = [table for table in tables if table in included_tables]

    table_schemas = {}
    for table in tables:
        table_df = database.get_table(table).limit(n_rows).fetch()
        table_schemas[table] = get_dataframe_schema(table_df)

    return table_schemas
