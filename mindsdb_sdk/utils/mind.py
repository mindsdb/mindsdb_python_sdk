from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import uuid4

import requests
from logging import getLogger

logger = getLogger(__name__)


# Define the Mind entity
class Mind:
    """
    Mind entity
    """

    def __init__(self, name):
        self.name = name


class DataSourceConfig(BaseModel):
    """
    Represents a data source that can be made available to a Mind.
    """
    id: str = Field(default_factory=lambda: uuid4().hex)

    # Description for underlying agent to know, based on context, whether to access this data source.
    description: str


class DatabaseConfig(DataSourceConfig):
    """
    Represents a database that can be made available to a Mind.
    """

    # Integration name (e.g. postgres)
    type: str

    # Args for connecting to database.
    connection_args: dict

    # Tables to make available to the Mind (defaults to ALL).
    tables: List[str] = []


# Create mind entity util function
def create_mind(
        base_url: str,
        api_key: str,
        name: str,
        data_source_configs: List[DataSourceConfig] = None,
        model: Optional[str] = None,
) -> Mind:
    """
    Create a mind entity in LiteLLM proxy.

    Args:
        base_url (str): MindsDB base URL
        api_key (str): MindsDB API key
        name (str): Mind name
        data_source_configs (List[DataSourceConfig]): Data sources to make available to the mind
        model: Model orchestrating the AI reasoning loop

    Returns:
        Mind: Mind entity
    """

    url = f"{base_url.rstrip('/')}/minds"
    headers = {"Authorization": f"Bearer {api_key}"}
    payload = {
        "name": name,
        "data_source_configs": [d.model_dump() for d in data_source_configs],
        "model": model
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            error_message = e.response.json()
        except Exception:
            error_message = str(e)
        logger.error(f"Failed to create mind: {error_message}")
        raise e
    except Exception as e:
        logger.error(f"Failed to create mind: {e}")
        raise e

    name = response.json()['name']

    return Mind(name=name)
