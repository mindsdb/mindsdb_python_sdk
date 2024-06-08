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


# Create mind entity util function
def create_mind(
        base_url: str,
        api_key: str,

        name: str,
        description: str,
        model: str,
        data_source_type: str,
        data_source_connection_args: dict,
) -> Mind:
    """
    Create a mind entity in LiteLLM proxy.

    Args:
        base_url: MindsDB base URL
        api_key: MindsDB API key
        name: Mind name
        description: Mind description
        model: Model orchestrating the AI reasoning loop
        data_source_type: Data source type
        data_source_connection_args: Data source connection arguments

    Returns:
        Mind: Mind entity
    """
    url = f"{base_url}/minds"
    headers = {"Authorization": f"Bearer {api_key}"}
    payload = {
        "name": name,
        "description": description,
        "model": model,
        "data_source_type": data_source_type,
        "data_source_connection_args": data_source_connection_args
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to create mind: {e.response.json()}")
        raise e

    name = response.json()['name']

    return Mind(name=name)
