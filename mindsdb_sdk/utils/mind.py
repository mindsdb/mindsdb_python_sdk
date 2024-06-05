import requests


# Define the Mind entity
class Mind:
    """
    Mind entity
    """

    def __init__(self, name):
        self.name = name


# Create mind entity util function
def create(
        base_url: str,
        api_key: str,
        model: str,
        connection_args: dict,
        data_source: str,
        description: str,
) -> Mind:
    """
    Create a mind entity in LiteLLM proxy.

    Args:
        base_url: MindsDB base URL
        api_key: MindsDB API key
        model: Model name
        connection_args: Connection arguments
        data_source: Data source
        description: Description

    Returns:
        Mind: Mind entity
    """
    url = f"{base_url}/minds"
    headers = {"Authorization": f"Bearer {api_key}"}
    payload = {
        "model": model,
        "connection_args": connection_args,
        "data_source": data_source,
        "description": description
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    name = response.json()['name']

    return Mind(name=name)
