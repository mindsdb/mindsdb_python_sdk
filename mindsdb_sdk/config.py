from mindsdb_sdk.connectors.rest_api import RestAPI


class Config():
    """
    **Configuration for MindsDB**
    
    This class provides methods to set and get the various configuration aspects of MindsDB.
    
    Working with configuration:

    Set default LLM configuration:

    >>> server.config.set_default_llm(
    ...     provider='openai',
    ...     model_name='gpt-4',
    ...     api_key='sk-...'
    ... )

    Get default LLM configuration:

    >>> llm_config = server.config.get_default_llm()
    >>> print(llm_config)

    Set default embedding model:

    >>> server.config.set_default_embedding_model(
    ...     provider='openai',
    ...     model_name='text-embedding-ada-002',
    ...     api_key='sk-...'
    ... )

    Get default embedding model:

    >>> embedding_config = server.config.get_default_embedding_model()

    Set default reranking model:

    >>> server.config.set_default_reranking_model(
    ...     provider='openai',
    ...     model_name='gpt-4',
    ...     api_key='sk-...'
    ... )

    Get default reranking model:

    >>> reranking_config = server.config.get_default_reranking_model()
    """
    def __init__(self, api: RestAPI):
        self.api = api
    
    def set_default_llm(
        self,
        provider: str,
        model_name: str,
        api_key: str = None,
        **kwargs
    ):
        """
        Set the default LLM configuration for MindsDB.

        :param provider: The name of the LLM provider (e.g., 'openai', 'google').
        :param model_name: The name of the model to use.
        :param api_key: Optional API key for the provider.
        :param kwargs: Additional parameters for the LLM configuration.
        """
        config = {
            "default_llm": {
                "provider": provider,
                "model_name": model_name,
                "api_key": api_key,
                **kwargs
            }
        }
        self.api.update_config(config)
        
    def get_default_llm(self):
        """
        Get the default LLM configuration for MindsDB.

        :return: Dictionary containing the default LLM configuration.
        """
        return self.api.get_config().get("default_llm", {})
        
    def set_default_embedding_model(
        self,
        provider: str,
        model_name: str,
        api_key: str = None,
        **kwargs
    ):
        """
        Set the default embedding model configuration for MindsDB.

        :param provider: The name of the embedding model provider (e.g., 'openai', 'google').
        :param model_name: The name of the embedding model to use.
        :param api_key: Optional API key for the provider.
        :param kwargs: Additional parameters for the embedding model configuration.
        """
        config = {
            "default_embedding_model": {
                "provider": provider,
                "model_name": model_name,
                "api_key": api_key,
                **kwargs
            }
        }
        self.api.update_config(config)
        
    def get_default_embedding_model(self):
        """
        Get the default embedding model configuration for MindsDB.

        :return: Dictionary containing the default embedding model configuration.
        """
        return self.api.get_config().get("default_embedding_model", {})
        
    def set_default_reranking_model(
        self,
        provider: str,
        model_name: str,
        api_key: str = None,
        **kwargs
    ):
        """
        Set the default reranking model configuration for MindsDB.

        :param provider: The name of the reranking model provider (e.g., 'openai', 'google').
        :param model_name: The name of the reranking model to use.
        :param api_key: Optional API key for the provider.
        :param kwargs: Additional parameters for the reranking model configuration.
        """
        config = {
            "default_reranking_model": {
                "provider": provider,
                "model_name": model_name,
                "api_key": api_key,
                **kwargs
            }
        }
        self.api.update_config(config)
        
    def get_default_reranking_model(self):
        """
        Get the default reranking model configuration for MindsDB.

        :return: Dictionary containing the default reranking model configuration.
        """
        return self.api.get_config().get("default_reranking_model", {})
        