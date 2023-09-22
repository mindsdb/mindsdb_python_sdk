from mindsdb_sdk.server import Server

from mindsdb_sdk.connectors.rest_api import RestAPI


def connect(url: str = None, login: str = None, password: str = None, is_managed: bool = False) -> Server:
    """
    Create connection to mindsdb server

    :param url: url to mindsdb server
    :param login: user login, for cloud version it contents email
    :param password: user password to login (for cloud version)
    :param is_managed: whether or not the URL points to a managed instance
    :return: Server object

    Examples
    --------

    >>> import mindsdb_sdk

    Connect to local server

    >>> server = mindsdb_sdk.connect()
    >>> server = mindsdb_sdk.connect('http://127.0.0.1:47334')

    Connect to cloud server

    >>> server = mindsdb_sdk.connect(login='a@b.com', password='-')
    >>> server = mindsdb_sdk.connect('https://cloud.mindsdb.com', login='a@b.com', password='-')

    Connect to MindsDB pro

    >>> server = mindsdb_sdk.connect('http://<YOUR_INSTANCE_IP>', login='a@b.com', password='-', is_managed=True)

    """
    if url is None:
        if login is not None:
            # default is cloud
            url = 'https://cloud.mindsdb.com'
        else:
            # is local
            url = 'http://127.0.0.1:47334'

    api = RestAPI(url, login, password, is_managed)

    return Server(api)