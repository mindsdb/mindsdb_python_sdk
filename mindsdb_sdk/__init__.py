from .server import Server


def connect(url=None, email=None, password=None):
    if url is None:
        if email is not None:
            # default is cloud
            url = 'https://cloud.mindsdb.com'
        else:
            # is local
            url = 'http://127.0.0.1:47334'

    return Server(url=url, email=email, password=password)
