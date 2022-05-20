

class Files:
    def __init__(self, proxy):
        self._proxy = proxy

    def __delitem__(self, name):
        self._proxy.delete(f'/files/{name}')
