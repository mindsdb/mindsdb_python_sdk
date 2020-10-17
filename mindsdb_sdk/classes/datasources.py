class DataSources():
    def __init__(self, proxy):
        self._proxy = proxy

    def list_info(self):
        return self._proxy.get('/datasources')

    def keys(self):
        self.ls_info()
        return [x['name'] for x in self._proxy.get('/datasources')]

    def __getitem__(self, key):
        return self._proxy.get(f'/datasources/{key}')

    def __len__(self) -> int:
        return len(self.keys())

    def __delete__(self, name):
        self._proxy.delete(name)
    
    def __setitem__(self, name, params):
        '''
        params is a dictionary that can contain:
        * file - File path
        * ulr - Url to file
        * source - file | url | <integration id>

        and if source == <integration id>:
            * query
            + additional integration specific params (see docs in mindsdb)
        '''
        if 'file' in params:
            # Do some file reading and post multipart
            pass
        else:
            self._proxy.put(f'/datasources/{name}', json=params)

    def analyze(self, datasource):
        name = datasource['name'] if isinstance(datasource, dict) else datasource
        return self._proxy.get(f'/datasources/{name}/analyze')