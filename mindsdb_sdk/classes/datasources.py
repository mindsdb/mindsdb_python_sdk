import time


class DataSource():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        self._analysis = None

    def get_info(self):
        return self._proxy.get(f'/datasources/{self.name}')



    @property
    def analysis(self, wait_seconds=360):
        if self._analysis is None:
            analysis = self._proxy.get(f'/datasources/{self.name}/analyze')
            for i in range(wait_seconds):
                if 'status' in analysis and analysis['status'] == 'analyzing':
                    time.sleep(10)
                analysis = self._proxy.get(f'/datasources/{self.name}/analyze')

            if 'status' in analysis and analysis['status'] == 'analyzing':
                raise Exception(f'Analysis not yet ready after waiting for {wait_seconds}, consider setting the `wait_seconds` to a higher value or reporting a bug if you think this should not take as long as it does for your dataset.')
            self._analysis = analysis
        return self._analysis

    def __iter__(self):
        return iter(list(self.analysis.keys()))

    def __len__(self):
        return len(list(self.analysis.keys()))

    def __getitem__(self, k):
        return self.analysis[k]

    def __delete__(self):
        self._proxy.delete(f'/datasources/{self.name}')


class DataSources():
    def __init__(self, proxy):
        self._proxy = proxy

    def list_info(self):
        return self._proxy.get('/datasources')

    def list_datasources(self):
        return [DataSource(self._proxy, x['name']) for x in self._proxy.get('/datasources')]

    def __getitem__(self, name):
        return DataSource(self._proxy, name)

    def __len__(self) -> int:
        return len(self.list_datasources())

    def __delitem__(self, name):
        self._proxy.delete(f'/datasources/{name}')

    def __setitem__(self, name, params):
        '''
        params is a dictionary that can contain:
        * file - File path
        * df - pandas dataframe
        * ulr - Url to file
        * source - file | url | <integration id>
        * source_type - file | ??

        and if source == <integration id>:
            * query
            + additional integration specific params (see docs in mindsdb)
        '''
        files = {}
        data = {}
        for k in params:
            if k in ['file', 'df']:
                files[k] = params[k]
            else:
                data[k] = params[k]
        if len(files) == 0:
            files = None
        self._proxy.put(f'/datasources/{name}', files=files, data=data)
