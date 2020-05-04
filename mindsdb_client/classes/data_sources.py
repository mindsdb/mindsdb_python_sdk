from mindsdb_client.classes.data_source import DataSource

class DataSources(object):
    _proxy = None
    _datasources = {}
    def __init__(self, proxy):
        self._proxy = proxy
        self.update()

    def __getitem__(self, key):
        return self._datasources[key]

    def names(self):
        return list(self._datasources.keys())

    def update(self):
        data = self._proxy.get_datasources()

        new_names = [x['name'] for x in data]
        unwanted_keys = set(self._datasources.keys()) - set(new_names)
        for key in unwanted_keys:
            del self._datasources[key]

        for ds in data:
            if ds['name'] in self._datasources:
                self._datasources[ds['name']].update(ds)
            else:
                self._datasources[ds['name']] = DataSource(ds)

    def add(self, name: str, path: str = None, url: str = None):
        if isinstance(path, str) and len(path) > 0:
            self._proxy.put_datasource(name, path)
        elif isinstance(url, str) and len(url) > 0:
            self._proxy.put_datasource_by_url(name, url)
        else:
            raise Exception('path or url must be declared')
        self.update()

    def delete(self, name):
        self._proxy.delete_datasource(name)
        self.update()

    def analyze(self, name):
        analysis = self._proxy.analyze_datasource(name)
        self.update()
        return analysis

    def get_data(self, name):
        data = self._proxy.get_datasource_data(name)
        return data

    def get_file(self, name):
        content, filename = self._proxy.get_datasource_file(name)
        return content, filename
