import time
import os
from tempfile import NamedTemporaryFile


class DataSource():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        self._analysis = None

    def get_info(self):
        try:
            resp = self._proxy.get(f'/datasources/{self.name}')
        except Exception:
            return None
        return resp

    @property
    def analysis(self, wait_seconds=360):
        if self._analysis is None:
            analysis = self._proxy.get(f'/datasources/{self.name}/analyze')
            for _ in range(wait_seconds):
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

        UPDATE: the general or (single) datasource type is pandas.DataFrame
        so only this type being handled in initial version
        '''
        files = {}
        data = {}
        for k in params:
            if k not in ['file', 'df', 'source', 'source_type']:
                data[k] = params[k]
            else:
                files[k] = params[k]

        if not files:
            files = None
            self._proxy.put(f'/datasources/{name}', files=files, data=data)
            return

        if 'df' in files:
            with NamedTemporaryFile(mode='w+', newline='') as src_file:
                files['df'].to_csv(path_or_buf=src_file, index=False)
                src_file.flush()
                src_file.seek(os.SEEK_SET)
                files['file'] = (src_file.name.split('/')[-1], src_file, 'text/csv')

                files['source_type'] = (None, 'file')
                files['source'] = (None, src_file.name.split('/')[-1])
                del files['df']

                files['name'] = (None, name)
                self._proxy.put(f'/datasources/{name}', files=files, data=data, params_processing=False)
