import time
import os
from tempfile import NamedTemporaryFile
from mindsdb_sdk.helpers.net_helpers import sending_attempts
from mindsdb_sdk.helpers.exceptions import DataSourceException


class DataSource():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        self._analysis = None

    @sending_attempts(exception_type=DataSourceException)
    def get_info(self):
        return self._proxy.get(f'/datasources/{self.name}')

    @sending_attempts(exception_type=DataSourceException, delay=1)
    def _get_analyze_data(self):
        return self._proxy.get(f'/datasources/{self.name}/analyze')

    def analyze(self, wait_seconds=360):
        if self._analysis is None:
            threshold = time.time() + wait_seconds
            analysis = self._get_analyze_data()
            while time.time() < threshold and ('status' in analysis and analysis['status'] == 'analyzing'):
                time.sleep(10)
                analysis = self._get_analyze_data()

            if 'status' in analysis and analysis['status'] == 'analyzing':
                raise Exception(f'Analysis not yet ready after waiting for {wait_seconds}, consider setting the `wait_seconds` to a higher value or reporting a bug if you think this should not take as long as it does for your dataset.')
            self._analysis = analysis
        return self._analysis

    def get_data(self, offset=0, limit=1000, filters=None):
        params = {
            'page[offset]': offset,
            'page[size]': limit,
        }
        if filters is not None:
            params.update(filters)

        return self._proxy.get(f'/datasources/{self.name}/data/', params=params)

    # TODO delete it ?
    # def __iter__(self):
    #     return iter(list(self.analyze().keys()))

    def __len__(self):
        return self.get_data(limit=1)['rowcount']

    def __getitem__(self, k):

        if isinstance(k, slice):
            offset = k.start
            if offset is None:
                offset = 0
            limit = k.stop - offset

            return self.get_data(offset=offset, limit=limit)['data']

        else:
            data = self.get_data(offset=k, limit=1)['data']
            if len(data) == 0:
                raise IndexError('Record not found')

    def __delete__(self):
        self._proxy.delete(f'/datasources/{self.name}')



class DataSources():
    def __init__(self, proxy):
        self._proxy = proxy

    @sending_attempts(exception_type=DataSourceException)
    def list_info(self):
        return self._proxy.get('/datasources')

    def list_datasources(self):
        return [DataSource(self._proxy, x['name']) for x in self.list_info()]

    def __getitem__(self, name):
        datasources = (x['name'] for x in self.list_info())
        return DataSource(self._proxy, name) if name in datasources else None

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

        src_fd = None
        if 'df' in files:
            src_fd = NamedTemporaryFile(mode='w+', newline='')
            files['df'].to_csv(path_or_buf=src_fd, index=False)
            src_fd.flush()
            src_fd.seek(0, os.SEEK_SET)
            files['file'] = src_fd
            del files['df']
        if 'file' in files:
            src = files['file']
            if isinstance(src, str):
                src_fd = open(src, 'r')
            # check if it is a file-like object
            elif hasattr(src, 'read'):
                src_fd = src
            else:
                raise Exception(f"unknown type files['file']: {files['file']}")

        with src_fd:
            files['file'] = (src_fd.name.split('/')[-1], src_fd, 'text/csv')

            files['source_type'] = (None, 'file')
            files['source'] = (None, src_fd.name.split('/')[-1])

            files['name'] = (None, name)
            self._proxy.put(f'/datasources/{name}', files=files, data=data, params_processing=False)
