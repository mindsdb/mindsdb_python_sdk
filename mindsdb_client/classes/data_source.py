class DataSource(object):
    _data = None

    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def update(self, new_data):
        _data = new_data
