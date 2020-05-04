class Predictor(object):
    _data = None
    _proxy = None

    def __init__(self, data, proxy):
        self._proxy = proxy
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def update(self, new_data):
        _data = new_data

    def predict(self, when):
        return self._proxy.predict(self._data['name'], when)

    def download(self):
        return self._proxy.download_predictor(self._data['name'])
