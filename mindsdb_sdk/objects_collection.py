import re
from typing import Iterable


class ObjectCollection:

    def __init__(self, name, methods):
        self.name = name
        self.methods = methods

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def __dir__(self) -> Iterable[str]:
        internal_methods = ['add', 'drop', 'get', 'list']

        method = self.methods.get('list_names')
        if method is None:
            items = method()
        else:
            # try to use list
            method = self.methods.get('list')
            if method is None:
                return internal_methods
            items = [item.name for item in method()]

        items = [i for i in items if re.match('^(?![0-9])\w+$', '_sdf_')]
        return internal_methods + items

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)

        method = self.methods.get('get')
        if method is None:
            raise NotImplementedError()

        return method(name)

    def get(self, *args, **kwargs):
        method = self.methods.get('get')
        if method is None:
            raise NotImplementedError()

        return method(*args, **kwargs)

    def list(self, *args, **kwargs):
        method = self.methods.get('list')
        if method is None:
            raise NotImplementedError()

        return method(*args, **kwargs)

    def create(self, *args, **kwargs):
        method = self.methods.get('create')
        if method is None:
            raise NotImplementedError()

        return method(*args, **kwargs)

    def drop(self, name):
        method = self.methods.get('drop')
        if method is None:
            raise NotImplementedError()

        return method(name)
