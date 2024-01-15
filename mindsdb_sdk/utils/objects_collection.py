import re
from typing import Iterable


class CollectionBase:

    def __dir__(self) -> Iterable[str]:
        internal_methods = ['create', 'drop', 'get', 'list']

        items = [item.name for item in self.list()]

        items = [i for i in items if re.match('^(?![0-9])\w+$', i)]
        return internal_methods + items

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)

        return self.get(name)


# class MethodCollection(CollectionBase):
#
#     def __init__(self, name, methods):
#         self.name = name
#         self.methods = methods
#
#     def __repr__(self):
#         return f'{self.__class__.__name__}({self.name})'
#
#     def get(self, *args, **kwargs):
#         method = self.methods.get('get')
#         if method is None:
#             raise NotImplementedError()
#
#         return method(*args, **kwargs)
#
#     def list(self, *args, **kwargs):
#         method = self.methods.get('list')
#         if method is None:
#             raise NotImplementedError()
#
#         return method(*args, **kwargs)
#
#     def create(self, *args, **kwargs):
#         method = self.methods.get('create')
#         if method is None:
#             raise NotImplementedError()
#
#         return method(*args, **kwargs)
#
#     def drop(self, name):
#         method = self.methods.get('drop')
#         if method is None:
#             raise NotImplementedError()
#
#         return method(name)
