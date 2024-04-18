from contextvars import ContextVar

context_storage = ContextVar('create_context')


def set_context(name, value):
    data = context_storage.get({})
    data[name] = value

    context_storage.set(data)


def get_context(name):

    data = context_storage.get({})
    return data.get(name)


def set_saving(name):
    set_context('saving', name)


def is_saving():
    return get_context('saving') is not None

