from contextvars import ContextVar

context_storage = ContextVar('create_context')


def set_context(name: str, value: str):
    """
    Set context value to variable

    :param name: variable name
    :param value: variable value
    """
    data = context_storage.get({})
    data[name] = value

    context_storage.set(data)


def get_context(name: str) -> str:
    """
    Get context value fom variable

    :param name: variable name
    :return: variable value
    """

    data = context_storage.get({})
    return data.get(name)


def set_saving(name: str):
    """
    Set name of saving object to context

    :param name: namve of the object
    """
    set_context('saving', name)


def is_saving() -> bool:
    """
    Returns true if object is saved at the moment
    """

    return get_context('saving') is not None

