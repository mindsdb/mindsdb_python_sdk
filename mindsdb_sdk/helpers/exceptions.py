"""This module contains set of solution-specific exceptions"""


class SDKException(Exception):
    def __init__(self, ret):
        self.ret = ret


class DataSourceException(SDKException):
    pass


class IntegrationException(SDKException):
    pass


class PredictorException(SDKException):
    pass


class AccessorException(SDKException):
    pass
