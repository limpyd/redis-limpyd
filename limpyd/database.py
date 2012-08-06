# -*- coding:utf-8 -*-


from limpyd import redis_connect, DEFAULT_CONNECTION_SETTINGS
from limpyd.exceptions import *

from logging import getLogger
log = getLogger(__name__)


class RedisDatabase(object):
    """
    A RedisDatabase regroups some models and handles the connection to Redis for
    them.
    Each model must have a database entry, but many (or all) can share the same
    RedisDatabase object (so each of these models will be stored on the same
    Redis server+database)
    In a database, two models with the same namespace (empty by default) cannot
    have the same name (defined by the class name)
    """
    _connection = None
    discard_cache = False
    # _models keep an entry for each defined model on this database
    _models = dict()

    def __init__(self, **connection_settings):
        self.connection_settings = connection_settings or DEFAULT_CONNECTION_SETTINGS
        super(RedisDatabase, self).__init__()

    def _add_model(self, model):
        """
        Save this model as one existing on this database, to deny many models
        with same namespace and name
        """
        if model._name in self._models:
            raise ImplementationError(
                'A model with namespace "%s" and name "%s" is already defined '
                'on this database' % (model.namespace, model.__name__))
        self._models[model._name] = model

    @property
    def connection(self):
        """
        A simple property on the instance that return the connection stored on
        the class
        """
        if not self._connection:
            self._connection = redis_connect(self.connection_settings)
        return self._connection
