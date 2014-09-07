# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import str
from future.builtins import object

import redis

from limpyd.exceptions import *

from logging import getLogger
log = getLogger(__name__)


DEFAULT_CONNECTION_SETTINGS = dict(
    host="localhost",
    port=6379,
    db=0
)


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
    _connections = {}  # class level cache

    def __init__(self, **connection_settings):
        self._connection = None  # Instance level cache
        self.reset(**(connection_settings or DEFAULT_CONNECTION_SETTINGS))
        # _models keep an entry for each defined model on this database
        self._models = dict()
        super(RedisDatabase, self).__init__()

    def connect(self, **settings):
        """
        Connect to redis and cache the new connection
        """
        # compute a unique key for this settings, for caching. Work on the whole
        # dict without directly using known keys to allow the use of unix socket
        # connection or any other (future ?) way to connect to redis
        if not settings:
            settings = self.connection_settings
        connection_key = ':'.join([str(settings[k]) for k in sorted(settings)])
        if connection_key not in self._connections:
            self._connections[connection_key] = redis.StrictRedis(
                                            decode_responses=True, **settings)
        return self._connections[connection_key]

    def reset(self, **connection_settings):
        """
        Set the new connection settings to be used and reset the connection
        cache so the next redis call will use these settings.
        """
        self.connection_settings = connection_settings
        self._connection = None

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

    def _use_for_model(self, model):
        """
        Update the given model to use the current database. Do it also for all
        of its subclasses if they share the same database. (so it's easy to
        call use_database on an abstract model to use the new database for all
        subclasses)
        """
        original_database = getattr(model, 'database', None)

        def get_models(model):
            """
            Return the model and all its submodels that are on the same database
            """
            model_database = getattr(model, 'database', None)
            if model_database == self:
                return []
            models = [model]
            for submodel in model.__subclasses__():
                if getattr(submodel, 'database', None) == model_database:
                    models += get_models(submodel)
            return models

        # put the model and all its matching submodels on the new database
        models = get_models(model)
        for _model in models:
            if not _model.abstract:
                self._add_model(_model)
                del original_database._models[_model._name]
            _model.database = self

        # return updated models
        return models

    @property
    def connection(self):
        """
        A simple property on the instance that return the connection stored on
        the class
        """
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def has_scripting(self):
        """
        Returns True if scripting is available. Checks are done in the client
        library (redis-py) AND the redis server. Resut is cached, so done only
        one time.
        """
        if not hasattr(self, '_has_scripting'):
            try:
                version = float('%s.%s' %
                    tuple(self.connection.info().get('redis_version').split('.')[:2]))
                self._has_scripting = version >= 2.5 \
                    and hasattr(self.connection, 'register_script')
            except:
                self._has_scripting = False
        return self._has_scripting
