# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import str
from future.builtins import object

import redis

from limpyd.exceptions import *
from limpyd.indexes import EqualIndex

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

    default_indexes = [EqualIndex]

    def __init__(self, **connection_settings):
        self._connection = None  # Instance level cache
        self.reset(**(connection_settings or DEFAULT_CONNECTION_SETTINGS))
        # _models keep an entry for each defined model on this database
        self._models = dict()
        super(RedisDatabase, self).__init__()

    @classmethod
    def get_default_indexes(cls):
        if cls.default_indexes is not None:
            return cls.default_indexes
        return []

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
        with same namespace and name.
        If the model already exists, check if it is the same. It can happen if the
        module is imported twice in different ways.
        If it's a new model or an existing and valid one, return the model in database: the
        one added or the existing one
        """
        name = model._name
        existing = self._models.get(name, None)
        if not existing:
            self._models[name] = model
        elif model.__name__ != existing.__name__ or model._creation_source != existing._creation_source:
            raise ImplementationError(
                'A model with namespace "%s" and name "%s" is already defined '
                'on this database' % (model.namespace, model.__name__))
        return self._models[name]

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

    @property
    def redis_version(self):
        """Return the redis version as a tuple"""
        if not hasattr(self, '_redis_version'):
            self._redis_version = tuple(
                map(int, self.connection.info().get('redis_version').split('.')[:3])
            )
        return self._redis_version

    def support_scripting(self):
        """
        Returns True if scripting is available. Checks are done in the client
        library (redis-py) AND the redis server. Result is cached, so done only
        one time.
        """
        if not hasattr(self, '_support_scripting'):
            try:
                self._support_scripting = self.redis_version >= (2, 5) \
                    and hasattr(self.connection, 'register_script')
            except:
                self._support_scripting = False
        return self._support_scripting

    def support_zrangebylex(self):
        """
        Returns True if zrangebylex is available. Checks are done in the client
        library (redis-py) AND the redis server. Result is cached, so done only
        one time.
        """
        if not hasattr(self, '_support_zrangebylex'):
            try:
                self._support_zrangebylex = self.redis_version >= (2, 8, 9) \
                    and hasattr(self.connection, 'zrangebylex')
            except:
                self._support_zrangebylex = False
        return self._support_zrangebylex

    def call_script(self, script_dict, keys=None, args=None):
        """Call a redis script with keys and args

        The first time we call a script, we register it to speed up later calls.
        We expect a dict with a ``lua`` key having the script, and the dict will be
        updated with a ``script_object`` key, with the content returned by the
        the redis-py ``register_script`` command.

        Parameters
        ----------
        script_dict: dict
            A dict with a ``lua`` entry containing the lua code. A new key, ``script_object``
            will be added after that.
        keys: list of str
            List of the keys that will be read/updated by the lua script
        args: list of str
            List of all the args expected by the script.

        Returns
        -------
        Anything that will be returned by the script

        """
        if keys is None:
            keys = []
        if args is None:
            args = []
        if 'script_object' not in script_dict:
            script_dict['script_object'] = self.connection.register_script(script_dict['lua'])
        return script_dict['script_object'](keys=keys, args=args, client=self.connection)

    def scan_keys(self, match=None, count=None):
        """Take a pattern expected by the redis `scan` command and iter on all matching keys

        Parameters
        ----------
        match: str
            The pattern of keys to look for
        count: int, default to None (redis uses 10)
            Hint for redis about the number of expected result

        Yields
        -------
        str
            All keys found by the scan, one by one. A key can be returned multiple times, it's
            related to the way the SCAN command works in redis.

        """
        cursor = 0
        while True:
            cursor, keys = self.connection.scan(cursor, match=match, count=count)
            for key in keys:
                yield key
            if not cursor or cursor == '0':  # string for redis.py < 2.10
                break


class Lock(redis.client.Lock):
    """
    Override the default Lock class to manage the fact that we use ``decode_responses=True``
    but it is not taken into account in python 3 with redis 2.10, as the lock token in
    ``threading.local`` is saved as ``bytes`` but the value retrieved from redis is decoded.
    So the equal check doesn't work.
    So we decode the value in ``threading.local`` to make this check work.
    See https://github.com/andymccurdy/redis-py/issues/694
    """

    def do_release(self, expected_token):

        if isinstance(expected_token, bytes) and \
                self.redis.connection_pool.connection_kwargs.get('decode_responses', False):
            expected_token = expected_token.decode()

        super(Lock, self).do_release(expected_token)
