# -*- coding:utf-8 -*-

from redis.client import StrictPipeline
from redis.exceptions import WatchError

from limpyd import redis_connect, DEFAULT_CONNECTION_SETTINGS
from limpyd.fields import RedisField
from limpyd.exceptions import *

from logging import getLogger
log = getLogger(__name__)


class RedisDatabase(object):
    _connection = None
    pipeline_mode = False
    # _models keep an entry for each defined model on this database
    _models = dict()

    def __init__(self, connection_settings=None):
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

    def pipeline(self, transaction=True):
        """
        A replacement to the default redis pipeline method which manage saving
        and restoring of the connection.
        ALL calls to redis for the current database will pass via the pipeline.
        So if you don't use watches, all getters will not return any value, but
        results will be available via the pipe.execute() call.
        Please refer to the redis-py documentation.
        To use with "with":
        ###
        # Simple multi/exec
        with database.pipeline() as pipe:
            # do some stuff...
            result = pipe.execute()
        ###
        # Simple pipeline (no transaction, no atomicity)
        with database.pipeline(transaction=False) as pipe:
            # do some stuff...
            result = pipe.execute()
        ###
        # Advanced use with watch
        with database.pipeline() as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(watches)
                    # get watched stuff
                    pipe.multi()
                    # do some stuff...
                    return pipe.execute()
                except WatchError:
                    continue
        """
        return _Pipeline(self, transaction=transaction)

    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single arguement which is a Pipeline object.
        """
        with self.pipeline(True) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func(pipe)
                    return pipe.execute()
                except WatchError:
                    continue


class _Pipeline(StrictPipeline):
    """
    A subclass of the redis pipeline class used by the databae object, which
    save its internal connection and replace it by the pipeline, allowing
    all redis calls to be managed by this pipeline
    """

    def __init__(self, database, transaction=True):
        self._database = database
        self._original_connection = database._connection
        self._original_pipeline_mode = database.pipeline_mode
        super(_Pipeline, self).__init__(
            connection_pool=database._connection.connection_pool,
            response_callbacks=database._connection.response_callbacks,
            transaction=transaction,
            shard_hint=None)
        database._connection = self
        database.pipeline_mode = True

    def watch(self, *names):
        """
        Override the default watch method to allow the user to pass RedisField
        objects as names, which will be translated to their real keys and passed
        to the default watch method
        """
        watches = []
        for watch in names:
            if isinstance(watch, RedisField):
                watch = watch.key
            watches.append(watch)
        return super(_Pipeline, self).watch(*watches)

    def reset(self):
        self._database._connection = self._original_connection
        self._database.pipeline_mode = self._original_pipeline_mode
        super(_Pipeline, self).reset()
