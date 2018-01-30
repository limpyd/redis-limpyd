# -*- coding:utf-8 -*-
from __future__ import unicode_literals

import threading

from redis.client import StrictPipeline
from redis.exceptions import WatchError

from limpyd.database import RedisDatabase
from limpyd.fields import RedisField


class PipelineDatabase(RedisDatabase):
    """
    In addition to the functionalities of the default RedisDatabase object, this
    one provide an abstraction to the Pipeline object from
    redis-py, to use its stored connection. This pipeline method returns an
    object, as in redis-py, which provide a watch method attending key names,
    but you can simply provide limpyd fields.
    As for redis-py, a convenience method exists to handle pipeline associated
    to the watch command: transaction.
    """

    def __init__(self, **connection_settings):
        super(PipelineDatabase, self).__init__(**connection_settings)
        self._pipelined_connection = None
        self._direct_connection = None

    def pipeline(self, transaction=True, share_in_threads=False):
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
        return _Pipeline(self, transaction=transaction, share_in_threads=share_in_threads)

    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single arguement which is a Pipeline object.
        """
        with self.pipeline(True, share_in_threads=kwargs.get('share_in_threads', False)) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func(pipe)
                    return pipe.execute()
                except WatchError:
                    continue

    @property
    def _connection(self):
        """
        If we have a pipeline, shared in thread, or in the good thread, return it
        else use the direct connection
        """
        if self._pipelined_connection is not None:
            if self._pipelined_connection.share_in_threads or \
                    threading.current_thread().ident == self._pipelined_connection.current_thread_id:
                return self._pipelined_connection


        return self._direct_connection

    @_connection.setter
    def _connection(self, value):
        """
        If the value is a pipeline, save it as the connection to use for pipelines if to be shared
        in threads or goot thread. Do not remove the direct connection.
        If it is not a pipeline, clear it, and set the direct connection again.
        """
        if isinstance(value, _Pipeline):
            self._pipelined_connection = value
        else:
            self._direct_connection = value
            self._pipelined_connection = None


class _Pipeline(StrictPipeline):
    """
    A subclass of the redis pipeline class used by the database object, which
    save its internal connection and replace it by the pipeline, allowing
    all redis calls to be managed by this pipeline
    """

    def __init__(self, database, transaction=True, share_in_threads=False):
        self._database = database
        self._original_connection = database._connection
        self.share_in_threads = share_in_threads
        if not self.share_in_threads:
            self.current_thread_id = threading.current_thread().ident
        super(_Pipeline, self).__init__(
            connection_pool=database._connection.connection_pool,
            response_callbacks=database._connection.response_callbacks,
            transaction=transaction,
            shard_hint=None)
        database._connection = self

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

    def __exit__(self, exc_type, exc_value, traceback):
        self._database._connection = self._original_connection
        super(_Pipeline, self).__exit__(exc_type, exc_value, traceback)
