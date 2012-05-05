# -*- coding:utf-8 -*-

from logging import getLogger

from limpyd import get_connection
from limpyd.utils import make_key, memoize_command
from limpyd.exceptions import *

log = getLogger(__name__)

__all__ = [
    'HashableField',
    'RedisField',
    'RedisProxyCommand',
    'MetaRedisProxy',
    'SortedSetField',
    'StringField',
]


class MetaRedisProxy(type):

    def __new__(mcs, name, base, dct):
        it = type.__new__(mcs, name, base, dct)
        available_commands = set(it.available_getters + it.available_modifiers)
        setattr(it, "available_commands", available_commands)
        return it


class RedisProxyCommand(object):

    __metaclass__ = MetaRedisProxy
    available_getters = tuple()
    available_modifiers = tuple()
    available_commands = available_getters + available_modifiers

    def __getattr__(self, name):
        """
        Return the function in redis when not found in the abstractmodel.
        """
        return lambda *args, **kwargs: self._traverse_command(name, *args, **kwargs)

    @memoize_command()
    def _traverse_command(self, name, *args, **kwargs):
        """Add the key to the args and call the Redis command."""
        # TODO: implement instance level cache
        if not name in self.available_commands:
            raise AttributeError("%s is not an available command for %s" % (name, self.__class__.__name__))
        attr = getattr(self.connection, "%s" % name)
        key = self.key
        log.debug(u"Requesting %s with key %s and args %s" % (name, key, args))
        return attr(key, *args, **kwargs)

    class transaction(object):

        def __init__(self, instance):
            # instance is your model instance
            self.instance = instance

        def __enter__(self):
            # Replace the current connection with a pipeline
            # to buffer all the command made in the with statement
            # Not working with getters: pipeline methods return pipeline instance, so
            # a .get() made with a pipeline does not have the same behaviour
            # than a .get() made with a client
            connection = get_connection()
            self.pipe = connection.pipeline()
            self.instance._connection = self.pipe
            return self.pipe

        def __exit__(self, *exc_info):
            self.pipe.execute()


class RedisField(RedisProxyCommand):
    """
    Wrapper to help use the redis data structures.
    """

    proxy_setter = None

    def __init__(self, *args, **kwargs):
        self.indexable = False
        if "default" in kwargs:
            self.default = kwargs["default"]

    @property
    def key(self):
        return self.make_key(
            self._instance.__class__.__name__.lower(),
            self._instance.pk,
            self.name,
        )

    @property
    def connection(self):
        if not self._instance:
            raise TypeError('Cannot use connection without instance')
        return self._instance.connection

    def __copy__(self):
        new_copy = self.__class__()
        new_copy.__dict__ = self.__dict__
        return new_copy

    def make_key(self, *args):
        return make_key(*args)

    def delete(self):
        """
        Delete the field from redis.
        """
        # Default value, just delete the storage key
        # (More job could be done by specific field classes)
        self.connection.delete(self.key)


class IndexableField(RedisField):
    """
    Base field for the indexable fields.

    Store data in index at save.
    Retrieve instances from these indexes.
    """

    def __init__(self, *args, **kwargs):
        super(IndexableField, self).__init__(*args, **kwargs)
        self.indexable = kwargs.get("indexable", False)
        self.unique = kwargs.get("unique", False)
        if self.unique:
            if "default" in dir(self):  # do not use hasattr, as it will call getattr
                raise ImplementationError('Cannot set "default" and "unique" together!')
            self.indexable = True

    def _traverse_command(self, name, *args, **kwargs):
        # TODO manage transaction
        if self.indexable and name in self.available_modifiers:
            self.deindex()
        result = super(IndexableField, self)._traverse_command(name, *args, **kwargs)
        if self.indexable and name in self.available_modifiers:
            self.index()
        return result

    def index(self):
        # Has traverse_commande is blind, and can't infer the final value from
        # commands like ``append`` or ``setrange``, we let the command process
        # then check the result, and raise before modifying the indexes if the
        # value was not unique, and then remove the key
        # We should try a better algo
        getter = getattr(self, self.proxy_getter)
        value = getter()
        if value:
            value = value.decode('utf-8')  # FIXME centralize utf-8 handling?
        key = self.index_key(value)
        if self.unique:
            # Lets check if the index key already exist for another instance
            index = self.connection.smembers(key)
            if len(index) > 1:
                raise UniquenessError("Multiple values indexed for unique field %s: %s" % (self.name, index))
            elif len(index) == 1:
                indexed_instance_pk = index.pop()
                if indexed_instance_pk != self._instance.pk:
                    self.connection.delete(self.key)
                    raise UniquenessError('Key %s already exists (for instance %s)' % (key, indexed_instance_pk))
        # Do index => create a key to be able to retrieve parent pk with
        # current field value
        log.debug("indexing %s with key %s" % (key, self._instance.pk))
        return self.connection.sadd(key, self._instance.pk)

    def deindex(self):
        """
        Remove stored index if needed.
        """
        getter = getattr(self, self.proxy_getter)
        value = getter()
        if value:
            value = value.decode('utf-8')
            key = self.index_key(value)
            return self.connection.srem(key, self._instance.pk)
        else:
            return True  # True?

    def delete(self):
        self.deindex()
        super(IndexableField, self).delete()

    def index_key(self, value):
        # Ex. bikemodel:name:{bikename}
        if not self.indexable:
            raise ValueError("Field %s is not indexable, cannot ask its index_key" % self.name)
        return self.make_key(
            self._parent_class,
            self.name,
            value,
        )

    def populate_instance_pk_from_index(self, value):
        key = self.index_key(value)
        pk = self.connection.get(key)
        if pk:
            self._instance._pk = pk
        else:
            raise ValueError("Can't retrieve instance pk with %s = %s" % (self.name, value))


class StringField(IndexableField):

    proxy_getter = "get"
    proxy_setter = "set"
    available_getters = ('get', 'getbit', 'getrange', 'getset', 'strlen')
    available_modifiers = ('append', 'decr', 'decrby', 'getset', 'incr', 'incrby', 'incrbyfloat', 'set', 'setbit', 'setnx', 'setrange')


class SortedSetField(RedisField):

    proxy_setter = "zadd"
    available_getters = ('zcard', 'zcount', 'zrange', 'zrangebyscore', 'zrank', 'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore')
    available_modifiers = ('zadd', 'zincrby', 'zrem', 'zremrangebyrank', 'zremrangebyscore')


class HashableField(RedisField):
    """Field stored in the parent object hash."""

    proxy_getter = "hget"
    proxy_setter = "hset"
    available_getters = ('hexists', 'hget')
    available_modifiers = ('hincrby', 'hincrbyfloat', 'hset', 'hsetnx')

    @property
    def key(self):
        return self._instance.key

    def _traverse_command(self, name, *args, **kwargs):
        """Add key AND the hash field to the args, and call the Redis command."""
        # self.name is the name of the hash key field
        args = list(args)
        args.insert(0, self.name)
        return super(HashableField, self)._traverse_command(name, *args, **kwargs)
        
    def delete(self):
        """
        We need to delete only the field in the parent hash.
        """
        self.connection.hdel(self.key, self.name)
        super(HashableField, self).delete()
