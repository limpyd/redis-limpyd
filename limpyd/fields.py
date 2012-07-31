# -*- coding:utf-8 -*-

from logging import getLogger
from redis.exceptions import RedisError

from limpyd import redis_connect, DEFAULT_CONNECTION_SETTINGS
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
    'ListField',
    'SetField',
    'PKField',
    'AutoPKField',
]


def make_func(name):
    """
    Return a function which call _traverse_command for the given name.
    Used to bind redis commands to our own calls
    """
    def func(self, *args, **kwargs):
        return self._traverse_command(name, *args, **kwargs)
    return func


class MetaRedisProxy(type):

    def __new__(mcs, name, base, dct):
        it = type.__new__(mcs, name, base, dct)
        available_commands = set(it.available_getters + it.available_modifiers)
        setattr(it, "available_commands", available_commands)
        for command_name in [c for c in available_commands if not hasattr(it, c)]:
            setattr(it, command_name, make_func(command_name))
        return it


class RedisProxyCommand(object):

    __metaclass__ = MetaRedisProxy
    available_getters = tuple()
    available_modifiers = tuple()
    available_commands = available_getters + available_modifiers

    @memoize_command()
    def _traverse_command(self, name, *args, **kwargs):
        """Add the key to the args and call the Redis command."""
        # TODO: implement instance level cache
        if not name in self.available_commands:
            raise AttributeError("%s is not an available command for %s" % (name, self.__class__.__name__))
        attr = getattr(self.connection, "%s" % name)
        key = self.key
        log.debug(u"Requesting %s with key %s and args %s" % (name, key, args))
        result = attr(key, *args, **kwargs)
        result = self.post_command(
            sender=self,
            name=name,
            result=result,
            args=args,
            kwargs=kwargs
        )
        return result

    def post_command(self, sender, name, result, args, kwargs):
        return result

    @classmethod
    def get_connection(cls):
        """
        Create (or get from cache) a redis connection with settings set on the
        class via CONNECTION_SETTINGS, or use the default ones
        """
        return redis_connect(getattr(cls, 'CONNECTION_SETTINGS', {}) or DEFAULT_CONNECTION_SETTINGS)

    @property
    def connection(self):
        """
        A simple property on the instance that return the connection stored on
        the class
        """
        return self.get_connection()

    def init_cache(self):
        # Implemented in fields and models.
        pass

    def get_cache(self):
        # Implemented in fields and models.
        pass

    def has_cache(self):
        """
        Is the cache already initialized?
        """
        try:
            cache = self.get_cache()
            return True
        except (KeyError, AttributeError):
            return False

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
    unique = False

    def __init__(self, *args, **kwargs):
        self.indexable = False
        self.cacheable = kwargs.get('cacheable', True)
        if "default" in kwargs:
            self.default = kwargs["default"]

    def proxy_get(self):
        """
        A helper to easily call the proxy_getter of the field
        """
        getter = getattr(self, self.proxy_getter)
        return getter()

    def proxy_set(self, value):
        """
        A helper to easily call the proxy_setter of the field
        """
        setter = getattr(self, self.proxy_setter)
        return setter(value)

    def init_cache(self):
        """
        Create the field cache key, or flush it if it already exists.
        """
        if self.cacheable:
            self._instance._cache[self.name] = {}

    def get_cache(self):
        """
        Return the local cache dict.
        """
        return self._instance._cache[self.name]

    @property
    def key(self):
        return self.make_key(
            self._instance.__class__.__name__.lower(),
            self._instance.get_pk(),
            self.name,
        )

    @property
    def sort_wildcard(self):
        """
        Key used to sort models on this field.
        """
        return self.make_key(
            self._model.__name__.lower(),
            "*",
            self.name,
        )

    @property
    def connection(self):
        if not self._model:
            raise TypeError('A field cannot use a connection if not linked to a model')
        return self._model.get_connection()

    def __copy__(self):
        """
        In the RedisModel metaclass and constructor, we need to copy the fields
        to new ones. It can be done via the copy function of the copy module.
        This __copy__ method handles the copy by creating a new field with same
        attributes, without ignoring private attributes
        """
        new_copy = self.__class__(**self.__dict__)
        for attr_name in ('name', '_instance', '_model'):
            if hasattr(self, attr_name):
                setattr(new_copy, attr_name, getattr(self, attr_name))
        return new_copy

    def make_key(self, *args):
        return make_key(*args)

    def delete(self):
        """
        Delete the field from redis.
        """
        result = self._delete_key()
        if self.cacheable:
            # delete cache
            self.init_cache()
        return result

    def _delete_key(self):
        """
        Delete the field specific key.
        """
        return self.connection.delete(self.key)

    def post_command(self, sender, name, result, args, kwargs):
        # By default, let the instance manage the post_modify signal
        return self._instance.post_command(
                   sender=self,
                   name=name,
                   result=result,
                   args=args,
                   kwargs=kwargs
               )


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
            if hasattr(self, "default"):
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

    def index_value(self, value):
        # Has traverse_commande is blind, and can't infer the final value from
        # commands like ``append`` or ``setrange``, we let the command process
        # then check the result, and raise before modifying the indexes if the
        # value was not unique, and then remove the key
        # We should try a better algo
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
                if indexed_instance_pk != self._instance.get_pk():
                    self.connection.delete(self.key)
                    raise UniquenessError('Key %s already exists (for instance %s)' % (key, indexed_instance_pk))
        # Do index => create a key to be able to retrieve parent pk with
        # current field value
        log.debug("indexing %s with key %s" % (key, self._instance.get_pk()))
        return self.connection.sadd(key, self._instance.get_pk())

    def index(self):
        """
        Index the current value of the field
        """
        self.index_value(self.proxy_get())

    def deindex_value(self, value):
        """
        Remove stored index if needed.
        """
        if value:
            value = value.decode('utf-8')
            key = self.index_key(value)
            return self.connection.srem(key, self._instance.get_pk())
        else:
            return True  # True?

    def deindex(self):
        """
        Deindex the current value of the field
        """
        self.deindex_value(self.proxy_get())

    def delete(self):
        if self.indexable:
            self.deindex()
        return super(IndexableField, self).delete()

    def index_key(self, value):
        # Ex. bikemodel:name:{bikename}
        if not self.indexable:
            raise ValueError("Field %s is not indexable, cannot ask its index_key" % self.name)
        return self.make_key(
            self._model._name,
            self.name,
            value,
        )

    def populate_instance_pk_from_index(self, value):
        key = self.index_key(value)
        pk = self.connection.get(key)
        if pk:
            self._instance._pk = int(pk)
        else:
            raise ValueError("Can't retrieve instance pk with %s = %s" % (self.name, value))


class StringField(IndexableField):

    proxy_getter = "get"
    proxy_setter = "set"
    available_getters = ('get', 'getbit', 'getrange', 'getset', 'strlen')
    available_modifiers = ('append', 'decr', 'decrby', 'getset', 'incr', 'incrby', 'incrbyfloat', 'set', 'setbit', 'setnx', 'setrange')


class IndexableMultiValuesField(IndexableField):
    """
    It's a base class for SetField, SortedSetField and ListField, to manage
    indexes when their constructor got the param "indexable" set to True.
    Indexes need more work than for simple IndexableFields as we have here many
    values in each field.
    A naive implementation is to simply deindex all existing values, call the
    wanted redis command, then reindex all.
    When possible, each commands of each impacted fields are done by catching
    values to index/deindex, to only do this work for needed values.
    It's the case for almost all defined commands, except ones where bulk
    removing is done, as zremrange* for sorted set and ltrim (and lrem in some
    cases) for lists (for these commands, the naive algorithm defined above is
    used, so use them carefully).
    See the _traverse_command method below to know how values to index/deindex
    are defined.
    """

    # The "_commands_to_proxy" dict take redis commands as keys, and proxy
    # method names as values. There proxy_methods must take the real command
    # name in first parameter, and a *args to pass needed values. Some proxy
    # methods defined here are _add, _rem and _pop. Their goal is to simplify
    # management of values to index/deindex for simple redis commands.
    _commands_to_proxy = {}

    def index(self, values=None):
        """
        Index all values stored in the field, or only given ones if any.
        """
        if values is None:
            values = self.proxy_get()
        for value in values:
            self.index_value(value)

    def deindex(self, values=None):
        """
        Deindex all values stored in the field, or only given ones if any.
        """
        if values is None:
            values = self.proxy_get()
        for value in values:
            self.deindex_value(value)

    def _traverse_command(self, name, *args, **kwargs):
        """
        The only difference with the method from IndexableField is that we can
        specify which values to index/deindex, by passing two arguments in kwargs:
        "_to_index" and "_to_deindex".
        If one of these arguments is None, or not given, all values will be
        indexed or deindexed.
        But if an empty list/set is given, no values are indexed/deindexed.
        """

        # Commands defined in "_commands_to_proxy" are handled here, to call
        # their proxy methods. Then, these proxy methods can call this
        # _traverse_command method with a named argument "_bypass_proxy" to True
        # to really do (de)indexing and execute the command
        bypass_proxy = kwargs.pop('_bypass_proxy', False)
        if name in self._commands_to_proxy and not bypass_proxy:
            command = getattr(self, self._commands_to_proxy[name])
            return command(name, *args)

        values_to_index = kwargs.pop('_to_index', None)
        values_to_deindex = kwargs.pop('_to_deindex', None)

        # TODO manage transaction
        if self.indexable and name in self.available_modifiers:
            self.deindex(values_to_deindex)

        # we don't call _traverse_command from IndexableField, but the one from
        # RedisField because we manage indexes manually here
        result = super(IndexableField, self)._traverse_command(name, *args, **kwargs)

        if self.indexable and name in self.available_modifiers:
            self.index(values_to_index)

        return result

    def _add(self, command, *args):
        """
        Helper for commands that only remove values from the field.
        Added values will be indexed.
        """
        return self._traverse_command(command, *args, _to_index=args, _to_deindex=[], _bypass_proxy=True)

    def _rem(self, command, *args):
        """
        Helper for commands that only remove values from the field.
        Removed values will be deindexed.
        """
        return self._traverse_command(command, *args, _to_index=[], _to_deindex=args, _bypass_proxy=True)

    def _pop(self, command, *args):
        """
        Helper for commands that pop a value from the field, returning it while
        removing it.
        The returned value will be deindexed
        """
        # we don't call _traverse_command from IndexableField, but the one from
        # RedisField because we manage indexes manually here
        result = super(IndexableField, self)._traverse_command(command)
        if self.indexable and result is not None:
            self.deindex([result])
        return result


class SortedSetField(IndexableMultiValuesField):
    """
    A field with values stored in a sorted set.
    If the indexable argument is set to True on the constructor, all stored
    values will be indexed. But when using zremrange* commands, all content will
    be deindexed and then reindexed as we have no way no know which values are
    removed. So use it carefuly. On the contrary, zadd, zrem and zincrby are
    optimized to only index/deindex updated values
    """

    proxy_getter = "zmembers"
    proxy_setter = "zadd"
    available_getters = ('zcard', 'zcount', 'zrange', 'zrangebyscore', 'zrank', 'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore')
    available_modifiers = ('zadd', 'zincrby', 'zrem', 'zremrangebyrank', 'zremrangebyscore')

    _commands_to_proxy = {
        'zrem': '_rem',
    }

    def zmembers(self):
        """
        Used as a proxy_getter to get all values stored in the field.
        """
        return self.zrange(0, -1)

    def zadd(self, *args, **kwargs):
        """
        We do the same computation of the zadd method of StrictRedis to keep keys
        to index them (instead of indexing the whole set)
        Members (value/score) can be passed:
            - in *args, with score followed by the value, 0+ times (to respect
              the redis order)
            - in **kwargs, with value as key and score as value
        Example: zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        keys = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            keys.extend(args[1::2])
        for pair in kwargs.iteritems():
            keys.append(pair[0])
        return self._traverse_command('zadd', *args, _to_index=keys, _to_deindex=[], **kwargs)

    def zincrby(self, value, amount=1):
        """
        This command update a score of a given value. But it can be a new value
        of the sorted set, so we index it.
        """
        return self._traverse_command('zincrby', value, amount, _to_index=[value], _to_deindex=[])


class SetField(IndexableMultiValuesField):
    """
    A field with values stored in a redis set.
    If the indexable argument is set to True on the constructor, all stored
    values will be indexed.
    sadd, srem and spop commands are optimized to index/deindex only needed values
    """

    proxy_getter = "smembers"
    proxy_setter = "sadd"
    available_getters = ('scard', 'sismember', 'smembers', 'srandmember')
    available_modifiers = ('sadd', 'spop', 'srem',)

    _commands_to_proxy = {
        'sadd': '_add',
        'srem': '_rem',
        'spop': '_pop',
    }


class ListField(IndexableMultiValuesField):
    """
    A field with values stored in a list.
    If the indexable argument is set to True on the constructor, all stored
    values will be indexed (one entry in the index for each value even if the
    value is stored many times in the list). But when using ltrim, all content
    will be deindexed and then reindexed as we have no way no know which values
    are removed. So use it carefuly. On the contrary, linsert, *pop, *push*,
    lset are optimized to only index/deindex updated values. lrem is optimized
    only if the "count" attribute is set to 0
    """

    proxy_getter = "lmembers"
    proxy_setter = "lpush"
    available_getters = ('lindex', 'llen', 'lrange')
    available_modifiers = ('linsert', 'lpop', 'lpush', 'lpushx', 'lrem', 'lset', 'ltrim', 'rpop', 'rpush', 'rpushx')

    _commands_to_proxy = {
        'lpop': '_pop',
        'rpop': '_pop',
        'lpush': '_add',
        'rpush': '_add',
        'lpushx': '_pushx',
        'rpushx': '_pushx',
    }

    def lmembers(self):
        """
        Used as a proxy_getter to get all values stored in the field.
        """
        return self.lrange(0, -1)

    def linsert(self, where, refvalue, value):
        return self._traverse_command('linsert', where, refvalue, value, _to_index=[value], _to_deindex=[])

    def _pushx(self, command, *args):
        """
        Helper for lpushx and rpushx, that only index the new values if the list
        existed when the command was called
        """
        # we don't call _traverse_command from IndexableField, but the one from
        # RedisField because we manage indexes manually here
        result = super(IndexableField, self)._traverse_command(command, *args)
        if result and self.indexable:
            self.index(args)
        return result

    def lrem(self, count, value):
        """
        If count is 0, we remove all elements equal to value, so we know we have
        nothing to index, and this value to deindex. In other case, we don't
        know how much elements will remain in the list, so we have to do a full
        deindex/reindex. So do it carefuly.
        """
        to_index = to_deindex = None
        if not count:
            to_index = []
            to_deindex = [value]
        return self._traverse_command('lrem', count, value, _to_index=to_index, _to_deindex=to_deindex)

    def lset(self, index, value):
        """
        Before setting the new value, get the previous one to deindex it. Then
        call the command and index the new value, if exists
        TODO: Need transaction
        """
        to_deindex = []
        old_value = self.lindex(index)
        if old_value is not None:
            to_deindex = [old_value]
        return self._traverse_command('lset', index, value, _to_index=[value], _to_deindex=to_deindex)


class HashableField(IndexableField):
    """Field stored in the parent object hash."""

    proxy_getter = "hget"
    proxy_setter = "hset"
    available_getters = ('hexists', 'hget')
    available_modifiers = ('hincrby', 'hincrbyfloat', 'hset', 'hsetnx')

    @property
    def key(self):
        return self._instance.key

    @property
    def sort_wildcard(self):
        return "%s->%s" % (self._model.sort_wildcard(), self.name)

    def _traverse_command(self, name, *args, **kwargs):
        """Add key AND the hash field to the args, and call the Redis command."""
        # self.name is the name of the hash key field
        args = list(args)
        args.insert(0, self.name)
        return super(HashableField, self)._traverse_command(name, *args, **kwargs)

    def _delete_key(self):
        """
        We need to delete only the field in the parent hash.
        """
        return self.connection.hdel(self.key, self.name)

    def hdel(self):
        """
        A simple proxy to the main delete method, but here to provide the real
        redis command name
        """
        return self.delete()


class PKField(RedisField):
    """
    This type of field is used as a primary key.
    There must be one, and only one instance of this field (or a subclass) on a
    model.
    If no PKField is defined on a model, an AutoPKField is automatically added.
    A PKField has no auto-increment, a pk must be passed to constructor.
    """

    # Use only a simple getter and setter. We take all control on the setter.
    proxy_getter = "get"
    proxy_setter = "set"
    available_getters = ('get',)
    available_modifiers = ('set',)

    name = 'pk'  # Default name ok the pk, can be changed by declaring a new PKField
    unique = True  # Not an indexable field, but can be usefull in loops
    _auto_increment = False  # False for PKField, True for AutoPKField
    _auto_added = False  # True only if automatically added by limpyd
    _set = False  # True when set for the first (and unique) time

    def __copy__(self):
        """
        Overload the behaviour of the copy method to copy specific fields
        """
        new_copy = super(PKField, self).__copy__()
        new_copy._auto_increment = self._auto_increment
        new_copy._auto_added = self._auto_added
        return new_copy

    def normalize(self, value):
        """
        Simple method to always have the same kind of value
        It can be overriden by converting to int
        """
        return str(value)

    def get_new(self, value):
        """
        Validate that a given new pk to set is always set, and return it
        """
        if value is None:
            raise ValueError('The pk for %s is not "auto-increment", you must fill it' % \
                            self._model._name)
        return value

    @property
    def collection_key(self):
        """
        Property that return the name of the key in Redis where are stored
        all the exinsting pk for the model hosting this PKField
        """
        return '%s:collection' % self._model._name

    def exists(self, value):
        """
        Return True if the given pk value exists for the given class
        """
        return self.connection.sismember(self.collection_key, value)

    def collection(self):
        """
        Return all available primary keys for the given class
        """
        return self.connection.smembers(self.collection_key)

    def set(self, value):
        """
        Override the default setter to check uniqueness, deny updating, and add
        the new pk to the model's collection.
        The value is not saved as a field in redis, because we don't need this.
        On an instance, we have the _pk attribute with the pk value, and when
        we ask for a collection, we get somes pks which can be used to instanciate
        new objects (holding the pk value in _pk)
        """
        # Deny updating of an already set pk
        if self._set:
            raise ValueError('A primary key cannot be updated')

        # Validate and return the value to be used as a pk
        value = self.normalize(self.get_new(value))

        # Check that this pk does not already exist
        if self.exists(value):
            raise UniquenessError('PKField %s already exists for model %s)' % (value, self._instance.__class__))

        # Tell the model the pk is now set
        self._instance._pk = value
        self._set = True

        # We have a new pk, so add it to the collection
        log.debug("Adding %s in %s collection" % (value, self._model._name))
        self.connection.sadd(self.collection_key, value)

        # Finally return 1 as we did a real redis call to the set command
        return 1

    def get(self):
        """
        We do not call the default getter as we have the value cached in the
        instance in its _pk attribute
        """
        return self.normalize(self._instance._pk)

    def delete(self):
        raise ImplementationError('PKField cannot be deleted directly.')


class AutoPKField(PKField):
    """
    A subclass of PKField that implement auto-increment. Models with an
    AutoPKField cannot pass pk to constructors, they are always set by
    incrementing the last pk used
    """
    _auto_increment = True

    def get_new(self, value):
        """
        Validate that a given new pk to set is always set to None, then return
        a new pk
        """
        if value is not None:
            raise ValueError('The pk for %s is "auto-increment", you must not fill it' % \
                            self._model._name)
        key = self._instance.make_key(self._model._name, 'max_pk')
        return self.connection.incr(key)
