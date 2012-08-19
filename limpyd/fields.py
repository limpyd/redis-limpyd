# -*- coding:utf-8 -*-

from logging import getLogger
from copy import copy
from redis.exceptions import RedisError

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


class MetaRedisProxy(type):
    """
    This metaclass create the class normally, then takes a list of redis
    commands found in the "available_commands" class attribute, and for each one
    create the corresponding method if it not exists yet. Created methos simply
    call _traverse_command.
    """

    def __new__(mcs, name, base, dct):
        it = super(MetaRedisProxy, mcs).__new__(mcs, name, base, dct)
        available_commands = set(it.available_getters + it.available_modifiers)
        setattr(it, "available_commands", available_commands)
        for command_name in [c for c in available_commands if not hasattr(it, c)]:
            setattr(it, command_name, it._make_command_method(command_name))
        return it


class RedisProxyCommand(object):

    __metaclass__ = MetaRedisProxy
    available_getters = tuple()
    available_modifiers = tuple()
    available_commands = available_getters + available_modifiers

    @classmethod
    def _make_command_method(cls, command_name):
        """
        Return a function which call _traverse_command for the given name.
        Used to bind redis commands to our own calls
        """
        def func(self, *args, **kwargs):
            return self._traverse_command(command_name, *args, **kwargs)
        return func

    @memoize_command()
    def _traverse_command(self, name, *args, **kwargs):
        """
        Add the key to the args and call the Redis command.
        """
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
        """
        Call after we got the result of a redis command.
        By default, does nothing, but must return a value.
        """
        return result

    @classmethod
    def get_connection(cls):
        """
        Return the connection from the database
        """
        return cls.database.connection

    @property
    def connection(self):
        """
        A simple property on the instance that return the connection stored on
        the class
        """
        return self.get_connection()

    def init_cache(self):
        """
        Initialize the cache Must be implemented in fields and models.
        """
        pass

    def get_cache(self):
        """
        Retrieve the cache Must be implemented in fields and models.
        """
        pass

    def has_cache(self):
        """
        Is the cache already initialized?
        """
        try:
            self.get_cache()
        except (KeyError, AttributeError):
            return False
        else:
            return True


class RedisField(RedisProxyCommand):
    """
    Base class for all fields using redis data structures.
    """
    # The "_commands_to_proxy" dict take redis commands as keys, and proxy
    # method names as values. There proxy_methods must take the real command
    # name in first parameter, and a *args+**kwargs to pass needed values.
    # Their goal is to simplify management of values to index/deindex for simple
    # redis commands.
    _commands_to_proxy = {}

    _creation_order = 0  # internal class counter to keep fields ordered

    proxy_setter = None
    unique = False
    _copy_conf = {
        'args': [],
        'kwargs': ['cacheable', 'default'],
        'attrs': ['name', '_instance', '_model', 'indexable', 'unique']
    }

    def __init__(self, *args, **kwargs):
        """
        Manage all field attributes
        """
        self.indexable = False
        self.cacheable = kwargs.get('cacheable', True)
        if "default" in kwargs:
            self.default = kwargs["default"]

        self.indexable = kwargs.get("indexable", False)
        self.unique = kwargs.get("unique", False)
        if self.unique:
            if hasattr(self, "default"):
                raise ImplementationError('Cannot set "default" and "unique" together!')
            self.indexable = True

        # keep fields ordered
        self._creation_order = RedisField._creation_order
        RedisField._creation_order += 1

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
        """
        A property to return the key used in redis for the current field.
        """
        return self.make_key(
            self._instance._name,
            self._instance.get_pk(),
            self.name,
        )

    @property
    def database(self):
        """
        A simple shortcut to access the database property of the field's instance
        """
        if not self._model:
            raise TypeError('A field cannot use a database if not linked to a model')
        return self._model.database

    @property
    def sort_wildcard(self):
        """
        Key used to sort models on this field.
        """
        return self.make_key(
            self._model._name,
            "*",
            self.name,
        )

    @property
    def connection(self):
        """
        A simple shortcut to get the connections of the field's instance's model
        """
        if not self._model:
            raise TypeError('A field cannot use a connection if not linked to a model')
        return self._model.get_connection()

    def __copy__(self):
        """
        In the RedisModel metaclass and constructor, we need to copy the fields
        to new ones. It can be done via the copy function of the copy module.
        This __copy__ method handles the copy by creating a new field with same
        attributes, without ignoring private attributes.
        Configuration of args and kwargs to pass to the constructor, and
        attributes to copy is done in the _copy_conf attribute of the class, a
        dict with 3 entries:
          - args: list of attributes names to pass as *args to the constructor
          - kwargs: list of attributes names to pass as **kwargs to the
                    constructor. If a tuple is used instead of a simple string
                    in the list, its first entry will be the kwarg name, and
                    the second the name of the attribute to copy
          - attrs: list of attributes names to copy (with "=") from the old
                   object to the new one
        """
        # prepare unnamed arguments
        args = [getattr(self, arg) for arg in self._copy_conf['args']]

        # prepare named arguments
        kwargs = {}
        for arg in self._copy_conf['kwargs']:
            # if arg is a tuple, the first entry will be the named kwargs, and
            # the second will be the name of the attribute to copy
            name = arg
            if isinstance(arg, tuple):
                name, arg = arg
            if hasattr(self, arg):
                kwargs[name] = getattr(self, arg)

        # create the new instance
        new_copy = self.__class__(*args, **kwargs)

        # then copy attributes
        for attr_name in self._copy_conf['attrs']:
            if hasattr(self, attr_name):
                setattr(new_copy, attr_name, getattr(self, attr_name))

        return new_copy

    def make_key(self, *args):
        """
        Simple shortcut to the make_key global function to create a redis key
        based on all given arguments.
        """
        return make_key(*args)

    def delete(self):
        """
        Delete the field from redis.
        """
        if self.indexable:
            self.deindex()
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
        """
        Call after we got the result of a redis command.
        By default, let the instance manage the post_modify signal
        """
        return self._instance.post_command(
                   sender=self,
                   name=name,
                   result=result,
                   args=args,
                   kwargs=kwargs
               )

    def exists(self):
        """
        Call the exists command to check if the redis key exists for the current
        field
        """
        try:
            key = self.key
        except DoesNotExist:
            """
            If the object doesn't exists anymore, its PK is deleted, so the
            "self.key" call will raise a DoesnotExist exception. We catch it
            to return False, as the field doesn't exists too.
            """
            return False
        else:
            return self.connection.exists(key)

    def _traverse_command(self, name, *args, **kwargs):
        """
        In addition to the default _traverse_command, we manage indexes.
        Values to specifically deindex and index can be passed in kwargs via the
        "to_index" and "to_deindex" arguments (without them, the whole field
        will be deindexed and/or indexed)
        It's also possible to pass two callbacks as kwargs:
        - "pre_callback" will be executed before starting the whole stuff, ie
          before starting deindexaction. It takes the command name, and *args
          and **kwargs. Local args and kwargs will be updated with the result of
          the call to this callback
        - "post_callback" will be executed after the whole stuff is done, ie
          after the indexation is done. It takes the command's result and return
          a final one.

        """
        available_params = ('to_deindex', 'to_index', 'pre_callback', 'post_callback')
        params = dict((key, kwargs.pop(key, None)) for key in available_params)

        # if we have a proxy, call it to get update args and kwargs, to get
        # value(s) to deindex and index, and to get some callbacks
        if name in self._commands_to_proxy:
            command = getattr(self, self._commands_to_proxy[name])
            (args, kwargs, new_params) = command(name, *args, **kwargs)
            params = dict((key, new_params.get(key, None)) for key in available_params)

        # call the pre_callback if we have one to update args and kwargs
        if params.get('pre_callback', None) is not None:
            (args, kwargs) = params['pre_callback'](name, *args, **kwargs)

        # deindex given values (or all in the field if none)
        if self.indexable and name in self.available_modifiers:
            self.deindex(params['to_deindex'])

        # ask redis to run the command
        result = super(RedisField, self)._traverse_command(name, *args, **kwargs)

        # index given values (or all in the field if none)
        if self.indexable and name in self.available_modifiers:
            self.index(params['to_index'])

        # call the post_callback if we have one, to update the command's result
        if params.get('post_callback', None) is not None:
            result = params['post_callback'](result)

        return result

    def index_value(self, value):
        """
        index a specific value for this field.
        Has traverse_commande is blind, and can't infer the final value from
        commands like ``append`` or ``setrange``, we let the command process
        then check the result, and raise before modifying the indexes if the
        value was not unique, and then remove the key.
        We should try a better algo because we can lose data if the
        UniquenessError is raised.
        """
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

    def index(self, value=None):
        """
        Index the current value of the field
        """
        if value is None:
            value = self.proxy_get()
        self.index_value(value)

    def deindex_value(self, value):
        """
        Remove stored index if needed.
        """
        if value:
            key = self.index_key(value)
            return self.connection.srem(key, self._instance.get_pk())
        else:
            return True  # True?

    def deindex(self, value=None):
        """
        Deindex the current value of the field
        """
        if value is None:
            value = self.proxy_get()
        self.deindex_value(value)

    def index_key(self, value):
        """
        Return the redis key used to store all pk of objects having the given
        value. It's the index's key.
        """
        # Ex. bikemodel:name:{bikename}
        if not self.indexable:
            raise ValueError("Field %s is not indexable, cannot ask its index_key" % self.name)
        if value and isinstance(value, str):
            value = value.decode('utf-8')
        return self.make_key(
            self._model._name,
            self.name,
            value,
        )


class StringField(RedisField):

    proxy_getter = "get"
    proxy_setter = "set"
    available_getters = ('get', 'getbit', 'getrange', 'getset', 'strlen')
    available_modifiers = ('append', 'decr', 'decrby', 'getset', 'incr', 'incrby', 'incrbyfloat', 'set', 'setbit', 'setnx', 'setrange')

    _commands_to_proxy = {
        'getset': '_set',
        'set': '_set',
    }

    def _set(self, command, *args, **kwargs):
        """
        Helper for commands that only set a value to the field.
        The value is either in the kwargs, or as the first argument of the args.
        """
        value = kwargs.get('value', args[0])
        return (args, kwargs, {'to_index': value, 'to_deindex': None})


class MultiValuesField(RedisField):
    """
    It's a base class for SetField, SortedSetField and ListField, to manage
    indexes when their constructor got the param "indexable" set to True.
    Indexes need more work than for simple RedisField as we have here many
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

    def _add(self, command, *args, **kwargs):
        """
        Helper for commands that only remove values from the field.
        Added values will be indexed.
        """
        return (args, kwargs, {'to_index': args, 'to_deindex': []})

    def _rem(self, command, *args, **kwargs):
        """
        Helper for commands that only remove values from the field.
        Removed values will be deindexed.
        """
        return (args, kwargs, {'to_index': [], 'to_deindex': args})

    def _pop(self, command, *args, **kwargs):
        """
        Helper for commands that pop a value from the field, returning it while
        removing it.
        The returned value will be deindexed
        """
        result = (args, kwargs, {'to_index': [], 'to_deindex': []})

        if self.indexable:

            def deindex_result(command_result):
                if command_result is not None:
                    self.deindex([command_result])
                return command_result

            result[2]['post_callback'] = deindex_result

        return result


class SortedSetField(MultiValuesField):
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
        return self._traverse_command('zadd', *args, to_index=keys, to_deindex=[], **kwargs)

    def zincrby(self, value, amount=1):
        """
        This command update a score of a given value. But it can be a new value
        of the sorted set, so we index it.
        """
        return self._traverse_command('zincrby', value, amount, to_index=[value], to_deindex=[])


class SetField(MultiValuesField):
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


class ListField(MultiValuesField):
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
        return self._traverse_command('linsert', where, refvalue, value, to_index=[value], to_deindex=[])

    def _pushx(self, command, *args, **kwargs):
        """
        Helper for lpushx and rpushx, that only index the new values if the list
        existed when the command was called
        """
        result = (args, kwargs, {'to_index': [], 'to_deindex': []})

        if self.indexable:

            def index_args(command_result):
                if command_result:
                    self.index(args)
                return command_result

            result[2]['post_callback'] = index_args

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
        return self._traverse_command('lrem', count, value, to_index=to_index, to_deindex=to_deindex)

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
        return self._traverse_command('lset', index, value, to_index=[value], to_deindex=to_deindex)


class HashableField(RedisField):
    """Field stored in the parent object hash."""

    proxy_getter = "hget"
    proxy_setter = "hset"
    available_getters = ('hget', )
    available_modifiers = ('hincrby', 'hincrbyfloat', 'hset', 'hsetnx')

    _commands_to_proxy = {
        'hset': '_set',
    }

    @property
    def key(self):
        return self._instance.key

    @property
    def sort_wildcard(self):
        return "%s->%s" % (self._model.sort_wildcard(), self.name)

    def _traverse_command(self, name, *args, **kwargs):
        """Add key AND the hash field to the args, and call the Redis command."""
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

    def hexists(self):
        """
        Call the hexists command to check if the redis hash key exists for the
        current field
        """
        try:
            key = self.key
        except DoesNotExist:
            """
            If the object doesn't exists anymore, its PK is deleted, so the
            "self.key" call will raise a DoesNotExist exception. We catch it
            to return False, as the field doesn't exists too.
            """
            return False
        else:
            return self.connection.hexists(key, self.name)
    exists = hexists

    def _set(self, command, *args, **kwargs):
        """
        Helper for commands that only set a value to the field.
        The value is either in the kwargs, or as the second argument of the args
        (the first one is the hash entry)
        """
        value = kwargs.get('value', args[1])
        return (args, kwargs, {'to_index': value, 'to_deindex': None})


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
    indexable = False  # Not an `indexable` field...
    unique = True  # ... but `unique` can be usefull in loops
    _auto_increment = False  # False for PKField, True for AutoPKField
    _auto_added = False  # True only if automatically added by limpyd
    _set = False  # True when set for the first (and unique) time

    _copy_conf = copy(RedisField._copy_conf)
    _copy_conf['attrs'] += ['_auto_increment', '_auto_added']

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

    def exists(self, value=None):
        """
        Return True if the given pk value exists for the given class.
        If no value is given, we use the value of the current field, which
        is the value of the "_pk" attribute of its instance.
        """
        try:
            if not value:
                value = self.get()
        except AttributeError:
            # If the instance is deleted, the _pk attribute doesn't exist
            # anymore. So we catch the AttributeError to return False (this pk
            # field doesn't exist anymore) in this specific case
            return False
        else:
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
