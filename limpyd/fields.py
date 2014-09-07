# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.utils import iteritems, iterkeys
from future.builtins import str
from future.builtins import zip
from future.utils import with_metaclass

from logging import getLogger
from copy import copy

from redis.exceptions import RedisError
from redis.client import Lock

from limpyd.utils import make_key, normalize
from limpyd.exceptions import *

log = getLogger(__name__)

__all__ = [
    'InstanceHashField',
    'RedisField',
    'RedisProxyCommand',
    'MetaRedisProxy',
    'SortedSetField',
    'StringField',
    'ListField',
    'SetField',
    'PKField',
    'AutoPKField',
    'HashField',
]


class MetaRedisProxy(type):
    """
    This metaclass create the class normally, then takes a list of redis
    commands found in the "available_*" class attributes, and for each one
    create the corresponding method if it not exists yet. Created methods simply
    call _call_command.
    """

    @staticmethod
    def class_can_have_commands(klass):
        """
        Return False if the given class inherits from RedisProxyCommand, which
        indicates that it can handle its own sets of available commands.
        If not a subclass of RedisProxyCommand, (for example "object", to create
        a simple mixin), returns False.
        """
        try:
            return issubclass(klass, RedisProxyCommand)
        except NameError:
            # We pass here if we are workong on RedisProxyCommand itself, which
            # is not yet defined in this case
            return False

    def __new__(mcs, name, base, dct):
        """
        Create methods for all redis commands available for the given class.
        """
        it = super(MetaRedisProxy, mcs).__new__(mcs, name, base, dct)

        # It the class we are working on is not aimed to directly have its own
        # commands defined, we don't try to manage them.
        # It's needed to use mixins, which must be based on `object`.
        # See contrib.related.*RelatedFieldMixin to see an example of such a
        # mixin

        if any([mcs.class_can_have_commands(one_base) for one_base in base]):

            # make sure we have a set for each list of type of command
            for attr in ('available_getters', 'available_modifiers', ):
                setattr(it, attr, set(getattr(it, attr, ())))

            # add simplest set: getters, modidiers, all
            it.available_commands = it.available_getters.union(it.available_modifiers)

            # create a method for each command
            for command_name in it.available_commands:
                if not hasattr(it, command_name):
                    setattr(it, command_name, it._make_command_method(command_name))

        return it


class RedisProxyCommand(with_metaclass(MetaRedisProxy)):

    @classmethod
    def _make_command_method(cls, command_name):
        """
        Return a function which call _call_command for the given name.
        Used to bind redis commands to our own calls
        """
        def func(self, *args, **kwargs):
            return self._call_command(command_name, *args, **kwargs)
        return func

    def _call_command(self, name, *args, **kwargs):
        """
        Check if the command to be executed is a modifier, to connect the object.
        Then call _traverse_command.
        """
        obj = getattr(self, '_instance', self)  # _instance if a field, self if an instance

        # The object may not be already connected, so if we want to update a
        # field, connect it before.
        # If the object as no PK yet, let the object create itself
        if name in self.available_modifiers and obj._pk and not obj.connected:
            obj.connect()

        # Give priority to a "_call_{commmand}" method
        meth = getattr(self, '_call_%s' % name, self._traverse_command)
        return meth(name, *args, **kwargs)

    def _traverse_command(self, name, *args, **kwargs):
        """
        Add the key to the args and call the Redis command.
        """
        if not name in self.available_commands:
            raise AttributeError("%s is not an available command for %s" %
                                 (name, self.__class__.__name__))
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


class RedisField(RedisProxyCommand):
    """
    Base class for all fields using redis data structures.
    """
    # The "_commands_to_proxy" dict take redis commands as keys, and proxy
    # method names as values. These proxy_methods must take the real command
    # name in first parameter, and a *args+**kwargs to pass needed values.
    # Their goal is to simplify management of values to index/deindex for simple
    # redis commands.
    _commands_to_proxy = {}

    _creation_order = 0  # internal class counter to keep fields ordered

    proxy_setter = None
    unique = False
    _copy_conf = {
        'args': [],
        'kwargs': ['lockable', 'default'],
        'attrs': ['name', '_instance', '_model', 'indexable', 'unique']
    }

    def __init__(self, *args, **kwargs):
        """
        Manage all field attributes
        """
        self.lockable = kwargs.get('lockable', True)
        if "default" in kwargs:
            self.default = kwargs["default"]

        self.indexable = kwargs.get("indexable", False)
        self.unique = kwargs.get("unique", False)
        if self.unique:
            if hasattr(self, "default"):
                raise ImplementationError('Cannot set "default" and "unique" together!')
            self.indexable = True

        self._reset_index_cache()

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
        if isinstance(value, (list, tuple, set)):
            result = setter(*value)
        elif isinstance(value, dict):
            result = setter(**value)
        else:
            result = setter(value)
        return result

    @property
    def key(self):
        """
        A property to return the key used in redis for the current field.
        """
        return self.make_key(
            self._instance._name,
            self._instance.pk.get(),
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
        return self._call_command('delete')

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
            "self.key" call will raise a DoesNotExist exception. We catch it
            to return False, as the field doesn't exists too.
            """
            return False
        else:
            return self.connection.exists(key)

    def _attach_to_model(self, model):
        """
        Attach the current field to a model. Can be overriden to do something
        when a model is set
        """
        self._model = model

    def _attach_to_instance(self, instance):
        """
        Attach the current field to an instance of a model. Can be overriden to
        do something when an instance is set
        """
        self._instance = instance
        self.lockable = self.lockable and instance.lockable

    def _call_command(self, name, *args, **kwargs):
        """
        Add lock management and call parent.
        """
        meth = super(RedisField, self)._call_command
        if self.indexable and name in self.available_modifiers:
            with FieldLock(self):
                try:
                    result = meth(name, *args, **kwargs)
                except:
                    self._rollback_index()
                    raise
                else:
                    return result
                finally:
                    self._reset_index_cache()
        else:
            return meth(name, *args, **kwargs)

    def _rollback_index(self):
        """
        Restore the index in its previous status, using deindexed/indexed values
        temporarily stored.
        """
        _indexed_keys = set(self._indexed_keys)
        _deindexed_keys = set(self._deindexed_keys)
        for key in _indexed_keys:
            self.remove_index(key)
        for key in _deindexed_keys:
            self.add_index(key)

    def _reset_index_cache(self):
        """
        Reset attributes used to store deindexed/indexed values, used to
        rollback the index when something failed.
        """
        self._indexed_keys = set()
        self._deindexed_keys = set()

    def add_index(self, key):
        """
        Create an index key => instance.pk.
        As traverse_commande is blind, and can't infer the final value from
        commands like ``append`` or ``setrange``, we let the command process
        then check the result, and raise before modifying the indexes if the
        value was not unique, and then remove the key.
        We should try a better algo because we can lose data if the
        UniquenessError is raised.
        """
        if self.unique:
            # Lets check if the index key already exist for another instance
            index = self.connection.smembers(key)
            if len(index) > 1:
                # this may not happen !
                raise UniquenessError("Multiple values indexed for unique field %s: %s" %
                                      (self.name, index))
            elif len(index) == 1:
                indexed_instance_pk = index.pop()
                if indexed_instance_pk != self._instance.pk.get():
                    self.connection.delete(self.key)
                    raise UniquenessError('Key %s already exists (for instance %s)' %
                                          (key, indexed_instance_pk))
        # Do index => create a key to be able to retrieve parent pk with
        # current field value
        log.debug("indexing %s with key %s" % (key, self._instance.pk.get()))
        result = self.connection.sadd(key, self._instance.pk.get())
        self._indexed_keys.add(key)
        return result

    def index(self, value=None):
        """
        Handle field index process.
        """
        assert self.indexable, "Field not indexable"

        if value is None:
            value = self.proxy_get()
        if value is not None:
            key = self.index_key(value)
            self.add_index(key)

    def remove_index(self, key):
        self.connection.srem(key, self._instance.pk.get())
        self._deindexed_keys.add(key)

    def deindex(self, value=None):
        """
        Run process of deindexing field value(s).
        """
        assert self.indexable, "Field not indexable"

        if value is None:
            value = self.proxy_get()
        if value is not None:
            key = self.index_key(value)
            self.remove_index(key)

    def index_key(self, value, *args):
        """
        Return the redis key used to store all pk of objects having the given
        value. It's the index's key.
        """
        # Ex. bikemodel:name:{bikename}
        if not self.indexable:
            raise ValueError("Field %s is not indexable, cannot ask its index_key" % self.name)
        value = self.from_python(value)
        return self.make_key(
            self._model._name,
            self.name,
            value,
        )

    def from_python(self, value):
        """
        Coerce a value before using it in Redis.
        """
        return normalize(value)

    def _reset(self, command, *args, **kwargs):
        """
        Shortcut for commands that reset values of the field.
        All will be deindexed and reindexed.
        """
        if self.indexable:
            self.deindex()
        result = self._traverse_command(command, *args, **kwargs)
        if self.indexable:
            self.index()
        return result

    def _reindex_from_result(self, command, *args, **kwargs):
        """
        Same as _reset, but uses Redis return value to reindex, to
        save one query.
        """
        if self.indexable:
            self.deindex()
        result = self._traverse_command(command, *args, **kwargs)
        if self.indexable and result is not None:
            self.index(result)
        return result

    def _del(self, command, *args, **kwargs):
        """
        Shortcut for commands that remove all values of the field.
        All will be deindexed.
        """
        if self.indexable:
            self.deindex()
        return self._traverse_command(command, *args, **kwargs)
    _call_delete = _del


class SingleValueField(RedisField):
    """
    A simple parent class for StringField, InstanceHashField and PKField, all field
    types handling a single value.
    """

    def _call_set(self, command, value, *args, **kwargs):
        """
        Helper for commands that only set a value to the field.
        """
        if self.indexable:
            current = self.proxy_get()
            if normalize(current) != normalize(value):
                if current is not None:
                    self.deindex(current)
                if value is not None:
                    self.index(value)
        return self._traverse_command(command, value, *args, **kwargs)


class StringField(SingleValueField):

    proxy_getter = "get"
    proxy_setter = "set"

    available_getters = ('get', 'getbit', 'getrange', 'strlen', 'bitcount', )
    available_modifiers = ('delete', 'getset', 'set', 'append', 'decr',
                           'incr', 'incrbyfloat', 'setbit', 'setnx',
                           'setrange', )

    _call_getset = SingleValueField._call_set
    _call_append = _call_setrange = _call_setbit = SingleValueField._reset
    _call_decr = SingleValueField._reindex_from_result
    _call_incr = SingleValueField._reindex_from_result
    _call_incrbyfloat = SingleValueField._reindex_from_result

    def _call_setnx(self, command, value):
        """
        Index only if value has been set.
        """
        result = self._traverse_command(command, value)
        if self.indexable and value is not None and result:
            self.index(value)
        return result


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

    def _add(self, command, *args, **kwargs):
        """
        Shortcut for commands that only add values to the field.
        Added values will be indexed.
        """
        if self.indexable:
            self.index(args)
        return self._traverse_command(command, *args, **kwargs)

    def _rem(self, command, *args, **kwargs):
        """
        Shortcut for commands that only remove values from the field.
        Removed values will be deindexed.
        """
        if self.indexable:
            self.deindex(args)
        return self._traverse_command(command, *args, **kwargs)

    def _pop(self, command, *args, **kwargs):
        """
        Shortcut for commands that pop a value from the field, returning it while
        removing it.
        The returned value will be deindexed
        """
        result = self._traverse_command(command, *args, **kwargs)
        if self.indexable:
            self.deindex([result])
        return result

    def index(self, values=None):
        """
        Index all values stored in the field, or only given ones if any.
        """
        assert self.indexable, "Field not indexable"

        if values is None:
            values = self.proxy_get()
        for value in values:
            if value is not None:
                key = self.index_key(value)
                self.add_index(key)

    def deindex(self, values=None):
        """
        Deindex all values stored in the field, or only given ones if any.
        """
        assert self.indexable, "Field not indexable"

        if not values:
            values = self.proxy_get()
        for value in values:
            if value is not None:
                key = self.index_key(value)
                self.remove_index(key)


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

    available_getters = ('zcard', 'zcount', 'zrange', 'zrangebyscore',
                         'zrank', 'zrevrange', 'zrevrangebyscore',
                         'zrevrank', 'zscore', )
    available_modifiers = ('delete', 'zadd', 'zincrby', 'zrem',
                           'zremrangebyrank', 'zremrangebyscore', )

    _call_zrem = MultiValuesField._rem
    _call_zremrangebyscore = _call_zremrangebyrank = RedisField._reset

    def zmembers(self):
        """
        Used as a proxy_getter to get all values stored in the field.
        """
        return self.zrange(0, -1)

    def _call_zadd(self, command, *args, **kwargs):
        """
        We do the same computation of the zadd method of StrictRedis to keep keys
        to index them (instead of indexing the whole set)
        Members (value/score) can be passed:
            - in *args, with score followed by the value, 0+ times (to respect
              the redis order)
            - in **kwargs, with value as key and score as value
        Example: zadd(1.1, 'my-key', 2.2, 'name1', 'name2', name3=3.3, name4=4.4)
        """
        if self.indexable:
            keys = []
            if args:
                if len(args) % 2 != 0:
                    raise RedisError("ZADD requires an equal number of "
                                     "values and scores")
                keys.extend(args[1::2])
            keys.extend(kwargs)  # add kwargs keys (values to index)
            self.index(keys)
        return self._traverse_command(command, *args, **kwargs)

    def _call_zincrby(self, command, value, *args, **kwargs):
        """
        This command update a score of a given value. But it can be a new value
        of the sorted set, so we index it.
        """
        if self.indexable:
            self.index([value])
        return self._traverse_command(command, value, *args, **kwargs)

    @staticmethod
    def coerce_zadd_args(*args, **kwargs):
        """
        Take arguments attended by a zadd call, named or not, and return a flat list
        that can be used.
        A callback can be called with all "values" (as *args) if defined as the
        `values_callback` named argument. Real values will then be the result of
        this callback.
        """
        values_callback = kwargs.pop('values_callback', None)

        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(args)

        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])

        values = pieces[1::2]
        if values_callback:
            values = values_callback(*values)

        scores = pieces[0::2]

        pieces = []
        for z in zip(scores, values):
            pieces.extend(z)

        return pieces


class SetField(MultiValuesField):
    """
    A field with values stored in a redis set.
    If the indexable argument is set to True on the constructor, all stored
    values will be indexed.
    sadd, srem and spop commands are optimized to index/deindex only needed values
    """

    proxy_getter = "smembers"
    proxy_setter = "sadd"

    available_getters = ('scard', 'sismember', 'smembers', 'srandmember', )
    available_modifiers = ('delete', 'sadd', 'srem', 'spop', )

    _call_sadd = MultiValuesField._add
    _call_srem = MultiValuesField._rem
    _call_spop = MultiValuesField._pop


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
    proxy_setter = "rpush"

    available_getters = ('lindex', 'llen', 'lrange', )
    available_modifiers = ('delete', 'linsert', 'lpop', 'lpush', 'lpushx',
                           'lrem', 'rpop', 'rpush', 'rpushx', 'lset',
                           'ltrim', )

    _call_lpop = _call_rpop = MultiValuesField._pop
    _call_lpush = _call_rpush = MultiValuesField._add
    _call_ltrim = RedisField._reset

    def lmembers(self):
        """
        Used as a proxy_getter to get all values stored in the field.
        """
        return self.lrange(0, -1)

    def _pushx(self, command, *args, **kwargs):
        """
        Helper for lpushx and rpushx, that only index the new values if the list
        existed when the command was called
        """
        result = self._traverse_command(command, *args, **kwargs)
        if self.indexable and result:
            self.index(args)
        return result
    _call_lpushx = _pushx
    _call_rpushx = _pushx

    def _call_lrem(self, command, count, value, *args, **kwargs):
        """
        If count is 0, we remove all elements equal to value, so we know we have
        nothing to index, and this value to deindex. In other case, we don't
        know how much elements will remain in the list, so we have to do a full
        deindex/reindex. So do it carefuly.
        """
        if not count:
            if self.indexable:
                self.deindex([value])
            return self._traverse_command(command, count, value, *args, **kwargs)
        else:
            return self._reset(command, count, value, *args, **kwargs)

    def _call_lset(self, command, index, value, *args, **kwargs):
        """
        Before setting the new value, get the previous one to deindex it. Then
        call the command and index the new value, if exists
        """
        if self.indexable:
            old_value = self.lindex(index)
            self.deindex([old_value])
        result = self._traverse_command(command, index, value, *args, **kwargs)
        if self.indexable:
            self.index([value])
        return result

    def _call_linsert(self, command, where, refvalue, value):
        result = self._traverse_command(command, where, refvalue, value)
        if self.indexable and result != -1:
            self.index([value])
        return result


class HashField(MultiValuesField):

    proxy_getter = "hgetall"
    proxy_setter = "hmset"

    available_getters = ('hget', 'hgetall', 'hmget', 'hkeys', 'hvals',
                         'hlen', )
    available_modifiers = ('delete', 'hdel', 'hmset', 'hsetnx', 'hset',
                           'hincrby', 'hincrbyfloat', )

    def _call_hmset(self, command, *args, **kwargs):
        if self.indexable:
            current = self.proxy_get()
            _to_deindex = dict((k, current[k]) for k in iterkeys(kwargs) if k in current)
            self.deindex(_to_deindex)
            self.index(kwargs)
        return self._traverse_command(command, kwargs)

    def _call_hset(self, command, key, value):
        if self.indexable:
            current = self.hget(key)
            if current != value:
                if current is not None:
                    self.deindex({key: current})
                if value is not None:
                    self.index({key: value})
        return self._traverse_command(command, key, value)

    def _call_hincrby(self, command, key, amount):
        if self.indexable:
            current = self.hget(key)
            if current is not None:
                self.deindex({key: current})
        result = self._traverse_command(command, key, amount)
        if self.indexable:
            self.index({key: result})
        return result
    _call_hincrbyfloat = _call_hincrby

    def _call_hdel(self, command, *args):
        if self.indexable:
            current = self.proxy_get()
            self.deindex(dict((k, current[k]) for k in args if k in current))
        return self._traverse_command(command, *args)

    def _call_hsetnx(self, command, key, value):
        result = self._traverse_command(command, key, value)
        if self.indexable and result:
            # hsetnx returns 1 if key has been set
            self.index({key: value})
        return result

    def _call_hmget(self, command, *args):
        # redispy needs a list, not args
        return self._traverse_command(command, args)

    def index_key(self, value, field_name):
        """
        Manage hash->field_name in the final key.
        """
        # Ex. email:headers:content_type:{content_type}
        if not self.indexable:
            raise ValueError("HashField %s is not indexable, cannot ask its index_key" % self.name)
        value = self.from_python(value)
        return self.make_key(
            self._model._name,
            self.name,
            field_name,
            value,
        )

    def index(self, values=None):
        """
        Deal with dicts and field names.
        """
        assert self.indexable, "Field not indexable"

        if values is None:
            values = self.proxy_get()
        for field_name, value in iteritems(values):
            if value is not None:
                key = self.index_key(value, field_name)
                self.add_index(key)

    def deindex(self, values=None):
        """
        Deal with dicts and field names.
        """
        assert self.indexable, "Field not indexable"

        if values is None:
            values = self.proxy_get()
        for field_name, value in iteritems(values):
            if value is not None:
                key = self.index_key(value, field_name)
                self.remove_index(key)

    def hexists(self, key):
        """
        Call the hexists command to check if the redis hash key exists for the
        current field
        """
        try:
            hashkey = self.key
        except DoesNotExist:
            """
            If the object doesn't exists anymore, its PK is deleted, so the
            "self.key" call will raise a DoesNotExist exception. We catch it
            to return False, as the field doesn't exists too.
            """
            return False
        else:
            return self.connection.hexists(hashkey, key)
    exists = hexists


class InstanceHashField(SingleValueField):
    """Field stored in the parent object hash."""

    proxy_getter = "hget"
    proxy_setter = "hset"

    available_getters = ('hget', )
    available_modifiers = ('hdel', 'hset', 'hsetnx', 'hincrby',
                           'hincrbyfloat', )

    _call_hset = SingleValueField._call_set
    _call_hdel = RedisField._del

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
        return super(InstanceHashField, self)._traverse_command(name, *args, **kwargs)

    def delete(self):
        """
        Delete the field from redis, only the hash entry
        """
        return self._call_command('hdel')

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


class PKField(SingleValueField):
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

    def _validate(self, value):
        """
        Validate that a given new pk to set is always set, and return it.
        The returned value should be normalized, and will be used without check.
        """
        if value is None:
            raise ValueError('The pk for %s is not "auto-increment", you must fill it' %
                            self._model._name)
        value = self.normalize(value)

        # Check that this pk does not already exist
        if self.exists(value):
            raise UniquenessError('PKField %s already exists for model %s)' %
                                  (value, self._instance.__class__))

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
        except (AttributeError, DoesNotExist):
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
        value = self._validate(value)

        # Tell the model the pk is now set
        self._instance._set_pk(value)
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
        if not hasattr(self, '_instance'):
            raise ImplementationError("Impossible to get the PK of an unbound field")
        if not hasattr(self._instance, '_pk'):
            raise DoesNotExist("The current object doesn't exists anymore")

        if not self._instance._pk:
            self.set(value=None)

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

    def _validate(self, value):
        """
        Validate that a given new pk to set is always set to None, then return
        a new pk
        """
        if value is not None:
            raise ValueError('The pk for %s is "auto-increment", you must not fill it' %
                            self._model._name)
        key = self._instance.make_key(self._model._name, 'max_pk')
        return self.normalize(self.connection.incr(key))


class FieldLock(Lock):
    """
    This subclass of the Lock object is used to add a lock on the field. It will
    be used on write operations to block writes for other instances on this
    field, during all operations needed to do a deindex+write+index.
    Only one lock is done on a specific field for a specific model. If during
    lock, another one is asked in the same thread, we assume that it's a
    operation that must be done during the main lock and we don't wait for
    release.
    """

    def __init__(self, field, timeout=5, sleep=0.1):
        """
        Save the field and create a real lock,, using the correct connection
        and a computed lock key based on the names of the field and its model.
        """
        self.field = field
        self.sub_lock_mode = False
        super(FieldLock, self).__init__(
            redis=field._model.get_connection(),
            name=make_key(field._model._name, 'lock-for-update', field.name),
            timeout=timeout,
            sleep=sleep,
        )

    def _get_already_locked_by_model(self):
        """
        A lock is self_locked if already set for the current field+model on the current
        thread.
        """
        return self.field._model._is_field_locked(self.field)

    def _set_already_locked_by_model(self, value):
        if value:
            self.field._model._mark_field_as_locked(self.field)
        else:
            self.field._model._unmark_field_as_locked(self.field)

    already_locked_by_model = property(_get_already_locked_by_model, _set_already_locked_by_model)

    def acquire(self, *args, **kwargs):
        """
        Really acquire the lock only if it's not a sub-lock. Then save the
        sub-lock status.
        """
        if not self.field.lockable:
            return
        if self.already_locked_by_model:
            self.sub_lock_mode = True
            return
        self.already_locked_by_model = True
        super(FieldLock, self).acquire(*args, **kwargs)

    def release(self, *args, **kwargs):
        """
        Really release the lock only if it's not a sub-lock. Then save the
        sub-lock status and mark the model as unlocked.
        """
        if not self.field.lockable:
            return
        if self.sub_lock_mode:
            return
        super(FieldLock, self).release(*args, **kwargs)
        self.already_locked_by_model = self.sub_lock_mode = False

    def __exit__(self, *args, **kwargs):
        """
        Mark the model as unlocked.
        """
        super(FieldLock, self).__exit__(*args, **kwargs)
        if not self.field.lockable:
            return
        if not self.sub_lock_mode:
            self.already_locked_by_model = False
