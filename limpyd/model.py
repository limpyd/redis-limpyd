# -*- coding:utf-8 -*-

from logging import getLogger

from limpyd import get_connection
from limpyd.fields import *
from limpyd.utils import make_key
from limpyd.exceptions import *

__all__ = ['RedisModel', ]

log = getLogger(__name__)


class MetaRedisModel(MetaRedisProxy):
    """
    Manage fields.
    """
    def __new__(mcs, name, base, dct):
        it = type.__new__(mcs, name, base, dct)
        # We make invisible for user that fields where class properties
        _fields = []
        _hashable_fields = []
        attrs = dir(it)
        for attr_name in attrs:
            if attr_name.startswith("_"):
                continue
            attr = getattr(it, attr_name)
            if isinstance(attr, RedisField):
                _fields.append(attr_name)
                attr.name = attr_name
                attr._parent_class = name.lower()
                setattr(it, "_redis_attr_%s" % attr_name, attr)
                delattr(it, attr_name)
                if isinstance(attr, HashableField):
                    _hashable_fields.append(attr_name)
        setattr(it, "_fields", _fields)
        setattr(it, "_hashable_fields", _hashable_fields)
        return it


class RedisModel(RedisProxyCommand):
    """
    Base redis model.
    """

    __metaclass__ = MetaRedisModel

    def __init__(self, *args, **kwargs):
        """
        Init or retrieve an object storage in Redis.

        Here whats init manages:
        - no args, no kwargs: just instanciate in a python way, no connection to
          redis
        - some kwargs == instanciate, connect, and set the properties received
        - one arg == get from pk
        """
        # --- Meta stuff
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = getattr(self, "_redis_attr_%s" % attr_name)
            # Copy it, to avoid sharing fields between model instances
            newattr = attr.__class__(**attr.__dict__)
            newattr.name = attr.name
            newattr._parent_class = attr._parent_class
            newattr._instance = self
            setattr(self, attr_name, newattr)

        # Prepare stored connection
        self._connection = None

        # Init the pk storage (must be a field later)
        self._pk = None

        # Prepare command internal caching
        self.init_cache()

        # Validate arguments
        if len(args) > 0 and len(kwargs) > 0:
            raise ValueError('Cannot use args and kwargs to instanciate.')

        # --- Instanciate new from kwargs
        if len(kwargs) > 0:
            # First check unique fields
            # (More robust than trying to manage a "pseudotransaction", as
            # redis do not has "real" transactions)
            # Here we do not set anything, in case one unique field fails
            for field_name, value in kwargs.iteritems():
                field = getattr(self, field_name)
                if field.unique and self.exists(**{field_name: value}):
                    raise UniquenessError(u"Field `%s` must be unique. "
                                           "Value `%s` yet indexed." % (field.name, value))

            # Do instanciate
            for field_name, value in kwargs.iteritems():
                field = getattr(self, field_name)
                setter = getattr(field, field.proxy_setter)
                setter(value)

        # --- Instanciate from DB
        if len(args) == 1:
            value = args[0]
            exists = self.connection.sismember(self.collection_key(), value)
            if exists:
                self._pk = value
            else:
                raise ValueError("No %s found with pk %s" % (self.__class__.__name__, value))

    def init_cache(self):
        """
        Call it to init or clear the command cache.
        """
        self._cache = {}

    @property
    def connection(self):
        if self._connection is None:
            self._connection = get_connection()
        return self._connection

    @property
    def key(self):
        return self.make_key(self.__class__.__name__.lower(), self.pk)

    @property
    def pk(self):
        if not self._pk:
            key = self.make_key(self.__class__.__name__.lower(), 'pk')
            self._pk = self.connection.incr(key)
            # We have created it, so add it to the collection
            log.debug("Adding %s in %s collection" % (self._pk, self.__class__.__name__))
            self.connection.sadd(self.collection_key(), self._pk)
            # Default must be setted only at first initialization
            self.set_defaults()
        return self._pk

    def set_defaults(self):
        """
        Set default values to fields, if they are not yet populated.
        """
        for field_name in self._fields:
            field = getattr(self, field_name)
            if "default" in dir(field):
                setter = getattr(field, field.proxy_setter)
                getter = getattr(field, field.proxy_getter)
                has_value = getter()
                if has_value is None:
                    setter(field.default)

    @classmethod
    def collection_key(cls):
        return '%s:collection' % cls.__name__.lower()

    @classmethod
    def collection(cls, **kwargs):
        """
        Return a list of pk, eventually filtered by kwargs.
        """
        # We cannot use the current connection here, as we have no instance
        connection = get_connection()
        index_keys = list()
        for field_name, value in kwargs.iteritems():
            field = getattr(cls, "_redis_attr_%s" % field_name)
            index_keys.append(field.index_key(value))
        if len(index_keys) == 0:
            # No kwargs, we want all the collection
            index_keys.append(cls.collection_key())
        return connection.sinter(index_keys)

    @classmethod
    def exists(cls, **kwargs):
        """
        A model with the values defined by kwargs exists in db?

        `kwargs` are mandatory.
        """
        if not kwargs:
            raise ValueError(u"`Exists` method requires at least one kwarg.")
        return len(cls.collection(**kwargs)) > 0

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Retrieve one instance from db according to given kwargs.

        Optionnaly, one arg could be used to retrieve it from pk.
        """
        if len(args) == 1:  # Guess it's a pk
            pk = args[0]
        elif kwargs:
            result = cls.collection(**kwargs)
            if len(result) == 0:
                raise DoesNotExist(u"No object matching filter: %s" % kwargs)
            elif len(result) > 1:
                raise ValueError(u"More than one object matching filter: %s" % kwargs)
            else:
                pk = int(result.pop())
        else:
            raise ValueError("Invalid `get` usage with args %s and kwargs %s" % (args, kwargs))
        return cls(pk)

    @classmethod
    def get_or_connect(cls, **kwargs):
        """
        Try to retrieve an object in db, and create it if it does not exist.
        """
        try:
            inst = cls.get(**kwargs)
            created = False
        except DoesNotExist:
            inst = cls(**kwargs)
            created = True
        except Exception:
            raise
        return inst, created

    @classmethod
    def make_key(cls, *args):
        return make_key(*args)

    # --- Hash management
    @property
    def key(self):
        return self.make_key(
            self.__class__.__name__.lower(),
            self.pk,
            "hash",
        )

    def hmget(self, *args):
        if len(args) == 0:
            args = self._hashable_fields
        else:
            if not any(arg in self._hashable_fields for arg in args):
                raise ValueError("Only hashable fields can be used here.")
        # from *args to on list arg
        return self.connection.hmget(self.key, args)

    def hmset(self, **kwargs):
        if not any(kwarg in self._hashable_fields for kwarg in kwargs.keys()):
            raise ValueError("Only hashable fields can be used here.")
        # from kwargs to one dict arg
        return self.connection.hmset(self.key, kwargs)


class TestModel(RedisModel):

    name = StringField(unique=True)
    foo = HashableField(indexable=True)
