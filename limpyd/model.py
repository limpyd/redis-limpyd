# -*- coding:utf-8 -*-

from copy import copy

from limpyd import get_connection
from limpyd.fields import *
from limpyd.utils import make_key

__all__ = ['RedisModel', 'StringField', 'SortedSetField']

class MetaRedisModel(type):
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
            if attr_name.startswith("_"): continue
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
        """
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = copy(getattr(self, "_redis_attr_%s" % attr_name))
            attr._instance = self
            setattr(self, attr_name, attr)

        # Prepare stored connection
        self._connection = None

        # Init the pk storage (must be a field later)
        self._pk = None
        
        # If a kwargs is present, that means that we want to retrieve or create
        # the object
        if len(kwargs) == 1:  # Only one kwargs to retrieve instance, for now
            name = kwargs.keys()[0]
            value = kwargs.values()[0]
            if name == "pk":
                # pk is not a field for now
                exists = self.connection.sismember(self.collection_key(), value)
                if exists:
                    self._pk = value
                else:
                    raise ValueError("No %s found with pk %s" % (self.__class__.__name__, value))
            else:
                field = getattr(self, name)
                if field.indexable is True:
                    try:
                        field.populate_instance_pk_from_index(value)
                    except ValueError:
                        # No id, we have to create the object
                        pass
        # TODO: is the purpose of this lib to instanciate the fields from the kwargs?

    @property
    def connection(self):
        if self._connection is None:
            self._connection = get_connection()
        return self._connection

    @classmethod
    def collection_key(cls):
        return '%s:collection' % cls.__name__.lower()

    @classmethod
    def collection(cls, **kwargs):
        # TODO: implement filters from kwargs
        # We cannot use the current connection here, as we have no instance
        connection = get_connection()
        return connection.smembers(cls.collection_key())

    @property
    def key(self):
        return self.make_key(self.__class__.__name__.lower(), self.pk)

    @property
    def pk(self):
        if not self._pk:
            key = self.make_key(self.__class__.__name__.lower(), 'pk')
            self._pk = self.connection.incr(key)
            # We have created it, so add it to the collection
#            print "Adding %s in %s collection" % (self._pk, self.__class__.__name__)
            self.connection.sadd(self.collection_key(), self._pk)
        return self._pk

    @classmethod
    def exists(cls, **kwargs):
        if not len(kwargs) == 1:
            raise ValueError("FIXME only one kwarg at a time")
        field_name = kwargs.keys()[0]
        value = kwargs.values()[0]
        field = getattr(cls, "_redis_attr_%s" % field_name)
        return field.exists(value)
    
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

    name = StringField(indexable=True)
    foo = HashableField(indexable=True)
