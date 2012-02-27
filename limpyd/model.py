# -*- coding:utf-8 -*-

from copy import copy

from limpyd import get_connection
from limpyd.fields import *

__all__ = ['RedisModel', 'StringField', 'SortedSetField']

class MetaRedisModel(type):
    """
    Manage fields.
    """
    def __new__(mcs, name, base, dct):
        it = type.__new__(mcs, name, base, dct)
        # We make invisible for user that they where class property
        _fields = []
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
        setattr(it, "_fields", _fields)
        return it

class RedisModel(object):
    """
    Base redis model.
    """
    
    __metaclass__ = MetaRedisModel
    
    @classmethod
    def connection(cls):
        return get_connection()

    def __init__(self, *args, **kwargs):
        """
        Fetch all data from redis storage.
        """
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = copy(getattr(self, "_redis_attr_%s" % attr_name))
            attr._instance = self
            setattr(self, attr_name, attr)
        
        # If a kwargs is present, that means that we want to retrieve or create
        # the object
        if len(kwargs) == 1:  # Only one kwargs to retrieve instance, for now
            name = kwargs.keys()[0]
            value = kwargs.values()[0]
            if name == "pk":
                # pk is not a field for now
                exists = self.connection().sismember(self.collection_key(), value)
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

    def save(self):
        """
        Save the object data, taking care of id management.
        """
        save_dict = {}
        for attr_name in self._hashable_fields:
            save_dict[attr_name] = getattr(self, attr_name)
        self.connection().hmset(self.key, save_dict)

    @classmethod
    def collection_key(cls):
        return '%s:collection' % cls.__name__.lower()

    @classmethod
    def collection(cls):
        return cls.connection().smembers(cls.collection_key())

    @property
    def key(self):
        return "%s:%s" % (self.__class__.__name__.lower(), self.pk)

    @property
    def pk(self):
        if not hasattr(self, "_pk"):
            key = "%s:pk" % self.__class__.__name__.lower()
            self._pk = self.connection().incr(key)
            # We have created it, so add it to the collection
#            print "Adding %s in %s collection" % (self._pk, self.__class__.__name__)
            self.connection().sadd(self.collection_key(), self._pk)
        return self._pk

    @classmethod
    def exists(cls, **kwargs):
        if not len(kwargs) == 1:
            raise ValueError("FIXME only one kwarg at a time")
        field_name = kwargs.keys()[0]
        value = kwargs.values()[0]
        field = getattr(cls, "_redis_attr_%s" % field_name)
        return field.exists(value)


class TestModel(RedisModel):

    name = StringField(indexable=True)

class Bike(RedisModel):
    name = StringField(indexable=True)
    wheels = StringField(default=2)

