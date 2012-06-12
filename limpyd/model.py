# -*- coding:utf-8 -*-

from logging import getLogger
from copy import copy

from limpyd import get_connection
from limpyd.fields import *
from limpyd.utils import make_key
from limpyd.exceptions import *

__all__ = ['RedisModel', ]

log = getLogger(__name__)


class MetaRedisModel(MetaRedisProxy):
    """
    We make invisible for user that fields were class properties
    """
    def __new__(mcs, name, base, attrs):
        it = type.__new__(mcs, name, base, attrs)
        field_parent_class = name.lower()

        # init (or get from parents) lists of redis fields
        _fields = list(it._fields) if hasattr(it, '_fields') else []
        _hashable_fields = list(it._hashable_fields) if hasattr(it, '_hashable_fields') else []

        # Did we have already pk field ?
        pk_field = getattr(it, '_redis_attr_pk', None)

        # First loop on new attributes for this class to find fields and primary key
        local_fields = []
        for attr_name in attrs:
            if attr_name.startswith("_"):
                continue
            attr = getattr(it, attr_name)
            if not isinstance(attr, RedisField):
                continue
            attr.name = attr_name
            if isinstance(attr, PKField):
                # Check and save the primary key
                if pk_field:
                    # We have a new pk_field, check if the previous one was auto
                    # added to the model to remove it
                    if pk_field._auto_added:
                        _fields.remove(pk_field.name)
                    else:
                        raise ImplementationError(
                            'Only one PKField field is allowed on %s' % name)
                pk_field = attr
            local_fields.append(attr)

        # Auto create missing primary key
        if not pk_field:
            pk_field = AutoPKField()
            pk_field._auto_added = True
            local_fields.insert(0, pk_field)

        # Loop on fields to prepare them
        for field in local_fields:
            field._parent_class = field_parent_class
            _fields.append(field.name)
            setattr(it, "_redis_attr_%s" % field.name, field)
            if field.name in attrs:
                delattr(it, field.name)
            if isinstance(attr, HashableField):
                _hashable_fields.append(field.name)

        # Each field need to access its parent model, even if the model is
        # the class and not an instance (for collections)
        # So we have to set the current class as the parent_class of each field,
        # and to do so we have to have to create a copy of the field to hold
        # this value (no share of fields between model classes), to avoid
        # collision in collections names
        for field_name in _fields:
            key = "_redis_attr_%s" % field_name
            field = getattr(it, key)
            if field._parent_class != field_parent_class:
                ownfield = copy(field)
                ownfield._parent_class = field_parent_class
                setattr(it, key, ownfield)

        # Save usefull attributes on the final model
        setattr(it, "_fields", _fields)
        setattr(it, "_hashable_fields", _hashable_fields)
        setattr(it, "_pk_field", pk_field.name)
        if pk_field.name != 'pk':
            setattr(it, "_redis_attr_pk", getattr(it, "_redis_attr_%s" % pk_field.name))

        return it


class RedisModel(RedisProxyCommand):
    """
    Base redis model.
    """

    __metaclass__ = MetaRedisModel

    cacheable = True
    DoesNotExist = DoesNotExist

    def __init__(self, *args, **kwargs):
        """
        Init or retrieve an object storage in Redis.

        Here whats init manages:
        - no args, no kwargs: just instanciate in a python way, no connection to
          redis
        - some kwargs == instanciate, connect, and set the properties received
        - one arg == get from pk
        """
        self.cacheable = self.__class__.cacheable

        # --- Meta stuff
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = getattr(self, "_redis_attr_%s" % attr_name)
            # Copy it, to avoid sharing fields between model instances
            newattr = copy(attr)
            newattr._instance = self
            # Force field.cacheable to False if it's False for the model
            newattr.cacheable = newattr.cacheable and self.cacheable
            setattr(self, attr_name, newattr)

        # The `pk` field always exists, even if the real pk has another name
        if self._pk_field != 'pk':
            setattr(self, 'pk', getattr(self, self._pk_field))
        # Cache of the pk value
        self._pk = None

        # Prepare stored connection
        self._connection = None

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
                if field_name == 'pk':
                    field_name = self._pk_field
                if field_name not in self._fields:
                    raise ValueError(u"`%s` is not a valid field name "
                                      "for `%s`." % (field_name, self.__class__.__name__))
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
            if self.exists(pk=value):
                self._pk = self.pk.normalize(value)
            else:
                raise ValueError("No %s found with pk %s" % (self.__class__.__name__, value))

    def init_cache(self):
        """
        Call it to init or clear the command cache.
        """
        if self.cacheable:
            self._cache = {}

    @property
    def connection(self):
        if self._connection is None:
            self._connection = get_connection()
        return self._connection

    def get_pk(self):
        if not self._pk:
            self.pk.set(None)
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
    def collection(cls, **kwargs):
        """
        Return a set of pk, eventually filtered by kwargs.
        Specific work is done if the pk is in the kwargs
        """
        # We cannot use the current connection here, as we have no instance
        connection = get_connection()

        # Exclude primary key from fields
        query_fields = {}
        check_pk, pk_value = False, None

        for field_name, value in kwargs.iteritems():
            if cls._field_is_pk(field_name):
                pk_value = value
                check_pk = True
            else:
                query_fields[field_name] = value

        # Specific work if we have a pk
        if check_pk:
            try:
                # try to get the object
                obj = cls(pk_value)
            except ValueError:
                # A non existing pk = empty result
                return set()
            else:
                # Existing object, check all fields
                if query_fields:
                    for field_name, value in query_fields.iteritems():
                        field = getattr(obj, field_name)
                        getter = getattr(field, field.proxy_getter)
                        if getter() != value:
                            return set()

                # no others fields, or all passed tests, return the pk
                return set([obj.pk.normalize(pk_value)])

        elif not query_fields:
            # No pk, no other kwargs, return all the collection
            return cls._redis_attr_pk.collection()

        # Prepare a list of sets for each query parameter
        index_keys = []
        for field_name, value in query_fields.iteritems():
            field = getattr(cls, "_redis_attr_%s" % field_name)
            index_keys.append(field.index_key(value))

        # Return intersection of all sets to get matching entries
        return connection.sinter(index_keys)

    @classmethod
    def instances(cls, **kwargs):
        """
        Like collection method, but yield instances.
        """
        for pk in cls.collection(**kwargs):
            yield cls(pk)

    @classmethod
    def _field_is_pk(cls, name):
        """
        Check if the given field is the one from the primary key.
        It can be the plain "pk" name, or the real pk field name
        """
        return name in ('pk', cls._redis_attr_pk.name)

    @classmethod
    def exists(cls, **kwargs):
        """
        A model with the values defined by kwargs exists in db?

        `kwargs` are mandatory.
        """
        if not kwargs:
            raise ValueError(u"`Exists` method requires at least one kwarg.")

        # special case to check for a simple pk
        if len(kwargs) == 1 and cls._field_is_pk(kwargs.keys()[0]):
            return cls._redis_attr_pk.exists(kwargs.values()[0])

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
                pk = result.pop()
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
            self.get_pk(),
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
