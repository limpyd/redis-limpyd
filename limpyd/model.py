# -*- coding:utf-8 -*-

from logging import getLogger
from copy import copy

from limpyd.fields import *
from limpyd.utils import make_key
from limpyd.exceptions import *
from limpyd.database import RedisDatabase

__all__ = ['RedisModel', ]

log = getLogger(__name__)


class MetaRedisModel(MetaRedisProxy):
    """
    We make invisible for user that fields were class properties
    """
    def __new__(mcs, name, base, attrs):
        is_abstract = attrs.get('abstract', False)

        it = type.__new__(mcs, name, base, attrs)

        if not is_abstract:
            if not hasattr(it, 'database') or not isinstance(it.database, RedisDatabase):
                raise ImplementationError(
                    'You must define a database for the model %s' % name)
            if not getattr(it, 'namespace', None):
                raise ImplementationError(
                    'You must define a namespace for the model %s' % name)
            it._name = ':'.join((it.namespace, name.lower()))
            it.database._add_model(it)

        # init (or get from parents) lists of redis fields
        _fields = list(it._fields) if hasattr(it, '_fields') else []
        _hashable_fields = list(it._hashable_fields) if hasattr(it, '_hashable_fields') else []

        # Did we have already pk field ?
        pk_field = getattr(it, '_redis_attr_pk', None)

        # First loop on new attributes for this class to find fields and
        # primary key, and validate the eventually found PKField
        own_fields = []
        for attr_name in attrs:
            if attr_name.startswith("_"):
                continue
            attr = getattr(it, attr_name)
            if not isinstance(attr, RedisField):
                continue
            attr.name = attr_name  # each field must know its name
            if isinstance(attr, PKField):
                # Check and save the primary key
                if pk_field:
                    # If a PKField already exists, remove the previously auto-added
                    if pk_field._auto_added:
                        _fields.remove(pk_field.name)
                    else:
                        raise ImplementationError(
                            'Only one PKField field is allowed on %s' % name)
                pk_field = attr
            own_fields.append(attr)

        # We have to store the name of the class on which a field is attached
        # to compute needed redis keys.
        # For this, a model and its subclasses must not share fields, so we
        # copy existing ones (from the parent class) to the current class.
        for field_name in _fields:
            key = "_redis_attr_%s" % field_name
            field = getattr(it, key)
            ownfield = copy(field)
            ownfield._model = it
            setattr(it, key, ownfield)

        # Auto create missing primary key (it will always be called in RedisModel)
        if not pk_field:
            pk_field = AutoPKField()
            pk_field._auto_added = True
            own_fields.append(pk_field)

        # Loop on new fields to prepare them
        for field in own_fields:
            field._model = it
            _fields.append(field.name)
            setattr(it, "_redis_attr_%s" % field.name, field)
            if field.name in attrs:
                delattr(it, field.name)
            if isinstance(attr, HashableField):
                _hashable_fields.append(field.name)

        # Save usefull attributes on the final model
        setattr(it, "_fields", _fields)
        setattr(it, "_hashable_fields", _hashable_fields)
        if pk_field.name != 'pk':
            setattr(it, "_redis_attr_pk", getattr(it, "_redis_attr_%s" % pk_field.name))
        setattr(it, "abstract", is_abstract)

        return it


class RedisModel(RedisProxyCommand):
    """
    Base redis model.
    """

    __metaclass__ = MetaRedisModel

    namespace = None  # all models in an app may have the same namespace
    cacheable = True
    abstract = True
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
        pk_field_name = getattr(self, "_redis_attr_pk").name
        if pk_field_name != 'pk':
            setattr(self, 'pk', getattr(self, pk_field_name))
        # Cache of the pk value
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
                if field_name == 'pk':
                    # always use the real field name, not always pk
                    field_name = pk_field_name
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
                field.proxy_set(value)

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

    def get_cache(self):
        """
        Return the local cache dict.
        """
        return self._cache[self.name]

    def get_pk(self):
        if not hasattr(self, '_pk'):
            raise DoesNotExist("The current object doesn't exists anymore")
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
            if hasattr(field, "default"):
                has_value = field.proxy_get()
                if has_value is None:
                    field.proxy_set(field.default)

    @classmethod
    def collection(cls, **kwargs):
        """
        Return a set of pk, eventually filtered by kwargs.
        Specific work is done if the pk is in the kwargs
        """
        query_fields = kwargs.copy()

        # Check if we have a pk and if so, do specific work
        pk_fields = [k for k in kwargs.keys() if cls._field_is_pk(k)]
        if pk_fields:
            if len(pk_fields) > 1:
                raise ValueError("You must use only one pk field in filtering")
            field_name = pk_fields[0]
            value = kwargs[field_name]
            query_fields.pop(field_name)
            try:
                # try to get the object
                obj = cls(value)
            except ValueError:
                # A non existing pk = empty result
                return set()
            else:
                # Existing object, check all fields
                if query_fields:
                    for obj_field_name, obj_value in query_fields.iteritems():
                        field = getattr(obj, obj_field_name)
                        if field.proxy_get() != obj_value:
                            return set()

                # no others fields, or all test ok, return the pk
                return set([obj.pk.normalize(value)])

        if not query_fields:
            # No pk, no other kwargs, return all the collection
            return cls._redis_attr_pk.collection()

        # Prepare a list of sets for each query parameter
        index_keys = []
        for field_name, value in query_fields.iteritems():
            field = getattr(cls, "_redis_attr_%s" % field_name)
            index_keys.append(field.index_key(value))

        # Return intersection of all sets to get matching entries
        return cls.get_connection().sinter(index_keys)

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
            self._name,
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

    def delete(self):
        """
        Delete the instance from redis storage.
        """
        # Delete each field
        for field_name in self._fields:
            field = getattr(self, field_name)
            if not isinstance(field, PKField):
                # pk has no stored key
                field.delete()
        # Remove the pk from the model collection
        self.connection.srem(self._redis_attr_pk.collection_key, self._pk)
        # Deactivate the instance
        delattr(self, "_pk")
