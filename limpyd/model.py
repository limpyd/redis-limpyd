# -*- coding:utf-8 -*-

from logging import getLogger
from copy import copy
import threading

from limpyd.fields import *
from limpyd.utils import make_key, make_cache_key
from limpyd.exceptions import *
from limpyd.database import RedisDatabase
from limpyd.collection import CollectionManager

__all__ = ['RedisModel', ]

log = getLogger(__name__)
threadlocal = threading.local()

class MetaRedisModel(MetaRedisProxy):
    """
    We make invisible for user that fields were class properties
    """
    def __new__(mcs, name, base, attrs):

        it = super(MetaRedisModel, mcs).__new__(mcs, name, base, attrs)
        is_abstract = attrs.get('abstract', False)
        setattr(it, "abstract", is_abstract)

        if not is_abstract:
            if not hasattr(it, 'database') or not isinstance(it.database, RedisDatabase):
                raise ImplementationError(
                    'You must define a database for the model %s' % name)
            if not getattr(it, 'namespace', None):
                it.namespace = ''
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

        # limit to redis fields
        redis_fields = [(k, v) for k, v in attrs.items() if not k.startswith('_') and isinstance(v, RedisField)]
        # and keep them by declaration order
        redis_fields = [(k, v) for k, v in sorted(redis_fields, key=lambda f: f[1]._creation_order)]

        for (field_name, field) in redis_fields:
            field.name = field_name  # each field must know its name
            if isinstance(field, PKField):
                # Check and save the primary key
                if pk_field:
                    # If a PKField already exists, remove the previously auto-added
                    if pk_field._auto_added:
                        _fields.remove(pk_field.name)
                    else:
                        raise ImplementationError(
                            'Only one PKField field is allowed on %s' % name)
                pk_field = field
            own_fields.append(field)

        # We have to store the name of the class on which a field is attached
        # to compute needed redis keys.
        # For this, a model and its subclasses must not share fields, so we
        # copy existing ones (from the parent class) to the current class.
        for field_name in _fields:
            key = "_redis_attr_%s" % field_name
            field = getattr(it, key)
            ownfield = copy(field)
            ownfield._attach_to_model(it)
            setattr(it, key, ownfield)

        # Auto create missing primary key (it will always be called in RedisModel)
        if not pk_field:
            pk_field = AutoPKField()
            pk_field._auto_added = True
            own_fields.append(pk_field)

        # Loop on new fields to prepare them
        for field in own_fields:
            field._attach_to_model(it)
            _fields.append(field.name)
            setattr(it, "_redis_attr_%s" % field.name, field)
            if field.name in attrs:
                delattr(it, field.name)
            if isinstance(field, HashableField):
                _hashable_fields.append(field.name)

        # keep the pk as first field
        _fields.remove(pk_field.name)
        _fields.insert(0, pk_field.name)

        # Save usefull attributes on the final model
        setattr(it, "_fields", _fields)
        setattr(it, "_hashable_fields", _hashable_fields)
        if pk_field.name != 'pk':
            setattr(it, "_redis_attr_pk", getattr(it, "_redis_attr_%s" % pk_field.name))

        return it


class RedisModel(RedisProxyCommand):
    """
    Base redis model.
    """

    __metaclass__ = MetaRedisModel

    namespace = None  # all models in an app may have the same namespace
    cacheable = True
    lockable = True
    abstract = True
    DoesNotExist = DoesNotExist

    _commands = {
        'getters': ('hmget', ),
        'full_modifiers': ('hmset', ),
        'partial_modifiers': (),
    }

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
        self.lockable = self.__class__.lockable

        # --- Meta stuff
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = getattr(self, "_redis_attr_%s" % attr_name)
            # Copy it, to avoid sharing fields between model instances
            newattr = copy(attr)
            newattr._attach_to_instance(self)
            # Force field.cacheable and lockable to False if it's False for the model
            newattr.cacheable = newattr.cacheable and self.cacheable
            newattr.lockable = newattr.lockable and self.lockable
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

        # save name of fields given when creating the instance, to avoid setting
        # them in the `_set_default` method
        self._init_fields= set()

        # --- Instanciate new from kwargs
        if len(kwargs) > 0:
            # First check unique fields
            # (More robust than trying to manage a "pseudotransaction", as
            # redis do not has "real" transactions)
            # Here we do not set anything, in case one unique field fails
            kwargs_pk_field_name = None
            for field_name, value in kwargs.iteritems():
                if self._field_is_pk(field_name):
                    if kwargs_pk_field_name:
                        raise ValueError(u'You cannot pass two values for the '
                                           'primary key (pk and %s)' % pk_field_name)
                    kwargs_pk_field_name = field_name
                    # always use the real field name, not always pk
                    field_name = pk_field_name
                if field_name not in self._fields:
                    raise ValueError(u"`%s` is not a valid field name "
                                      "for `%s`." % (field_name, self.__class__.__name__))
                field = getattr(self, field_name)
                if field.unique and self.exists(**{field_name: value}):
                    raise UniquenessError(u"Field `%s` must be unique. "
                                           "Value `%s` yet indexed." % (field.name, value))
                self._init_fields.add(field_name)

            # Do instanciate, starting by the pk and respecting fields order
            if kwargs_pk_field_name:
                self.pk.set(kwargs[kwargs_pk_field_name])
            for field_name in self._fields:
                if field_name not in kwargs or self._field_is_pk(field_name):
                    continue
                field = getattr(self, field_name)
                field.proxy_set(kwargs[field_name])

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
        the "__self__" if used to store cache for the model, other keys are for
        its fields.
        """
        if self.cacheable:
            self._cache = {'__self__': {}}

    def get_cache(self):
        """
        Return the local cache dict.
        """
        return self._cache['__self__']

    def get_pk(self):
        """
        Return the primary key of the instance.
        If the `_pk` attribute doesn't exist, it's because the instance was deleted.
        And if it's present but empty, it's because it's a new instance without
        primary key so we ask for a new one. Then, as the object is new, with a pk,
        we save default values for fields.
        """
        if not hasattr(self, '_pk'):
            raise DoesNotExist("The current object doesn't exists anymore")
        if not self._pk:
            self.pk.set(None)
            # Default must be set only at first initialization
            self._set_defaults()
        return self._pk

    def _set_defaults(self):
        """
        Set default values to fields. We assume that they are not yet populated
        as this method is called in `get_pk`, just after creation of a new pk.
        """
        for field_name in self._fields:
            if field_name in self._init_fields:
                continue
            field = getattr(self, field_name)
            if hasattr(field, "default"):
                field.proxy_set(field.default)
        delattr(self, '_init_fields')

    @classmethod
    def collection(cls, **filters):
        collection = CollectionManager(cls)
        return collection(**filters)

    @classmethod
    def instances(cls, **filters):
        # FIXME Keep as shortcut or remove for clearer API?
        return cls.collection(**filters).instances()

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

        # get only the first element of the unsorted collection (the fastest)
        try:
            cls.collection(**kwargs).sort(by='nosort')[0]
        except IndexError:
            return False
        else:
            return True

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

    @classmethod
    def sort_wildcard(cls):
        """
        Used to sort Hashfield. See Hashfield.sort_widlcard.
        """
        return cls.make_key(
            cls._name,
            "*",
            "hash",
        )

    def hmget(self, *keys):
        """
        This command on the model allow getting many hashable fields with only
        one redis call. You should pass hash name to retrieve as arguments.
        Try to get values from local cache if possible.
        """

        # manage arguments: redis-py waits for a list, but it's not concistent
        # with the rest of the api, so we accept a list as a single argument,
        # or a list as *keys
        if len(keys) == 1 and isinstance(keys[0], (list, tuple)):
            keys = keys[0]

        # use all hashable fields if no arguments
        elif len(keys) == 0:
            keys = self._hashable_fields

        # check that each field is a HashableField
        else:
            if not any(key in self._hashable_fields for key in keys):
                raise ValueError("Only hashable fields can be used here.")

        # get values from cache if we can
        cached = {}
        to_retrieve = []
        retrieved = []
        if self.cacheable:
            # we do the cache stuff only if the object is cacheable, to avoid
            # useless computations if not
            for field_name in keys:
                field = getattr(self, field_name)
                if field.cacheable and field.has_cache():
                    field_cache = field.get_cache()
                    haxh = make_cache_key('hget', field_name)
                    if haxh in field_cache:
                        cached[field_name] = field_cache[haxh]
                        continue
                # field not cached, we need to retrieve it
                to_retrieve.append(field_name)
        else:
            # object not cacheable, retrieve all fields
            to_retrieve = keys

        if to_retrieve:
            # call redis if some keys are not cached (waits for a list)
            retrieved = self._traverse_command('hmget', to_retrieve)

        if cached:
            # we have some fields cached, return the values in the right order
            retrieved_dict = dict(zip(to_retrieve, retrieved))
            retrieved = []
            for field_name in keys:
                if field_name in cached:
                    retrieved.append(cached[field_name])
                else:
                    retrieved.append(retrieved_dict[field_name])

        return retrieved

    def hmset(self, mapping=None, **kwargs):
        """
        This command on the model allow setting many hashable fields with only
        one redis call. You should pass kwargs with field names as keys, with
        their value.
        Index and cache are managed for indexable and/or cacheable fields.
        """

        # manage arguments: redis-py waits for a dict, but it's not concistent
        # with the rest of the api, so we accept a dict as a single argument,
        # or a dict as **kwargs
        if kwargs:
            if mapping:
                raise ValueError('hmset accepts either a dict as unique '
                                 'argument (mapping), OR as **kwargs, not both')
            mapping = kwargs

        if not any(mapping in self._hashable_fields for mapping in mapping.keys()):
            raise ValueError("Only hashable fields can be used here.")

        indexed = []

        # main try block to revert indexes if something fail
        try:

            # Set indexes for indexable fields.
            for field_name, value in mapping.items():
                field = getattr(self, field_name)
                if field.indexable:
                    field.deindex()
                    field.index_value(value)
                    indexed.append((field, value))

            # Call redis (waits for a dict)
            result = self._traverse_command('hmset', mapping)

            # Clear the cache for each cacheable field
            if self.cacheable:
                for field_name, value in mapping.items():
                    field = getattr(self, field_name)
                    if not field.cacheable or not field.has_cache():
                        continue
                    field_cache = field.get_cache()
                    field_cache.clear()
            return result

        except:
            # We revert indexes previously set if we have an exception, then
            # really raise the error
            for field, new_value in indexed:
                old_value = field.hget()
                field.deindex_value(new_value)
                field.hset(old_value)
            raise

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

    @classmethod
    def _thread_lock_storage(cls):
        """
        We mark each locked field in a thread, to allow other operations in the
        same thread on the same field (for the same instance or others). This
        way, operations within the lock, in the same thread, can bypass it (the
        whole set of operations must be locked)
        """
        if not hasattr(threadlocal, 'limpyd_locked_fields'):
            threadlocal.limpyd_locked_fields = {}
        if cls._name not in threadlocal.limpyd_locked_fields:
            threadlocal.limpyd_locked_fields[cls._name] = set()
        return threadlocal.limpyd_locked_fields[cls._name]

    @classmethod
    def _mark_field_as_locked(cls, field):
        cls._thread_lock_storage().add(field.name)

    @classmethod
    def _unmark_field_as_locked(cls, field):
        if field.name in cls._thread_lock_storage():
            cls._thread_lock_storage().remove(field.name)

    @classmethod
    def _is_field_locked(cls, field):
        return field.name in cls._thread_lock_storage()

