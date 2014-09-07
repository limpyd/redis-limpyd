# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.utils import iteritems, iterkeys
from future.utils import with_metaclass

from logging import getLogger
from copy import copy
import threading

from limpyd.fields import *
from limpyd.utils import make_key
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
        it.abstract = attrs.get('abstract', False)

        if not it.abstract:
            if not hasattr(it, 'database') or not isinstance(it.database, RedisDatabase):
                raise ImplementationError(
                    'You must define a database for the model %s' % name)
            if not getattr(it, 'namespace', None):
                it.namespace = ''
            it._name = ':'.join((it.namespace, name.lower()))
            it.database._add_model(it)

        # init (or get from parents) lists of redis fields
        _fields = list(it._fields) if hasattr(it, '_fields') else []
        _instancehash_fields = list(it._instancehash_fields) if hasattr(it,
                                                                    '_instancehash_fields') else []

        # Did we have already pk field ?
        pk_field = getattr(it, '_redis_attr_pk', None)

        # First loop on new attributes for this class to find fields and
        # primary key, and validate the eventually found PKField
        own_fields = []

        # limit to redis fields
        redis_fields = [(k, v) for k, v in attrs.items() if not k.startswith('_')
                                                            and isinstance(v, RedisField)]
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
            # remove the original field from the class
            if field.name in attrs:
                delattr(it, field.name)
            # and attach it to the model with its prefixed name
            field._attach_to_model(it)
            _fields.append(field.name)
            setattr(it, "_redis_attr_%s" % field.name, field)
            # save InstanceHashFields in a special list
            if isinstance(field, InstanceHashField):
                _instancehash_fields.append(field.name)

        # keep the pk as first field
        _fields.remove(pk_field.name)
        _fields.insert(0, pk_field.name)

        # Save usefull attributes on the final model
        it._fields = _fields
        it._instancehash_fields = _instancehash_fields
        if pk_field.name != 'pk':
            it._redis_attr_pk = getattr(it, "_redis_attr_%s" % pk_field.name)

        return it


class RedisModel(with_metaclass(MetaRedisModel, RedisProxyCommand)):
    """
    Base redis model.
    """

    namespace = None  # all models in an app may have the same namespace
    lockable = True
    abstract = True
    collection_manager = CollectionManager
    DoesNotExist = DoesNotExist

    available_getters = ('hmget', 'hgetall', 'hkeys', 'hvals', 'hlen')
    available_modifiers = ('hmset', 'hdel')

    def __init__(self, *args, **kwargs):
        """
        Init or retrieve an object storage in Redis.

        Here whats init manages:
        - no args, no kwargs: just instanciate in a python way, no connection to
          redis
        - some kwargs == instanciate, connect, and set the properties received
        - one arg == get from pk
        """
        self.lockable = self.__class__.lockable

        # set to True when the instance's PK will be tested for existence in redis
        self._connected = False

        # --- Meta stuff
        # Put back the fields with the original names
        for attr_name in self._fields:
            attr = getattr(self, "_redis_attr_%s" % attr_name)
            # Copy it, to avoid sharing fields between model instances
            newattr = copy(attr)
            newattr._attach_to_instance(self)
            setattr(self, attr_name, newattr)

        # The `pk` field always exists, even if the real pk has another name
        pk_field_name = getattr(self, "_redis_attr_pk").name
        if pk_field_name != 'pk':
            self.pk = getattr(self, pk_field_name)
        # Cache of the pk value
        self._pk = None

        # change the get_field method to use the instance related one instead
        # of the classmethod
        self.get_field = self.get_instance_field

        # Validate arguments
        if len(args) > 0 and len(kwargs) > 0:
            raise ValueError('Cannot use args and kwargs to instanciate.')

        # save name of fields given when creating the instance, to avoid setting
        # them in the `_set_default` method
        self._init_fields = set()

        # --- Instanciate new from kwargs
        if len(kwargs) > 0:
            # First check unique fields
            # (More robust than trying to manage a "pseudotransaction", as
            # redis do not has "real" transactions)
            # Here we do not set anything, in case one unique field fails
            kwargs_pk_field_name = None
            for field_name, value in iteritems(kwargs):
                if self._field_is_pk(field_name):
                    if kwargs_pk_field_name:
                        raise ValueError(u'You cannot pass two values for the '
                                           'primary key (pk and %s)' % pk_field_name)
                    kwargs_pk_field_name = field_name
                    # always use the real field name, not always pk
                    field_name = pk_field_name
                if not self.has_field(field_name):
                    raise ValueError(u"`%s` is not a valid field name "
                                      "for `%s`." % (field_name, self.__class__.__name__))
                field = self.get_field(field_name)
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
                field = self.get_field(field_name)
                field.proxy_set(kwargs[field_name])

        # --- Instanciate from DB
        if len(args) == 1:
            self._pk = self.pk.normalize(args[0])
            self.connect()

    def connect(self):
        """
        Connect the instance to redis by checking the existence of its primary
        key. Do nothing if already connected.
        """
        if self.connected:
            return
        pk = self._pk
        if self.exists(pk=pk):
            self._connected = True
        else:
            self._pk = None
            self._connected = False
            raise DoesNotExist("No %s found with pk %s" % (self.__class__.__name__, pk))

    @classmethod
    def lazy_connect(cls, pk):
        """
        Create an object, setting its primary key without testing it. So the
        instance is not connected
        """
        instance = cls()
        instance._pk = instance.pk.normalize(pk)
        instance._connected = False
        return instance

    @property
    def connected(self):
        """
        A property to check if the model is connected to redis (ie if it as a
        primary key checked for existence)
        """
        return self._connected

    @classmethod
    def use_database(cls, database):
        """
        Transfert the current model to the new database. Move subclasses to the
        new database two if they actually share the same one (so it's easy to
        call use_database on an abstract model to use the new database for all
        subclasses)
        """
        return database._use_for_model(cls)

    @classmethod
    def has_field(cls, field_name):
        """
        Return True if the given field name is an allowed field for this model
        """
        return field_name == 'pk' or field_name in cls._fields

    @classmethod
    def get_class_field(cls, field_name):
        """
        Return the field object with the given name (for the class, the fields
        are in the "_redis_attr_%s" form)
        """
        if not cls.has_field(field_name):
            raise AttributeError('"%s" is not a field for the model "%s"' %
                                 (field_name, cls.__name__))

        field = getattr(cls, '_redis_attr_%s' % field_name)

        return field

    # at the class level, we use get_class_field to get a field
    # but in __init__, we update it to use get_instance_field
    get_field = get_class_field

    def get_instance_field(self, field_name):
        """
        Return the field object with the given name (works for a bound instance)
        """
        if not self.has_field(field_name):
            raise AttributeError('"%s" is not a field for the model "%s"' %
                                 (field_name, self.__class__.__name__))

        field = getattr(self, field_name)

        return field

    def _set_pk(self, value):
        """
        Use the given value as the instance's primary key, if it doesn't have
        one yet (it must be used only for new instances). Then save default values.
        """
        if self._pk:
            raise ImplementationError('Something wrong happened, the PK was already set !')
        self._pk = value
        self._connected = True
        # Default must be set only at first initialization
        self._set_defaults()

    def _set_defaults(self):
        """
        Set default values to fields. We assume that they are not yet populated
        as this method is called just after creation of a new pk.
        """
        for field_name in self._fields:
            if field_name in self._init_fields:
                continue
            field = self.get_field(field_name)
            if hasattr(field, "default"):
                field.proxy_set(field.default)
        delattr(self, '_init_fields')

    @classmethod
    def collection(cls, manager=None, **filters):
        if not manager:
            manager = cls.collection_manager
        collection = manager(cls)
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
        return name in ('pk', cls.get_field('pk').name)

    @classmethod
    def exists(cls, **kwargs):
        """
        A model with the values defined by kwargs exists in db?

        `kwargs` are mandatory.
        """
        if not kwargs:
            raise ValueError(u"`Exists` method requires at least one kwarg.")

        # special case to check for a simple pk
        if len(kwargs) == 1 and cls._field_is_pk(list(kwargs.keys())[0]):
            return cls.get_field('pk').exists(list(kwargs.values())[0])

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
            # special case to check for a simple pk
            if len(kwargs) == 1 and cls._field_is_pk(list(kwargs.keys())[0]):
                pk = list(kwargs.values())[0]
            else:  # case with many filters
                result = cls.collection(**kwargs).sort(by='nosort')
                if len(result) == 0:
                    raise DoesNotExist(u"No object matching filter: %s" % kwargs)
                elif len(result) > 1:
                    raise ValueError(u"More than one object matching filter: %s" % kwargs)
                else:
                    pk = result[0]
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
            self.pk.get(),
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

    def hmget(self, *args):
        """
        This command on the model allow getting many instancehash fields with only
        one redis call. You must pass hash name to retrieve as arguments.
        """
        if args and not any(arg in self._instancehash_fields for arg in args):
            raise ValueError("Only InstanceHashField can be used here.")

        return self._call_command('hmget', args)

    def hmset(self, **kwargs):
        """
        This command on the model allow setting many instancehash fields with only
        one redis call. You must pass kwargs with field names as keys, with
        their value.
        """
        if kwargs and not any(kwarg in self._instancehash_fields for kwarg in iterkeys(kwargs)):
            raise ValueError("Only InstanceHashField can be used here.")

        indexed = []

        # main try block to revert indexes if something fail
        try:

            # Set indexes for indexable fields.
            for field_name, value in iteritems(kwargs):
                field = self.get_field(field_name)
                if field.indexable:
                    indexed.append(field)
                    field.deindex()
                    field.index(value)

            # Call redis (waits for a dict)
            result = self._call_command('hmset', kwargs)

            return result

        except:
            # We revert indexes previously set if we have an exception, then
            # really raise the error
            for field in indexed:
                field._rollback_index()
            raise
        finally:
            for field in indexed:
                field._reset_index_cache()

    def hdel(self, *args):
        """
        This command on the model allow deleting many instancehash fields with
        only one redis call. You must pass hash names to retrieve as arguments
        """
        if args and not any(arg in self._instancehash_fields for arg in args):
            raise ValueError("Only InstanceHashField can be used here.")

        # Set indexes for indexable fields.
        for field_name in args:
            field = self.get_field(field_name)
            if field.indexable:
                field.deindex()

        # Return the number of fields really deleted
        return self._call_command('hdel', *args)

    def delete(self):
        """
        Delete the instance from redis storage.
        """
        # Delete each field
        for field_name in self._fields:
            field = self.get_field(field_name)
            if not isinstance(field, PKField):
                # pk has no stored key
                field.delete()
        # Remove the pk from the model collection
        self.connection.srem(self.get_field('pk').collection_key, self._pk)
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
