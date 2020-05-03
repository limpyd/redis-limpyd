# -*- coding:utf-8 -*-
from __future__ import unicode_literals

from future.utils import iteritems, iterkeys
from future.utils import with_metaclass

from collections import defaultdict
from logging import getLogger
from copy import copy
import inspect
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

            # Get the caller (parent of the call of the `__new__`` method of the metaclass, which
            # may be the current metaclass, or a one that inherit from it), with it's source file
            # and line number
            # It will be used in ``database._add_model``, if a model with the same name
            # already exists, to check if it's the same one.
            parent_frame = inspect.currentframe().f_back
            while True:
                f_code = parent_frame.f_code
                f_locals = parent_frame.f_locals
                if f_code.co_name == '__new__':
                    try:
                        # handle direct metaclass usage
                        if issubclass(f_locals['mcs'], MetaRedisModel):
                            parent_frame = parent_frame.f_back
                            continue
                    except Exception:
                        pass
                    # handle `future.utils.with_metaclass` and `six.with_metaclass`
                    parent_cls = str(f_locals.get('cls'))
                    if 'metaclass' in parent_cls and ('six.' in parent_cls or 'future.' in parent_cls):
                        parent_frame = parent_frame.f_back
                        continue
                break
            it._creation_source = (parent_frame.f_code.co_filename, parent_frame.f_lineno)
            it_in_db = it.database._add_model(it)
            # If the returned model is not the same, it's the one from the database
            # we already added for this name, so we use it
            if it_in_db is not it:
                return it_in_db

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
            # use default indexes if indexable with no index defined
            if field.indexable and not field.index_classes:
                field.index_classes = field.get_default_indexes()[::1]

        # keep the pk as first field
        _fields.remove(pk_field.name)
        _fields.insert(0, pk_field.name)

        # Save usefull attributes on the final model
        it._fields = _fields
        it._instancehash_fields = _instancehash_fields
        if pk_field.name != 'pk':
            it._redis_attr_pk = getattr(it, "_redis_attr_%s" % pk_field.name)

        # Tell index classes that fields are now ready
        for field in it.get_fields():
            if field is it._redis_attr_pk:
                continue
            for index_class in field.index_classes:
                index_class._field_model_ready(it, field)

        it._multi_fields_index_for_filtering = [
            index
            for field in it.get_fields()
            for index in field._indexes if not index.filter_single_field
        ]

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
    default_indexes = None

    available_getters = {'hmget', 'hgetall', 'hkeys', 'hvals', 'hlen', }
    available_modifiers = {'hmset', 'hdel', }

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
        for field in self.get_class_fields():
            # Copy it, to avoid sharing fields between model instances
            new_field = copy(field)
            new_field._attach_to_instance(self)
            setattr(self, field.name, new_field)

        # The `pk` field always exists, even if the real pk has another name
        pk_field_name = getattr(self, "_redis_attr_pk").name
        if pk_field_name != 'pk':
            self.pk = getattr(self, pk_field_name)
        # Cache of the pk value
        self._pk = None

        # change the get_field(s) method to use the instance related ones instead
        # of the classmethod
        self.get_field = self.get_instance_field
        self.get_fields = self.get_instance_fields

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
                if field.unique:
                    field.check_uniqueness(value)
                self._init_fields.add(field_name)

            # handle uniqueness check for multi-fields indexes
            if self._multi_fields_index_for_filtering and any(index.unique for index in self._multi_fields_index_for_filtering):
                passed_fields = {
                    field.name: kwargs[field.name]
                    for field in self.fields
                    if field.name in kwargs and not self._field_is_pk(field.name)
                }
                if passed_fields:
                    handled_together = defaultdict(list)
                    for index in self._multi_fields_index_for_filtering:
                        if not index.unique:
                            continue
                        handled_fields_tuples = index.can_filter_fields([(field_name, None) for field_name in passed_fields])
                        for handled_fields in handled_fields_tuples:
                            handled_together[handled_fields].append(index)
                    for handled_fields, indexes in handled_together.items():
                        for index in indexes:
                            index.check_uniqueness_at_init({
                                field_name: passed_fields[field_name]
                                for field_name in dict(handled_fields)
                            })

            # Do instanciate, starting by the pk and respecting fields order
            if kwargs_pk_field_name:
                self.pk.set(kwargs[kwargs_pk_field_name])
            try:
                for field in self.fields:
                    if field.name not in kwargs or self._field_is_pk(field.name):
                        continue
                    field.proxy_set(kwargs[field.name])
            except UniquenessError:
                # may be raised if things were added in the meantime. TODO: add lock at model level to avoid this ?
                self.delete()
                raise

        # --- Instanciate from DB
        if len(args) == 1:
            self._pk = self.pk.normalize(args[0])
            self.connect()

    @classmethod
    def get_default_indexes(cls):
        if cls.default_indexes is not None:
            return cls.default_indexes
        return cls.database.get_default_indexes()

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

        return getattr(cls, '_redis_attr_%s' % field_name)

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

        return getattr(self, field_name)

    @classmethod
    def get_class_fields(cls):
        for field_name in cls._fields:
            yield cls.get_field(field_name)

    # at the class level, we use get_class_fields to get the fields
    # but in __init__, we update it to use get_instance_fields
    get_fields = get_class_fields

    def get_instance_fields(self):
        for field_name in self._fields:
            yield self.get_field(field_name)
    fields = property(get_instance_fields)

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
        for field in self.fields:
            if field.name in self._init_fields:
                continue
            if hasattr(field, "default"):
                field.proxy_set(field.default)

    @classmethod
    def collection(cls, manager=None, **filters):
        if not manager:
            manager = cls.collection_manager
        collection = manager(cls)
        return collection(**filters)

    @classmethod
    def instances(cls, lazy=False, **filters):
        # FIXME Keep as shortcut or remove for clearer API?
        return cls.collection(**filters).instances(lazy=lazy)

    @classmethod
    def from_pks(cls, pks, lazy=False):
        """Returns a generator with one instance for each pk that exist"""
        meth = cls.lazy_connect if lazy else cls
        for pk in pks:
            try:
                yield meth(pk)
            except DoesNotExist:
                continue

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
                    try:
                        pk = result[0]
                    except IndexError:
                        # object was deleted between the `len` check and now
                        raise DoesNotExist(u"No object matching filter: %s" % kwargs)

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
            try:
                inst = cls(**kwargs)
                created = True
            except UniquenessError:
                # This can happen if two ``get_or_connect`` where called in a very very short timespan:
                # - Call 1 checks for existence
                # - It does not exist and will create one
                # - At this moment call 2 checks for existence
                # - It does not exist and will create one
                # - At this moment call 1 create the redis instance
                # - But when call 2 wants to create the instance, it exists
                # To solve this, in case of `UniquenessError`, it means it's already created so we
                # can just get it from redis
                inst = cls.get(**kwargs)
                created = False
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
            if self.connected:
                for field in indexed:
                    field._rollback_indexes()
            raise
        finally:
            for field in indexed:
                field._reset_indexes_rollback_caches(self.pk.get())

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
        for field in self.fields:
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

    def scan_keys(self, count=None):
        """Iter on all the key related to the current instance fields, using redis SCAN command

        Parameters
        ----------
        count: int, default to None (redis uses 10)
            Hint for redis about the number of expected result

        Yields
        -------
        str
            All keys found by the scan, one by one. A key can be returned multiple times, it's
            related to the way the SCAN command works in redis.

        """

        pattern = self.make_key(
            self._name,
            self.pk.get(),
            '*'
        )

        return self.database.scan_keys(pattern, count)

    @classmethod
    def scan_model_keys(cls, count=None):
        """Iter on all the key related to the current model, using redis SCAN command

        Parameters
        ----------
        count: int, default to None (redis uses 10)
            Hint for redis about the number of expected result

        Yields
        -------
        str
            All keys found by the scan, one by one. A key can be returned multiple times, it's
            related to the way the SCAN command works in redis.

        """

        pattern = cls.make_key(
            cls._name,
            "*",
        )

        return cls.database.scan_keys(pattern, count)

    def __hash_key(self):
        """Elements used in __hash__ and __eq__ of an instance"""
        return self.__class__, self.pk.get()

    def __hash__(self):
        return hash(self.__hash_key())

    def __eq__(self, other):
        try:
            return self.__hash_key() == other.__hash_key()
        except AttributeError:
            return NotImplemented

    def __repr__(self):
        return u'%s (pk=%s)>' % (super(RedisModel, self).__repr__()[:-2], self.pk.get())
