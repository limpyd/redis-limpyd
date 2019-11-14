# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import str, object
from future.utils import PY3
from past.builtins import str as oldstr

from collections import defaultdict
from itertools import product
from logging import getLogger
import threading

from limpyd.exceptions import ImplementationError, LimpydException, UniquenessError
from limpyd.utils import make_key, unique_key

logger = getLogger(__name__)


class BaseIndex(object):
    """Base of all indexes

    Configurable attributes
    -----------------------
    These are class attributes that can be changed via ``configure``:

    handle_uniqueness: bool
        If ``True``, the index is able to check for uniqueness. When many indexes are used, only
        the first with this flag set to ``True`` have to do the work.
    prefix: str
        If defined, will be used as a prefix to the suffix in the collection
        For example, with a prefix "foo" and the suffix "eq": "myfield__foo__eq="
        May be defined at the class level for a subclass, or by calling the ``configure``
        class method.
        This prefix can also be used by the indexes to store the data at a different place than
        the same index without prefix.
    key: str
        If set, will override the key used by the index.
        Two indexes for the same field of the same type must not have the same key or data will be
        saved at the same place.
        Note that the default key is None for `EqualIndex`, `text-range` for
        `TextRangeIndex` and `number-range` for `NumberRangeIndex`...
    transform: callable
        None by default. If defined, a function that will transform the value to be used as the
        reference for the index, before the call to ``normalize_value``.
        If can be extraction of a date, or any computation.
        The filter in the collection will then have to use a transformed value, for example
        ``birth_date__year=1976`` if the transform take a date and transform it to a year.
        This callable can accept one (``value``) or two (``self``, ``value``) arguments

    Class attributes
    ----------------
    handled_suffixes: set of str
        The suffixes in the filter keys allowed for this index.
        If one of them is ``None``, it can be used when no suffix is given.
        For example `collection(foo=1)` can be used for the `None` suffix.
        And `collection(foo__eq=1)` can be used for the `eq` suffix.
    configurable_attrs : Set[str]
        Name of attributes that can be passed to ``configure``
    supported_key_types : Set[str]
        Redis key types supported by the index.
        May include: 'set', 'zset' or 'list'
    filter_single_field : bool
        Tell if the index can be used to filter a field independently than others.

    Parameters
    -----------
    field: RedisField
        The field object using the instance of this index

    """

    handled_suffixes = set()
    handle_uniqueness = False
    key = None
    prefix = None
    transform = None
    filter_single_field = True

    configurable_attrs = {
        'prefix', 'transform', 'handle_uniqueness', 'key', 'name'
    }

    supported_key_types = set()

    def __init__(self, field):
        """Attach the index to the given field and prepare the internal cache"""
        self.field = field
        self._rollback_cache = defaultdict(
            lambda: defaultdict(  # one dict by thread id
                lambda: {  # one dict by instance pk
                    'indexed_values': set(),
                    'deindexed_values': set(),
                }
            )
        )

    def __repr__(self):
        return u'%s (field=%s)>' % (
            super(BaseIndex, self).__repr__()[:-2],
            repr(self.field),
        )

    @classmethod
    def handle_configurable_attrs(cls, **kwargs):
        """Handle attributes that can be passed to ``configure``.

        For the parameters, see ``configure``.

        Returns
        -------
        Union[str, None]
            The name passed in ``kwargs``
        dict
            The attrs extracted from kwargs and prepared
        kwargs
            A copy of ``kwargs`` without the handled fields, like ``name`` and the ones extracted.
            The dict must be empty once gone through this method in all the class tree, else
            a ``TypeError`` exception will be raised in ``configure``.

        """

        kwargs = kwargs.copy()

        attrs = {}
        for key in ('prefix', 'handle_uniqueness', 'key'):
            if key in kwargs and key in cls.configurable_attrs:
                attrs[key] = kwargs.pop(key)

        if 'transform' in kwargs:
            attrs['transform'] = staticmethod(kwargs.pop('transform'))

        name = kwargs.pop('name', None)

        return name, attrs, kwargs

    @classmethod
    def configure(cls, **kwargs):
        """Create a new index class with the given info

        This allow to avoid creating a new class when only few changes are
        to be made.

        Accepts all, and only, attributes defined in ``configurable_attrs``.

        Parameters
        ----------
        kwargs: dict
            name: str
                The name of the new multi-index class. If not set, it will be the same
                as the current class

        For the other parameters, see ``BaseIndex`` docstring.

        Returns
        -------
        type
            A new class based on `cls`, with the new attributes set

        """

        name, attrs, kwargs = cls.handle_configurable_attrs(**kwargs)

        if kwargs:
            raise TypeError('%s.configure only accepts these named arguments: %s' % (
                cls.__name__,
                ', '.join(cls.configurable_attrs)
            ))

        return type((str if PY3 else oldstr)(name or cls.__name__), (cls, ), attrs)

    def can_handle_suffix(self, suffix):
        """Tell if the current index can be used for the given filter suffix

        Parameters
        ----------
        suffix: str
            The filter suffix we want to check. Must not includes the "__" part.

        Returns
        -------
        bool
            ``True`` if the index can handle this suffix, ``False`` otherwise.

        """
        try:
            return self.remove_prefix(suffix) in self.handled_suffixes
        except IndexError:
            return False

    def normalize_value(self, value, transform=True):
        """Prepare the given value to be stored in the index

        It first calls ``transform_value`` if ``transform`` is ``True``, then
        calls the ``from_python`` method of the field, then cast the result
        to a str.

        Parameters
        ----------
        value: any
            The value to normalize
        transform: bool
            If ``True`` (the default), the value will be passed to ``transform_value``.
            The returned value will then be used.

        Returns
        -------
        str
            The value, normalized

        """
        if transform:
            value = self.transform_value(value)
        return str(self.field.from_python(value))

    def transform_value(self, value):
        """Convert the value to be stored.

        This does nothing by default but subclasses can change this.
        Then the index will be able to filter on the transformed value.
        For example if the transform capitalizes some text, the filter
        would be ``myfield__capitalized__eq='FOO'``

        """
        if not self.transform:
            return value

        try:
            # we store a staticmethod but we accept a method taking `self` and `value`
            return self.transform(self, value)
        except TypeError as e:
            if 'argument' in str(e):  # try to limit only to arguments error
                return self.transform(value)

    @classmethod
    def remove_prefix(cls, suffix):
        """"Remove the class prefix from the suffix

        The collection pass the full suffix used in the filters to the index.
        But to know if it is valid, we have to remove the prefix to get the
        real suffix, for example to check if the suffix is handled by the index.

        Parameters
        -----------
        suffix: str
            The full suffix to split

        Returns
        -------
        str or None
            The suffix without the prefix. None if the resting suffix is ''

        Raises
        ------
        IndexError:
            If the suffix doesn't contain the prefix

        """

        if not cls.prefix:
            return suffix
        if cls.prefix == suffix:
            return None
        return (suffix or '').split(cls.prefix + '__')[1] or None

    @property
    def connection(self):
        """Shortcut to get the redis connection of the field tied to this index

        Returns
        -------
        Redis
            The redis connection object used to talk to redis.

        """
        return self.field.connection

    @property
    def model(self):
        """Shortcut to get the model tied to the field tied to this index

        Returns
        -------
        Type[RedisModel]
            The model tied to the field tied to the current index

        """
        return self.field._model

    def _get_rollback_cache(self, pk):
        return self._rollback_cache[threading.current_thread().ident][pk]

    def _reset_rollback_cache(self, pk):
        """Reset the cache used to potentially rollback the indexes for an instance

        Parameters
        ----------
        pk : Any
            The primary key of the instance for which to reset the rollback cache

        """
        self._rollback_cache[threading.current_thread().ident].pop(pk, None)

    def _rollback(self, pk):
        """Restore the index in its previous state

        Parameters
        ----------
        pk : Any
            The primary key of the instance for which to rollback the index

        This uses values that were indexed/deindexed since the last call
        to `_reset_rollback_cache`.
        This is used when an error is encountered while updating a value,
        to return to the previous state
        """

        # use `set()`, before the loop, to avoid using self set that may be updated during the process
        cache = self._get_rollback_cache(pk)
        indexed_values = set(cache.get('indexed_values'))
        deindexed_values = set(cache.get('deindexed_values'))

        for args in indexed_values:
            self.remove(pk, *args)
        for args in deindexed_values:
            self.add(pk, *args, check_uniqueness=False)

    def _check_key_accepted_key_types(self, accepted_key_types):
        """Check if key types accepted from the collection match the ones supported by the index.

        Parameters
        ----------
        accepted_key_types : Iterable[str]
            The key types accepted by the collection calling this index.

        Raises
        ------
        ImplementationError
            If there is no match between the key types supported by this index and the ones
            accepted by the collection

        """
        if not accepted_key_types:
            return
        if self.supported_key_types.isdisjoint(accepted_key_types):
            raise ImplementationError(
                '%s can only return keys of type %s' % (
                    self.__class__.__name__,
                    ', '.join(self.supported_key_types)
                )
            )

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Returns the index keys to be used by the collection for the given args

        Parameters
        -----------

        suffix: str
            The suffix used in the filter that called this index
            Useful if the index supports many suffixes doing different things

        args: tuple
            All the "values" to take into account to get the indexed entries.
            In general, the real indexed value is the last entries and the previous
            ones are "additional" information, for example the sub-field name in
            case of a HashField

        kwargs: dict
            accepted_key_types: iterable
                If set, the returned key must be of one of the given redis type.
                May include: 'set', 'zset' or 'list'
                MUST be passed as a named argument

        Returns
        -------
        list of tuple
            An index may return many keys. So it's a list with, each one being
            a tuple with three entries:
                - str
                    The redis key to use
                - str
                    The redis type of the key
                - bool
                    True if the key is a temporary key that must be deleted
                    after the computation of the collection

        """
        raise NotImplementedError

    def check_uniqueness(self, pk, *args):
        """For a unique index, check if the given args are not used twice

        To implement this method in subclasses, get pks for the value (via `args`)
        then call ``assert_pks_uniqueness`` (see in ``EqualIndex``)

        Parameters
        ----------
        pk: Any
            The pk of the instance for which its ok to have the value.
        args: tuple
            All the values to take into account to check the indexed entries

        Raises
        ------
        UniquenessError
            If the uniqueness is not respected.

        """

        raise NotImplementedError

    @property
    def unique_index_name(self):
        """Get a string to describe the index in case of UniquenessError"""
        return 'unique field %s.%s' % (self.model.__name__, self.field.name)

    def assert_pks_uniqueness(self, pks, exclude, get_display_value):
        """Check uniqueness of pks

        Parameters
        -----------
        pks: iterable
            The pks to check for uniqueness. If more than one different,
            it will raise. If only one and different than `exclude`, it will
            raise too.
        exclude: str
            The pk that we accept to be the only one in `pks`. For example
            the pk of the instance we want to check for uniqueness: we don't
            want to raise if the value is the one already set for this instance
        get_display_value: callable
            Called to display the value in the error message.

        Raises
        ------
        UniquenessError
            - If at least two different pks
            - If only one pk that is not the `exclude` one

        """
        pks = list(set(pks))
        if len(pks) > 1:
            # this may not happen !
            raise UniquenessError(
                "Multiple values indexed for %s: %s" % (self.unique_index_name, pks)
            )
        elif len(pks) == 1 and (not exclude or pks[0] != exclude):
            raise UniquenessError(
                'Value "%s" already indexed for %s (for instance %s)' % (
                    get_display_value(), self.unique_index_name, pks[0]
                )
            )

    def add(self, pk, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        Parameters
        ----------
        pk : Any
            The primary key of the instance we want to add to the index
        args: tuple
            All the values to take into account to define the index entry
        kwargs: dict
            check_uniqueness: Optional[bool]
                When ``True`` (the default), if the index is unique, the uniqueness will
                be checked before indexing
                If passed, it MUST be passed as a named argument

        Raises
        ------
        UniquenessError
            If `check_uniqueness` is ``True``, the index unique, and the uniqueness not respected.

        """
        raise NotImplementedError

    def remove(self, pk, *args, **kwargs):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        Parameters
        ----------
        pk : Any
            The primary key of the instance we want to remove from the index
        args: tuple
            All the values to take into account to define the index entry

        """
        raise NotImplementedError

    def get_all_storage_keys(self):
        """Returns the keys to be removed by `clear` in aggressive mode

        Returns
        -------
        set
            The set of all keys that matches the keys used by this index.

        """
        raise NotImplementedError

    def clear(self, chunk_size=1000, aggressive=False):
        """Will deindex all the value for the current field

        Parameters
        ----------
        chunk_size: int
            Default to 1000, it's the number of instances to load at once if not in aggressive mode.
        aggressive: bool
            Default to ``False``. When ``False``, the actual collection of instances will
            be ran through to deindex all the values.
            But when ``True``, the database keys will be scanned to find keys that matches the
            pattern of the keys used by the index. This is a lot faster and may find forsgotten keys.
            But may also find keys not related to the index.
            Should be set to ``True`` if you are not sure about the already indexed values.

        Examples
        --------

        >>> MyModel.get_field('myfield').get_index().clear()

        """
        if aggressive:
            keys = self.get_all_storage_keys()
            with self.model.database.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.delete(key)
                pipe.execute()

        else:
            start = 0
            while True:
                instances = self.model.collection().sort().instances(lazy=True)[start:start + chunk_size]
                for instance in instances:
                    field = instance.get_instance_field(self.field.name)
                    value = field.proxy_get()
                    if value is not None:
                        field.deindex(value, only_index=self)

                if len(instances) < chunk_size:  # not enough data, it means we are done
                    break

                start += chunk_size

    def rebuild(self, chunk_size=1000, aggressive_clear=False):
        """Rebuild the whole index for this field.

        Parameters
        ----------
        chunk_size: int
            Default to 1000, it's the number of instances to load at once.
        aggressive_clear: bool
            Will be passed to the `aggressive` argument of the `clear` method.
            If `False`, all values will be normally deindexed. If `True`, the work
            will be done at low level, scanning for keys that may match the ones used by the index

        Examples
        --------

        >>> MyModel.get_field('myfield').get_index().rebuild()

        """
        self.clear(chunk_size=chunk_size, aggressive=aggressive_clear)

        start = 0
        while True:
            instances = self.model.collection().sort().instances(lazy=True)[start:start + chunk_size]
            for instance in instances:
                field = instance.get_instance_field(self.field.name)
                value = field.proxy_get()
                if value is not None:
                    field.index(value, only_index=self)

            if len(instances) < chunk_size:  # not enough data, it means we are done
                break

            start += chunk_size

    @classmethod
    def _field_model_ready(cls, model, field):
        """Called when a model is ready, for each field..

        May be used by a subclass to handle indexes related but not directly tied to this field

        Parameters
        ----------
        field : RedisField
            The field for which the indexes are ready

        """
        pass

    def _unique_key(self, prefix=None):
        """
        Create a unique key.
        """
        prefix_parts = [self.model._name, '__index__', self.__class__.__name__.lower()]
        if prefix:
            prefix_parts.append(prefix)
        return unique_key(
            self.connection,
            prefix=make_key(*prefix_parts)
        )


class EqualIndex(BaseIndex):
    """Default simple equal index."""

    handled_suffixes = {None, 'eq', 'in'}
    handle_uniqueness = True
    supported_key_types = {'set'}

    def union_filtered_in_keys(self, dest_key, *source_keys):
        """Do a union of the given `source_keys` at the redis level, into `dest_key`

        Parameters
        ----------
        dest_key : str
            The key where to store the result of the union
        source_keys : str
            The keys to union

        """
        self.connection.sunionstore(dest_key, source_keys)

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Return the set used by the index for the given "value" (`args`)

        For the parameters, see ``BaseIndex.get_filtered_keys``

        """

        self._check_key_accepted_key_types(kwargs.get('accepted_key_types'))

        # special "in" case: we get n keys and make an unionstore with them then return this key
        if suffix == 'in':

            args = list(args)
            values = set(args.pop())

            if not values:
                return []  # no keys

            in_keys = [
                self.get_storage_key(transform_value=False, *(args+[value]))
                for value in values
            ]

            tmp_key = self._unique_key('tmp')
            self.union_filtered_in_keys(tmp_key, *in_keys)

            return [(tmp_key, 'set', True)]

        # do not transform because we already have the value we want to look for
        return [(self.get_storage_key(transform_value=False, *args), 'set', False)]

    def get_storage_key(self, *args, **kwargs):
        """Return the redis key where to store the index for the given "value" (`args`)

        For this index, we store all PKs having the same value for a field in the same
        set. Key has this form:
        model-name:field-name:sub-field-name:normalized-value
        The ':sub-field-name part' is repeated for each entry in *args that is not the final value

        Parameters
        -----------
        kwargs: dict
            transform_value: bool
                Default to ``True``. Tell the call to ``normalize_value`` to transform
                the value or not
        args: tuple
            All the "values" to take into account to get the storage key.

        Returns
        -------
        str
            The redis key to use

        """

        args = list(args)
        value = args.pop()

        parts = [
            self.model._name,
            self.field.name,
        ] + args

        if self.prefix:
            parts.append(self.prefix)

        if self.key:
            parts.append(self.key)

        normalized_value = self.normalize_value(value, transform=kwargs.get('transform_value', True))

        parts.append(normalized_value)

        return self.field.make_key(*parts)

    def get_all_storage_keys(self):
        """Returns the keys to be removed by `clear` in aggressive mode

        For the parameters, see BaseIndex.get_all_storage_keys

        """

        parts1 = [
            self.model._name,
            self.field.name,
        ]

        parts2 = parts1 + ['*']  # for indexes taking args, like for hashfields

        if self.prefix:
            parts1.append(self.prefix)
            parts2.append(self.prefix)

        if self.key:
            parts1.append(self.key)
            parts2.append(self.key)

        parts1.append('*')
        parts2.append('*')

        return set(
            self.model.database.scan_keys(self.field.make_key(*parts1))
        ).union(
            set(
                self.model.database.scan_keys(self.field.make_key(*parts2))
            )
        )

    def get_uniqueness_members(self, key):
        """Get from redis all the members of the given index `key` used to check for uniqueness.

        Parameters
        ----------
        key : str
            The index key we want the members from.

        Returns
        -------
        List[str]
            The members of the index `key`.

        """
        return list(self.connection.smembers(key))

    def check_uniqueness(self, pk, *args, **kwargs):
        """Check if the given "value" (via `args`) is unique or not.

        Parameters
        ----------
        kwargs: dict
            key: str
                When given, it will be used instead of calling ``get_storage_key``
                for the given args
                MUST be passed as a keyword argument

        For the other parameters, see ``BaseIndex.check_uniqueness``

        """

        if not self.field.unique:
            return

        key = kwargs.get('key', None)
        if key is None:
            key = self.get_storage_key(*args)

        # Lets check if the index key already exist for another instance
        pks = self.get_uniqueness_members(key)

        self.assert_pks_uniqueness(pks, pk, lambda: list(args)[-1])

    def store(self, key, pk, **kwargs):
        """Store data in the index in redis

        Parameters
        ----------
        key : str
            The key in which to store data
        pk : Any
            The pk of the instance to save in the index.
        kwargs : Any
            This is the ``kwargs`` passed to ``.add``. May be used by subclasses.

        Returns
        -------
        bool
            If we asked redis to do something. Always ``True`` for this index but may vary in
            subclasses.

        """
        self.connection.sadd(key, pk)
        return True

    def unstore(self, key, pk, **kwargs):
        """Remove data from the index in redis

        Parameters
        ----------
        key : str
            The key from which to remove data
        pk : Any
            The pk of the instance to remove from the index.
        kwargs : Any
            This is the ``kwargs`` passed to ``.add``. May be used by subclasses.

        Returns
        -------
        bool
            If we asked redis to do something. Always ``True`` for this index but may vary in
            subclasses.

        """
        self.connection.srem(key, pk)
        return True

    def add(self, pk, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        For the parameters, see ``BaseIndex.add``

        """

        check_uniqueness = kwargs.get('check_uniqueness', True)

        key = self.get_storage_key(*args)
        if self.field.unique and check_uniqueness:
            self.check_uniqueness(pk, key=key, *args)

        # Do index => create a key to be able to retrieve parent pk with
        # current field value]
        logger.debug("adding %s to index %s" % (pk, key))
        if self.store(key, pk, **kwargs):
            self._get_rollback_cache(pk)['indexed_values'].add(tuple(args))

    def remove(self, pk, *args, **kwargs):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        For the parameters, see ``BaseIndex.remove``

        """

        key = self.get_storage_key(*args)
        logger.debug("removing %s from index %s" % (pk, key))
        if self.unstore(key, pk, **kwargs):
            self._get_rollback_cache(pk)['deindexed_values'].add(tuple(args))


class BaseRangeIndex(BaseIndex):
    """Base of indexes using sorted-set to do range filtering (lt, gte...)"""

    handle_uniqueness = True
    lua_filter_script = NotImplemented
    supported_key_types = {'set', 'zset'}

    def get_storage_key(self, *args):
        """Return the redis key where to store the index for the given "value" (`args`)

        For this index, we store all PKs for a field in the same sorted-set.
        Key has this form:
        model-name:field-name:sub-field-name:index-key-name
        The ':sub-field-name part' is repeated for each entry in *args that is not the final value

        Parameters
        -----------
        args: tuple
            All the "values" to take into account to get the storage key. The last entry,
            the final value, is not used.

        Returns
        -------
        str
            The redis key to use

        """

        args = list(args)
        args.pop()  # final value, not needed for the storage key

        parts = [
            self.model._name,
            self.field.name,
        ] + args

        if self.prefix:
            parts.append(self.prefix)

        if self.key:
            parts.append(self.key)

        return self.field.make_key(*parts)

    def get_all_storage_keys(self):
        """Returns the keys to be removed by `clear` in aggressive mode

        For the parameters, see BaseIndex.get_all_storage_keys

        """

        parts1 = [
            self.model._name,
            self.field.name,
        ]

        parts2 = parts1 + ['*']  # for indexes taking args, like for hashfields

        if self.prefix:
            parts1.append(self.prefix)
            parts2.append(self.prefix)

        if self.key:
            parts1.append(self.key)
            parts2.append(self.key)

        return set(
            self.model.database.scan_keys(self.field.make_key(*parts1))
        ).union(
            set(
                self.model.database.scan_keys(self.field.make_key(*parts2))
            )
        )

    def prepare_data_to_store(self, pk, value, **kwargs):
        """Prepare the value and score to be stored in the zset

        Parameters
        ----------
        pk: Any
            The pk, that will be stringified
        value: Any
            The value, to normalize, to use for indexing
        kwargs: Any
            Passed by the add/remove methods as they receive them.

        Returns
        -------
        str
            The string ready to use as member of the sorted set.
        float
            The score for this member of the sorted set

        """
        raise NotImplementedError

    def check_uniqueness(self, pk, *args, **kwargs):
        """Check if the given "value" (via `args`) is unique or not.

        For the parameters, see ``BaseIndex.check_uniqueness``

        """

        if not self.field.unique:
            return

        key = self.get_storage_key(*args)
        value = list(args)[-1]
        pks = self.get_pks_for_filter(key, 'eq', self.normalize_value(value))

        self.assert_pks_uniqueness(pks, pk, lambda: value)

    def store(self, key, member, score):
        """Store data in the index in redis

        Parameters
        ----------
        key : str
            The key in which to store data
        member : Any
            The member to store in the sorted set.
        score : Union[int, float, None]
            The score to use to store the `member`.

        Returns
        -------
        bool
            If we asked redis to do something. Will be ``True`` except if `score` is ``None``, in
            which case we don't ask redis to save the member.

        """
        if score is None:
            return False
        self.connection.zadd(key, {member: score})
        return True

    def unstore(self, key, member, score):
        """Remove data from the index in redis

        Parameters
        ----------
        key : str
            The key from which to remove data
        member : Any
            The member to remove from the sorted set.
        score : Union[int, float, None]
            The score for the member, but not used here (may be in subclasses)

        Returns
        -------
        bool
            If we asked redis to do something. Always ``True`` for this index but may vary in
            subclasses.

        """
        self.connection.zrem(key, member)
        return True

    def add(self, pk, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        For the parameters, see ``BaseIndex.add``

        Notes
        -----
        If the score returned by ``prepare_data_to_store`` is None, nothing will be added to the
        index. If it not a valid float, an error will be raised.

        """

        check_uniqueness = kwargs.get('check_uniqueness', True)

        if self.field.unique and check_uniqueness:
            self.check_uniqueness(pk, *args)

        key = self.get_storage_key(*args)

        args = list(args)
        value = args[-1]

        logger.debug("adding %s to index %s" % (pk, key))
        member, score = self.prepare_data_to_store(pk, value, **kwargs)
        if self.store(key, member, score):
            self._get_rollback_cache(pk)['indexed_values'].add(tuple(args))

    def remove(self, pk, *args, **kwargs):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        For the parameters, see ``BaseIndex.remove``

        """

        key = self.get_storage_key(*args)

        args = list(args)
        value = args[-1]

        logger.debug("removing %s from index %s" % (pk, key))
        member, score = self.prepare_data_to_store(pk, value, **kwargs)
        if self.unstore(key, member, score):
            self._get_rollback_cache(pk)['deindexed_values'].add(tuple(args))

    def get_boundaries(self, filter_type, value):
        """Compute the boundaries to pass to the sorted-set command depending of the filter type

        Parameters
        ----------
        filter_type: str
            One of the filter suffixes in ``self.handled_suffixes``
        value: str
            The normalized value for which we want the boundaries

        Returns
        -------
        tuple
            A tuple with three entries, the begin and the end of the boundaries to pass
            to sorted-set command, and in third a value to exclude from the result when
            querying the sorted-set

        """

        raise ImplementationError

    def call_script(self, key, tmp_key, key_type, start, end, exclude, *args):
        """Call the lua scripts with given keys and args

        Parameters
        -----------
        key: str
            The key of the index sorted-set
        tmp_key: str
            The final temporary key where to store the filtered primary keys
        key_type: str
            The type of temporary key to use, either 'set' or 'zset'
        start: str
            The "start" argument to pass to the filtering sorted-set command
        end: str
            The "end" argument to pass to the filtering sorted-set command
        exclude: any
            A value to exclude from the filtered pks to save to the temporary key
        args: list
            Any other argument to be passed by a subclass will be passed as addition
            args to the script.

        """
        self.model.database.call_script(
            # be sure to use the script dict at the class level
            # to avoid registering it many times
            script_dict=self.__class__.lua_filter_script,
            keys=[key, tmp_key],
            args=[key_type, start, end, exclude or ""] + list(args)  # None is refused by redis-py so we pass ""
        )

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Returns the index key for the given args "value" (`args`)

        For the parameters, see ``BaseIndex.get_filtered_keys``

        Notes
        -----
        The process of reading from the sorted-set, extracting the primary keys, excluding some
        values if needed, and putting the primary keys in a set or zset, is done in lua at the
        redis level.

        """
        accepted_key_types = kwargs.get('accepted_key_types')
        self._check_key_accepted_key_types(accepted_key_types)

        key_type = 'set' if not accepted_key_types or 'set' in accepted_key_types else 'zset'
        tmp_key = self._unique_key('tmp')
        args = list(args)

        # special "in" case: we get n keys and make an unionstore with them then return this key
        if suffix == 'in':

            values = set(args.pop())

            if not values:
                return []  # no keys

            in_keys = [
                self.get_filtered_keys('eq', *(args+[value]), **kwargs)[0][0]
                for value in values
            ]

            if key_type == 'set':
                self.connection.sunionstore(tmp_key, in_keys)
            else:
                self.connection.zunionstore(tmp_key, in_keys)

            # we can delete the temporary keys
            for in_key in in_keys:
                self.connection.delete(in_key)

            return [(tmp_key, key_type, True)]

        key = self.get_storage_key(*args)
        value = self.normalize_value(args[-1], transform=False)

        real_suffix = self.remove_prefix(suffix)

        start, end, exclude = self.get_boundaries(real_suffix, value)
        self.call_script(key, tmp_key, key_type, start, end, exclude)

        return [(tmp_key, key_type, True)]

    def get_pks_for_filter(self, key, filter_type, value):
        """Extract the pks from the zset key for the given type and value

        This is used for the uniqueness check

        Parameters
        ----------
        key: str
            The key of the redis sorted-set to use
        filter_type: str
            One of ``self.handled_suffixes``
        value:
            The normalized value for which we want the pks

        Returns
        -------
        list
            The list of instances PKs extracted from the sorted set

        """

        raise NotImplementedError


class TextRangeIndex(BaseRangeIndex):
    """Index allowing to filter on something greater/less than a value

    We use the zrangebylex redis command that was created for this very purpose

    See Also
    ---------
    https://redis.io/topics/indexes#lexicographical-indexes

    """

    handled_suffixes = {None, 'eq', 'gt', 'gte', 'lt', 'lte', 'startswith', 'in'}
    key = 'text-range'
    separator = u':%s-SEPARATOR:' % key.upper()

    lua_filter_script = {
        # we extract members of the sorted-set via zrangebylex
        # then we split the value and pk, on the separator
        # if the value is the one in exclude, we ignore it
        # and we add every pk to a set or zset depending on the asked type
        # if a zset, we use the returned position as a score for each member
        # we do this in block of 100 to avoid storing to many temporary things
        # in memory
        'lua': """
            local source_key, dest_type, dest_key = KEYS[1], ARGV[1], KEYS[2]
            local lex_start, lex_end = ARGV[2], ARGV[3]
            local exclude, separator = ARGV[4], ARGV[5]
            local start, block_size = 0, 100

            while true do
                local members = redis.call('zrangebylex', source_key, lex_start, lex_end, 'limit', start, block_size)
                if members[1] == nil then -- nothing returned, we are done
                    break
                end
                local result, nb_results = {}, 0;
                for i, member in ipairs(members) do
                    -- split to get value and pk (do it reverse to split on the last separator only)
                    local first_pos, last_pos = member:reverse():find(separator:reverse(), 1, true)
                    first_pos = member:len() - last_pos  -- real position of last separator

                    -- only add if nothing to exclude, or the rest is not the exclude
                    if not exclude or member:sub(1, first_pos) ~= exclude then
                        nb_results = nb_results + 1
                        result[nb_results] = member:sub(first_pos + separator:len() + 1)
                    end
                end
                -- call sadd/zadd only if we have something to put in
                if nb_results > 0 then  -- sadly, no "continue" in lua :(
                    if dest_type == 'set' then
                        redis.call('sadd', dest_key, unpack(result))
                    else
                        -- zadd expect args this way: score member score member ...
                        local args = {}
                        for i, member in ipairs(result) do
                            args[2*i-1], args[2*i] = i-1, member
                        end
                        redis.call('zadd', dest_key, unpack(args))
                    end
                end
                -- if we got less than the max, it means we are done
                if members[block_size] == nil then
                    break
                end
                -- loop again for the next block
                start = start + block_size
            end
            -- return the key, because why not
            return dest_key
        """
    }

    def prepare_data_to_store(self, pk, value, **kwargs):
        """Prepare the value to be stored in the zset

        For the parameters, see BaseRangeIndex.prepare_data_to_store

        We add a string "value:pk" to the storage sorted-set, with a score of 0.
        Then when filtering will get then lexicographical ordered
        And we'll later be able to extract the pk for each returned values
        """
        value = self.normalize_value(value)
        return self.separator.join([value, str(pk)]), 0

    def _extract_value_from_storage(self, string):
        """Taking a string that was a member of the zset, extract the value and pk

        Parameters
        ----------
        string: str
            The member extracted from the sorted set

        Returns
        -------
        tuple
            Tuple with the value and the pk, extracted from the string

        """
        parts = string.split(self.separator)
        pk = parts.pop()
        return self.separator.join(parts), pk

    def get_boundaries(self, filter_type, value):
        """Compute the boundaries to pass to zrangebylex depending of the filter type

        The third return value, ``exclude`` is ``None`` except for the filters
        `lt` and `gt` because we cannot explicitly exclude it when
         querying the sorted-set

        For the parameters, see BaseRangeIndex.store

        Notes
        -----
        For zrangebylex:
        - `(` means "not included"
        - `[` means "included"
        - `\xff` is the last char, it allows to say "starting with"
        - `-` alone means "from the very beginning"
        - `+` alone means "to the very end"

        """

        assert filter_type in self.handled_suffixes

        start = '-'
        end = '+'
        exclude = None

        if filter_type in (None, 'eq'):
            # we include the separator to only get the members with the exact value
            start = u'[%s%s' % (value, self.separator)
            end = start.encode('utf-8') + b'\xff'

        elif filter_type == 'gt':
            # starting at the value, excluded
            start = u'(%s' % value
            exclude = value

        elif filter_type == 'gte':
            # starting at the value, included
            start = u'[%s' % value

        elif filter_type == 'lt':
            # ending with the value, excluded
            end = u'(%s' % value
            exclude = value

        elif filter_type == 'lte':
            # ending with the value, included (but not starting with, hence the separator)
            end = u'[%s%s' % (value, self.separator)
            end = end.encode('utf-8') + b'\xff'

        elif filter_type == 'startswith':
            # using `\xff` to simulate "startswith"
            start = u'[%s' % value
            end = start.encode('utf-8') + b'\xff'

        return start, end, exclude

    def get_pks_for_filter(self, key, filter_type, value):
        """Extract the pks from the zset key for the given type and value

        For the parameters, see BaseRangeIndex.get_pks_for_filter
        """
        start, end, exclude = self.get_boundaries(filter_type, value)
        members = self.connection.zrangebylex(key, start, end)
        if exclude is not None:
            # special case where we don't want the exact given value, but we cannot
            # exclude it from the sorted set directly
            return [
                member_pk
                for member_value, member_pk in
                [self._extract_value_from_storage(member) for member in members]
                if member_value != exclude
            ]
        else:
            return [self._extract_value_from_storage(member)[-1] for member in members]

    def call_script(self, key, tmp_key, key_type, start, end, exclude, *args):
        """Call the lua scripts with given keys and args

        We add the separator to the arguments to be passed to the script

        For the parameters, see BaseRangeIndex.call_script

        """

        args = list(args)
        args.append(self.separator)

        super(TextRangeIndex, self).call_script(
            key, tmp_key, key_type, start, end, exclude, *args
        )


class NumberRangeIndex(BaseRangeIndex):

    handled_suffixes = {None, 'eq', 'gt', 'gte', 'lt', 'lte', 'in'}
    key = 'number-range'
    raise_if_not_float = False

    lua_filter_script = {
        # we extract members of the sorted-set via zrangebyscore
        # and we add every pk to a set or zset depending on the asked type
        # if a zset, we use the returned position as a score for each member
        # we do this in block of 100 to avoid storing to many temporary things
        # in memory
        'lua': """
            local source_key, dest_type, dest_key = KEYS[1], ARGV[1], KEYS[2]
            local score_start, score_end = ARGV[2], ARGV[3]
            local start, block_size = 0, 100

            while true do
                local members = redis.call('zrangebyscore', source_key, score_start, score_end, 'limit', start, block_size)
                if members[1] == nil then -- nothing returned, we are done
                    break
                end
                -- call sadd/zadd only if we have something to put in
                if #members > 0 then  -- sadly, no "continue" in lua :(
                    if dest_type == 'set' then
                        redis.call('sadd', dest_key, unpack(members))
                    else
                        -- zadd expect args this way: score member score member ...
                        local args = {}
                        for i, member in ipairs(members) do
                            args[2*i-1], args[2*i] = i-1, member
                        end
                        redis.call('zadd', dest_key, unpack(args))
                    end
                end
                -- if we got less than the max, it means we are done
                if members[block_size] == nil then
                    break
                end
                -- loop again for the next block
                start = start + block_size
            end
            -- return the key, because why not
            return dest_key
        """
    }

    def normalize_value(self, value, transform=True):
        """Prepare the given value to be stored in the index

        For the parameters, see BaseIndex.normalize_value

        Raises
        ------
        ValueError
            If ``raise_if_not_float`` is True and the value cannot
            be casted to a float.

        """
        if transform:
            value = self.transform_value(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            if self.raise_if_not_float:
                raise ValueError('Invalid value %s for field %s.%s' % (
                    value, self.model.__name__, self.field.name
                ))
            return 0

    def prepare_data_to_store(self, pk, value, **kwargs):
        """Prepare the value to be stored in the zset

        We simple store the pk as a member of the sorted set with the value being the score

        For the parameters, see BaseRangeIndex.prepare_data_to_store

        """
        return pk, self.normalize_value(value)

    def get_boundaries(self, filter_type, value):
        """Compute the boundaries to pass to the sorted-set command depending of the filter type

        The third return value, ``exclude`` is always ``None`` because we can easily restrict the
        score to filter on in the sorted-set.

        For the parameters, see BaseRangeIndex.store

        Notes
        -----
        For zrangebyscore:
        - `(` means "not included"
        - `-inf` alone means "from the very beginning"
        - `+inf` alone means "to the very end"
        """

        assert filter_type in self.handled_suffixes

        start = '-inf'
        end = '+inf'
        exclude = None

        if filter_type in (None, 'eq'):
            # only one score
            start = end = value

        elif filter_type == 'gt':
            start = '(%s' % value

        elif filter_type == 'gte':
            start = value

        elif filter_type == 'lt':
            end = '(%s' % value

        elif filter_type == 'lte':
            end = value

        return start, end, exclude

    def get_pks_for_filter(self, key, filter_type, value):
        """Extract the pks from the zset key for the given type and value

        For the parameters, see BaseRangeIndex.get_pks_for_filter
        """
        start, end, __ = self.get_boundaries(filter_type, value)  # we have nothing to exclude
        return self.connection.zrangebyscore(key, start, end)


class _MultiFieldsIndexMixin(object):
    """Mixin for multi-fields indexing"""

    filter_single_field = False
    unique = False
    handle_uniqueness = False

    @classmethod
    def handle_configurable_attrs(cls, **kwargs):
        """Deny passing ``handle_uniqueness``"""
        cls.configurable_attrs -= {'handle_uniqueness'}
        return super(_MultiFieldsIndexMixin, cls).handle_configurable_attrs(**kwargs)

    def can_filter_fields(self, fields_and_suffixes):
        """Tell if the index can handle the given fields + suffixes

        It's a wrapper around ``_can_filter_fields`` that does the real work. This wrapper
        cache the result.

        Parameters
        ----------
        fields_and_suffixes : List[Tuple[str, Union[str, None]]]
            The fields and suffixes to check.
            It's a list with each entr being a tuple containing the field name and the suffix.

        Returns
        -------
        List[Iterable[Tuple[str, Union[str, None]]]]
            A list of fields+suffixes that can be handled together among the given ones
            Each entry is an iterable, each entry of this iterable being a tuple with two values:
            the field name, and the suffix.
            The main list holds all the possible combinations
            Example:
            [
                (('name', 'eq'), ('priority', None), ('foo', 'eq')),
                (('name', 'eq'), ('priority', None), ('foo', 'in')),
                (('name', None), ('priority', None), ('foo', 'eq')),
                (('name', None), ('priority', None), ('foo', 'in')),
            ]

        """

        if not hasattr(self, '_can_filter_fields_cache'):
            self._can_filter_fields_cache = {}

        fields_and_suffixes = tuple(sorted(fields_and_suffixes, key=lambda val: (val[0], val[1] or '')))
        if fields_and_suffixes not in self._can_filter_fields_cache:
            handled = self._can_filter_fields(fields_and_suffixes)
            if handled:
                field_names, field_suffixes = zip(*handled)
                handled = [tuple(zip(field_names, product_values)) for product_values in product(*field_suffixes)]
            self._can_filter_fields_cache[fields_and_suffixes] =  handled

        return self._can_filter_fields_cache[fields_and_suffixes]

    def _can_filter_fields(self, fields_and_suffixes):
        """Tell if the index can handle the given fields + suffixes

        For the parameters, see ``can_filter_fields``

        Returns
        -------
        List[Tuple[str, Set[Union[None, str]]]
            If the index can handle the given fields and suffixes, it will return a list with one
            tuple for each field managed by the index. Each tuple has two entries:
            - the field name
            - a set with all handled suffixes among the ones asked in `fields_and_suffixes`.
            If the index cannot handle the given fields and suffixes, returns an empty list.
            Example:
                [('name', {None, 'eq'}), ('priority', {None}), ('foo', {'in', 'eq')}
            Note: the order may be important in your index. In general, the field to which the
            index is attached (defined in the model) should be first.

        """
        raise NotImplementedError

    def check_uniqueness_at_init(self, values):
        """If the index is ``unique``, check that ``values`` are unique and can be inserted

        Parameters
        ----------
        values : Dict[str, Any]
            The values we want to insert. A dict with field names as keys, and values to set as
            values.
            Can contains fields not managed by the index (they will be ignored) but must contain
            all the fields managed by the index.

        """
        raise NotImplementedError
