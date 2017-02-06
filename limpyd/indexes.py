# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import object, str

from logging import getLogger

from limpyd.exceptions import ImplementationError, UniquenessError

logger = getLogger(__name__)


class BaseIndex(object):
    """Base of all indexes

    Class Attributes
    -----------------
    handled_suffixes: set of str
        The suffixes in the filter keys allowed for this index.
        If one of them is ``None``, it can be used when no suffix is given.
        For example `collection(foo=1)` can be used for the `None` suffix.
        And `collection(foo__eq=1)` can be used for the `eq` suffix.
    handle_uniqueness: bool
        If ``True``, the index is able to check for uniqueness. When many index are used, only
        the first with this flag set to ``True`` have to do the work.

    Parameters
    -----------
    field: RedisField
        The field object using the instance of this index

    """

    handled_suffixes = set()
    handle_uniqueness = False

    def __init__(self, field):
        """Attach the index to the given field and prepare the internal cache"""
        self.field = field
        self._reset_cache()

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
        return suffix in self.handled_suffixes

    def normalize_value(self, value):
        """Prepare the given value to be stored in the index

        It calls the ``from_python`` method of the field, then cast the result
        to a str.

        Parameters
        ----------
        value: any
            The value to normalize

        Returns
        -------
        str
            The value, normalized

        """
        return str(self.field.from_python(value))

    @property
    def connection(self):
        """Shortcut to get the redis connection of the field tied to this index"""
        return self.field.connection

    @property
    def model(self):
        """Shortcut to get the model tied to the field tied to this index"""
        return self.field._model

    @property
    def instance(self):
        """Shortcut to get the instance tied to the field tied to this index"""
        return self.field._instance

    def _reset_cache(self):
        """Reset attributes used to potentially rollback the indexes"""
        self._indexed_values = set()
        self._deindexed_values = set()

    def _rollback(self):
        """Restore the index in its previous state

        This uses values that were indexed/deindexed since the last call
        to `_reset_cache`.
        This is used when an error is encountered while updating a value,
        to return to the previous state
        """

        # to avoid using self set that may be updated during the process
        indexed_values = set(self._indexed_values)
        deindexed_values = set(self._deindexed_values)

        for args in indexed_values:
            self.remove(*args)
        for args in deindexed_values:
            self.add(*args, check_uniqueness=False)

    def get_filtered_key(self, suffix, *args, **kwargs):
        """Returns the index key for the given args

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
        tuple
            A tuple with three entries
            - str
                The redis key to use
            - str
                The redis type of the key
            - bool
                True if the key is a temporary key that must be deleted
                after the computation of the collection

        """
        raise NotImplementedError

    def check_uniqueness(self, *args):
        """For a unique index, check if the given args are not used twice

        To implement this method in subclasses, get pks for the value (via `args`)
        then call ``assert_pks_uniqueness`` (see in ``EqualIndex``)

        Parameters
        ----------
        args: tuple
            All the values to take into account to check the indexed entries

        Raises
        ------
        UniquenessError
            If the uniqueness is not respected.

        Returns
        -------
        None

        """

        raise NotImplementedError

    def assert_pks_uniqueness(self, pks, exclude, value):
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
        value: any
            Only to be displayed in the error message.

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
                "Multiple values indexed for unique field %s.%s: %s" % (
                    self.model.__name__, self.field.name, pks
                )
            )
        elif len(pks) == 1 and (not exclude or pks[0] != exclude):
            self.connection.delete(self.field.key)
            raise UniquenessError(
                'Value "%s" already indexed for unique field %s.%s (for instance %s)' % (
                    self.normalize_value(value), self.model.__name__, self.field.name, pks[0]
                )
            )

    def add(self, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        Parameters
        ----------
        args: tuple
            All the values to take into account to define the index entry
        kwargs: dict
            check_uniqueness: bool
                When ``True`` (the default), if the index is unique, the uniqueness will
                be checked before indexing
                MUST be passed as a named argument

        Raises
        ------
        UniquenessError
            If `check_uniqueness` is ``True``, the index unique, and the uniqueness not respected.

        """
        raise NotImplementedError

    def remove(self, *args):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        Parameters
        ----------
        args: tuple
            All the values to take into account to define the index entry

        """
        raise NotImplementedError


class EqualIndex(BaseIndex):
    """Default simple equal index.

    It can be overridden to create transformative index by overriding:
    - handled_suffixes
    - index_key_name
    - transform_normalized_value_for_storage

    Examples
    --------

    To create an 'reverse' index where the user could do `name__reverse_eq='oof'`
    to get an object with the value of 'foo', use this index:

    >>> class ReverseEqualIndex(EqualIndex):
    ...     handled_suffixes = {'reverse_eq'}
    ...     index_key_name = 'reverse-equal'
    ...
    ...     def transform_normalized_value_for_storage(self, value):
    ...         return value[::-1]


    """

    handled_suffixes = {None, 'eq'}
    handle_uniqueness = True
    index_key_name = None

    def transform_normalized_value_for_storage(self, value):
        """Convert the value to be used in the storage key.

        This does nothing in this equal index but may be changed for a
        transformative index based on this one.

        """
        return value

    def get_filtered_key(self, suffix, *args, **kwargs):
        """Return the set used by the index for the given "value" (`args`)

        For the parameters, see ``BaseIndex.get_filtered_key``

        """

        accepted_key_types = kwargs.get('accepted_key_types', None)

        if accepted_key_types and 'set' not in accepted_key_types:
            raise ImplementationError(
                '%s can only return keys of type "set"' % self.__class__.__name__
            )

        return self.get_storage_key(transform_value=False, *args), 'set', False

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
                Default to ``True``. When ``True``, ``transform_normalized_value_for_storage``
                is called with the normalized value, else it is used directly.
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

        if self.index_key_name:
            parts.append(self.index_key_name)

        normalized_value = self.normalize_value(value)
        if kwargs.get('transform_value', True):
            normalized_value = self.transform_normalized_value_for_storage(normalized_value)

        parts.append(normalized_value)

        return self.field.make_key(*parts)

    def check_uniqueness(self, *args, **kwargs):
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
        pk = self.instance.pk.get()
        pks = list(self.connection.smembers(key))

        self.assert_pks_uniqueness(pks, pk, list(args)[-1])

    def add(self, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        For the parameters, see ``BaseIndex.add``

        """

        check_uniqueness = kwargs.get('check_uniqueness', True)

        key = self.get_storage_key(*args)
        if self.field.unique and check_uniqueness:
            self.check_uniqueness(key=key, *args)

        # Do index => create a key to be able to retrieve parent pk with
        # current field value]
        pk = self.instance.pk.get()
        logger.debug("adding %s to index %s" % (pk, key))
        self.connection.sadd(key, pk)
        self._indexed_values.add(tuple(args))

    def remove(self, *args):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        For the parameters, see ``BaseIndex.remove``

        """

        key = self.get_storage_key(*args)
        pk = self.instance.pk.get()
        logger.debug("removing %s from index %s" % (pk, key))
        self.connection.srem(key, pk)
        self._deindexed_values.add(tuple(args))
