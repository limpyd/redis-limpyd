# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import object, str

from logging import getLogger

from limpyd.exceptions import ImplementationError, LimpydException, UniquenessError
from limpyd.utils import unique_key

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


class TextRangeIndex(BaseIndex):
    """Index allowing to filter on something greater/less than a value

    We use the zrangebylex redis command that was created for this very purpose

    See Also
    ---------
    https://redis.io/topics/indexes#lexicographical-indexes

    """

    handled_suffixes = {None, 'eq', 'gt', 'gte', 'lt', 'lte', 'startswith'}
    handle_uniqueness = True
    index_key_name = 'text-range'

    separator = u':%s-SEPARATOR:' % index_key_name.upper()

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
            local lex_start, lex_end = ARGV[3], ARGV[4]
            local separator, exclude = ARGV[2],  ARGV[5]
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

    def __init__(self, field):
        super(TextRangeIndex, self).__init__(field)

        try:
            model = self.model
        except AttributeError:
            # index not yet tied to an field tied to a model
            pass
        else:
            if not self.model.database.support_zrangebylex():
                raise LimpydException(
                    'Your redis version %s does not seems to support ZRANGEBYLEX '
                    'so range indexes are not usable' % (
                        '.'.join(str(part) for part in self.model.database.redis_version)
                    )
                )

    def get_storage_key(self, *args):
        """Return the redis key where to store the index for the given "value" (`args`)

        For this index, we store all PKs having for a field in the same sorted-set.
        Key has this form:
        model-name:field-name:sub-field-name:text-range
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
        ] + args + [
            self.index_key_name,
        ]

        return self.field.make_key(*parts)

    def check_uniqueness(self, *args, **kwargs):
        """Check if the given "value" (via `args`) is unique or not.

        For the parameters, see ``BaseIndex.check_uniqueness``

        """

        if not self.field.unique:
            return

        try:
            pk = self.instance.pk.get()
        except AttributeError:
            pk = None

        key = self.get_storage_key(*args)
        value = list(args)[-1]
        pks = self.get_pks_for_filter(key, 'eq', self.normalize_value(value))

        self.assert_pks_uniqueness(pks, pk, value)

    def _prepare_value_for_storage(self, value, pk):
        """Prepare the value to be stored in the zset: value and pk separated

        Parameters
        ----------
        value: any
            The value, to normalize, to use
        pk: any
            The pk, that will be stringified

        Returns
        -------
        str
            The string ready to use as member of the sorted set.

        """
        normalized_value = self.normalize_value(value)
        return self.separator.join([normalized_value, str(pk)])

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

    def add(self, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        For the parameters, see ``BaseIndex.add``

        """

        check_uniqueness = kwargs.get('check_uniqueness', True)

        if self.field.unique and check_uniqueness:
            self.check_uniqueness(*args)

        key = self.get_storage_key(*args)

        args = list(args)
        value = args[-1]

        # We add a string "value:pk" to the storage sorted-set, with a score of 0.
        # Then when filtering will get then lexicographical ordered
        # And we'll later be able to extract the pk for each returned values

        pk = self.instance.pk.get()
        logger.debug("adding %s to index %s" % (pk, key))
        self.connection.zadd(key, 0, self._prepare_value_for_storage(value, pk))
        self._indexed_values.add(tuple(args))

    def remove(self, *args):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        For the parameters, see ``BaseIndex.remove``

        """

        key = self.get_storage_key(*args)

        args = list(args)
        value = args[-1]

        pk = self.instance.pk.get()
        logger.debug("removing %s from index %s" % (pk, key))
        self.connection.zrem(key, self._prepare_value_for_storage(value, pk))
        self._deindexed_values.add(tuple(args))

    def get_lex_boundaries(self, filter_type, value):
        """Compute the boundaries to pass to zrangebylex depending of the filter type

        Parameters
        ----------
        filter_type: str
            One of the filter suffixes in ``self.handled_suffixes``
        value: str
            The normalized value for which we want the boundaries

        Returns
        -------
        tuple
            A tuple with two entries, the begin and the end of the boundaries to pass
            to zrangebylex

        Notes
        -----
        For zrangebylex:
        - `(` means "not included"
        - `[` means "included"
        - `\xff` is the last char, it allows to say "starting with"

        """

        assert filter_type in self.handled_suffixes

        start = '-'  # from the very start
        end = '+'  # to the very end

        if filter_type in (None, 'eq'):
            # we include the separator to only get the members with the exact value
            start = u'[%s%s' % (value, self.separator)
            end = start.encode('utf-8') + b'\xff'

        elif filter_type == 'gt':
            # starting at the value, excluded
            start = u'(%s' % value

        elif filter_type == 'gte':
            # starting at the value, included
            start = u'[%s' % value

        if filter_type == 'lt':
            # ending with the value, excluded
            end = u'(%s' % value

        elif filter_type == 'lte':
            # ending with the value, included (but not starting with, hence the separator)
            end = u'[%s%s' % (value, self.separator)
            end = end.encode('utf-8') + b'\xff'

        elif filter_type == 'startswith':
            # using `\xff` to simulate "startswith"
            start = u'[%s' % value
            end = start.encode('utf-8') + b'\xff'

        return start, end

    def get_pks_for_filter(self, key, filter_type, value):
        """Extract the pks from the zset key for the given type and value

        It is used by the uniqueness check to extract the pks for the given value

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
        start, end = self.get_lex_boundaries(filter_type, value)
        members = self.connection.zrangebylex(key, start, end)
        if filter_type in ('lt', 'gt'):
            # special case where we don't want the exact given value, but we cannot
            # exclude it from the sorted set directly
            return [
                member_pk
                for member_value, member_pk in
                [self._extract_value_from_storage(member) for member in members]
                if member_value != value
            ]
        else:
            return [self._extract_value_from_storage(member)[-1] for member in members]

    def get_filtered_key(self, suffix, *args, **kwargs):
        """Returns the index key for the given args "value" (`args`)

        Parameters
        ----------
        kwargs: dict
            use_lua: bool
            Default to ``True``, if scripting is supported.
            If ``True``, the process of reading from the sorted-set, extracting
            the primary keys, excluding some values if needed, and putting the
            primary keys in a set or zset, is done in lua at the redis level.
            Else, data is fetched, manipulated here, then returned to redis.

        For the other parameters, see ``BaseIndex.get_filtered_key``

        """

        accepted_key_types = kwargs.get('accepted_key_types', None)

        if accepted_key_types\
                and 'set' not in accepted_key_types and 'zset' not in accepted_key_types:
            raise ImplementationError(
                '%s can only return keys of type "set" or "zset"' % self.__class__.__name__
            )

        use_lua = self.model.database.support_scripting() and kwargs.get('use_lua', True)

        key = self.get_storage_key(*args)
        tmp_key = unique_key(self.connection)
        key_type = 'set' if not accepted_key_types or 'set' in accepted_key_types else 'zset'
        value = self.normalize_value(list(args)[-1])

        if use_lua:
            start, end = self.get_lex_boundaries(suffix, value)
            self.model.database.call_script(
                # be sure to use the script dict at the class level
                # to avoid registering it many times
                script_dict=self.__class__.lua_filter_script,
                keys=[key, tmp_key],
                args=[key_type, self.separator, start, end,
                      value if suffix in {'lt', 'gt'} else None],
            )

        else:
            pks = self.get_pks_for_filter(key, suffix, value)
            if pks:
                if key_type == 'set':
                    self.connection.sadd(tmp_key, *pks)
                else:
                    self.connection.zadd(tmp_key, **{pk: idx for idx, pk in enumerate(pks)})

        return tmp_key, key_type, True
