# -*- coding:utf-8 -*-
from __future__ import unicode_literals


from future.builtins import object
from collections import namedtuple
from copy import copy
from itertools import product
from operator import itemgetter

from limpyd.utils import make_key, unique_key
from limpyd.exceptions import *
from limpyd.fields import SingleValueField

ParsedFilter = namedtuple('ParsedFilter', ['index', 'suffix', 'extra_field_parts', 'value', 'related_filters'])


NONE_SLICE = slice(None, None, None)


class CollectionResults(object):
    def __init__(self, data, func=None):
        self.data = data
        self.func = func
        if func:
            self._get_entry = self._get_func_entry
            self._iter_all_entries = self._iter_func_all_entries
            self._iter_some_entries = self._iter_func_some_entries
        else:
            self._get_entry = self._get_direct_entry
            self._iter_all_entries = self._iter_direct_all_entries
            self._iter_some_entries = self._iter_direct_some_entries
        self._index = -1
        self.length = len(data)

    def __len__(self):
        return self.length

    def __bool__(self):
        return bool(self.length)

    def _get_direct_entry(self, index):
        return self.data[index]

    def _get_func_entry(self, index):
        return self.func(self.data[index])

    def _iter_direct_some_entries(self, data):
        return iter(data)

    def _iter_func_some_entries(self, data):
        for entry in data:
            try:
                yield self.func(entry)
            except DoesNotExist:
                continue

    def _iter_direct_all_entries(self):
        return iter(self.data)

    def _iter_func_all_entries(self):
        for entry in self.data:
            try:
                yield self.func(entry)
            except DoesNotExist:
                continue

    def __next__(self):
        if self._index >= self.length - 1:
            raise StopIteration()
        self._index += 1
        try:
            return self._get_entry(self._index)
        except DoesNotExist:
            return self.__next__()

    def __iter__(self):
        self._index = -1
        return self._iter_all_entries()
    next = __next__

    def __eq__(self, other):
        if isinstance(other, list):
            return list(self) == other
        if isinstance(other, set):
            return set(self) == other
        if isinstance(other, self.__class__):
            return self.data == other.data
        return super(CollectionResults, self).__eq__(other)

    def __getitem__(self, arg):
        if not isinstance(arg, (int, slice)):
            raise TypeError
        if isinstance(arg, slice):
            return list(self._iter_some_entries(self.data[arg]))
        return self._get_entry(arg)

    MAX_REPR_ITEMS = 20

    def __repr__(self):
        data = self[:self.MAX_REPR_ITEMS + 1]
        if len(data) > self.MAX_REPR_ITEMS:
            data[-1] = "... (%s remaining elements truncated)..." % (len(self) - self.MAX_REPR_ITEMS)
        return '<%s %r>' % (self.__class__.__name__, data)


class CollectionManager(object):
    """
    Retrieve objects collection, optionnaly slice and order it.
    CollectionManager is lazy: it will call redis only when evaluating it
    (iterating, slicing, forcing as list...)

    API:
    MyModel.collection(**filters) => return the whole collection, eventually filtered.
    MyModel.collection().sort(by='field') => return the collection sorted.
    MyModel.collection().sort(by='field')[:10] => slice the sorted collection.
    MyModel.collection().instances() => return the instances

    Note:
    Slicing a collection will force a sort.
    """

    _accepted_key_types = {'set'}  # Type of keys indexes are allowed to return

    # time between a first call to __len__ followed by a collection retrieval
    FINAL_SET_TTL = 300

    def __init__(self, model):
        self.model = model
        self._lazy_collection = {  # Store infos to make the requested collection
            'sets': [],  # store sets to use (we'll intersect them)
            'pks': set(),  # store special filter on pk
        }
        self._instances = False  # True when instances are asked
                                 # instead of raw pks
        self._lazy_instances = False  # If True will return instances
                                      # without testing if pk exist
        self._sort = None  # Will store sorting parameters
        self._sort_limits = None  # Will store slice parameters (start and num)
        self._len = None  # Store the result of the final collection, to avoid
                          # having to compute the whole thing twice when doing
                          # `list(Model.collection)` (a `list` will call
                          # __iter__ AND __len__)
        self._len_mode = True   # Set to True if the __len__ method is directly
                                # called (to avoid some useless computations)
                                # True by default to manage the __iter__ + __len__
                                # case, specifically set to False in other cases

        self._collection_cache = None  # will hold the result extracted from redis
        self._cache_iterator_function = None  # function to apply to cached reults to get
                                              # instances, values dicts.

        self._final_set = None  # when __len__ alone is called, the final set is computed and
                                # its key stored here, to have fast access during FINAL_SET_TTL
                                # seconds if followed by a collection retrieval
        self._final_set_deletable = False  # if the key stored in _final_set must have an expire
                                           # applied, and deleted after the collection retrieval is
                                           # done

    @property
    def connection(self):
        return self.model.get_connection()

    def clone(self):
        new = self.__class__(self.model)
        new._lazy_collection = {key: copy(value) for key, value in self._lazy_collection.items()} if self._lazy_collection is not None else None
        new._instances = self._instances
        new._lazy_instances = self._lazy_instances
        new._sort = self._sort.copy() if self._sort is not None else None
        new._sort_limits = self._sort_limits.copy() if self._sort_limits is not None else None
        new._len = self._len
        new._len_mode = self._len_mode
        new._collection_cache = None
        new._cache_iterator_function = None
        new._final_set = None
        new._final_set_deletable = False
        return new

    def _get_from_results_cache(self, apply_slice=None):
        if apply_slice is not None and not isinstance(apply_slice, slice):
            if self._cache_iterator_function:
                return self._cache_iterator_function(self._collection_cache[apply_slice])
            return self._collection_cache[apply_slice]

        results = self._collection_cache[apply_slice] if apply_slice is not None else self._collection_cache
        return CollectionResults(results, self._cache_iterator_function)

    def __iter__(self):
        self._reset_if_sort_limits(True)
        self._len_mode = False
        self._fetch_collection()
        return self._get_from_results_cache()

    @staticmethod
    def _optimize_slice(the_slice, can_reverse):
        """

        Parameters
        ----------
        the_slice: slice
            The python slice to optimize
        can_reverse: bool
            If we can ask redis to reverse the data before slicing.

        Returns
        -------
        tuple
            Four elements:
            - optimized: bool
                If the slice is optimized
            - start: int or None
                start item to pass to the redis command
                If None, we want from the very start
            - count: int or None
                number of items to ask the redi command
                If None, we want everything from the start
            - rev: bool
                If we should ask redis to work the reverse way
            - python_slice
                The slice to be applied on the python side

        """
        step = 1 if the_slice.step in (1, None) else the_slice.step
        start = (the_slice.start or 0) if step > 0 else the_slice.start
        stop = the_slice.stop

        if start is not None and stop is not None:

            # cases where we know the result is always empty
            if step > 0 and (0 <= stop <= start or stop <= start < 0):
                return True, None, None, False, NONE_SLICE

            if step < 0 and (0 <= start <= stop or start <= stop < 0):
                return True, None, None, False, NONE_SLICE

            # if start and stop are on the same side, we can optimize

            # simplest case: start and stop are positive
            if start >= 0 and stop > 0:
                if step < 0:
                    start, stop = stop + 1, start + 1
                return True, start, stop - start, False, slice(None, None, step if step != 1 else None)

            # more complicated case: start and stop are negative
            if start < 0 and stop < 0 and can_reverse:
                if step < 0:
                    start, stop = stop + 1, start + 1
                step = -step
                return True, - stop, stop - start, True, slice(None, None, step if step != 1 else None)

        # for positive step, if we have a positive start, and no end or negative one, we have to retrieve all from
        # the start and slice here
        if step > 0 and (start is not None and start >= 0) and (stop is None or stop < 0):
            return True, start, None, False, slice(None, stop, step)

        # the reverse for negative step
        if step < 0 and (start is None or start < 0) and (stop is not None and stop >= 0):
            return True, stop + 1, None, False, slice(start, None, step)

        # in all other cases we recover the whole collection and slice it in python
        return False, None, None, False, slice(start, stop, step)

    def _getitem(self, arg):
        if self._collection_cache is not None:
            return self._get_from_results_cache(apply_slice=arg)

        self._len_mode = False
        self._sort_limits = {}
        if isinstance(arg, slice):
            # A slice has been requested
            # We try to reduce the data to get back from redis
            # when it's possible depending of the slice arguments.
            # We use `self._sort_limits` to tell redis how to slice what
            # it'll got from the `sort` command. We may also reverse
            # `_sort['desc']`(the set will be read in reverse way as we wanted
            # by redis).
            # At the end we return the collection (a sliced collection
            # is no more chainable, so we do not return `self`

            optimized, start, count, rev, python_slice = self._optimize_slice(
                arg,
                can_reverse=True if not self._sort else self._sort.get('by', '') != 'nosort'
            )

            self._optimized_slicing = optimized

            if optimized and start is None and count is None and python_slice == NONE_SLICE:
                return []

            if start is not None or count is not None:
                self._sort_limits['start'] = start or 0
                self._sort_limits['num'] = -1 if count is None else count

            if rev:
                if self._sort is None: self._sort = {}
                self._sort['desc'] = not self._sort.get('desc', False)

            self._fetch_collection(apply_slice=python_slice)
            return self._get_from_results_cache()

        else:
            # A single item has been requested
            # Nevertheless, use the redis pagination, to minimize
            # data transfer and use the fast redis offset system
            start = arg
            self._sort_limits['num'] = 1  # one element
            if start >= 0:
                self._sort_limits['start'] = start
                self._fetch_collection()
                return self._get_from_results_cache(apply_slice=0)
            else:
                # we sort the result in the reverse way, mark the final result as
                # reversed to re-reverse it at the end
                if self._sort is None: self._sort = {}
                self._sort['desc'] = not self._sort.get('desc', False)
                self._sort_limits['start'] = - start - 1
                self._fetch_collection()
                return self._get_from_results_cache(apply_slice=0)

    def __getitem__(self, arg):
        self._reset_if_sort_limits(True)
        if not isinstance(arg, (int, slice)):
            raise TypeError
        self._len_mode = False
        return self._getitem(arg)

    def _get_pk(self):
        """
        Return None if we don't have any filter on a pk, the pk if we have one,
        or raise a ValueError if we have more than one.
        For internal use only.
        """
        pk = None
        if self._lazy_collection['pks']:
            if len(self._lazy_collection['pks']) > 1:
                raise ValueError('Too much pks !')
            pk = list(self._lazy_collection['pks'])[0]
        return pk

    def _prepare_sort_options(self, has_pk):
        """
        Prepare "sort" options to use when calling the collection, depending
        on "_sort" and "_sort_limits" attributes
        """
        sort_options = {}
        if self._sort is not None and not has_pk:
            sort_options.update(self._sort)
        if self._sort_limits is not None:
            if 'start' in self._sort_limits and 'num' not in self._sort_limits:
                self._sort_limits['num'] = -1
            elif 'num' in self._sort_limits and 'start' not in self._sort_limits:
                self._sort_limits['start'] = 0
            sort_options.update(self._sort_limits)
        if not sort_options and self._sort is None:
            sort_options = None
        return sort_options

    def _cache_empty_collection(self):
        is_len_mode = self._len_mode
        self._len_mode = False
        self._collection_cache = []
        self._len = 0
        if is_len_mode:
            return
        return self._collection_cache

    def _reset_if_sort_limits(self, clear_sort_limits):
        if self._collection_cache is not None and self._sort_limits:
            self._collection_cache = self._cache_iterator_function = self._len = None
            if self._final_set and self._final_set_deletable:
                self.connection.delete(self._final_set)
            self._final_set, self._final_set_deletable = None, False
            if clear_sort_limits:
                self._sort_limits = None

    def _fetch_collection(self, apply_slice=None):
        """
        Effectively retrieve data according to lazy_collection.
        """

        self._reset_if_sort_limits(False)

        if self._collection_cache is not None:
            return

        conn = self.connection
        self._len = 0

        # The collection fails (empty) if more than one pk or if the only one
        # doesn't exists
        try:
            pk = self._get_pk()
        except ValueError:
            self._cache_empty_collection()
            return
        else:
            if pk is not None and not self.model.get_field('pk').exists(pk):
                self._cache_empty_collection()
                return

        # Prepare options and final set to get/sort
        sort_options = self._prepare_sort_options(bool(pk))

        # we call expire to have time to work on the key
        # expire returns 0 if the key does not exist
        if self._final_set and (not self._final_set_deletable or self.connection.expire(self._final_set, self.FINAL_SET_TTL)):
            final_set, delete_set_later = self._final_set, self._final_set_deletable
        else:
            self._final_set, self._final_set_deletable = None, False
            final_set, delete_set_later = self._get_final_set(
                                                self._lazy_collection['sets'],
                                                pk, sort_options)
        try:
            # fill the collection
            if final_set is None:
                if pk and not self._lazy_collection['sets']:
                    # we have a pk without other sets
                    if self._len_mode:
                        self._len = 1
                        return
                    # and no needs to get values so we can simply return the pk
                    collection = {pk}
                else:
                    # we have nothing
                    self._cache_empty_collection()
                    return
            else:
                if self._len_mode:
                    # compute the sets and call redis to count wanted values
                    self._len = self._collection_length(final_set)
                    self._final_set = final_set
                    self._final_set_deletable = delete_set_later
                    if delete_set_later:
                        self.connection.expire(self._final_set, self.FINAL_SET_TTL)
                    return
                # compute the sets and call redis to retrieve wanted values
                collection = self._final_redis_call(final_set, sort_options)
        finally:
            if not self._len_mode and delete_set_later:
                conn.delete(final_set)

        # Format return values if needed
        self._collection_cache, self._cache_iterator_function = self._prepare_results(collection, apply_slice=apply_slice)
        self._len = len(self._collection_cache)

    def _final_redis_call(self, final_set, sort_options):
        """
        The final redis call to obtain the values to return from the "final_set"
        with some sort options.
        """

        conn = self.connection
        if sort_options is not None:
            # a sort, or values, call the SORT command on the set
            return conn.sort(final_set, **sort_options)
        else:
            # no sort, nor values, simply return the full set
            return conn.smembers(final_set)

    def _collection_length(self, final_set):
        """
        Return the length of the final collection, directly asking redis for the
        count without calling sort
        """
        return self.connection.scard(final_set)

    def _to_instance(self, pk):
        meth = self.model.lazy_connect if self._lazy_instances else self.model
        return meth(pk)

    def _prepare_results(self, results, _len_hint=None, apply_slice=None):
        """
        Called in _collection to prepare results from redis before returning
        them.
        """

        # cache the len for future use
        self._len = _len_hint if _len_hint is not None else len(results)

        results = list(results)

        if apply_slice is not None:
            results = results[apply_slice]

        return results, (self._to_instance if self._instances else None)

    def _prepare_parsed_filter(self, parsed_filter):
        """Get and validate keys info from given parsed_filter

        Parameters
        ----------
        parsed_filter : ParsedFilter
            The parsed filter for which to extract keys

        Yields
        -------
        Tuple[str, str, bool]
            One or many entries as given by the ``get_filtered_keys`` method of the index tied to
            the `parsed_filter`. See ``see BaseIndex.get_filtered_keys``.

        """
        for index_key, key_type, is_tmp in parsed_filter.index.get_filtered_keys(
                    parsed_filter.suffix,
                    accepted_key_types=self._accepted_key_types,
                    *(parsed_filter.extra_field_parts + [parsed_filter.value]),
                    related_filters=parsed_filter.related_filters
                ):
            if key_type not in self._accepted_key_types:
                raise ValueError('The index key returned by the index %s is not valid' % (
                    parsed_filter.index.__class__.__name__
                ))
            yield index_key, key_type, is_tmp

    def _prepare_sets(self, sets):
        """
        Return all sets in self._lazy_collection['sets'] to be ready to be used
        to intersect them. Called by _get_final_set, to use in subclasses.
        Must return a tuple with a set of redis set keys, and another with
        new temporary keys to drop at the end of _get_final_set
        """

        final_sets = set()
        tmp_keys = set()

        for set_ in self._reduce_related_filters(sets):
            if isinstance(set_, str):
                final_sets.add(set_)
            elif isinstance(set_, ParsedFilter):
                for index_key, key_type, is_tmp in self._prepare_parsed_filter(set_):
                    final_sets.add(index_key)
                    if is_tmp:
                        tmp_keys.add(index_key)
            else:
                raise ValueError('Invalid filter type')

        return final_sets, tmp_keys

    def _reduce_related_filters(self, sets):
        """Try to replace single fields filters by multi-fields ones

        Parameters
        ----------
        sets : List
            See ``_prepare_sets``

        Returns
        -------
            List
                List of updated "sets", with some ``ParsedFilter`` that may have been removed
                and new ones, handling many fields at once, added.

        """
        if not self.model._multi_fields_index_for_filtering:
            return sets

        parsed_filters, other_sets = [], []
        for set_ in sets:
            (parsed_filters if isinstance(set_, ParsedFilter) else other_sets).append(set_)

        fields_and_suffixes = [
            ((parsed_filter.index.field.name,  parsed_filter.suffix), parsed_filter)
            for parsed_filter in parsed_filters
        ]

        multi_handled = set()
        handled_together = {}
        for index in self.model._multi_fields_index_for_filtering:
            handled_fields_tuples = index.can_filter_fields(list(map(itemgetter(0), fields_and_suffixes)))
            for handled_fields in handled_fields_tuples:
                if handled_fields in handled_together:
                    # group of fields already managed by an index
                    continue
                handled_together[handled_fields] = index
                multi_handled.update(handled_fields)

        many_filters = []
        if not handled_together:
            single_filters = parsed_filters
        else:
            single_filters = [
                parsed_filter for field_and_suffix, parsed_filter in fields_and_suffixes
                if field_and_suffix not in multi_handled
            ]

            handled_fields_sets = tuple(map(set, handled_together))
            for handled_fields, index in handled_together.items():

                # ignore the index if indexed fields is a subset of another index we have
                handled_fields_set = set(handled_fields)
                if any(handled_fields_set < other_handled_fields_set for other_handled_fields_set in handled_fields_sets):
                    continue

                groups = []
                for field_name, suffix in handled_fields:
                    group = []
                    for field_and_suffix, parsed_filter in fields_and_suffixes:
                        if (field_name, suffix) == field_and_suffix:
                            group.append(parsed_filter)
                    groups.append(group)
                for filters in product(*groups):
                    first_filter = filters[0]
                    many_filters.append(ParsedFilter(
                        index, first_filter.suffix, first_filter.extra_field_parts, first_filter.value, {
                            other_filter.index.field.name:
                                (other_filter.extra_field_parts + [other_filter.value], other_filter.suffix)
                            for other_filter in filters[1:]
                        }
                    ))

        # raise if we still have fields that cannot be filtered by themselves
        for parsed_filter in single_filters:
            if not parsed_filter.index.filter_single_field:
                field = parsed_filter.index.field
                key_parts = parsed_filter.extra_field_parts + [field.name]
                if parsed_filter.suffix:
                    key_parts.append(parsed_filter.suffix)
                raise ImplementationError(
                    'No index found to manage filter "%s" for field %s.%s' % (
                        '__'.join(key_parts), field._model.__name__, field.name
                    )
                )

        return many_filters + single_filters + other_sets

    def _get_final_set(self, sets, pk, sort_options):
        """
        Called by _collection to get the final set to work on. Return the name
        of the set to use, and a list of keys to delete once the collection is
        really called (in case of a computed set based on multiple ones)
        """
        conn = self.connection
        all_sets = set()
        tmp_keys = set()

        if pk is not None and not sets and not (sort_options and sort_options.get('get')):
            # no final set if only a pk without values to retrieve
            return None, False

        elif sets or pk:
            if sets:
                new_sets, new_tmp_keys = self._prepare_sets(sets)
                all_sets.update(new_sets)
                tmp_keys.update(new_tmp_keys)
            if pk is not None:
                # create a set with the pk to do intersection (and to pass it to
                # the store command to retrieve values if needed)
                tmp_key = self._unique_key('tmp')
                conn.sadd(tmp_key, pk)
                all_sets.add(tmp_key)
                tmp_keys.add(tmp_key)

        else:
            # no sets or pk, use the whole collection instead
            all_sets.add(self.model.get_field('pk').collection_key)

        if not all_sets:
            delete_set_later = False
            final_set = None
        elif len(all_sets) == 1:
            # if we have only one set, we  delete the set after calling
            # collection only if it's a temporary one, and we do not delete
            # it right now
            final_set = all_sets.pop()
            if final_set in tmp_keys:
                delete_set_later = True
                tmp_keys.remove(final_set)
            else:
                delete_set_later = False
        else:
            # more than one set, do an intersection on all of them in a new key
            # that will must be deleted once the collection is called.
            delete_set_later = True
            final_set = self._combine_sets(all_sets, self._unique_key('final'))

        if tmp_keys:
            conn.delete(*tmp_keys)

        # return the final set to work on, and a flag if we later need to delete it
        return final_set, delete_set_later

    def _combine_sets(self, sets, final_set):
        """
        Given a list of set, combine them to create the final set that will be
        used to make the final redis call.
        """
        self.connection.sinterstore(final_set, list(sets))
        return final_set

    def __call__(self, **filters):
        return self.clone()._add_filters(**filters)

    def _field_is_pk(self, field_name):
        """Check if the given name is the pk field, suffixed or not with "__eq" """
        if self.model._field_is_pk(field_name):
            return True
        if field_name.endswith('__eq') and self.model._field_is_pk(field_name[:-4]):
            return True
        return False

    def _parse_filter_key(self, key):
        # Each key can have optional subpath
        # We pass it as args to the field, which is responsable
        # from handling them
        # We only manage here the suffix handled by a filter

        key_path = key.split('__')
        field_name = key_path.pop(0)
        field = self.model.get_field(field_name)

        if not field.indexable:
            raise ImplementationError(
                'Field %s.%s is not indexable' % (
                    field._model.__name__, field.name
                )
            )

        other_field_parts = key_path[:field._field_parts - 1]

        if len(other_field_parts) + 1 != field._field_parts:
            raise ImplementationError(
                'Unexpected number of parts in filter %s for field %s.%s' % (
                    key, field._model.__name__, field.name
                )
            )

        rest = key_path[field._field_parts - 1:]
        index_suffix = None if not rest else '__'.join(rest)
        indexes = [index for index in field._indexes if index.can_handle_suffix(index_suffix)]
        # start by looking for a single-field index
        for index in indexes:
            if index.filter_single_field:
                index_to_use = index
                break
        else:
            # if not found, check for multi-fields index
            for index in indexes:
                if not index.filter_single_field:
                    index_to_use = index
                    break
            else:
                raise ImplementationError(
                    'No index found to manage filter "%s" for field %s.%s' % (
                        key, field._model.__name__, field.name
                    )
                )

        return index_to_use, index_suffix, other_field_parts

    def _add_filters(self, **filters):
        """Define self._lazy_collection according to filters."""
        for key, value in filters.items():
            if self._field_is_pk(key):
                pk = self.model.get_field('pk').normalize(value)
                self._lazy_collection['pks'].add(pk)
            else:
                # store the info to call the index later, in ``_prepare_sets``
                # (to avoid doing extra work if the collection is never called)
                index, suffix, extra_field_parts = self._parse_filter_key(key)
                parsed_filter = ParsedFilter(index, suffix, extra_field_parts, value, None)
                self._lazy_collection['sets'].append(parsed_filter)

        return self

    def __len__(self):
        self._reset_if_sort_limits(True)
        if self._len is None:
            self._len_mode = True
            self._fetch_collection()
        return self._len

    def __repr__(self):
        self._reset_if_sort_limits(True)
        self._len_mode = False
        self._fetch_collection()
        results = self._get_from_results_cache()
        return repr(results).replace('%s' % results.__class__.__name__, self.__class__.__name__, 1)

    def __bool__(self):
        return bool(len(self))

    def __eq__(self, other):
        self._reset_if_sort_limits(True)
        self._len_mode = False
        self._fetch_collection()
        return self._get_from_results_cache() == other

    def instances(self, lazy=False):
        """
        Ask the collection to return a list of instances.
        If lazy is set to True, the instances returned by the
        collection won't have their primary key checked for existence.
        """
        clone = self.clone()
        clone._reset_result_type()
        clone._instances = True
        clone._lazy_instances = lazy
        return clone

    def _get_simple_fields(self):
        """
        Return a list of the names of all fields that handle simple values
        (StringField or InstanceHashField), that redis can use to return values via
        the sort command
        """
        return [
            field.name for field in self.model.get_fields()
            if isinstance(field, SingleValueField)
        ]

    def primary_keys(self):
        """
        Ask the collection to return a list of primary keys. It's the default
        but if `instances`, `values` or `values_list` was previously called,
        a call to `primary_keys` restore this default behaviour.
        """
        clone = self.clone()
        clone._reset_result_type()
        return clone

    def _reset_result_type(self):
        """
        Reset the type of values attened for the collection (ie cancel a
        previous "instances" call)
        """
        self._instances = False

    def _coerce_by_parameter(self, parameters):
        if "by" in parameters:
            by = parameters['by']
            # Manage desc option
            if by.startswith('-'):
                parameters['desc'] = True
                by = by[1:]
            if self.model._field_is_pk(by):
                # don't use field, the final set, which contains pks, will be sorted directly
                del parameters['by']
            elif self.model.has_field(by):
                # if we have a field, use its redis wildcard form
                field = self.model.get_field(by)
                parameters['by'] = field.sort_wildcard
        return parameters

    def _apply_sort(self, **parameters):
        """
        Parameters:
        `by`: pass either a field name or a wildcard string to sort on
              prefix with `-` to make a desc sort.
        `alpha`: set it to True to sort lexicographically instead of numerically.
        """
        parameters = self._coerce_by_parameter(parameters)
        self._sort = parameters
        return self

    def sort(self, **parameters):
        return self.clone()._apply_sort(**parameters)

    def _unique_key(self, prefix=None):
        """
        Create a unique key.
        """
        prefix_parts = [self.model._name, '__collection__']
        if prefix:
            prefix_parts.append(prefix)
        return unique_key(
            self.connection,
            prefix=make_key(*prefix_parts)
        )
