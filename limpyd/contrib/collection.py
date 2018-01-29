# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import str
from future.builtins import zip
from future.builtins import object

from itertools import islice, chain
from collections import namedtuple
from copy import deepcopy

from limpyd.model import RedisModel
from limpyd.collection import CollectionManager, ParsedFilter
from limpyd.fields import (SetField, ListField, SortedSetField, MultiValuesField,
                           RedisField, SingleValueField)
from limpyd.exceptions import DoesNotExist
from limpyd.contrib.database import PipelineDatabase

SORTED_SCORE = 'sorted_score'
DEFAULT_STORE_TTL = 60


RawFilter = namedtuple('RawFilter', ['name', 'value'])


class ExtendedCollectionManager(CollectionManager):

    _accepted_key_types = {'set', 'zset', 'list'}  # Type of keys indexes are allowed to return

    scripts = {
        'list_to_set': {
            # add all members of the list in a new set
            'lua': """
                redis.call('del', KEYS[2])
                for i, member in ipairs(redis.call('lrange', KEYS[1], 0, -1)) do
                    redis.call('sadd', KEYS[2], member)
                end
                return 1
            """,
        },
    }

    def __init__(self, cls):
        super(ExtendedCollectionManager, self).__init__(cls)

        self._lazy_collection['intersects'] = set()

        self._has_sortedsets = False
        self._sort_by_sortedset = None

        self._store = False
        self.stored_key = False
        self._stored_len = None

        self._values = None  # Will store parameters used to retrieve values

    def _list_to_set(self, list_key, set_key):
        """
        Store all content of the given ListField in a redis set.
        Use scripting if available to avoid retrieving all values locally from
        the list before sending them back to the set
        """
        if self.cls.database.support_scripting():
            self.cls.database.call_script(
                # be sure to use the script dict at the class level
                # to avoid registering it many times
                script_dict=self.__class__.scripts['list_to_set'],
                keys=[list_key, set_key]
            )
        else:
            conn = self.cls.get_connection()
            conn.sadd(set_key, *conn.lrange(list_key, 0, -1))

    @property
    def _collection(self):
        """
        Effectively retrieve data according to lazy_collection.
        If we have a stored collection, without any result, return an empty list
        """
        old_sort_limits_and_len_mode = None if self._sort_limits is None else self._sort_limits.copy(), self._len_mode
        old_sorts = None if self._sort is None else self._sort.copy(),\
                    None if self._sort_by_sortedset is None else self._sort_by_sortedset.copy()
        try:
            if self.stored_key and not self._stored_len:
                if self._len_mode:
                    self._len = 0
                    self._len_mode = False
                self._sort_limits = {}
                return []

            # Manage sort desc added by original `__getitem__` when we sort by score
            if self._sort_by_sortedset and self._sort and self._sort.get('desc'):
                self._sort = None
                self._sort_by_sortedset['desc'] = not self._sort_by_sortedset.get('desc', False)

            return super(ExtendedCollectionManager, self)._collection
        finally:
            self._sort_limits, self._len_mode = old_sort_limits_and_len_mode
            self._sort, self._sort_by_sortedset = old_sorts

    def _prepare_sets(self, sets):
        """
        The original "_prepare_sets" method simple return the list of sets in
        _lazy_collection, know to be all keys of redis sets.
        As the new "intersect" method can accept different types of "set", we
        have to handle them because we must return only keys of redis sets.
        """

        if self.stored_key and not self.stored_key_exists():
            raise DoesNotExist('This collection is based on a previous one, '
                               'stored at a key that does not exist anymore.')

        conn = self.cls.get_connection()

        all_sets = set()
        tmp_keys = set()
        lists = []

        def add_key(key, key_type=None, is_tmp=False):
            if not key_type:
                key_type = conn.type(key)
            if key_type == 'set':
                all_sets.add(key)
            elif key_type == 'zset':
                all_sets.add(key)
                self._has_sortedsets = True
            elif key_type == 'list':
                # if only one list, and no sets, at the end we'll directly use the list
                # else lists will be converted to sets
                lists.append(key)
            elif key_type == 'none':
                # considered as an empty set
                 all_sets.add(key)
            else:
                raise ValueError('Cannot use redis key %s of type %s for filtering' % (
                    key, key_type
                ))
            if is_tmp:
                tmp_keys.add(key)

        for set_ in sets:
            if isinstance(set_, str):
                add_key(set_)
            elif isinstance(set_, ParsedFilter):

                value = set_.value
                # We have a RedisModel and we'll use its pk, or a RedisField
                # (single value) and we'll use its value
                if isinstance(value, RedisModel):
                    value = value.pk.get()
                elif isinstance(value, SingleValueField):
                    value = value.proxy_get()
                elif isinstance(value, RedisField):
                    raise ValueError(u'Invalid filter value for %s: %s' % (set_.index.field.name, value))

                for index_key, key_type, is_tmp in set_.index.get_filtered_keys(
                            set_.suffix,
                            accepted_key_types=self._accepted_key_types,
                            *(set_.extra_field_parts + [value])
                        ):
                    if key_type not in self._accepted_key_types:
                        raise ValueError('The index key returned by the index %s is not valid' % (
                            set_.index.__class__.__name__
                        ))
                    add_key(index_key, key_type, is_tmp)

            elif isinstance(set_, SetField):
                # Use the set key. If we need to intersect, we'll use
                # sunionstore, and if not, store accepts set
                add_key(set_.key, 'set')
            elif isinstance(set_, SortedSetField):
                # Use the sorted set key. If we need to intersect, we'll use
                # zinterstore, and if not, store accepts zset
                add_key(set_.key, 'zset')
            elif isinstance(set_, (ListField, _StoredCollection)):
                add_key(set_.key, 'list')
            elif isinstance(set_, tuple) and len(set_):
                # if we got a list or set, create a redis set to hold its values
                tmp_key = self._unique_key()
                conn.sadd(tmp_key, *set_)
                add_key(tmp_key, 'set', True)
            else:
                raise ValueError('Invalid filter type')

        if lists:
            if not len(all_sets) and len(lists) == 1:
                # only one list, nothing else, we can return the list key
                all_sets = {lists[0]}
            else:
                # we have many sets/lists, we need to convert them to sets
                for list_key in lists:
                    # many sets, convert the list to a simple redis set
                    tmp_key = self._unique_key()
                    self._list_to_set(list_key, tmp_key)
                    add_key(tmp_key, 'set', True)

        return all_sets, tmp_keys

    def filter(self, **filters):
        """
        Add more filters to the collection
        """
        return self._add_filters(**filters)

    def intersect(self, *sets):
        """
        Add a list of sets to the existing list of sets to check. Returns self
        for chaining.
        Each "set" represent a list of pk, the final goal is to return only pks
        matching the intersection of all sets.
        A "set" can be:
        - a string: considered as the name of a redis set, sorted set or list
            (if a list, values will be stored in a temporary set)
        - a list, set or tuple: values will be stored in a temporary set
        - a SetField: we will directly use it's content on redis
        - a ListField or SortedSetField: values will be stored in a temporary
            set (except if we want a sort or values and it's the only "set" to
            use)
        """
        sets_ = set()
        for set_ in sets:
            if isinstance(set_, (list, set)):
                set_ = tuple(set_)
            elif isinstance(set_, MultiValuesField) and not getattr(set_, '_instance', None):
                raise ValueError('%s passed to "intersect" must be bound'
                                 % set_.__class__.__name__)
            elif not isinstance(set_, (tuple, str, MultiValuesField, _StoredCollection)):
                raise ValueError('%s is not a valid type of argument that can '
                                 'be used as a set. Allowed are: string (key '
                                 'of a redis set, sorted set or list), '
                                 'limpyd multi-values field ('
                                 'SetField, ListField or SortedSetField), or '
                                 'real python set, list or tuple' % set_)
            if isinstance(set_, SortedSetField):
                self._has_sortedsets = True
            sets_.add(set_)

        self._lazy_collection['intersects'].update(sets_)
        return self

    def _combine_sets(self, sets, final_set):
        """
        Given a list of set, combine them to create the final set that will be
        used to make the final redis call.
        If we have a least a sorted set, use zinterstore insted of sunionstore
        """
        if self._has_sortedsets:
            self.cls.get_connection().zinterstore(final_set, list(sets))
        else:
            final_set = super(ExtendedCollectionManager, self)._combine_sets(sets, final_set)
        return final_set

    def _final_redis_call(self, final_set, sort_options):
        """
        The final redis call to obtain the values to return from the "final_set"
        with some sort options.
        IIf we have at leaset a sorted set and if we don't have any sort
        options, call zrange on the final set wich is the result of a call to
        zinterstore.
        """
        conn = self.cls.get_connection()

        # we have a sorted set without need to sort, use zrange
        if self._has_sortedsets and sort_options is None:

            return conn.zrange(final_set, 0, -1)

        # we have a stored collection, without other filter, and no need to
        # sort, use lrange
        if self.stored_key and not self._lazy_collection['sets']\
                and len(self._lazy_collection['intersects']) == 1\
                and (sort_options is None or sort_options == {'by': 'nosort'}):

            return conn.lrange(final_set, 0, -1)

        # normal call
        return super(ExtendedCollectionManager, self)._final_redis_call(
                                                        final_set, sort_options)

    def _collection_length(self, final_set):
        """
        Return the length of the final collection, directly asking redis for the
        count without calling sort
        """
        conn = self.cls.get_connection()

        # we have a sorted set without need to sort, use zcard
        if self._has_sortedsets:
            return conn.zcard(final_set)

        # we have a stored collection, without other filter, use llen
        elif self.stored_key and not self._lazy_collection['sets']\
                and len(self._lazy_collection['intersects']) == 1:

            return conn.llen(final_set)

        # normal call
        return super(ExtendedCollectionManager, self)._collection_length(final_set)

    def sort(self, **parameters):
        """
        Enhance the default sort method to accept a new parameter "by_score", to
        use instead of "by" if you want to sort by the score of a sorted set.
        You must pass to "by_sort" the key of a redis sorted set (or a
        sortedSetField attached to an instance)
        """
        self._sort_by_sortedset = None
        is_sortedset = False
        if parameters.get('by_score'):
            if parameters.get('by'):
                raise ValueError("You can't use `by` and `by_score` in the same "
                                 "call to `sort`.")
            by = parameters.get('by_score', None)
            if isinstance(by, SortedSetField) and getattr(by, '_instance', None):
                by = by.key
            elif not isinstance(by, str):
                by = None

            if by is None:
                raise ValueError('To sort by sorted set, you must pass a '
                                 'SortedSetFied (attached to a model) or a '
                                 'string representing the key of a redis zset '
                                 'to the `by_score` named argument')
            is_sortedset = True
            parameters['by'] = by
            del parameters['by_score']

        else:
            # allow passing a field, not only a field name
            by = parameters.get('by')
            if by and isinstance(by, RedisField):
                parameters['by'] = by.name

        super(ExtendedCollectionManager, self).sort(**parameters)

        if is_sortedset:
            self._sort_by_sortedset = self._sort
            self._sort = None

        return self

    def _zset_to_keys(self, key, values=None, alpha=False):
        """
        Convert a redis sorted set to a list of keys, to be used by sort.
        Each key is on the following format, for each value in the sorted set:
            ramdom_string:value-in-the-sorted-set => score-of-the-value
        The random string is the same for all keys.
        If values is not None, only these values from the sorted set are saved
        as keys.
        If a value in values is not on the sorted set, it's still saved as a key
        but with a default value ('' is alpha is True, else '-inf')
        """
        conn = self.cls.get_connection()
        default = '' if alpha else '-inf'
        if values is None:
            # no values given, we get scores from the whole sorted set
            result = conn.zrange(key, start=0, end=-1, withscores=True)
            values = list(islice(chain.from_iterable(result), 0, None, 2))
        else:
            # we have values, we'll get only their scores

            if isinstance(self.cls.database, PipelineDatabase):
                # if available, use the pipeline of our database to get all
                # scores in one redis call
                with self.cls.database.pipeline(transaction=False) as pipe:
                    for value in values:
                        pipe.zscore(key, value)
                    scores = pipe.execute()
            else:
                # no pipeline, we have to do a call for each value
                scores = []
                for value in values:
                    scores.append(conn.zscore(key, value))

            # combine values and scores in one list
            result = []
            for index, value in enumerate(values):
                score = scores[index]
                if score is None:
                    score = default
                result.append((value, score))

        # create a temporary key for each (value,score) tuple
        base_tmp_key = self._unique_key()
        conn.set(base_tmp_key, 'working...')  # only to "reserve" the main tmp key
        tmp_keys = []
        # use a mapping dict (tmp_key_with_value=>score) to use in mset
        mapping = {}
        for value, score in result:
            tmp_key = '%s:%s' % (base_tmp_key, value)
            tmp_keys.append(tmp_key)
            mapping[tmp_key] = score
        # set all keys in one call
        conn.mset(mapping)

        return base_tmp_key, tmp_keys

    def _prepare_sort_by_score(self, values, sort_options):
        """
        Create the key to sort on the sorted set references in
        self._sort_by_sortedset and adapte sort options
        """
        # create the keys
        base_tmp_key, tmp_keys = self._zset_to_keys(
                                    key=self._sort_by_sortedset['by'],
                                    values=values,
                                    )
        # ask to sort on our new keys
        sort_options['by'] = '%s:*' % base_tmp_key

        # retrieve original sort parameters
        for key in ('desc', 'alpha', 'get', 'store'):
            if key in self._sort_by_sortedset:
                sort_options[key] = self._sort_by_sortedset[key]

        # if we want to get the score with values/values_list
        if sort_options.get('get'):
            try:
                pos = sort_options['get'].index(SORTED_SCORE)
            except:
                pass
            else:
                sort_options['get'][pos] = '%s:*' % base_tmp_key

        return base_tmp_key, tmp_keys

    def _prepare_results(self, results):
        """
        Sort results by score if not done before (faster, if we have no values to
        retrieve, or slice)
        """
        # if we want a result sorted by a score, and if we have a full result
        # (no slice), we can do it know, by creating keys for each values with
        # the sorted set score, and sort on them
        if self._sort_by_sortedset_after and (len(results) > 1 or self._values):
            conn = self.cls.get_connection()

            sort_params = {}
            base_tmp_key, tmp_keys = self._prepare_sort_by_score(results, sort_params)

            # compose the set to sort
            final_set = '%s_final_set' % base_tmp_key
            conn.sadd(final_set, *results)

            # apply the sort
            results = conn.sort(final_set, **sort_params)

            # finally delete all temporary keys
            conn.delete(*(tmp_keys + [final_set, base_tmp_key]))

        if self._store:
            # if store, redis doesn't return result, so don't return anything here
            return

        if self._values and self._values['mode'] != 'flat':
            results = self._to_values(results)

        return super(ExtendedCollectionManager, self)._prepare_results(results)

    def _to_values(self, collection):
        """
        Regroup values in tuples or dicts for each "instance".
        Exemple: Given this result from redis: ['id1', 'name1', 'id2', 'name2']
         tuples: [('id1', 'name1'), ('id2', 'name2')]
         dicts:  [{'id': 'id1', 'name': 'name1'}, {'id': 'id2', 'name': 'name2'}]
        """
        result = zip(*([iter(collection)] * len(self._values['fields']['names'])))
        if self._values['mode'] == 'dicts':
            result = (dict(zip(self._values['fields']['names'], a_result)) for a_result in result)
        return result

    @property
    def _sort_by_sortedset_before(self):
        """
        Return True if we have to sort by set and do the stuff *before* asking
        redis for the sort
        """
        return self._sort_by_sortedset and self._sort_limits and (not self._lazy_collection['pks']
                                                            or self._want_score_value)

    @property
    def _sort_by_sortedset_after(self):
        """
        Return True if we have to sort by set and do the stuff *after* asking
        redis for the sort
        """
        return self._sort_by_sortedset and not self._sort_limits and (not self._lazy_collection['pks']
                                                                or self._want_score_value)

    @property
    def _want_score_value(self):
        """
        Return True if we want the score of the sorted set used to sort in the
        results from values/values_list
        """
        return self._values and SORTED_SCORE in self._values['fields']['names']

    def _prepare_sort_options(self, has_pk):
        """
        Prepare sort options for _values attributes.
        If we manager sort by score after getting the result, we do not want to
        get values from the first sort call, but only from the last one, after
        converting results in zset into keys
        """
        sort_options = super(ExtendedCollectionManager, self)._prepare_sort_options(has_pk)

        if self._values:
            # if we asked for values, we have to use the redis 'sort'
            # command, which is able to return other fields.
            if not sort_options:
                sort_options = {}
            sort_options['get'] = self._values['fields']['keys']

        if self._sort_by_sortedset_after:
            for key in ('get', 'store'):
                if key in self._sort_by_sortedset:
                    del self._sort_by_sortedset[key]
            if sort_options and (not has_pk or self._want_score_value):
                for key in ('get', 'store'):
                    if key in sort_options:
                        self._sort_by_sortedset[key] = sort_options.pop(key)
            if not sort_options:
                sort_options = None
        return sort_options

    def _get_final_set(self, sets, pk, sort_options):
        """
        Add intersects fo sets and call parent's _get_final_set.
        If we have to sort by sorted score, and we have a slice, we have to
        convert the whole sorted set to keys now.
        """
        if self._lazy_collection['intersects']:
            # if the intersect method was called, we had new sets to intersect
            # to the global set of sets.
            # And it there is no real filters, we had the set of the whole
            # collection because we cannot be sure that entries in "intersects"
            # are all real primary keys
            sets = sets[::]
            sets.extend(self._lazy_collection['intersects'])
            if not self._lazy_collection['sets'] and not self.stored_key:
                sets.append(self.cls.get_field('pk').collection_key)

        final_set, keys_to_delete_later = super(ExtendedCollectionManager,
                                    self)._get_final_set(sets, pk, sort_options)

        # if we have a slice and we want to sort by the score of a sorted set,
        # as redis sort command doesn't handle this, we have to create keys for
        # each value of the sorted set and sort on them
        # @antirez, y u don't allow this !!??!!
        if final_set and self._sort_by_sortedset_before:
            # TODO: if we have filters, maybe apply _zet_to_keys to only
            #       intersected values
            base_tmp_key, tmp_keys = self._prepare_sort_by_score(None, sort_options)
            # new keys have to be deleted once the final sort is done
            if not keys_to_delete_later:
                keys_to_delete_later = []
            keys_to_delete_later.append(base_tmp_key)
            keys_to_delete_later += tmp_keys

        return final_set, keys_to_delete_later

    def _add_filters(self, **filters):
        """
        In addition to the normal _add_filters, this one accept RedisField objects
        on the right part of a filter. The value will be fetched from redis when
        calling the collection.
        The filter value can also be a model instance, in which case its PK will
        be fetched when calling the collection, too.
        """
        string_filters = filters.copy()

        for key, value in filters.items():

            is_extended = False

            if isinstance(value, RedisField):
                # we will fetch the value when running the collection
                if (not isinstance(value, SingleValueField)
                    or getattr(value, '_instance', None) is None):
                    raise ValueError('If a field is used as a filter value, it '
                                     'must be a simple value field attached to '
                                     'an instance')
                is_extended = True

            elif isinstance(value, RedisModel):
                # we will fetch the PK when running the collection
                is_extended = True

            if is_extended:
                if self._field_is_pk(key):
                    # create an RawFilter which will be used in _get_pk
                    raw_filter = RawFilter(key, value)
                    self._lazy_collection['pks'].add(raw_filter)
                else:
                    # create an ParsedFilter which will be used in _prepare_sets
                    index, suffix, extra_field_parts = self._parse_filter_key(key)
                    parsed_filter = ParsedFilter(index, suffix, extra_field_parts, value)
                    self._lazy_collection['sets'].append(parsed_filter)

                string_filters.pop(key)

        super(ExtendedCollectionManager, self)._add_filters(**string_filters)

        return self

    def _get_pk(self):
        """
        Override the default _get_pk method to retrieve the real pk value if we
        have a SingleValueField or a RedisModel instead of a real PK value
        """
        pk = super(ExtendedCollectionManager, self)._get_pk()

        if pk is not None and isinstance(pk, RawFilter):
            # We have a RedisModel and we want its pk, or a RedisField
            # (single value) and we want its value
            if isinstance(pk.value, RedisModel):
                pk = pk.value.pk.get()
            elif isinstance(pk.value, SingleValueField):
                pk = pk.value.proxy_get()
            else:
                raise ValueError(u'Invalide filter value for a PK: %s' % pk.value)

        return pk

    def _coerce_fields_parameters(self, fields):
        """
        Used by values and values_list to get the list of fields to use in the
        redis sort command to retrieve fields.
        The result is a dict with two lists:
          - 'names', with wanted field names
          - 'keys', with keys to use in the sort command
        When sorting by score, we allow to retrieve the score in values/values_list.
        For this, just pass SORTED_SCORE (importable from contrib.collection) as
        a name to retrieve.
        If finally the result is not sorted by score, the value for this part
        will be None
        """
        try:
            sorted_score_pos = fields.index(SORTED_SCORE)
        except:
            sorted_score_pos = None
        else:
            fields = list(fields)
            fields.pop(sorted_score_pos)

        final_fields = {'names': [], 'keys': []}
        for field_name in fields:
            if self._field_is_pk(field_name):
                final_fields['names'].append(field_name)
                final_fields['keys'].append('#')
            else:
                if not self.cls.has_field(field_name):
                    raise ValueError("%s if not a valid field to get from collection"
                                     " for %s" % (field_name, self.cls.__name__))
                field = self.cls.get_field(field_name)
                if isinstance(field, MultiValuesField):
                    raise ValueError("It's not possible to get a MultiValuesField"
                                     " from a collection (asked: %s" % field_name)
                final_fields['names'].append(field_name)
                final_fields['keys'].append(field.sort_wildcard)

        if sorted_score_pos is not None:
            final_fields['names'].insert(sorted_score_pos, SORTED_SCORE)
            final_fields['keys'].insert(sorted_score_pos, SORTED_SCORE)

        return final_fields

    def store(self, key=None, ttl=DEFAULT_STORE_TTL):
        """
        Will call the collection and store the result in Redis, and return a new
        collection based on this stored result. Note that only primary keys are
        stored, ie calls to values/values_list are ignored when storing result.
        But choices about instances/values_list are transmited to the new
        collection.
        If no key is given, a new one will be generated.
        The ttl is the time redis will keep the new key. By default its
        DEFAULT_STORE_TTL, which is 60 secondes. You can pass None if you don't
        want expiration.
        """
        old_sort_limits_and_len_mode = None if self._sort_limits is None else self._sort_limits.copy(), self._len_mode
        try:
            self._store = True

            # save sort and values options
            sort_options = None
            if self._sort is not None:
                sort_options = self._sort.copy()
            values = None
            if self._values is not None:
                values = self._values
                self._values = None

            # create a key for storage
            store_key = key or self._unique_key()
            if self._sort is None:
                self._sort = {}
            self._sort['store'] = store_key

            # if filter by pk, but without need to get "values", no redis call is done
            # so force values to get a call to sort (to store result)
            if self._lazy_collection['pks'] and not self._values:
                self.values('pk')

            # call the collection
            self._len_mode = False
            self._collection

            # restore sort and values options
            self._store = False
            self._sort = sort_options
            self._values = values

            # create the new collection
            stored_collection = self.__class__(self.cls)
            stored_collection.from_stored(store_key)

            # apply ttl if needed
            if ttl is not None:
                self.cls.get_connection().expire(store_key, ttl)

            # set choices about instances/values from the current to the new collection
            for attr in ('_instances', '_instances_skip_exist_test', '_values'):
                setattr(stored_collection, attr, deepcopy(getattr(self, attr)))

            # finally return the new collection
            return stored_collection

        finally:
            self._sort_limits, self._len_mode = old_sort_limits_and_len_mode

    def from_stored(self, key):
        """
        Set the current collection as based on a stored one. The key argument
        is the key off the stored collection.
        """
        # only one stored key allowed
        if self.stored_key:
            raise ValueError('This collection is already based on a stored one')

        # prepare the collection
        self.stored_key = key
        self.intersect(_StoredCollection(self.cls.get_connection(), key))
        self.sort(by='nosort')  # keep stored order

        # count the number of results to manage empty result (to not behave like
        # expired key)
        self._stored_len = self.cls.get_connection().llen(key)

        return self

    def stored_key_exists(self):
        """
        Check the existence of the stored key (useful if the collection is based
        on a stored one, to check if the redis key still exists)
        """
        return self.cls.get_connection().exists(self.stored_key)

    def reset_result_type(self):
        """
        Reset the type of values attened for the collection (ie cancel a
        previous "instances" or "values" call)
        """
        self._values = None
        return super(ExtendedCollectionManager, self).reset_result_type()

    def values(self, *fields):
        """
        Ask the collection to return a list of dict of given fields for each
        instance found in the collection.
        If no fields are given, all "simple value" fields are used.
        """
        if not fields:
            fields = self._get_simple_fields()

        fields = self._coerce_fields_parameters(fields)

        self._instances = False
        self._values = {'fields': fields, 'mode': 'dicts'}
        return self

    def values_list(self, *fields, **kwargs):
        """
        Ask the collection to return a list of tuples of given fields (in the
        given order) for each instance found in the collection.
        If 'flat=True' is passed, the resulting list will be flat, ie without
        tuple. It's a valid kwarg only if only one field is given.
        If no fields are given, all "simple value" fields are used.
        """
        flat = kwargs.pop('flat', False)
        if kwargs:
            raise ValueError('Unexpected keyword arguments for the values method: %s'
                             % list(kwargs))

        if not fields:
            fields = self._get_simple_fields()

        if flat and len(fields) > 1:
            raise ValueError("'flat' is not valid when values is called with more than one field.")

        fields = self._coerce_fields_parameters(fields)

        self._instances = False
        self._values = {'fields': fields, 'mode': 'flat' if flat else 'tuples'}
        return self


class _StoredCollection(object):
    """
    Simple object to store the key of a stored collection, to be used in
    ExtendedCollectionManager based on a stored collection.
    The stored key is a list, so it's managed as a ListField (but we only need
    its key, and lmembers if no scripting)
    """
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key

    def lmembers(self):
        """
        Return the list of all members of the list, used by _list_to_set if
        no scripting
        """
        return self.connection.lrange(self.key, 0, -1)
