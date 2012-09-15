# -*- coding:utf-8 -*-

from itertools import islice, chain

from limpyd.collection import CollectionManager
from limpyd.fields import SetField, ListField, SortedSetField, MultiValuesField, RedisField
from limpyd.contrib.database import PipelineDatabase


class ExtendedCollectionManager(CollectionManager):

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
        'zset_to_set': {
            # add all members of the zset in a new set
            'lua': """
                redis.call('del', KEYS[2])
                for i, member in ipairs(redis.call('zrange', KEYS[1], 0, -1)) do
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

    def _call_script(self, script_name, keys=[], args=[]):
        """
        Call the given script. The first time we call a script, we register it
        to speed up later calls. Registration is done on the class because it's
        independant of the instance (self) (redis-py will handle the case of
        different redis servers)
        """
        conn = self.cls.get_connection()
        script = self.__class__.scripts[script_name]
        if 'script_object' not in script:
            script['script_object'] = conn.register_script(script['lua'])
        return script['script_object'](keys=keys, args=args, client=conn)

    def _list_to_set(self, list_field, set_key):
        """
        Store all content of the given ListField in a redis set.
        Use scripting if available to avoid retrieving all values locally from
        the list before sending them back to the set
        """
        if self.cls.database.has_scripting():
            self._call_script('list_to_set', keys=[list_field.key, set_key])
        else:
            self.cls.get_connection().sadd(set_key, *list_field.lmembers())

    def _sortedset_to_set(self, sortedset_field, set_key):
        """
        Store all content of the given SortedSetField in a redis set.
        Use scripting if available to avoid retrieving all values locally from
        the sorted set before sending them back to the set
        """
        if self.cls.database.has_scripting():
            self._call_script('zset_to_set', keys=[sortedset_field.key, set_key])
        else:
            self.cls.get_connection().sadd(set_key, *sortedset_field.zmembers())

    def _prepare_sets(self, sets):
        """
        The original "_prepare_sets" method simple return the list of sets in
        _lazy_collection, know to be all keys of redis sets.
        As the new "intersect" method can accept different types of "set", we
        have to handle them because we must return only keys of redis sets.
        """
        conn = self.cls.get_connection()

        all_sets = set()
        tmp_keys = set()

        for set_ in sets:
            if isinstance(set_, basestring):
                all_sets.add(set_)
            elif isinstance(set_, SetField):
                # Use the set key. If we need to intersect, we'll use
                # sunionstore, and if not, store accepts set
                all_sets.add(set_.key)
            elif isinstance(set_, SortedSetField):
                # Use the sorted set key. If we need to intersect, we'll use
                # zinterstore, and if not, store accepts zset
                all_sets.add(set_.key)
            elif isinstance(set_, ListField):
                # convert the list to a simple redis set
                tmp_key = self._unique_key()
                self._list_to_set(set_, tmp_key)
                tmp_keys.add(tmp_key)
                all_sets.add(tmp_key)
            elif isinstance(set_, tuple) and len(set_):
                # if we got a list or set, create a redis set to hold its values
                tmp_key = self._unique_key()
                conn.sadd(tmp_key, *set_)
                tmp_keys.add(tmp_key)
                all_sets.add(tmp_key)

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
        - a string: considered as a redis set's name
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
            elif not isinstance(set_, (tuple, basestring, MultiValuesField)):
                raise ValueError('%s is not a valid type of argument that can '
                                 'be used as a set. Allowed are: string (key '
                                 'of a redis set), limpyd multi-values field ('
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
        if self._has_sortedsets and sort_options is None:
            return self.cls.get_connection().zrange(final_set, 0, -1)
        return super(ExtendedCollectionManager, self)._final_redis_call(
                                                        final_set, sort_options)

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
            elif not isinstance(by, basestring):
                by = None

            if by is None:
                raise ValueError('To sort by sorted set, you must pass a '
                                 'SortedSetFied (attached to a model) or a '
                                 'string representing the key of a redis zset '
                                 'to the `by_score` named argument')
            is_sortedset = True
            parameters['by'] = by

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
        for key in ('desc', 'alpha'):
            if key in self._sort_by_sortedset:
                sort_options[key] = self._sort_by_sortedset[key]

        return base_tmp_key, tmp_keys

    def _prepare_results(self, results):
        """
        Sort results by score if not done before (faster, if we have no values to
        retrieve, or slice)
        """
        # if we want a result sorted by a score, and if we have a full result
        # (no slice or values), we can do it know, by creating keys for each 
        # values with the sorted set score, and sort on them
        if self._sort_by_sortedset and not (self._slice or self._values) and len(results) > 1:
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

        return results

    def _get_final_set(self, sets, pk, sort_options):
        """
        Add intersects fo sets and call parent's _get_final_set.
        If we have to sort by sorted score, and we have a slice or want values,
        we have to convert the whole sorted set to keys now.
        """
        if self._lazy_collection['intersects']:
            # if the intersect method was called, we had new sets to intersect
            # to the global set of sets.
            # And it there is no real filters, we had the set of the whole
            # collection because we cannot be sure that entries in "intersects"
            # are all real primary keys
            sets = sets.copy()
            sets.update(self._lazy_collection['intersects'])
            if not self._lazy_collection['sets']:
                sets.add(self.cls._redis_attr_pk.collection_key)

        final_set, keys_to_delete_later = super(ExtendedCollectionManager,
                                    self)._get_final_set(sets, pk, sort_options)

        # if we have a slice and we want to sort by the score of a sorted set,
        # as redis sort command doesn't handle this, we have to create keys for
        # each value of the sorted set and sort on them
        # @antirez, y u don't allow this !!??!!
        if self._sort_by_sortedset and (self._slice or self._values):
            # TODO: if we have filters, maybe apply _zet_to_keys to only
            #       intersected values
            base_tmp_key, tmp_keys = self._prepare_sort_by_score(None, sort_options)
            # new keys have to be deleted once the final sort is done
            if not keys_to_delete_later:
                keys_to_delete_later = []
            keys_to_delete_later.append(base_tmp_key)
            keys_to_delete_later += tmp_keys

        return final_set, keys_to_delete_later
