# -*- coding:utf-8 -*-

from limpyd.collection import CollectionManager
from limpyd.fields import SetField, ListField, MultiValuesField


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

    @property
    def _collection(self):
        if self._lazy_collection['intersects']:
            # if the intersect method was called, we had new sets to intersect
            # to the global set of sets, and we had the set of the whole
            # collection because we cannot be sure that entries in "intersects"
            # are real primary keys
            self._lazy_collection['sets'].update(self._lazy_collection['intersects'])
            self._lazy_collection['sets'].add(self.cls._redis_attr_pk.collection_key)

        return super(ExtendedCollectionManager, self)._collection

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
        return script['script_object'](
                                       keys=keys,
                                       args=args,
                                       client=conn)

    def _prepare_sets(self):
        """
        The original "_prepare_sets" method simple return the list of sets in
        _lazy_collection, know to be all keys of redis sets.
        As the new "intersect" method can accept different types of "set", we
        have to handle them because we must return only keys of redis sets.
        """
        conn = self.cls.get_connection()

        sets = set()
        tmp_keys = set()

        iter_sets = self._lazy_collection['sets']

        for set_ in iter_sets:
            if isinstance(set_, basestring):
                sets.add(set_)
            elif isinstance(set_, SetField):
                sets.add(set_.key)
            elif isinstance(set_, MultiValuesField):
                # list or sorted set: convert it to a simple redis set
                tmp_key = self._unique_key()
                if self.cls.database.has_scripting():
                    # if we have scripting enabled (both redis.py and server)
                    # use eval to create the set atomically without retrieving
                    # all content on our side
                    if isinstance(set_, ListField):
                        script_name = 'list_to_set'
                    else:
                        script_name = 'zset_to_set'
                    self._call_script(script_name, keys=[set_.key, tmp_key])
                else:
                    # no scripting, we have to fetch all values in the list/zset
                    # and then ut them back in a redis set.
                    members = set_.proxy_get()
                    conn.sadd(tmp_key, *members)
                tmp_keys.add(tmp_key)
                sets.add(tmp_key)
            elif isinstance(set_, tuple) and len(set_):
                # if we got a list or set, create a redis set to hold its values
                tmp_key = self._unique_key()
                conn.sadd(tmp_key, *set_)
                tmp_keys.add(tmp_key)
                sets.add(tmp_key)

        return sets, tmp_keys

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
            sets_.add(set_)
        self._lazy_collection['intersects'].update(sets_)
        return self
