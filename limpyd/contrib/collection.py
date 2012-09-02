# -*- coding:utf-8 -*-

from limpyd.collection import CollectionManager
from limpyd.fields import SetField, MultiValuesField


class ExtendedCollectionManager(CollectionManager):

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
        """
        sets_ = set()
        for set_ in sets:
            if isinstance(set_, (list, set)):
                set_ = tuple(set_)
            elif isinstance(set_, SetField) and not getattr(set_, '_instance', None):
                raise ValueError('SetField passed to "intersect" must be bound')
            elif not isinstance(set_, (tuple, basestring, SetField)):
                raise ValueError('%s is not a valid type of argument that can '
                                 'be used as a set. Allowed are: string (key '
                                 'of a redis set), limpyd SetField, or real '
                                 'real python set, list or tuple' % set_)
            sets_.add(set_)
        self._lazy_collection['intersects'].update(sets_)
        return self
