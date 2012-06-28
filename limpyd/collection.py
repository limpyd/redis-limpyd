# -*- coding:utf-8 -*-

from logging import getLogger


class CollectionManager(object):
    """
    Retrieve objects collection, optionnaly slice and order it.

    ..API:
        MyModel.collection() => return the whole collection.
        MyModel.collection().sort('field') => return the collection sorted.
        MyModel.collection().sort('field')[:10] => slice the sorted collection.
        MyModel.collection().instances() => return the instances

    ..note:
        only sorted collection are sliceable.
    """

    def __init__(self, cls):
        self.cls = cls
        # lazy_collection could be:
        # - None => means not populated
        # - a set() => means we already have the pks
        # - one or more keys => we have to get or intersect the set in redis
        self._lazy_collection = None
        self._instances = False  # True when instances are asked
                                 # instead of raw pks

    def __iter__(self):
        for pk in self._collection:
            yield self.cls(pk) if self._instances else pk

    @property
    def _collection(self):
        """
        Effectively retrieve data according to lazy_collection.
        """
        conn = self.cls.get_connection()
        collection = set()
        if isinstance(self._lazy_collection, dict):
            if "key" in self._lazy_collection:
                collection = conn.smembers(self._lazy_collection['key'])
            elif "keys" in self._lazy_collection:
                collection = conn.sinter(self._lazy_collection['keys'])
        elif isinstance(self._lazy_collection, set):
            collection = self._lazy_collection
        return list(collection)

    def __call__(self, **filters):
        """
        Define self._collection according to filters
        """
        # FIXME should we really implement the pk + filters option?
        # It could be cleaner to leave this kind of specific usage to the
        # implementer of the lib
        # FIXME review the whole algo, it lacks readability

        query_fields = filters.copy()

        # Some consistency check
        pk_fields = [k for k in filters.keys() if self.cls._field_is_pk(k)]
        if len(pk_fields) > 1:
            raise ValueError("You must use only one pk field in filtering")

        # --- No filters, return the whole collection
        if not query_fields:
            # No pk, no other kwargs, return all the collection
            self._lazy_collection = {
                "key": self.cls._redis_attr_pk.collection_key
            }


        # --- There is a pk in the filters
        #     Get the object, and check if requested filters match object
        #     values
        elif pk_fields:
            field_name = pk_fields[0]
            value = filters[field_name]
            query_fields.pop(field_name)
            try:
                # try to get the object
                obj = self.cls(value)
            except ValueError:  # FIXME use DoesNotExist
                # A non existing pk = empty result
                self._lazy_collection = set()
            else:
                # Existing object, check all fields
                fail = False
                if query_fields:
                    for obj_field_name, obj_value in query_fields.iteritems():
                        field = getattr(obj, obj_field_name)
                        if field.proxy_get() != obj_value:
                            # Some asked field value differs from the object
                            # Nothing can be returned
                            self._lazy_collection = set()
                            fail = True
                            break
                if not fail:
                    self._lazy_collection = set([obj.pk.normalize(value)])

        # --- Filters
        else:
            # Prepare a list of sets for each query parameter
            index_keys = []
            for field_name, value in query_fields.iteritems():
                field = getattr(self.cls, "_redis_attr_%s" % field_name)
                index_keys.append(field.index_key(value))

            # Return intersection of all sets to get matching entries
            self._lazy_collection = {"keys": index_keys}
        return self

    def __len__(self):
        return self._collection.__len__()

    def __repr__(self):
        return self._collection.__repr__()

    def pop(self):
        return self._collection.pop()

    def instances(self):
        self._instances = True
        return self