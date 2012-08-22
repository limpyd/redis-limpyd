# -*- coding:utf-8 -*-

from limpyd.utils import unique_key
from limpyd.exceptions import *


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

    def __init__(self, cls):
        self.cls = cls
        self._lazy_collection = None  # Store infos to make the requested
                                      # collection.
        self._instances = False  # True when instances are asked
                                 # instead of raw pks
        self._instances_skip_exist_test = False  # If True will return instances
                                                 # without testing if pk exist
        self._sort = None  # Will store sorting parameters
        self._len = None  # Store the result of the final collection, to avoid
                          # having to compute the whole thing twice when doing
                          # `list(Model.collection)` (a `list` will call
                          # __iter__ AND __len__)

    def __iter__(self):
        return self._collection.__iter__()

    def __getitem__(self, arg):
        if self._sort is None:
            # Force a sort
            # Redis need it, and getting items from their index whitout
            # sorting does not make sense
            self._sort = {}
        if isinstance(arg, slice):
            # A slice has been requested
            # so add it to the sort parameters
            # and return the collection (a scliced collection is no more
            # chainable, so we do not return `self`)
            start = arg.start or 0
            stop = arg.stop  # FIXME: what to do if no stop given?
            self._sort['start'] = start
            # Redis expects a number of elements
            # not a python style stop value
            self._sort['num'] = stop - start
            return self._collection
        else:
            # A single item has been requested
            # Nevertheless, use the redis pagination, to minimize
            # data transfert and use the fast redis offset system
            start = arg
            self._sort['start'] = start
            self._sort['num'] = 1  # one element
            return self._collection[0]

    @property
    def _collection(self):
        """
        Effectively retrieve data according to lazy_collection.
        Sorting is not available if a pk as been requested (this is linked
        to the fact that pk have no index, due to optimization reasons).
        """
        self._len = None
        conn = self.cls.get_connection()
        if "keys" in self._lazy_collection:
            if self._sort is not None:
                if len(self._lazy_collection['keys']) > 1:
                    # Optimization: store only if there is more
                    # than one set
                    tmp_key = self._unique_key()
                    conn.sinterstore(tmp_key, self._lazy_collection['keys'])
                    collection = conn.sort(tmp_key, **self._sort)
                    conn.delete(tmp_key)
                else:
                    collection = conn.sort(
                        self._lazy_collection['keys'][0],
                        **self._sort
                    )
            else:
                if len(self._lazy_collection['keys']) > 1:
                    collection = conn.sinter(self._lazy_collection['keys'])
                else:
                    collection = conn.smembers(self._lazy_collection['keys'][0])
        elif "pk" in self._lazy_collection:
            if self._sort is not None:
                raise ImplementationError("Cannot sort when using a pk parameter.")
            collection = set([self._lazy_collection['pk']])
        else:
            # Empty result
            collection = set()
        if self._instances:
            result = [self.cls(pk, _skip_exist_test=self._instances_skip_exist_test)
                                                                for pk in collection]
        else:
            result = list(collection)
        # cache the len for future use
        self._len = len(result)
        return result

    def __call__(self, **filters):
        """Define self._lazy_collection according to filters."""
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
                "keys": [self.cls._redis_attr_pk.collection_key]
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
                self._lazy_collection = {}
            else:
                # Existing object, check all fields
                fail = False
                if query_fields:
                    for obj_field_name, obj_value in query_fields.iteritems():
                        field = getattr(obj, obj_field_name)
                        if field.proxy_get() != obj_value:
                            # Some asked field value differs from the object
                            # Nothing can be returned
                            self._lazy_collection = {}
                            fail = True
                            break
                if not fail:
                    self._lazy_collection = {
                        "pk": obj.pk.normalize(value)
                    }

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
        if self._len is None:
            self._len = self._collection.__len__()
        return self._len

    def __repr__(self):
        return self._collection.__repr__()

    def pop(self):
        return self._collection.pop()

    def instances(self, skip_exist_test=False):
        """
        If skip_exist_test is set to True, the instances returned by the
        collection won't have their primary key checked for existence.
        """
        self._instances = True
        self._instances_skip_exist_test = skip_exist_test
        return self

    def _coerce_by_parameter(self, parameters):
        if "by" in parameters:
            by = parameters['by']
            # Manage desc option
            if by.startswith('-'):
                parameters['desc'] = True
                by = by[1:]
            try:
                # Is it a field name?
                field = getattr(self.cls, "_redis_attr_%s" % by)
            except AttributeError:
                # It's not a field, so keep the original string
                pass
            else:
                parameters['by'] = field.sort_wildcard
        return parameters

    def sort(self, **parameters):
        """
        Parameters:
        `by`: pass either a field name or a wildcard string to sort on
              use `-` to make a desc sort.
        `alpha`: set it to True to sort lexicographilcally instead of numerically.
        """
        parameters = self._coerce_by_parameter(parameters)
        self._sort = parameters
        return self

    def _unique_key(self):
        """
        Create a unique key.
        """
        return unique_key(self.cls.get_connection())
