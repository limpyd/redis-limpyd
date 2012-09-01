# -*- coding:utf-8 -*-

from limpyd.utils import unique_key
from limpyd.exceptions import *
from limpyd.fields import MultiValuesField


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
    MyModel.collection().values('foo', 'bar') => return a list of dictionnaries
                                                 for the wanted fields (or all)
    MyModel.collection().values_list('foo', 'bar') => return a list of tuples
                                                      for the wanted fields (or all)
    MyModel.collection().values_list('foo', flat=True) => return a flat list of
                                                          the wanted field

    Note:
    Slicing a collection will force a sort.
    """

    def __init__(self, cls):
        self.cls = cls
        self._lazy_collection = {}  # Store infos to make the requested
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
        self._values = None  # Will store parameters used to retrieve values

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
        """
        self._len = 0
        pk = self._lazy_collection.get('pk', None)

        # Quick check if the pk exists. If not, the collection fails (empty)
        if pk is not None and not self.cls._redis_attr_pk.exists(pk):
            return []

        conn = self.cls.get_connection()
        sets = self._lazy_collection.get('sets', None)
        collection = set()

        if self._values:
            # if we asked for values, we have to use the redis 'sort'
            # command, which is able to return other fields.
            if self._sort is None:
                self._sort = {}
            self._sort['get'] = self._values['fields']['keys']

        if pk is not None and not sets and (self._sort is None or self._values is None):
            # we have a pk without other sets, and no needs to get values
            # so we can simply return the pk
            collection = set([pk])

        else:
            set_, delete_key = self._get_final_set()

            if self._sort is not None:
                # a sort, or values, call the SORT command on the set
                collection = conn.sort(set_, **self._sort)
            else:
                # no sort, nor values, simply return the full set
                collection = conn.smembers(set_)

            if delete_key:
                # we were asked to delete the set's key, a temporary one
                conn.delete(set_)

        if self._instances:
            # we want instances, so create an object for each pk, without
            # checking for pk existence if asked
            result = [self.cls(pk, _skip_exist_test=self._instances_skip_exist_test)
                                                                for pk in collection]
        elif self._values and self._values['mode'] != 'flat':
            # Regroup values in tuples or dicts for each "instance".
            # Exemple: Given this result from redis: ['id1', 'name1', 'id2', 'name2']
            # tuples: [('id1', 'name1'), ('id2', 'name2')]
            # dicts:  [{'id': 'id1', 'name': 'name1'}, {'id': 'id2', 'name': 'name2'}]
            result = zip(*([iter(collection)] * len(self._values['fields']['names'])))
            if self._values['mode'] == 'dicts':
                result = [dict(zip(self._values['fields']['names'], a_result))
                                                    for a_result in result]
        else:
            # nothing particular to do with the result, simply return it as a list
            result = list(collection)

        # cache the len for future use
        self._len = len(result)
        return result

    def _get_final_set(self):
        """
        Called by _collection to get the final set to work on. Return the name
        of the set to use, and a flag if we have to delete it once the
        collection really called (in case of a computed set based on multiple
        ones)
        """
        conn = self.cls.get_connection()
        sets = []
        tmp_keys = []

        iter_sets = self._lazy_collection.get('sets', [])
        pk = self._lazy_collection.get('pk', None)

        def get_tmp_key():
            """
            Create a key to store a set of data, and flag it as temporary (to
            delete it when exiting _get_final_set)
            """
            tmp_key = self._unique_key()
            tmp_keys.append(tmp_key)
            return tmp_key

        if iter_sets or pk:
            if iter_sets:
                sets += iter_sets
            if pk is not None:
                # create a set with the pk to do intersection (and to pass it to
                # the store command to retrieve values if needed)
                tmp_key = get_tmp_key()
                conn.sadd(tmp_key, pk)
                sets.append(tmp_key)

        else:
            # no sets or pk, use the whole collection instead
            sets.append(self.cls._redis_attr_pk.collection_key)

        if len(sets) == 1:
            # if we have only one set, we  delete the set after calling
            # collection only if it's a temporary one, and we do not delete
            # it right now
            delete_set_later = bool(tmp_keys)
            final_set = sets[0]
            tmp_keys = []
        else:
            # more than one set, do an intersection on all of them in a new key
            # that will must be deleted once the collection is called.
            delete_set_later = True
            final_set = self._unique_key()
            conn.sinterstore(final_set, sets)

        if tmp_keys:
            conn.delete(*tmp_keys)

        # return the final set to work on, and a flag if we later need to delete it
        return (final_set, delete_set_later)

    def __call__(self, **filters):
        """Define self._lazy_collection according to filters."""

        query_fields = filters.copy()

        # Some consistency check
        pk_fields = [k for k in filters.keys() if self.cls._field_is_pk(k)]
        if len(pk_fields) > 1:
            raise ValueError("You must use only one pk field in filtering")

        # --- There is a pk in the filters
        if pk_fields:
            pk_field = pk_fields[0]
            pk = query_fields.pop(pk_field)
            self._lazy_collection['pk'] = self.cls._redis_attr_pk.normalize(pk)

        # --- Filters
        if query_fields:
            # Prepare a list of sets for each query parameter
            for field_name, value in query_fields.iteritems():
                field = getattr(self.cls, "_redis_attr_%s" % field_name)
                self._lazy_collection.setdefault('sets', []).append(field.index_key(value))

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
        self._values = None
        self._instances = True
        self._instances_skip_exist_test = skip_exist_test
        return self

    def _get_simple_fields(self):
        """
        Return a list of the names of all fields that handle simple values
        (StringField or HashableField), that redis can use to return values via
        the sort command (so, exclude all fields based on MultiValuesField)
        """
        fields = []
        for field_name in self.cls._fields:
            field = getattr(self.cls, "_redis_attr_%s" % field_name)
            if not isinstance(field, MultiValuesField):
                fields.append(field_name)
        return fields

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
                             % (kwargs.keys(),))

        if not fields:
            fields = self._get_simple_fields()

        if flat and len(fields) > 1:
            raise ValueError("'flat' is not valid when values is called with more than one field.")

        fields = self._coerce_fields_parameters(fields)

        self._instances = False
        self._values = {'fields': fields, 'mode': 'flat' if flat else 'tuples'}
        return self

    def _coerce_fields_parameters(self, fields):
        """
        Used by values and values_list to get the list of fields to use in the
        redis sort command to retrieve fields.
        The result is a dict with two lists:
          - 'names', with wanted field names
          - 'keys', with keys to use in the sort command
        """
        final_fields = {'names': [], 'keys': []}
        for field_name in fields:
            if self.cls._field_is_pk(field_name):
                final_fields['names'].append(field_name)
                final_fields['keys'].append('#')
            else:
                try:
                    field = getattr(self.cls, "_redis_attr_%s" % field_name)
                except AttributeError:
                    raise ValueError("%s if not a valid field to get from collection"
                                     " for %s" % (field_name, self.cls.__name__))
                else:
                    if isinstance(field, MultiValuesField):
                        raise ValueError("It's not possible to get a MultiValuesField"
                                         " from a collection (asked: %s" % field_name)
                    final_fields['names'].append(field_name)
                    final_fields['keys'].append(field.sort_wildcard)
        return final_fields

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
