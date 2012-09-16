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
        self._lazy_collection = {  # Store infos to make the requested collection
            'sets': set(),  # store sets to use (we'll intersect them)
            'pks': set(),  # store special filter on pk
        }
        self._instances = False  # True when instances are asked
                                 # instead of raw pks
        self._instances_skip_exist_test = False  # If True will return instances
                                                 # without testing if pk exist
        self._sort = None  # Will store sorting parameters
        self._slice = None  # Will store slice parameters (start and num)
        self._len = None  # Store the result of the final collection, to avoid
                          # having to compute the whole thing twice when doing
                          # `list(Model.collection)` (a `list` will call
                          # __iter__ AND __len__)
        self._values = None  # Will store parameters used to retrieve values

    def __iter__(self):
        return self._collection.__iter__()

    def __getitem__(self, arg):
        self._slice = {}
        if isinstance(arg, slice):
            # A slice has been requested
            # so add it to the sort parameters (via slice)
            # and return the collection (a scliced collection is no more
            # chainable, so we do not return `self`)
            start = arg.start or 0
            if start < 0:
                # in case of a negative start, we can't use redis sort so
                # we fetch all the collection before returning the wanted slice
                return self._collection[arg]
            self._slice['start'] = start
            stop = arg.stop
            # Redis expects a number of elements
            # not a python style stop value
            if stop is None:
                # negative value for the count return all
                self._slice['num'] = -1
            else:
                self._slice['num'] = stop - start
            return self._collection
        else:
            # A single item has been requested
            # Nevertheless, use the redis pagination, to minimize
            # data transfert and use the fast redis offset system
            start = arg
            if start > 0:
                self._slice['start'] = start
                self._slice['num'] = 1  # one element
                return self._collection[0]
            else:
                # negative index, we have to fetch the whole collection first
                return self._collection[start]

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
        on "_sort", "_slice" and "_values" attributes
        """
        sort_options = {}
        if self._sort is not None and not has_pk:
            sort_options.update(self._sort)
        if self._slice is not None:
            sort_options.update(self._slice)
        if self._values:
            # if we asked for values, we have to use the redis 'sort'
            # command, which is able to return other fields.
            sort_options['get'] = self._values['fields']['keys']
        if not sort_options and self._sort is None:
            sort_options = None
        return sort_options

    @property
    def _collection(self):
        """
        Effectively retrieve data according to lazy_collection.
        """
        try:  # try block to always reset the _slice in the "finally" part

            conn = self.cls.get_connection()
            self._len = 0

            # The collection fails (empty) if more than one pk or if the only one
            # doesn't exists
            try:
                pk = self._get_pk()
            except ValueError:
                return []
            else:
                if pk is not None and not self.cls._redis_attr_pk.exists(pk):
                    return []

            # Prepare options and final set to get/sort
            sort_options = self._prepare_sort_options(bool(pk))

            final_set, keys_to_delete = self._get_final_set(
                                                self._lazy_collection['sets'],
                                                pk, sort_options)

            # fill the collection
            if final_set is None:
                # final_set is None if we have a pk without other sets, and no
                # needs to get values so we can simply return the pk
                collection = set([pk])
            else:
                # compute the sets and call redis te retrieve wanted values
                collection = self._final_redis_call(final_set, sort_options)
                if keys_to_delete:
                    conn.delete(*keys_to_delete)

            # Format return values if needed
            collection = self._prepare_results(collection)

            if self._instances:
                result = self._to_instances(collection)
            elif self._values and self._values['mode'] != 'flat':
                result = self._to_values(collection)
            else:
                result = list(collection)

            # cache the len for future use
            self._len = len(result)

            return result

        except:  # raise original exception
            raise
        finally:  # always reset the slice, having an exception or not
            self._slice = {}

    def _final_redis_call(self, final_set, sort_options):
        """
        The final redis call to obtain the values to return from the "final_set"
        with some sort options.
        """
        conn = self.cls.get_connection()
        if sort_options is not None:
            # a sort, or values, call the SORT command on the set
            return conn.sort(final_set, **sort_options)
        else:
            # no sort, nor values, simply return the full set
            return conn.smembers(final_set)

    def _to_instances(self, pks):
        """
        Returns a list of instances for each given pk, respecting the condition
        about checking or not if a pk exists.
        """
        # we want instances, so create an object for each pk, without
        # checking for pk existence if asked
        return [self.cls(pk, _skip_exist_test=self._instances_skip_exist_test)
                                                           for pk in pks]

    def _to_values(self, collection):
        """
        Regroup values in tuples or dicts for each "instance".
        Exemple: Given this result from redis: ['id1', 'name1', 'id2', 'name2']
         tuples: [('id1', 'name1'), ('id2', 'name2')]
         dicts:  [{'id': 'id1', 'name': 'name1'}, {'id': 'id2', 'name': 'name2'}]
        """
        result = zip(*([iter(collection)] * len(self._values['fields']['names'])))
        if self._values['mode'] == 'dicts':
            result = [dict(zip(self._values['fields']['names'], a_result))
                                                for a_result in result]
        return result

    def _prepare_results(self, results):
        """
        Called in _collection to prepare results from redis before returning
        them. Does nothing here, but can be useful in subclasses.
        """
        return results

    def _prepare_sets(self, sets):
        """
        Return all sets in self._lazy_collection['sets'] to be ready to be used
        to intersect them. Called by _get_final_set, to use in subclasses.
        Must return a tuple with a set of redis set keys, and another with
        new temporary keys to drop at the end of _get_final_set
        """
        return (sets, set())

    def _get_final_set(self, sets, pk, sort_options):
        """
        Called by _collection to get the final set to work on. Return the name
        of the set to use, and a list of keys to delete once the collection is
        really called (in case of a computed set based on multiple ones)
        """
        conn = self.cls.get_connection()
        all_sets = set()
        tmp_keys = set()

        if pk is not None and not sets and not (sort_options and sort_options.get('get')):
            # no final set if only a pk without values to retrieve
            return (None, False)

        elif sets or pk:
            if sets:
                new_sets, new_tmp_keys = self._prepare_sets(sets)
                all_sets.update(new_sets)
                tmp_keys.update(new_tmp_keys)
            if pk is not None:
                # create a set with the pk to do intersection (and to pass it to
                # the store command to retrieve values if needed)
                tmp_key = self._unique_key()
                conn.sadd(tmp_key, pk)
                all_sets.add(tmp_key)
                tmp_keys.add(tmp_key)

        else:
            # no sets or pk, use the whole collection instead
            all_sets.add(self.cls._redis_attr_pk.collection_key)

        if len(all_sets) == 1:
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
            final_set = self._combine_sets(all_sets, self._unique_key())

        if tmp_keys:
            conn.delete(*tmp_keys)

        # return the final set to work on, and a flag if we later need to delete it
        return (final_set, [final_set] if delete_set_later else None)

    def _combine_sets(self, sets, final_set):
        """
        Given a list of set, combine them to create the final set that will be
        used to make the final redis call.
        """
        self.cls.get_connection().sinterstore(final_set, list(sets))
        return final_set

    def __call__(self, **filters):
        return self._add_filters(**filters)

    def _add_filters(self, **filters):
        """Define self._lazy_collection according to filters."""
        for field_name, value in filters.iteritems():
            if self.cls._field_is_pk(field_name):
                pk = self.cls._redis_attr_pk.normalize(value)
                self._lazy_collection['pks'].add(pk)
            else:
                field = getattr(self.cls, "_redis_attr_%s" % field_name)
                self._lazy_collection['sets'].add(field.index_key(value))

        return self

    def __len__(self):
        if self._len is None:
            self._len = self._collection.__len__()
        return self._len

    def __repr__(self):
        return self._collection.__repr__()

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
              prefix with `-` to make a desc sort.
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
