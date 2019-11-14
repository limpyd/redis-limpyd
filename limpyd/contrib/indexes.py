# -*- coding:utf-8 -*-

from __future__ import unicode_literals

from collections import defaultdict
from itertools import chain, product
from logging import getLogger

from limpyd.contrib.collection import ExtendedCollectionManager
from limpyd.exceptions import ImplementationError
from limpyd.fields import SingleValueField, HashField, MultiValuesField
from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex, EqualIndex, _MultiFieldsIndexMixin
from limpyd.utils import cached_property, unique_key

logger = getLogger(__name__)


class MultiIndexes(BaseIndex):
    """An index that is a proxy to many ones

    This must not be used directly as a class, but a new index class must be
    created by using the ``create`` class method

    Attributes
    ----------
    index_classes: list
        The index classes composing this multi-indexes class
    key: str
        A key to avoid collision with another index/multi-index
        that will be passed to a field.

    Examples
    --------

        >>> multi_index = MultiIndexes.compose([MyIndex, MyOtherIndex])
        >>> class MyModel(RedisModel):
        ...     field = StringField(indexes=[multi_index])

    """
    index_classes = []

    @classmethod
    def compose(cls, index_classes, key=None, transform=None, name=None):
        """Create a new class with the given index classes

        Parameters
        -----------
        index_classes: list
            The list of index classes to be used in the multi-index class to create
        name: str
            The name of the new multi-index class. If not set, it will be the same
            as the current class
        key: str
            A key to augment the default key of each index, to avoid collision.
        transform: callable
            None by default, can be set to a function that will transform the value to be indexed.
            This callable can accept one (`value`) or two (`self`, `value`) arguments

        """

        attrs = {}
        if index_classes:
            attrs['index_classes'] = index_classes

        klass = type(str(name or cls.__name__), (cls, ), attrs)

        # let the ``configure`` method manage some fields
        configure_attrs = {}
        if key is not None:
            configure_attrs['key'] = key
        if transform is not None:
            configure_attrs['transform'] = transform

        if configure_attrs:
            klass = klass.configure(**configure_attrs)

        return klass

    @cached_property
    def _indexes(self):
        """Instantiate the indexes only when asked

        Returns
        -------
        list
            A list of all indexes, tied to the field.

        """
        return [index_class(field=self.field) for index_class in self.index_classes]

    def can_handle_suffix(self, suffix):
        """Tell if one of the managed indexes  can be used for the given filter prefix

        For parameters, see BaseIndex.can_handle_suffix

        """
        for index in self._indexes:
            if index.can_handle_suffix(suffix):
                return True

        return False

    def _reset_rollback_cache(self, pk):
        """Reset attributes used to potentially rollback the indexes

        For the parameters, seen BaseIndex._reset_rollback_cache

        """
        for index in self._indexes:
            index._reset_rollback_cache(pk)

    def _rollback(self, pk):
        """Restore the index in its previous state

        For the parameters, seen BaseIndex._rollback

        """
        for index in self._indexes:
            index._rollback(pk)

    def get_unique_index(self):
        """Returns the first index handling uniqueness

        Returns
        -------
        BaseIndex
            The first index capable of handling uniqueness

        Raises
        ------
        IndexError
            If not index is capable of handling uniqueness

        """
        return [index for index in self._indexes if index.handle_uniqueness][0]

    @property
    def handle_uniqueness(self):
        """Tell if at least one of the indexes can handle uniqueness

        Returns
        -------
        bool
            ``True`` if this multi-index can handle uniqueness.

        """
        try:
            self.get_unique_index()
        except IndexError:
            return False
        else:
            return True

    def prepare_args(self, args, transform=True):
        """Prepare args to be used by a sub-index

        Parameters
        ----------
        args: list
            The while list of arguments passed to add, check_uniqueness, get_filtered_keys...
        transform: bool
            If ``True``, the last entry in `args`, ie the value, will be transformed.
            Else it will be kept as is.

        """
        updated_args = list(args)
        if transform:
            updated_args[-1] = self.transform_value(updated_args[-1])
        if self.key:
            updated_args.insert(-1, self.key)

        return updated_args

    def check_uniqueness(self, pk, *args):
        """For a unique index, check if the given args are not used twice

        For the parameters, seen BaseIndex.check_uniqueness

        """
        self.get_unique_index().check_uniqueness(pk, *self.prepare_args(args, transform=False))

    def add(self, pk, *args, **kwargs):
        """Add the instance tied to the field to all the indexes

        For the parameters, seen BaseIndex.add

        """

        check_uniqueness = kwargs.pop('check_uniqueness', False)
        args = self.prepare_args(args)

        for index in self._indexes:
            index.add(pk, *args, check_uniqueness=check_uniqueness and index.handle_uniqueness, **kwargs)
            if check_uniqueness and index.handle_uniqueness:
                check_uniqueness = False

    def remove(self, pk, *args, **kwargs):
        """Remove the instance tied to the field from all the indexes

        For the parameters, seen BaseIndex.remove

        """

        args = self.prepare_args(args)

        for index in self._indexes:
            index.remove(pk, *args, **kwargs)

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Returns the index keys to be used by the collection for the given args

        For the parameters, see BaseIndex.get_filtered_keys

        """

        args = self.prepare_args(args, transform=False)

        for index in self._indexes:
            if index.can_handle_suffix(suffix):
                return index.get_filtered_keys(suffix, *args, **kwargs)

    def get_all_storage_keys(self):
        """Returns the keys to be removed by `clear` in aggressive mode

        For the parameters, see BaseIndex.get_all_storage_keys
        """

        keys = set()
        for index in self._indexes:
            keys.update(index.get_all_storage_keys())

        return keys


# This is a multi-indexes managing the different parts of a date in the format YYYY-MM-SS
DateIndexParts = MultiIndexes.compose([
    NumberRangeIndex.configure(prefix='year', transform=lambda value: value[:4], handle_uniqueness=False, name='YearIndex'),
    NumberRangeIndex.configure(prefix='month', transform=lambda value: value[5:7], handle_uniqueness=False, name='MonthIndex'),
    NumberRangeIndex.configure(prefix='day', transform=lambda value: value[8:10], handle_uniqueness=False, name='DayIndex'),
], name='DateIndexParts')

# A simple TextRangeIndex to filter on a date  in the format YYYY-MM-SS
DateRangeIndex = TextRangeIndex.configure(key='date', transform=lambda value: value[:10], name='DateRangeIndex')

# A full usable index for fields holding dates (without time)
DateIndex = MultiIndexes.compose([DateRangeIndex, DateIndexParts], name='DateIndex')

# This is a multi-indexes managing the different parts of a tine in the format HH:MM:SS
TimeIndexParts = MultiIndexes.compose([
    NumberRangeIndex.configure(prefix='hour', transform=lambda value: value[0:2], handle_uniqueness=False, name='HourIndex'),
    NumberRangeIndex.configure(prefix='minute', transform=lambda value: value[3:5], handle_uniqueness=False, name='MinuteIndex'),
    NumberRangeIndex.configure(prefix='second', transform=lambda value: value[6:8], handle_uniqueness=False, name='SecondIndex'),
], name='TimeIndexParts')

# A simple TextRangeIndex to filter on a date  in the format HH:MM:SS
TimeRangeIndex = TextRangeIndex.configure(key='time', transform=lambda value: value[:8], name='TimeRangeIndex')

# A full usable index for fields holding times (without date)
TimeIndex = MultiIndexes.compose([TimeRangeIndex, TimeIndexParts], name='TimeIndex')

# A full usable index for fields holding dates+times, without filtering on hour/min/sec
# but only full field, full date and full time, and year, month, day
DateSimpleTimeIndex = MultiIndexes.compose([
    TextRangeIndex.configure(key='full', name='FullDateTimeRangeIndex'),
    DateRangeIndex.configure(prefix='date'),
    DateIndexParts,
    TimeRangeIndex.configure(prefix='time', transform=lambda value: value[11:])  # pass only time
], name='DateSimpleTimeIndex', transform=lambda value: value[:19])

# A full usable index for fields holding dates+times, with full filtering capabilities
DateTimeIndex = MultiIndexes.compose([
    DateSimpleTimeIndex,
    TimeIndexParts.configure(transform=lambda value: value[11:]),
], name='DateTimeIndex')

# And a simple datetime index without parts
SimpleDateTimeIndex = MultiIndexes.compose([
    TextRangeIndex.configure(key='full', name='FullDateTimeRangeIndex'),
    DateRangeIndex.configure(prefix='date'),
    TimeRangeIndex.configure(prefix='time', transform=lambda value: value[11:])  # pass only time
], name='SimpleDateTimeIndex', transform=lambda value: value[:19])


class _BaseRelatedIndex(BaseIndex):
    """Index attached to another index on another field

    This index does not handle data on its own for its field: when data is added/removed, it will
    ask the tied index to update its data.

    Configurable attributes
    -----------------------
    These are class attributes that can be changed via ``configure``:

    related_field : RedisField
        The field on the model that define the related index
    related_index_class : Type[BaseIndex]
        The index class defined for the indexed field

    """

    related_field = None
    related_index_class = None
    configurable_attrs = BaseIndex.configurable_attrs | {'related_field', 'related_index_class'}

    def __init__(self, field):
        """Tie ``related_field`` from the class to the right instance."""
        super(_BaseRelatedIndex, self).__init__(field)
        self.related_field = field._model.get_field(self.related_field.name)

    @cached_property
    def related_index(self):
        """Get (and cache) the related index to use

        Returns
        -------
        _BaseRelatedIndex
            The index instance tied to the related field.


        """
        return self.related_field.get_index(
            index_class=self.related_index_class,
            key=self.related_index_class.key,
            prefix=self.related_index_class.prefix,
        )

    @classmethod
    def handle_configurable_attrs(cls, related_index_class, related_field, **kwargs):
        """Handle attributes that can be passed to ``configure``.

        This method handle the ``related_field`` and `'related_index_class`` attribute added in this
        index class.

        Parameters
        ----------
        related_field : RedisField
            The field on the model that define the related index
        related_index_class : Type[BaseIndex]
            The related index defined for the indexed field

        For the other parameters, see ``BaseIndex.handle_configurable_attrs``.

        """
        name, attrs, kwargs = super(_BaseRelatedIndex, cls).handle_configurable_attrs(**kwargs)
        attrs['related_field'] = related_field
        attrs['related_index_class'] = related_index_class
        return name, attrs, kwargs


class _ScoredEqualIndex_RelatedIndex(_BaseRelatedIndex):
    """Index attached to the "score field" of ``ScoredEqualIndex``

    This index does not handle data on its own for its field: when data is added/removed, it will
    if ask the tied ``ScoredEqualIndex`` to update the score of the indexed value of the related
    field.
    """

    def add(self, pk, *args, **kwargs):
        """Do not save anything but ask the related index to update the score of its saved value"""
        self.related_index.score_updated(pk, float(args[-1]) if args[-1] is not None else None)
        self._get_rollback_cache(pk)['indexed_values'].add(tuple(args))

    def remove(self, pk, *args, **kwargs):
        """Do not remove anything but ask the related index to deindex its saved value"""
        self.related_index.score_updated(pk, None)
        self._get_rollback_cache(pk)['deindexed_values'].add(tuple(args))


class ScoredEqualIndex(EqualIndex):
    """Index acting like an EqualIndex but indexing values with a score from another field

    It allows filtering on a value and getting results automatically sorted by the related field
    in a single redis call.

    Notes
    -----
    - The scored field must be a subclass of ``SingleValueField``
    - The model must use ``ExtendedCollectionManager``
    - If the related field has no value, the instance will not be present in the index.

    Configurable attributes
    -----------------------
    These are class attributes that can be changed via ``configure``:

    score_field : str
        The name of the field on the model that will be used to get the score.

    """

    key = 'equal-scored'
    supported_key_types = {'zset'}

    score_field = None
    configurable_attrs = EqualIndex.configurable_attrs | {'score_field'}

    RelatedIndex = _ScoredEqualIndex_RelatedIndex

    @classmethod
    def _field_model_ready(cls, model, field):
        """Called when fields/indexes are ready, so we can add the private index to the score field

        For the parameters, see ``BaseIndex._field_model_ready``.

        Raises
        ------
        ImplementationError
            - If the model does not use ``ExtendedCollectionManager``
            - If the score field is not an other field of the same model
            - If the score field can hold many values (ie not a subclass of ``SingleValueField``)

        """
        super(ScoredEqualIndex, cls)._field_model_ready(model, field)

        score_field_name = cls.score_field

        # check that the model collection is correct
        if not model.collection_manager or not issubclass(model.collection_manager, ExtendedCollectionManager):
            raise ImplementationError("To use index %s on field %s, the model %s must use an ExtendedCollectionManager" % (
                cls.__name__,
                field.name,
                model.__name__,
            ))

        # check the score field match an existing field in the same model
        if not model.has_field(score_field_name):
            raise ImplementationError("%s is not an existing field for the index %s on %s.%s" % (
                score_field_name,
                cls.__name__,
                model.__name__,
                field.name,
            ))
        # but of course not self
        if score_field_name == field.name:
            raise ImplementationError("Index %s on %s.%s cannot use itself as score field" % (
                cls.__name__,
                model.__name__,
                field.name
            ))
        # and that this field contains a single value
        score_field = model.get_field(score_field_name)
        if not isinstance(score_field, SingleValueField):
            raise ImplementationError("Index %s on %s.%s must use a single value field as score field, not a %s" % (
                cls.__name__,
                model.__name__,
                field.name,
                score_field.__class__.__name__,
            ))

        # ok now we can save the field in our model class
        cls.score_field = score_field

        # and create the related index on the other field
        score_field.indexable = True
        score_field.index_classes.append(
            cls.RelatedIndex.configure(related_field=field, related_index_class=cls)
        )

    def __init__(self, field):
        """Get the instance of the score field on the model"""
        super(ScoredEqualIndex, self).__init__(field)
        self.score_field = field._model.get_field(self.score_field.name)

    @classmethod
    def handle_configurable_attrs(cls, score_field, **kwargs):
        """Handle attributes that can be passed to ``configure``.

        This method handle the ``score_field`` attribute added in this index class.

        Parameters
        ----------
        score_field : str
            The name of a field in the same model of the field for which this index is
            declared.

        For the other parameters, see ``BaseIndex.handle_configurable_attrs``.

        """

        name, attrs, kwargs = super(ScoredEqualIndex, cls).handle_configurable_attrs(**kwargs)
        attrs['score_field'] = score_field
        return name, attrs, kwargs

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Return the sorted set used by the index for the given "value" (`args`)

        For the parameters, see ``EqualIndex.get_filtered_keys``

        """
        return [
            (key, 'zset', is_tmp)
            for key, __, is_tmp
            in super(ScoredEqualIndex, self).get_filtered_keys(suffix, *args, **kwargs)
        ]

    def union_filtered_in_keys(self, dest_key, *source_keys):
        """Do a union of the given `source_keys` at the redis level, into `dest_key`

        For the parameters, see ``EqualIndex.union_filtered_in_keys``
        """
        self.connection.zunionstore(dest_key, source_keys)

    def get_uniqueness_key(self, base_key):
        return self.field.make_key(base_key, '__uniqueness__')

    def get_uniqueness_members(self, key):
        """Get from redis all the members of the given index `key` used to check for uniqueness.

        For the parameters, see ``EqualIndex.get_uniqueness_members``
        """
        return super(ScoredEqualIndex, self).get_uniqueness_members(self.get_uniqueness_key(key))

    def store(self, key, pk, **kwargs):
        """Store data in the index in redis

        For the parameters, see ``EqualIndex.store``

        Notes
        -----
        If the ``score`` is not passed in `kwargs`, then it will be retrieved from redis.

        Returns
        -------
        bool
            If we asked redis to do something. ``True`` except if the score field has is not set,
            in which case the value is not indexed.
        """
        score = kwargs.get('score') if 'score' in kwargs else self.score_field.get_for_instance(pk).proxy_get()
        if self.handle_uniqueness and self.field.unique:
            self.connection.sadd(self.get_uniqueness_key(key), pk)
        if score is None:
            return False
        self.connection.zadd(key, {pk: score})
        return True

    def unstore(self, key, pk, **kwargs):
        """Remove data from the index in redis

        For the parameters, see ``EqualIndex.unstore``
        """
        if self.handle_uniqueness and self.field.unique:
            self.connection.sadd(self.get_uniqueness_key(key), pk)
        self.connection.zrem(key, pk)
        return True

    def score_updated(self, pk, new_score):
        """Called by the related index on the related field to update the redis score when changed

        Parameters
        ----------
        pk : Any
            The primary key of the instance for which the score was updated
        new_score : Union[float, int, None]
            The new value of the score. If ``None``, it means than the field was unset, so we'll
            deindex the value.'

        """
        for parts in self.field._prepare_index_data(pk):
            if parts[-1] is None:
                continue
            if new_score is None:
                self.remove(pk, *parts, score=new_score)
            else:
                self.add(pk, *parts, score=new_score, check_uniqueness=False)

        self._reset_rollback_cache(pk)


class _EqualIndexWith_RelatedIndex(_MultiFieldsIndexMixin, _BaseRelatedIndex):
    """Index attached to the each of the "other fields" of ``EqualIndexWith``

    This index does not handle data on its own for its field: when data is added/removed, it will
    if ask the tied ``EqualIndexWith`` to update the indexed value of the related field.
    """

    handled_suffixes = {None, 'eq', 'in'}

    def can_filter_fields(self, fields_and_suffixes):
        """As a related index, cannot filter fields."""
        return []

    def add(self, pk, *args, **kwargs):
        """Do not save anything but ask the related index to update"""
        self.related_index.other_add(pk, self.field.name, *args)
        self._get_rollback_cache(pk)['indexed_values'].add(tuple(args))

    def remove(self, pk, *args, **kwargs):
        """Do not remove anything but ask the related index to update"""
        self.related_index.other_remove(pk, self.field.name, *args)
        self._get_rollback_cache(pk)['deindexed_values'].add(tuple(args))


class EqualIndexWith(_MultiFieldsIndexMixin, EqualIndex):
    """An index to index many fields together, for fast lookup. Can make fields unique together

    Notes
    -----
    - If one field is not set, the fields are not indexed
    - This index does not allow filtering on one or some of these fields, only all at once
    - The ``unique`` attribute cannot be set to ``True`` if one of the fields is a ``HashField``

    Configurable attributes
    -----------------------
    These are class attributes that can be changed via ``configure``:

    other_fields : List[str]
        The name of the other fields on the model that will be indexed with the one tied to this
        index
    unique : bool
        Default to ``False``. When ``True``, all fields managed by this index are unique together.


    """

    key = 'equal-with'
    handled_suffixes = {None, 'eq', 'in'}
    supported_key_types = {'set', 'zset'}
    other_fields = {}
    configurable_attrs = (EqualIndex.configurable_attrs | {'other_fields', 'unique'})

    RelatedIndex = _EqualIndexWith_RelatedIndex

    @classmethod
    def _field_model_ready(cls, model, field):
        """Called when fields/indexes are ready, so we can add the private index to the other fields

        For the parameters, see ``BaseIndex._field_model_ready``.

        Raises
        ------
        ImplementationError
            - If no other fields are set
            - If one of the other fields is not an other field of the same model
            - If unique is set to True, but one of the field is an HashField

        """
        super(EqualIndexWith, cls)._field_model_ready(model, field)

        if not cls.other_fields:
            raise ImplementationError("The index %s on %s.%s must have other_fields set" % (
                cls.__name__,
                model.__name__,
                field.name,
            ))

        other_fields = []
        for field_name in cls.other_fields:

            # check the score field match an existing field in the same model
            if not model.has_field(field_name):
                raise ImplementationError("%s is not an existing field for the index %s on %s.%s" % (
                    field_name,
                    cls.__name__,
                    model.__name__,
                    field.name,
                ))
            # but of course not self
            if field_name == field.name:
                raise ImplementationError("Index %s on %s.%s cannot use itself as other field" % (
                    cls.__name__,
                    model.__name__,
                    field.name
                ))

            # ok now we can save the field in our model class
            other_field = model.get_field(field_name)

            if cls.unique and isinstance(other_field, HashField):
                raise ImplementationError("Index %s on %s.%s cannot be unique if it includes an HashField (%s)" % (
                    cls.__name__,
                    model.__name__,
                    field.name,
                    field_name,
                ))

            # and create the related index on the other field
            other_field.indexable = True
            other_field.index_classes.append(
                cls.RelatedIndex.configure(related_field=field, related_index_class=cls)
            )

            other_fields.append(other_field)

        cls.other_fields = other_fields

    @classmethod
    def handle_configurable_attrs(cls, other_fields, unique=False, **kwargs):
        """Handle attributes that can be passed to ``configure``.

        This method handle the ``other_fields`` and ``unique`` attributes added in this index class.

        Parameters
        ----------
        other_fields : List[str]
            The name of the other fields on the model that will be indexed with the one tied to this
            index
        unique : bool
            Default to ``False``. When ``True``, all fields managed by this index are unique together

        For the other parameters, see ``BaseIndex.handle_configurable_attrs``.

        """
        name, attrs, kwargs = super(EqualIndexWith, cls).handle_configurable_attrs(**kwargs)
        attrs['other_fields'] = list(other_fields)
        attrs['unique'] = unique
        return name, attrs, kwargs

    def __init__(self, field):
        """Get the instances of the other fields on the model"""
        super(EqualIndexWith, self).__init__(field)
        self.other_fields = [
            field._model.get_field(other_field.name)
            for other_field in self.other_fields
        ]

    @property
    def handled_field_names(self):
        """Property to get the names of the field managed by this index, in order

        Yields
        ------
        str
            The name of the fields, one by one.

        """
        yield self.field.name
        for other_field in self.other_fields:
            yield other_field.name

    def _can_filter_fields(self, fields_and_suffixes):
        """Tell if the index can handle the given fields + suffixes

        For the parameters, see ``_MultiFieldsIndexMixin._can_filter_fields``

        """

        filters = defaultdict(set)
        for field_name, suffix in fields_and_suffixes:
            filters[field_name].add(suffix)

        handled = []
        for field_name in self.handled_field_names:
            if field_name not in filters:
                return []
            if not filters[field_name].intersection(self.handled_suffixes):
                return []
            handled.append((field_name, filters[field_name]))

        return handled

    def get_filtered_keys(self, suffix, *args, **kwargs):
        """Return the set used by the index for the given "value" (`args`)

        Parameters
        ----------
        kwargs['related_filters'] : Dict[str, Tuple(List, Union[str, None])]
            Mandatory named argument that contains the filters for the other fields than the one
            to which this index is attached
            All the other fields defined in the index must be present in this dict.
            The keys or the dict are the field names.
            The values are a tuple for each entry of the dict, containing two elements:
            - all the "values" to take into account for this field (see ``args`` argument of
               ``BaseIndex.get_filtered_keys``)
            - the suffix for this field (see ``suffix`` argument of ``BaseIndex.get_filtered_keys``)

        For the others parameters, see ``BaseIndex.get_filtered_keys``

        """
        self._check_key_accepted_key_types(kwargs.get('accepted_key_types'))

        related_filters = kwargs['related_filters']

        # if we have a `in` suffix, we have to handle it in a special way
        if suffix == 'in' or 'in' in {related_filters[field.name][1] for field in self.other_fields}:
            by_field = {}
            for field_name, field_args, field_suffix in chain(
                [(self.field.name, args, suffix)],
                [(field.name, ) + related_filters[field.name] for field in self.other_fields]
            ):
                field_args = list(field_args)
                value = field_args.pop()
                if field_suffix == 'in':
                    values = set(value)
                    if not values:
                        return []
                else:
                    values = [value]
                by_field[field_name] = [(field_args, value) for value in values]

            in_keys = []
            for all_fields_args in (zip(by_field, product_values) for product_values in product(*by_field.values())):
                call_args, call_kwargs = [], {}
                for field_name, [field_args, field_value] in all_fields_args:
                    if field_name == self.field.name:
                        call_args = field_args + [field_value]
                    else:
                        call_kwargs[field_name] = [field_args + [field_value]]
                in_keys.extend(
                    key for key, __
                    in self.get_storage_keys(
                        None, *call_args, other_args=call_kwargs,
                        transform_value=False
                    )
                )

            tmp_key = self._unique_key('tmp')
            self.union_filtered_in_keys(tmp_key, *in_keys)

            return [(tmp_key, 'set', True)]

        return [
            (key, 'set', False)
            for key, __
            in self.get_storage_keys(None, *args, other_args={
                field.name: [related_filters[field.name][0]]
                for field in self.other_fields
            }, transform_value=False)
        ]

    def get_storage_keys(self, pk, *args, **kwargs):
        """Return the redis key where to store the index for the given "values" (`args`)

        It can return many keys depending on the type of the other fields tied to this index.

        By default, it will get the keys by retrieving all the values for the other fields for the
        pk (will be only one for SingleValueField and many for MultiValuesField)

        If some of these other fields are filled in ``other_args``, the values from this dict
        will be used instead of fetching them.

        Parameters
        ----------
        pk : Optional[Any]
            The primary key of the instance for which we want the keys.
            If ``None``, all fields are expected to be found in `other_args`.
        args: tuple
            All the "values" to take into account for the field tied to the index to get the storage
             keys (see ``EqualIndex.get_storage_key``)
        kwargs: dict
            other_args : Optional[Dict[str, List[Tuple]]
                If set, will contains values of some others fields of the index we want the key for.
                For example if another field is a ``ListField`` having values 1 and 2, here we can
                say that we want only the keys for the value 2.
                The keys of the dict are name of fields tied to this index.
                The values are, for each one, a list of tuples, each of these tuples having the same
                info like in `args`: one tuple for each value for this field.
            transform_value: bool
                Default to ``True``. Tell the call to ``normalize_value`` to transform
                the value or not

        Returns
        -------
        List[Tuple[str, Union[Callable, None]]]
            Will return a list with one entry for each keys, each entry containing a tuple with two
            entries.
            The first is the key.
            The second is ``None`` if the index is not unique, else it's a callable to return
            the value to display in the UniquenessError message.

        """
        other_args = kwargs.get('other_args') or {}

        if args[-1] is None:
            return []

        args = list(args[:-1]) + [
            self.normalize_value(args[-1], transform=kwargs.get('transform_value', True))
        ]

        for field in self.other_fields:
            if field.name not in other_args:
                if not pk:
                    raise ImplementationError(
                        "Cannot get filtering storage key for index %s on "
                        "%s.%s without all fields given" % (
                            self.__class__.__name__,
                            self.model.__name__,
                            self.field.name,
                        )
                    )
                other_args[field.name] = field.get_for_instance(pk)._prepare_index_data(pk)
                # stop early if no data for a field to avoid fetching other fields
                if all([field_args[-1] is None for field_args in other_args[field.name]]):
                    return []

        entries = product([args], *[other_args[field.name] for field in self.other_fields])

        base_parts = [
            self.model._name,
            self.field.name,
        ]
        if self.prefix:
            base_parts.append(self.prefix)
        if self.key:
            base_parts.append(self.key)

        keys = []
        for key_entry in entries:
            if any([key_part[-1] is None for key_part in key_entry]):
                # at least one field has no data, so we skip this entry
                break
            parts = base_parts[:]
            parts.extend(chain.from_iterable(key_entry))

            def get_unique_value_func(key_entry_):
                # this will only be called in case of UniqunessError
                return lambda: ', '.join([
                    '%s=%s' % ('__'.join(map(str, (field_name, ) + tuple(key_parts[:-1]))), key_parts[-1])
                    for field_name, key_parts
                    in zip(
                        [self.field.name] + [field.name for field in self.other_fields],
                        key_entry_
                    )
                ])

            get_unique_value = get_unique_value_func(key_entry) if self.unique else None
            keys.append((self.field.make_key(*parts), get_unique_value))

        return keys

    @property
    def unique_index_name(self):
        """Get a string to describe the index in case of UniquenessError"""
        return 'unique together fields [%s] on %s' % (
            ', '.join([self.field.name] + [field.name for field in self.other_fields]),
            self.model.__name__
        )

    def check_uniqueness_at_init(self, values):
        """If the index is ``unique``, check that ``values`` are unique and can be inserted

        For the parameters, see ``_MultiFieldsIndexMixin.check_uniqueness_at_init``
        """

        if not self.unique:
            return
        args = [values.pop(self.field.name)]
        other_args = {
            field.name: [
                (val, ) for val in values[field.name]
            ] if isinstance(self.model.get_field(field.name), MultiValuesField)
            else [(values[field.name], )]
            for field in self.other_fields
        }
        keys = self.get_storage_keys(None, *args, other_args=other_args)
        self._check_uniqueness_in_keys(None, keys)

    def _check_uniqueness_in_keys(self, pk, keys):
        """Check uniqueness of pks in the given keys

        Parameters
        ----------
        pk: Any
            The pk of the instance for which its ok to have the value.
        keys: List[Tuple[str, Union[Callable, None]]]
            A list with one entry for each keys to check, each entry containing a tuple with two
            entries.
            The first is the key.
            The second is ``None`` if the index is not unique, else it's a callable to return
            the value to display in the UniquenessError message.

        Returns
        -------

        """
        if not self.unique:
            return
        for key, get_unique_value in keys:
            pks = self.get_uniqueness_members(key)
            self.assert_pks_uniqueness(pks, pk, get_unique_value)

    def add(self, pk, *args, **kwargs):
        """Add the instance tied to the field for the given "value" (via `args`) to the index

        Parameters
        ----------
        kwargs['other_args'] : dict
            Values for other fields. See ``get_storage_keys``.

        For the other parameters, see ``BaseIndex.add``

        """
        keys = self.get_storage_keys(pk, *args, other_args=kwargs.get('other_args'))
        if not keys:
            return
        if self.unique:
            self._check_uniqueness_in_keys(pk, keys)
        for key, __ in keys:
            logger.debug("adding %s to index %s" % (pk, key))
            if self.store(key, pk):
                self._get_rollback_cache(pk)['indexed_values'].add(tuple(args))

    def remove(self, pk, *args, **kwargs):
        """Remove the instance tied to the field for the given "value" (via `args`) from the index

        Parameters
        ----------
        kwargs['other_args'] : dict
            Values for other fields. See ``get_storage_keys``.

        For the other parameters, see ``BaseIndex.remove``

        """
        keys = self.get_storage_keys(pk, *args, other_args=kwargs.get('other_args'))
        if not keys:
            return
        for key, __ in keys:
            logger.debug("removing %s from index %s" % (pk, key))
            if self.unstore(key, pk):
                self._get_rollback_cache(pk)['deindexed_values'].add(tuple(args))  # TODO

    def other_add(self, pk, field_name, *args):
        """Called by the related index on a related field to update the index when changed

        Parameters
        ----------
        pk : Any
            The primary key of the instance for which the other field was updated
        field_name : str
            The name of the other updated field
        args: tuple
            All the values to take into account to define the index entry for the other field

        """
        if args[-1] is None:
            return
        for parts in self.field._prepare_index_data(pk):
            if parts[-1] is None:
                continue
            self.add(pk, *parts, other_args={field_name:[args]})

        self._reset_rollback_cache(pk)

    def other_remove(self, pk, field_name, *args):
        """Called by the related index on a related field to update the index when changed

        Parameters
        ----------
        pk : Any
            The primary key of the instance for which the other field was updated
        field_name : str
            The name of the other updated field
        args: tuple
            All the values to take into account to define the index entry for the other field

        """
        if args[-1] is None:
            return
        for parts in self.field._prepare_index_data(pk):
            if parts[-1] is None:
                continue
            self.remove(pk, *parts, other_args={field_name: [args]})

        self._reset_rollback_cache(pk)
