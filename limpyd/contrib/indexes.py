# -*- coding:utf-8 -*-

from __future__ import unicode_literals

from inspect import isclass
from logging import getLogger

from limpyd.contrib.collection import ExtendedCollectionManager
from limpyd.exceptions import ImplementationError
from limpyd.fields import SingleValueField
from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex, BaseRangeIndex, EqualIndex
from limpyd.utils import cached_property

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

    def _reset_cache(self):
        """Reset attributes used to potentially rollback the indexes

        For the parameters, seen BaseIndex._reset_cache

        """
        for index in self._indexes:
            index._reset_cache()

    def _rollback(self):
        """Restore the index in its previous state

        For the parameters, seen BaseIndex._rollback

        """
        for index in self._indexes:
            index._rollback()

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

    def check_uniqueness(self, *args):
        """For a unique index, check if the given args are not used twice

        For the parameters, seen BaseIndex.check_uniqueness

        """
        self.get_unique_index().check_uniqueness(*self.prepare_args(args, transform=False))

    def add(self, *args, **kwargs):
        """Add the instance tied to the field to all the indexes

        For the parameters, seen BaseIndex.add

        """

        check_uniqueness = kwargs.pop('check_uniqueness', False)
        args = self.prepare_args(args)

        for index in self._indexes:
            index.add(*args, check_uniqueness=check_uniqueness and index.handle_uniqueness, **kwargs)
            if check_uniqueness and index.handle_uniqueness:
                check_uniqueness = False

    def remove(self, *args, **kwargs):
        """Remove the instance tied to the field from all the indexes

        For the parameters, seen BaseIndex.remove

        """

        args = self.prepare_args(args)

        for index in self._indexes:
            index.remove(*args, **kwargs)

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


class _ScoredEqualIndex_RelatedIndex(BaseIndex):
    """Index attached to the "score field" of ``ScoredEqualIndex``

    This index does not handle data on its own for its field: when data is added/removed, it will
    if ask the tied ``ScoredEqualIndex`` to update te score of the indexed value of the related
    field.

    Configurable attributes
    -----------------------
    These are class attributes that can be changed via ``configure``:

    related_field : RedisField
        The field on the model that define the ``ScoredEqualIndex``
    related_index : ScoredEqualIndex
        The ``ScoredEqualIndex`` defined for the indexed field

    """

    related_field = None
    related_index = None
    configurable_attrs = BaseRangeIndex.configurable_attrs | {'related_field', 'related_index'}

    def __init__(self, field):
        """Tie ``related_field`` and ``related_index`` from the class to the right instances."""
        super(_ScoredEqualIndex_RelatedIndex, self).__init__(field)

        field_parent = getattr(field, '_model' if field.attached_to_model else '_instance')
        self.related_field = field_parent.get_field(self.related_field.name)

        related_index_class = self.related_index
        if not isclass(related_index_class):
            related_index_class = related_index_class.__class__

        self.related_index = self.related_field.get_index(
            index_class=related_index_class,
            key=related_index_class.key,
            prefix=related_index_class.prefix,
        )

    @classmethod
    def handle_configurable_attrs(cls, related_index, related_field, **kwargs):
        """Handle attributes that can be passed to ``configure``.

        This method handle the ``related_field`` and `'related_index`` attribute added in this
        index class.

        Parameters
        ----------
        related_field : RedisField
            The field on the model that define the ``ScoredEqualIndex``
        related_index : ScoredEqualIndex
            The ``ScoredEqualIndex`` defined for the indexed field

        For the other parameters, see ``BaseIndex.handle_configurable_attrs``.

        """
        name, attrs, kwargs = super(_ScoredEqualIndex_RelatedIndex, cls).handle_configurable_attrs(**kwargs)
        attrs['related_field'] = related_field
        attrs['related_index'] = related_index
        return name, attrs, kwargs

    def add(self, *args, **kwargs):
        """Do not save anything but ask the related index to update the score of its saved value"""
        self.related_index.score_updated(float(args[-1]) if args[-1] is not None else None)

    def remove(self, *args, **kwargs):
        """Do not remove anything but ask the related index to deindex its saved value"""
        self.related_index.score_updated(None)


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

    score_field : SingleValueField
        The field on the model that will be used to get the score.

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
        if score_field_name not in model._fields:
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
            cls.RelatedIndex.configure(related_field=field, related_index=cls)
        )

    def __init__(self, field):
        """Get the instance of the score field on the model"""
        super(ScoredEqualIndex, self).__init__(field)
        self.score_field = getattr(field, '_model' if field.attached_to_model else '_instance')\
            .get_field(self.score_field.name)

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

    def union_keys(self, dest_key, *source_keys):
        """Do a union of the given `source_keys` at the redis level, into `dest_key`

        For the parameters, see ``EqualIndex.union_keys``
        """
        self.connection.zunionstore(dest_key, source_keys)

    def get_members(self, key):
        """Get from redis all the members of the given index `key`.

        For the parameters, see ``EqualIndex.get_members``
        """
        return list(self.connection.zmembers(key))

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
        score = kwargs.get('score', self.score_field.proxy_get())
        if score is None:
            return False
        self.connection.zadd(key, {pk: score})
        return True

    def unstore(self, key, pk, **kwargs):
        """Remove data from the index in redis

        For the parameters, see ``EqualIndex.unstore``
        """
        self.connection.zrem(key, pk)
        return True

    def score_updated(self, new_score):
        """Called by the related index on the related field to update the redis score when changed

        Parameters
        ----------
        new_score : Union[float, int, None]
            The new value of the score. If ``None``, it means than the field was unset, so we'll
            deindex the value.'

        """
        for parts in self.field._prepare_index_data():
            if parts[-1] is None:
                continue
            if new_score is None:
                self.remove(*parts, score=new_score)
            else:
                self.add(*parts, score=new_score)

