# -*- coding:utf-8 -*-
from __future__ import unicode_literals

from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex
from limpyd.utils import cached_property


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

    def remove(self, *args):
        """Remove the instance tied to the field from all the indexes

        For the parameters, seen BaseIndex.remove

        """

        args = self.prepare_args(args)

        for index in self._indexes:
            index.remove(*args)

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
