# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from limpyd import fields
from limpyd.contrib.indexes import MultiIndexes, DateIndex, DateTimeIndex, TimeIndex
from limpyd.exceptions import ImplementationError, UniquenessError
from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex, EqualIndex

from ..base import LimpydBaseTest, skip_if_no_zrangebylex
from ..indexes import ReverseEqualIndex
from ..model import TestRedisModel


class MultiIndexesTestCase(LimpydBaseTest):

    def test_can_be_created_with_many_indexes(self):
        index_class = MultiIndexes.compose([TextRangeIndex, ReverseEqualIndex])

        self.assertTrue(issubclass(index_class, BaseIndex))
        self.assertTrue(issubclass(index_class, MultiIndexes))
        self.assertEqual(index_class.__name__, 'MultiIndexes')
        self.assertEqual(index_class.index_classes, [TextRangeIndex, ReverseEqualIndex])

        index_class = MultiIndexes.compose([TextRangeIndex, NumberRangeIndex], name='MyMultiIndex')
        self.assertEqual(index_class.__name__, 'MyMultiIndex')

    def test_multi_index_with_only_one_should_behave_like_the_one(self):
        index_class = MultiIndexes.compose([EqualIndex])

        class MultiIndexOneIndexTestModel(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[index_class], unique=True)

        obj1 = MultiIndexOneIndexTestModel(name="foo")
        pk1 = obj1.pk.get()
        obj2 = MultiIndexOneIndexTestModel(name="bar")
        pk2 = obj2.pk.get()

        # test without suffix
        self.assertSetEqual(
            set(MultiIndexOneIndexTestModel.collection(name='foo')),
            {pk1}
        )

        self.assertSetEqual(
            set(MultiIndexOneIndexTestModel.collection(name='bar')),
            {pk2}
        )

        self.assertSetEqual(
            set(MultiIndexOneIndexTestModel.collection(name='foobar')),
            set()
        )

        # test with suffix
        self.assertSetEqual(
            set(MultiIndexOneIndexTestModel.collection(name__eq='bar')),
            {pk2}
        )

        # test invalid suffix
        with self.assertRaises(ImplementationError):
            MultiIndexOneIndexTestModel.collection(name__gte='bar')

        # test uniqueness
        with self.assertRaises(UniquenessError):
            MultiIndexOneIndexTestModel(name="foo")

    def test_chaining_should_work(self):

        index_class = MultiIndexes.compose([
            MultiIndexes.compose([
                MultiIndexes.compose([
                    MultiIndexes.compose([
                        EqualIndex
                    ])
                ])
            ])
        ])

        class ChainingIndexTestModel(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[index_class], unique=True)

        obj1 = ChainingIndexTestModel(name="foo")
        pk1 = obj1.pk.get()
        obj2 = ChainingIndexTestModel(name="bar")
        pk2 = obj2.pk.get()

        with self.assertRaises(UniquenessError):
            ChainingIndexTestModel(name="foo")

        self.assertEqual(
            set(ChainingIndexTestModel.collection(name='foo')),
            {pk1}
        )

    def test_filtering(self):

        index_class = MultiIndexes.compose([
            EqualIndex.configure(
                prefix='first_letter',
                transform=lambda v: v[0] if v else '',
                handle_uniqueness=False
            ),
            EqualIndex
        ])

        class MultiIndexTestModel(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[index_class], unique=True)

        obj1 = MultiIndexTestModel(name="foo")
        pk1 = obj1.pk.get()
        obj2 = MultiIndexTestModel(name="bar")
        pk2 = obj2.pk.get()

        # we should not be able to add another with the same name
        with self.assertRaises(UniquenessError):
            MultiIndexTestModel(name="foo")

        # but we can with the first letter being the same
        # because our special index does not handle uniqueness
        obj3 = MultiIndexTestModel(name='baz')
        pk3 = obj3.pk.get()

        # access without prefix: the simple should be used
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name='foo')),
            {pk1}
        )

        # nothing with the first letter
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name='f')),
            set()
        )

        # the same with `eq` suffix
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__eq='foo')),
            {pk1}
        )
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__eq='f')),
            set()
        )

        # access with the suffix: the special index should be used
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__first_letter='b')),
            {pk2, pk3}
        )
        # also with the `eq` suffix
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__first_letter__eq='b')),
            {pk2, pk3}
        )

        # and nothing with the full name
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__first_letter='bar')),
            set()
        )

        # and it should work with both indexes
        self.assertSetEqual(
            set(MultiIndexTestModel.collection(name__first_letter='b', name='bar')),
            {pk2}
        )


class DateTimeModelTest(TestRedisModel):
    date = fields.InstanceHashField(indexable=True, indexes=[DateIndex])
    unique_date = fields.InstanceHashField(indexable=True, indexes=[DateIndex], unique=True)
    time = fields.InstanceHashField(indexable=True, indexes=[TimeIndex])
    unique_time = fields.InstanceHashField(indexable=True, indexes=[TimeIndex], unique=True)
    datetime = fields.InstanceHashField(indexable=True, indexes=[DateTimeIndex])
    unique_datetime = fields.InstanceHashField(indexable=True, indexes=[DateTimeIndex], unique=True)


@unittest.skipIf(*skip_if_no_zrangebylex)
class DateTimeIndexesTestCase(LimpydBaseTest):

    def test_date_index(self):
        obj1 = DateTimeModelTest(date='2015-12-16')
        pk1 = obj1.pk.get()
        obj2 = DateTimeModelTest(date='2014-09-07')
        pk2 = obj2.pk.get()
        obj3 = DateTimeModelTest(date='2015-06-12')
        pk3 = obj3.pk.get()
        obj4 = DateTimeModelTest(date='2016-12-31')
        pk4 = obj4.pk.get()

        # not unique so same date is ok
        obj5 = DateTimeModelTest(date='2015-12-16')
        pk5 = obj5.pk.get()

        # EqualIndex
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date='2015-12-16')),
            {pk1, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__gte='2015-06-12')),
            {pk1, pk3, pk4, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__gt='2015')),
            {pk1, pk3, pk4, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__lt='2015-07')),
            {pk2, pk3}
        )

        # year index
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__year=2015)),
            {pk1, pk3, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__year__lt=2015)),
            {pk2}
        )

        # month index
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__month=12)),
            {pk1, pk4, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(date__year=2015, date__month=12)),
            {pk1, pk5}
        )

    def test_date_unique_index(self):
        DateTimeModelTest(unique_date='2001-01-01')
        # can add on same year (diff month/day)
        DateTimeModelTest(unique_date='2001-02-02')
        # can add on same month (diff year/day)
        DateTimeModelTest(unique_date='2002-02-03')
        # can add on same day (diff year/month)
        DateTimeModelTest(unique_date='2003-03-03')

        # cannot add on same date
        with self.assertRaises(UniquenessError):
            DateTimeModelTest(unique_date='2003-03-03')

    def test_time_index(self):
        # constructed the same as DateIndex so only test for transforms

        obj1 = DateTimeModelTest(time='15:16:17')
        pk1 = obj1.pk.get()
        obj2 = DateTimeModelTest(time='05:06:07')
        pk2 = obj2.pk.get()

        self.assertSetEqual(
            set(DateTimeModelTest.collection(time__hour='15')),
            {pk1}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(time__minute='06')),
            {pk2}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(time__second='17')),
            {pk1}
        )

    def test_datetime_index(self):
        obj1 = DateTimeModelTest(datetime='2015-12-16 15:16:17')
        pk1 = obj1.pk.get()
        obj2 = DateTimeModelTest(datetime='2014-09-07 05:06:07')
        pk2 = obj2.pk.get()
        obj3 = DateTimeModelTest(datetime='2015-06-12 15:16:17')
        pk3 = obj3.pk.get()
        obj4 = DateTimeModelTest(datetime='2016-12-31 05:06:07')
        pk4 = obj4.pk.get()

        # not unique so same date is ok
        obj5 = DateTimeModelTest(datetime='2015-12-16 15:16:17')
        pk5 = obj5.pk.get()

        # check full date
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime='2015-12-16 15:16:17')),
            {pk1, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__gte='2015-06-12 1')),
            {pk1, pk3, pk4, pk5}
        )

        # check date
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__date='2015-12-16')),
            {pk1, pk5}
        )

        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__date__lt='2015-07')),
            {pk2, pk3}
        )

        # check year
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__year=2015)),
            {pk1, pk3, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__year__lt=2015)),
            {pk2}
        )

        # check time
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__time='15:16:17')),
            {pk1, pk3, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__time__lt='15')),
            {pk2, pk4}
        )

        # check hour
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__hour=15)),
            {pk1, pk3, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(datetime__hour__lt=15)),
            {pk2, pk4}
        )

        # be crazy, check all for '2015-12-16 15:16:17'
        # All are ended so it should work
        self.assertSetEqual(
            set(DateTimeModelTest.collection(
                datetime='2015-12-16 15:16:17',
                datetime__date='2015-12-16',
                datetime__year=2015,
                datetime__month=12,
                datetime__day=16,
                datetime__time='15:16:17',
                datetime__hour=15,
                datetime__minute=16,
                datetime__second=17
            )),
            {pk1, pk5}
        )

    def test_datetime_unique_index(self):

        DateTimeModelTest(unique_datetime='2001-01-01 01:01:01')
        # can add on same year (diff month/day/hour/min/sec)
        DateTimeModelTest(unique_datetime='2001-02-02 02:02:02')
        # can add on same month (diff year/day/hour/min/sec)
        DateTimeModelTest(unique_datetime='2002-02-03 03:03:03')
        # can add on same day (diff year/month/hour/min/sec)
        DateTimeModelTest(unique_datetime='2003-03-03 04:04:04')
        # can add on same hour (diff year/month/day/min/sec)
        DateTimeModelTest(unique_datetime='2004-04-04 04:05:05')
        # can add on same minute (diff year/month/day/hour/sec)
        DateTimeModelTest(unique_datetime='2005-05-05 05:05:06')
        # can add on same second (diff year/month/day/hour/min)
        DateTimeModelTest(unique_datetime='2006-06-06 06:06:06')

        # can add on same date (diff time)
        DateTimeModelTest(unique_datetime='2006-06-06 07:07:07')
        # can add on same time (diff date)
        DateTimeModelTest(unique_datetime='2007-07-07 07:07:07')

        # but cannot add the same full datetime
        with self.assertRaises(UniquenessError):
            DateTimeModelTest(unique_datetime='2007-07-07 07:07:07')
