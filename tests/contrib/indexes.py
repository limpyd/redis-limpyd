# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from limpyd import fields
from limpyd.contrib.collection import ExtendedCollectionManager
from limpyd.contrib.indexes import MultiIndexes, DateIndex, DateTimeIndex, SimpleDateTimeIndex, TimeIndex, ScoredEqualIndex, _ScoredEqualIndex_RelatedIndex, EqualIndexWith, _EqualIndexWith_RelatedIndex
from limpyd.contrib.related import RelatedModel, FKInstanceHashField
from limpyd.exceptions import ImplementationError, UniquenessError
from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex, EqualIndex
from limpyd.utils import unique_key

from ..base import LimpydBaseTest
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

    def test_cleaning(self):

        index_class = MultiIndexes.compose([
            EqualIndex.configure(
                prefix='first_letter',
                transform=lambda v: v[0] if v else '',
                handle_uniqueness=False
            ),
            EqualIndex
        ])

        class MultiIndexTestModel2(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[index_class], unique=True)

        pk1 = MultiIndexTestModel2(name="foo").pk.get()
        pk2 = MultiIndexTestModel2(name="bar").pk.get()

        index = MultiIndexTestModel2.get_field('name').get_index()

        # check the keys, we should have the ones from both included index
        keys = index.get_all_storage_keys()
        self.assertSetEqual(keys, {
            'tests:multiindextestmodel2:name:foo',
            'tests:multiindextestmodel2:name:bar',
            'tests:multiindextestmodel2:name:first_letter:b',
            'tests:multiindextestmodel2:name:first_letter:f',
        })

        # clear the index
        index.clear()

        # we should have nothing indexed
        self.assertSetEqual(set(MultiIndexTestModel2.collection(name='foo')), set())
        self.assertSetEqual(set(MultiIndexTestModel2.collection(name__first_letter='b')), set())

        # rebuild it
        index.rebuild()

        # everything should be indexed
        self.assertSetEqual(set(MultiIndexTestModel2.collection(name='foo')), {pk1})
        self.assertSetEqual(set(MultiIndexTestModel2.collection(name__first_letter='b')), {pk2})


class DateTimeModelTest(TestRedisModel):
    date = fields.InstanceHashField(indexable=True, indexes=[DateIndex])
    unique_date = fields.InstanceHashField(indexable=True, indexes=[DateIndex], unique=True)
    time = fields.InstanceHashField(indexable=True, indexes=[TimeIndex])
    unique_time = fields.InstanceHashField(indexable=True, indexes=[TimeIndex], unique=True)
    datetime = fields.InstanceHashField(indexable=True, indexes=[DateTimeIndex])
    simple_datetime = fields.InstanceHashField(indexable=True, indexes=[SimpleDateTimeIndex])
    unique_datetime = fields.InstanceHashField(indexable=True, indexes=[DateTimeIndex], unique=True)
    unique_simple_datetime = fields.InstanceHashField(indexable=True, indexes=[SimpleDateTimeIndex], unique=True)


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

    def test_simple_datetime_index(self):
        obj1 = DateTimeModelTest(simple_datetime='2015-12-16 15:16:17')
        pk1 = obj1.pk.get()
        obj2 = DateTimeModelTest(simple_datetime='2014-09-07 05:06:07')
        pk2 = obj2.pk.get()
        obj3 = DateTimeModelTest(simple_datetime='2015-06-12 15:16:17')
        pk3 = obj3.pk.get()
        obj4 = DateTimeModelTest(simple_datetime='2016-12-31 05:06:07')
        pk4 = obj4.pk.get()

        # not unique so same date is ok
        obj5 = DateTimeModelTest(simple_datetime='2015-12-16 15:16:17')
        pk5 = obj5.pk.get()

        # check full date
        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime='2015-12-16 15:16:17')),
            {pk1, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime__gte='2015-06-12 1')),
            {pk1, pk3, pk4, pk5}
        )

        # check date
        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime__date='2015-12-16')),
            {pk1, pk5}
        )

        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime__date__lt='2015-07')),
            {pk2, pk3}
        )

        # check time
        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime__time='15:16:17')),
            {pk1, pk3, pk5}
        )
        self.assertSetEqual(
            set(DateTimeModelTest.collection(simple_datetime__time__lt='15')),
            {pk2, pk4}
        )

        # be crazy, check all for '2015-12-16 15:16:17'
        # All are ended so it should work
        self.assertSetEqual(
            set(DateTimeModelTest.collection(
                simple_datetime='2015-12-16 15:16:17',
                simple_datetime__date='2015-12-16',
                simple_datetime__time='15:16:17',
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

    def test_simple_datetime_unique_index(self):

        DateTimeModelTest(unique_simple_datetime='2001-01-01 01:01:01')
        # can add on same year (diff month/day/hour/min/sec)
        DateTimeModelTest(unique_simple_datetime='2001-02-02 02:02:02')
        # can add on same month (diff year/day/hour/min/sec)
        DateTimeModelTest(unique_simple_datetime='2002-02-03 03:03:03')
        # can add on same day (diff year/month/hour/min/sec)
        DateTimeModelTest(unique_simple_datetime='2003-03-03 04:04:04')
        # can add on same hour (diff year/month/day/min/sec)
        DateTimeModelTest(unique_simple_datetime='2004-04-04 04:05:05')
        # can add on same minute (diff year/month/day/hour/sec)
        DateTimeModelTest(unique_simple_datetime='2005-05-05 05:05:06')
        # can add on same second (diff year/month/day/hour/min)
        DateTimeModelTest(unique_simple_datetime='2006-06-06 06:06:06')

        # can add on same date (diff time)
        DateTimeModelTest(unique_simple_datetime='2006-06-06 07:07:07')
        # can add on same time (diff date)
        DateTimeModelTest(unique_simple_datetime='2007-07-07 07:07:07')

        # but cannot add the same full datetime
        with self.assertRaises(UniquenessError):
            DateTimeModelTest(unique_simple_datetime='2007-07-07 07:07:07')


class ScoredEqualIndexModel(TestRedisModel):
    collection_manager = ExtendedCollectionManager
    priority = fields.InstanceHashField()
    queue_name = fields.InstanceHashField(
        indexable=True,
        indexes=[ScoredEqualIndex.configure(score_field='priority')]
    )


class ScoredEqualIndexTestCase(LimpydBaseTest):

    def test_non_existing_field(self):
        with self.assertRaises(ImplementationError):
            class ScoredEqualIndexModelWithNonExistingScoreField(TestRedisModel):
                collection_manager = ExtendedCollectionManager
                queue_name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[ScoredEqualIndex.configure(score_field='priority')]
                )

    def test_self_referencing_field(self):
        with self.assertRaises(ImplementationError):
            class ScoredEqualIndexModelWithSelfReferencingField(TestRedisModel):
                collection_manager = ExtendedCollectionManager
                queue_name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[ScoredEqualIndex.configure(score_field='queue_name')]
                )

    def test_multi_values_field(self):
        with self.assertRaises(ImplementationError):
            class ScoredEqualIndexModelWithMultiValuesScoreField(TestRedisModel):
                collection_manager = ExtendedCollectionManager
                data = fields.ListField()
                queue_name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[ScoredEqualIndex.configure(score_field='data')]
                )

    def test_invalid_collection_manager(self):
        with self.assertRaises(ImplementationError):
            class ScoredEqualIndexModelWithInvalidCollectionManager(TestRedisModel):
                priority = fields.InstanceHashField()
                queue_name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[ScoredEqualIndex.configure(score_field='priority')]
                )

    def test_score_field_is_linked(self):
        self_field = ScoredEqualIndexModel.get_field('queue_name')
        score_field = ScoredEqualIndexModel.get_field('priority')
        self.assertEqual(self_field.index_classes[0].score_field, score_field)

    def test_index_is_well_created_on_score_field_if_no_index(self):
        score_field = ScoredEqualIndexModel.get_field('priority')
        self.assertTrue(score_field.indexable)
        self.assertEqual(len(score_field.index_classes), 1)
        self.assertTrue(issubclass(score_field.index_classes[0], _ScoredEqualIndex_RelatedIndex))

    def test_index_is_well_created_on_score_field_if_existing_index(self):
        class ScoredEqualIndexModelWithIndexedScoreField(TestRedisModel):
            collection_manager = ExtendedCollectionManager
            priority = fields.InstanceHashField(indexable=True)
            queue_name = fields.InstanceHashField(
                indexable=True,
                indexes=[ScoredEqualIndex.configure(score_field='priority')]
            )
        score_field = ScoredEqualIndexModelWithIndexedScoreField.get_field('priority')
        self.assertTrue(score_field.indexable)
        self.assertEqual(len(score_field.index_classes), 2)
        self.assertTrue(issubclass(score_field.index_classes[0], EqualIndex))
        self.assertTrue(issubclass(score_field.index_classes[1], _ScoredEqualIndex_RelatedIndex))

    def test_indexing(self):

        zrange = lambda value: self.connection.zrange(ScoredEqualIndexModel.get_field('queue_name').get_index().get_storage_key(value), 0, -1, withscores=True)
        get_all_keys = lambda: ScoredEqualIndexModel.get_field('queue_name').get_index().get_all_storage_keys()

        # with only base field: indexed
        obj1 = ScoredEqualIndexModel(queue_name='foo')
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 0)
        self.assertListEqual(zrange('foo'), [])
        # with score field: indexed
        obj1.priority.hset(1)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 1)
        self.assertListEqual(zrange('foo'), [(obj1.pk.get(), 1.0)])
        # we can change the score
        obj1.priority.hset(2)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 1)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name__in=['foo', 'bar'])), 1)
        self.assertListEqual(zrange('foo'), [(obj1.pk.get(), 2.0)])
        # removing it will remove from the index
        obj1.priority.hdel()
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 0)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name__in=['foo', 'bar'])), 0)
        self.assertListEqual(zrange('foo'), [])
        # keeping the score...
        obj1.priority.hset(1)
        # ... but removing the value will remove from the index
        obj1.queue_name.delete()
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 0)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name__in=['foo', 'bar'])), 0)
        self.assertListEqual(zrange('foo'), [])
        # add another one with same name
        obj1.queue_name.hset('foo')
        obj2 = ScoredEqualIndexModel(priority=-2, queue_name='foo')
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 2)
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name__in=['foo', 'bar'])), 2)
        self.assertListEqual(zrange('foo'), [(obj2.pk.get(), -2.0), (obj1.pk.get(), 1.0)])
        # collection result is sorted by score field
        self.assertEqual(list(ScoredEqualIndexModel.collection(queue_name='foo')), [obj2.pk.get(), obj1.pk.get()])
        # add another with another name
        obj3 = ScoredEqualIndexModel(priority=-1, queue_name='bar')
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='foo')), 2)
        self.assertListEqual(zrange('foo'), [(obj2.pk.get(), -2.0), (obj1.pk.get(), 1.0)])
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name='bar')), 1)
        self.assertListEqual(zrange('bar'), [(obj3.pk.get(), -1.0)])
        self.assertEqual(len(ScoredEqualIndexModel.collection(queue_name__in=['foo', 'bar'])), 3)

        # we can get all keys
        all_keys = get_all_keys()
        self.assertSetEqual(all_keys, {
            'tests:scoredequalindexmodel:queue_name:equal-scored:foo',
            'tests:scoredequalindexmodel:queue_name:equal-scored:bar',
        })
        # and all content via zunionstore, sorted by score field
        tmp_key = unique_key(self.connection, 'tests')
        self.connection.zunionstore(tmp_key, keys=all_keys)
        self.assertListEqual(self.connection.zrange(tmp_key, 0, -1, withscores=True), [
            (obj2.pk.get(), -2.0),
            (obj3.pk.get(), -1.0),
            (obj1.pk.get(), 1.0),
        ])
        self.connection.delete(tmp_key)

    def test_uniqueness(self):
        class ScoredEqualIndexModel2(TestRedisModel):
            collection_manager = ExtendedCollectionManager
            priority = fields.InstanceHashField()
            queue_name = fields.InstanceHashField(
                indexable=True,
                unique=True,
                indexes=[ScoredEqualIndex.configure(score_field='priority')]
            )
        zrange = lambda value: self.connection.zrange(ScoredEqualIndexModel2.get_field('queue_name').get_index().get_storage_key(value), 0, -1, withscores=True)

        obj1 = ScoredEqualIndexModel2(queue_name='foo')
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel2(queue_name='foo')
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel2(priority=1, queue_name='foo')
        obj2 = ScoredEqualIndexModel2(queue_name='bar')
        with self.assertRaises(UniquenessError):
            obj2.queue_name.hset('foo')
        self.assertEqual(obj2.queue_name.hget(), 'bar')
        obj2.priority.hset(1)
        with self.assertRaises(UniquenessError):
            obj2.queue_name.hset('foo')
        self.assertEqual(obj2.queue_name.hget(), 'bar')
        self.assertIn(obj2.pk.get(), set(ScoredEqualIndexModel2.collection(queue_name='bar')))
        self.assertNotIn(obj2.pk.get(), set(ScoredEqualIndexModel2.collection(queue_name='foo')))
        self.assertListEqual(zrange('bar'), [(obj2.pk.get(), 1.0)])
        with self.assertRaises(UniquenessError):
            obj2.hmset(priority=2, queue_name='foo')
        self.assertEqual(obj2.queue_name.hget(), 'bar')
        self.assertEqual(obj2.priority.hget(), '1')
        self.assertIn(obj2.pk.get(), set(ScoredEqualIndexModel2.collection(queue_name='bar')))
        self.assertNotIn(obj2.pk.get(), set(ScoredEqualIndexModel2.collection(queue_name='foo')))
        self.assertListEqual(zrange('bar'), [(obj2.pk.get(), 1.0)])
        obj1.priority.hset(1)
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel2(queue_name='foo')
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel2(priority=1, queue_name='foo')

    def test_indexing_listfield(self):
        class ScoredEqualIndexModel3(TestRedisModel):
            collection_manager = ExtendedCollectionManager
            score = fields.InstanceHashField()
            main_field = fields.ListField(
                indexable=True,
                indexes=[ScoredEqualIndex.configure(score_field='score')]
            )

        obj1 = ScoredEqualIndexModel3(score=1, main_field=['foo', 'bar'])
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='foo')), 1)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='bar')), 1)
        obj1.score.delete()
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='foo')), 0)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='bar')), 0)
        obj1.score.hset(2)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='foo')), 1)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='bar')), 1)

        obj2 = ScoredEqualIndexModel3(score=1, main_field=['bar', 'baz'])
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='foo')), 1)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='bar')), 2)
        self.assertEqual(len(ScoredEqualIndexModel3.collection(main_field='baz')), 1)

        # ordered by score: obj2 (1) then obj1 (2)
        self.assertEqual(list(ScoredEqualIndexModel3.collection(main_field='bar')), [obj2.pk.get(), obj1.pk.get()])
        obj2.score.hset(3)
        self.assertEqual(list(ScoredEqualIndexModel3.collection(main_field='bar')), [obj1.pk.get(), obj2.pk.get()])

    def test_uniqueness_listfield(self):
        class ScoredEqualIndexModel4(TestRedisModel):
            collection_manager = ExtendedCollectionManager
            score = fields.InstanceHashField()
            main_field = fields.ListField(
                indexable=True,
                unique=True,
                indexes=[ScoredEqualIndex.configure(score_field='score')]
            )

        obj1 = ScoredEqualIndexModel4(main_field=['foo', 'bar'])
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel4(main_field=['foo'])
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel4(score=1, main_field=['foo'])
        obj2 = ScoredEqualIndexModel4(main_field=['baz'])
        with self.assertRaises(UniquenessError):
            obj2.main_field.lpush('foo')
        self.assertEqual(obj2.main_field.lmembers(), ['baz'])
        obj2.score.hset(1)
        with self.assertRaises(UniquenessError):
            obj2.main_field.lpush('foo')
        self.assertEqual(obj2.main_field.lmembers(), ['baz'])
        self.assertIn(obj2.pk.get(), set(ScoredEqualIndexModel4.collection(main_field='baz')))
        self.assertNotIn(obj2.pk.get(), set(ScoredEqualIndexModel4.collection(main_field='foo')))
        obj1.score.hset(1)
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel4(main_field=['foo'])
        with self.assertRaises(UniquenessError):
            ScoredEqualIndexModel4(score=1, main_field=['foo'])

    def test_indexing_hashfield(self):
        class ScoredEqualIndexModel5(TestRedisModel):
            collection_manager = ExtendedCollectionManager
            score = fields.InstanceHashField()
            main_field = fields.HashField(
                indexable=True,
                indexes=[ScoredEqualIndex.configure(score_field='score')]
            )

        obj1 = ScoredEqualIndexModel5(score=1, main_field={'foo': 'XFOO', 'bar': 'XBAR'})
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__foo='XFOO')), 1)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__bar='XFOO')), 0)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__bar='XBAR')), 1)
        obj1.score.delete()
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__foo='XFOO')), 0)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__bar='XBAR')), 0)
        obj1.score.hset(2)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__foo='XFOO')), 1)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__bar='XBAR')), 1)

        obj2 = ScoredEqualIndexModel5(score=1, main_field={'foo': 'XFOO2', 'bar': 'XBAR', 'baz': 'XBAZ'})
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__foo='XFOO')), 1)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__foo='XFOO2')), 1)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__bar='XBAR')), 2)
        self.assertEqual(len(ScoredEqualIndexModel5.collection(main_field__baz='XBAZ')), 1)
        self.assertEqual(list(ScoredEqualIndexModel5.collection(main_field__bar='XBAR')), [obj2.pk.get(), obj1.pk.get()])

    def test_with_related_model(self):
        class Queue(RelatedModel):
            database = LimpydBaseTest.database
            namespace = 'tests-scored'

            name = fields.InstanceHashField(unique=True)

        class Job(RelatedModel):
            database = LimpydBaseTest.database
            namespace = 'tests-scored'

            priority = fields.InstanceHashField()
            queue = FKInstanceHashField(
                Queue,
                related_name='jobs',
                indexes=[ScoredEqualIndex.configure(score_field='priority')]
            )

        queue = Queue(name='queue1')
        job1 = Job(queue=queue, priority=2)
        job2 = Job(queue=queue, priority=1)
        self.assertEqual(job1.queue.instance(), queue)
        self.assertEqual(job2.queue.instance(), queue)
        # reverse collection get results ordered by score field
        self.assertEqual(list(queue.jobs().instances()), [job2, job1])
        job1.priority.hset(-1)
        self.assertEqual(list(queue.jobs().instances()), [job1, job2])

        # if we remove the priority, we can still access the queue from the job, but not the
        # reverse because the job is not indexed anymore
        job2.priority.delete()
        self.assertEqual(job1.queue.instance(), queue)
        self.assertEqual(job2.queue.instance(), queue)
        self.assertEqual(list(queue.jobs().instances()), [job1])

        # we can call `get_storage_key` with the instance
        self.assertEqual(
            Job.get_field('queue').get_index().get_storage_key(queue),
            'tests-scored:job:queue:equal-scored:1'
        )


class EqualIndexWithOneFieldModel(TestRedisModel):
    collection_manager = ExtendedCollectionManager
    priority = fields.InstanceHashField()
    details = fields.InstanceHashField(indexable=True)
    name = fields.InstanceHashField(
        indexable=True,
        indexes=[EqualIndexWith.configure(other_fields=['priority'])]
    )


class EqualIndexWithManyFieldsModel(TestRedisModel):
    collection_manager = ExtendedCollectionManager
    priority = fields.InstanceHashField()
    details = fields.InstanceHashField(indexable=True)
    bar = fields.InstanceHashField()
    name = fields.InstanceHashField(
        indexable=True,
        indexes=[EqualIndexWith.configure(other_fields=['priority', 'foo', 'bar'])]
    )
    foo = fields.InstanceHashField()


class EqualIndexWithTestCase(LimpydBaseTest):

    def test_one_non_existing_field(self):
        with self.assertRaises(ImplementationError):
            class EqualIndexWithModelWithNonExistingOtherField(TestRedisModel):
                name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[EqualIndexWith.configure(other_fields=['priority'])]
                )

    def test_one_non_existing_field_among_others(self):
        with self.assertRaises(ImplementationError):
            class EqualIndexWithModelWithNonExistingOtherField(TestRedisModel):
                priority = fields.InstanceHashField()
                name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[EqualIndexWith.configure(other_fields=['priority', 'foo'])]
                )

    def test_one_self_referencing_field(self):
        with self.assertRaises(ImplementationError):
            class EqualIndexWithModelWithSelfReferencingField(TestRedisModel):
                name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[EqualIndexWith.configure(other_fields=['name'])]
                )

    def test_self_referencing_field_among_others(self):
        with self.assertRaises(ImplementationError):
            class EqualIndexWithModelWithSelfReferencingField(TestRedisModel):
                priority = fields.InstanceHashField()
                name = fields.InstanceHashField(
                    indexable=True,
                    indexes=[EqualIndexWith.configure(other_fields=['priority', 'name'])]
                )

    def test_other_field_is_linked(self):
        self_field = EqualIndexWithOneFieldModel.get_field('name')
        self.assertEqual(self_field.index_classes[0].other_fields, [
            EqualIndexWithOneFieldModel.get_field('priority'),
        ])

    def test_other_fields_are_linked_in_order(self):
        self_field = EqualIndexWithManyFieldsModel.get_field('name')
        self.assertEqual(self_field.index_classes[0].other_fields, [
            EqualIndexWithManyFieldsModel.get_field('priority'),
            EqualIndexWithManyFieldsModel.get_field('foo'),
            EqualIndexWithManyFieldsModel.get_field('bar'),
        ])

    def test_index_is_well_created_on_other_one_field_if_no_index(self):
        other_field = EqualIndexWithOneFieldModel.get_field('priority')
        self.assertTrue(other_field.indexable)
        self.assertEqual(len(other_field.index_classes), 1)
        self.assertTrue(issubclass(other_field.index_classes[0], _EqualIndexWith_RelatedIndex))

    def test_index_is_well_created_on_other_one_field_if_existing_index(self):
        class EqualIndexWithOneIndexedFieldModel(TestRedisModel):
            priority = fields.InstanceHashField(indexable=True)
            name = fields.InstanceHashField(
                indexable=True,
                indexes=[EqualIndexWith.configure(other_fields=['priority'])]
            )
        other_field = EqualIndexWithOneIndexedFieldModel.get_field('priority')
        self.assertTrue(other_field.indexable)
        self.assertEqual(len(other_field.index_classes), 2)
        self.assertTrue(issubclass(other_field.index_classes[0], EqualIndex))
        self.assertTrue(issubclass(other_field.index_classes[1], _EqualIndexWith_RelatedIndex))

    def test_index_is_well_created_on_other_many_fields_if_no_index(self):
        other_fields = {
            name: EqualIndexWithManyFieldsModel.get_field(name)
            for name in ['priority', 'foo', 'bar']
        }
        for field_name, field in other_fields.items():
            with self.subTest(field_name=field_name):
                self.assertTrue(field.indexable)
                self.assertEqual(len(field.index_classes), 1)
                self.assertTrue(issubclass(field.index_classes[0], _EqualIndexWith_RelatedIndex))

    def test_index_is_well_created_on_other_many_fields_if_existing_index(self):
        class EqualIndexWithManyIndexedFieldsModel(TestRedisModel):
            priority = fields.InstanceHashField(indexable=True)
            foo = fields.InstanceHashField(indexable=True)
            bar = fields.InstanceHashField(indexable=True)
            name = fields.InstanceHashField(
                indexable=True,
                indexes=[EqualIndexWith.configure(other_fields=['priority', 'foo', 'bar'])]
            )
        other_fields = {
            name: EqualIndexWithManyIndexedFieldsModel.get_field(name)
            for name in ['priority', 'foo', 'bar']
        }
        for field_name, field in other_fields.items():
            with self.subTest(field_name=field_name):
                self.assertTrue(field.indexable)
                self.assertEqual(len(field.index_classes), 2)
                self.assertTrue(issubclass(field.index_classes[0], EqualIndex))
                self.assertTrue(issubclass(field.index_classes[1], _EqualIndexWith_RelatedIndex))

    def test_indexing_one_other_field(self):
        pk1 = EqualIndexWithOneFieldModel(name='foo', priority=1, details='d1').pk.get()
        pk2 = EqualIndexWithOneFieldModel(name='foo', priority=1, details='d2').pk.get()
        pk3 = EqualIndexWithOneFieldModel(name='foo', priority=2, details='d1').pk.get()
        pk4 = EqualIndexWithOneFieldModel(name='bar', priority=1).pk.get()

        collection = EqualIndexWithOneFieldModel.collection

        # both filters in same call
        self.assertSetEqual(set(collection(name='foo', priority=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo', priority__eq=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo', priority=11)), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority=11)), set())
        self.assertSetEqual(set(collection(name='foo', priority__eq=11)), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=11)), set())
        self.assertSetEqual(set(collection(name='foofoo', priority=1)), set())
        self.assertSetEqual(set(collection(name__eq='foofoo', priority=1)), set())
        self.assertSetEqual(set(collection(name='foofoo', priority__eq=1)), set())
        self.assertSetEqual(set(collection(name__eq='foofoo', priority__eq=1)), set())
        # field can be used many times
        # in index order
        self.assertSetEqual(set(collection(name='foo', priority=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name='foo', priority=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name='foo', priority__eq=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name='foo', priority__eq=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name='foo', priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo', priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo', priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo', priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo', priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        # or any order
        self.assertSetEqual(set(collection(priority=1).filter(priority=2, name='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority=2, name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=2, name='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=2, name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=2, name='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=2, name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=2, name='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=2, name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority=1, name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority=1, name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=1, name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=1, name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=1, name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=1, name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=1, name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=1, name__eq='foo')), {pk1, pk2})
        # filters in different calls
        # in index order
        self.assertSetEqual(set(collection(name='foo').filter(priority=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo').filter(priority__eq=1)), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority__eq=1)), {pk1, pk2})
        # or any order
        self.assertSetEqual(set(collection(priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        # repeated in different calls
        # in index order
        self.assertSetEqual(set(collection(name='foo').filter(priority=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name='foo').filter(priority=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name='foo').filter(priority__eq=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name='foo').filter(priority__eq=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority__eq=1).filter(name='bar')), set())
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority__eq=1).filter(name__eq='bar')), set())
        self.assertSetEqual(set(collection(name='foo').filter(priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo').filter(priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo').filter(priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name='foo').filter(priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(name__eq='foo').filter(priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        # or any order
        self.assertSetEqual(set(collection(priority=1).filter(priority=2).filter(name='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority=2).filter(name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=2).filter(name='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=2).filter(name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=2).filter(name='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=2).filter(name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=2).filter(name='foo')), set())
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=2).filter(name__eq='foo')), set())
        self.assertSetEqual(set(collection(priority=1).filter(priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority=1).filter(priority__eq=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority=1).filter(name__eq='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=1).filter(name='foo')), {pk1, pk2})
        self.assertSetEqual(set(collection(priority__eq=1).filter(priority__eq=1).filter(name__eq='foo')), {pk1, pk2})

        # cannot filter a field on the multi-index alone
        with self.assertRaises(ImplementationError):
            set(collection(name='foo'))
        with self.assertRaises(ImplementationError):
            set(collection(name__eq='foo'))
        with self.assertRaises(ImplementationError):
            set(collection(priority=1))
        with self.assertRaises(ImplementationError):
            set(collection(priority__eq=1))

        # quick check other values
        self.assertSetEqual(set(collection(name='foo', priority=2)), {pk3})
        self.assertSetEqual(set(collection(name='bar', priority=1)), {pk4})

        # can filter with other fields
        self.assertSetEqual(set(collection(details='d1')), {pk1, pk3})
        self.assertSetEqual(set(collection(details='d1', name='foo', priority=1)), {pk1})
        self.assertSetEqual(set(collection(details='d1', name='foo', priority=2)), {pk3})
        with self.assertRaises(ImplementationError):
            set(collection(name__eq='foo', details='d1'))

        # test in suffix
        self.assertSetEqual(set(collection(name='foo', priority__in=[1, 2])), {pk1, pk2, pk3})
        self.assertSetEqual(set(collection(name='foo').filter(priority__in=[1, 2])), {pk1, pk2, pk3})
        self.assertSetEqual(set(collection(priority__in=[1, 2]).filter(name='foo')), {pk1, pk2, pk3})

        self.assertSetEqual(set(collection(name__in=['foo', 'bar'], priority=1)), {pk1, pk2, pk4})
        self.assertSetEqual(set(collection(name__in=['foo', 'bar']).filter(priority=1)), {pk1, pk2, pk4})
        self.assertSetEqual(set(collection(priority=1).filter(name__in=['foo', 'bar'])), {pk1, pk2, pk4})

        self.assertSetEqual(set(collection(name__in=['foo', 'bar'], priority__in=[1, 2])), {pk1, pk2, pk3, pk4})
        self.assertSetEqual(set(collection(name__in=['foo', 'bar'], priority__in=[1, 2], details='d1')), {pk1, pk3})
        self.assertSetEqual(set(collection(name__in=['foo', 'bar']).filter(priority__in=[1, 2])), {pk1, pk2, pk3, pk4})
        self.assertSetEqual(set(collection(priority__in=[1, 2]).filter(name__in=['foo', 'bar'])), {pk1, pk2, pk3, pk4})

    def test_indexing_one_many_other_fields(self):
        pk1 = EqualIndexWithManyFieldsModel(name='foo', priority=1, details='d1', foo=1, bar=2).pk.get()
        pk2 = EqualIndexWithManyFieldsModel(name='foo', priority=1, details='d2', foo=1, bar=2).pk.get()
        pk3 = EqualIndexWithManyFieldsModel(name='foo', priority=1, details='d1', foo=1, bar=22).pk.get()

        collection = EqualIndexWithManyFieldsModel.collection

        # classic filter
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar=2)), {pk1, pk2})
        # repeated but different
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar=2).filter(priority=2)), set())
        # repeated but same
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar=2).filter(priority=1)), {pk1, pk2})
        # with main field filtered later
        self.assertSetEqual(set(collection(priority=1, foo=1, bar=2).filter(name='bar')), set())
        self.assertSetEqual(set(collection(priority=1, foo=1, bar=2).filter(name='foo')), {pk1, pk2})
        # with other field
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar=2, details='d1')), {pk1})
        # partial index usage forbidden
        with self.assertRaises(ImplementationError):
            set(collection(priority=1, foo=1, bar=2))
        with self.assertRaises(ImplementationError):
            set(collection(priority=1, foo=1, bar=2, details='d1'))
        # test in suffix
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar__in=[2, 22])), {pk1, pk2, pk3})
        self.assertSetEqual(set(collection(name__in=['foo', 'bar'], priority__in=[1, 2], foo=1, bar__in=[2, 22])), {pk1, pk2, pk3})
        self.assertSetEqual(set(collection(name='foo', priority=1, foo=1, bar__in=[2, 22], details='d1')), {pk1, pk3})

    def test_many_multi_indexes(self):
        class EqualIndexWithManyIndexesModel(TestRedisModel):
            # foo is filterable:
            # - alone
            # - with bar+baz+qux via its own index
            # - with bar+baz via the index on baz
            # bar is filterable
            # - with foo+bar+qux via index on foo
            # - with foo+baz via the index on baz
            # baz is filterable:
            # - alone
            # - with foo+bar+qux via the index on fo
            # - with foo+bar via its own index
            # - with qux via its own index
            # qux is filterable
            # - alone
            # - with foo+bar+baz via index on foo
            # - with baz via index on baz
            foo = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndex,
                    EqualIndexWith.configure(other_fields=['bar', 'baz', 'qux']),
                    EqualIndexWith.configure(other_fields=['bar', 'baz', 'qux']),
                ]
            )
            bar = fields.InstanceHashField()
            baz = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['foo', 'bar']),
                    EqualIndexWith.configure(other_fields=['qux']),
                    # intentionally doubled, will be ignored while filtering (but not while indexing)
                    EqualIndexWith.configure(other_fields=['foo', 'bar']),
                ]
            )
            qux = fields.InstanceHashField(indexable=True)

        pk1 = EqualIndexWithManyIndexesModel(foo=1, bar=2, baz=3, qux=4).pk.get()
        pk2 = EqualIndexWithManyIndexesModel(foo=1, bar=2, baz=3, qux=4).pk.get()
        pk3 = EqualIndexWithManyIndexesModel(foo=1, bar=22, baz=333, qux=4444).pk.get()

        collection = EqualIndexWithManyIndexesModel.collection

        self.assertSetEqual(set(collection(foo=1)), {pk1, pk2, pk3})
        self.assertSetEqual(set(collection(foo=1, bar=2, baz=3, qux=4)), {pk1, pk2})  # many indexes usable
        self.assertSetEqual(set(collection(foo=1, bar=2, baz=3)), {pk1, pk2})
        with self.assertRaises(ImplementationError):
            set(collection(foo=1, bar=2))
        with self.assertRaises(ImplementationError):
            set(collection(foo=1, baz=3))
        with self.assertRaises(ImplementationError):
            set(collection(foo=1, bar=2, qux=4))
        self.assertSetEqual(set(collection(foo=1, baz=3, qux=4)), {pk1, pk2})  # use mutli baz+qux and single foo
        with self.assertRaises(ImplementationError):
            set(collection(bar=2))
        with self.assertRaises(ImplementationError):
            set(collection(baz=3))
        self.assertSetEqual(set(collection(baz=3, qux=4)), {pk1, pk2})
        self.assertSetEqual(set(collection(qux=4)), {pk1, pk2})

    def test_uniqueness(self):
        class EqualIndexWithOneFieldUniqueModel(TestRedisModel):
            priority = fields.InstanceHashField(indexable=True)
            details = fields.InstanceHashField(indexable=True)
            name = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['priority'], unique=True),
                    EqualIndex,
                ]
            )
        collection = EqualIndexWithOneFieldUniqueModel.collection
        obj1 = EqualIndexWithOneFieldUniqueModel(name='foo', priority=1, details='d1')
        pk1 = obj1.pk.get()
        with self.assertRaises(UniquenessError):
            EqualIndexWithOneFieldUniqueModel(name='foo', priority=1, details='d2')
        self.assertEqual(len(collection()), 1)
        self.assertEqual(len(collection(priority=1)), 1)
        self.assertEqual(len(collection(name='foo')), 1)
        obj2 = EqualIndexWithOneFieldUniqueModel(name='foo')
        pk2 = obj2.pk.get()
        with self.assertRaises(UniquenessError):
            obj2.priority.hset(1)
        self.assertIsNone(obj2.priority.hget())
        self.assertSetEqual(set(collection(priority=1)), {pk1})
        self.assertSetEqual(set(collection(priority=2)), set())
        self.assertSetEqual(set(collection(priority=1, name='foo')), {pk1})
        obj2.priority.hset(2)
        with self.assertRaises(UniquenessError):
            obj2.priority.hset(1)
        self.assertEqual(obj2.priority.hget(), '2')
        self.assertSetEqual(set(collection(priority=1)), {pk1})
        self.assertSetEqual(set(collection(priority=2)), {pk2})
        self.assertSetEqual(set(collection(priority=1, name='foo')), {pk1})
        self.assertSetEqual(set(collection(priority=2, name='foo')), {pk2})
        with self.assertRaises(UniquenessError):
            obj1.priority.hset(2)
        self.assertEqual(obj1.priority.hget(), '1')
        self.assertSetEqual(set(collection(priority=1)), {pk1})
        self.assertSetEqual(set(collection(priority=2)), {pk2})
        self.assertSetEqual(set(collection(priority=1, name='foo')), {pk1})
        self.assertSetEqual(set(collection(priority=2, name='foo')), {pk2})
        obj1.priority.hset(3)
        obj2.priority.hset(1)
        with self.assertRaises(UniquenessError):
            obj2.priority.hset(3)
        self.assertEqual(obj2.priority.hget(), '1')
        self.assertSetEqual(set(collection(priority=1)), {pk2})
        self.assertSetEqual(set(collection(priority=2)), set())
        self.assertSetEqual(set(collection(priority=3)), {pk1})
        self.assertSetEqual(set(collection(priority=1, name='foo')), {pk2})
        self.assertSetEqual(set(collection(priority=2, name='foo')), set())
        self.assertSetEqual(set(collection(priority=3, name='foo')), {pk1})

    def test_with_listfield(self):
        class EqualIndexWithListFieldModel(TestRedisModel):
            foo = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['bar', 'baz'], unique=False),
                    EqualIndex,
                ]
            )
            foo_unique = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['bar', 'baz'], unique=True),
                    EqualIndex,
                ]
            )
            bar = fields.ListField(indexable=True)
            baz = fields.ListField()

        collection = EqualIndexWithListFieldModel.collection

        obj1 = EqualIndexWithListFieldModel(foo='foo', bar=['BAR1', 'BAR2'], baz=['BAZ1'])
        pk1 = obj1.pk.get()
        obj2 = EqualIndexWithListFieldModel(foo='foo', bar=['BAR2'], baz=['BAZ1'])
        pk2 = obj2.pk.get()
        obj3 = EqualIndexWithListFieldModel(foo='foo', bar=['BAR3'], baz=['BAZ1'])
        pk3 = obj3.pk.get()
        obj4 = EqualIndexWithListFieldModel(foo='foo', bar=['BAR4'], baz=['BAZ1'])
        pk4 = obj4.pk.get()
        self.assertSetEqual(set(collection(foo='foo', bar='BAR1', baz='BAZ1')), {pk1})
        self.assertSetEqual(set(collection(foo='foo', bar='BAR2', baz='BAZ1')), {pk1, pk2})
        self.assertSetEqual(set(collection(foo='foo', bar__in=['BAR2', 'BAR3'], baz='BAZ1')), {pk1, pk2, pk3})

        obj1.foo_unique.hset('FOO')
        with self.assertRaises(UniquenessError):
            obj2.foo_unique.hset('FOO')
        self.assertIsNone(obj2.foo_unique.hget())
        self.assertSetEqual(set(collection(foo_unique='FOO')), {pk1})
        obj4.foo_unique.hset('FOO')
        with self.assertRaises(UniquenessError):
            obj4.bar.lpush('BAR1')
        self.assertListEqual(obj4.bar.lmembers(), ['BAR4'])
        self.assertSetEqual(set(collection(bar='BAR4')), {pk4})
        self.assertSetEqual(set(collection(bar='BAR1')), {pk1})
        with self.assertRaises(UniquenessError):
            EqualIndexWithListFieldModel(foo_unique='FOO', bar=['BAR1', 'BARzz'], baz=['BAZ1'])
        self.assertEqual(len(collection()), 4)
        obj1.bar.delete()
        obj4.bar.lpush('BAR1')
        self.assertSetEqual(set(collection(bar='BAR4')), set(pk4))
        self.assertSetEqual(set(collection(bar='BAR1')), {pk4})

        # with main field being a listfield too
        class EqualIndexWithListFieldModel2(TestRedisModel):
            foo = fields.ListField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['bar', 'baz'], unique=False),
                    EqualIndex,
                ]
            )
            bar = fields.ListField()
            baz = fields.InstanceHashField()

        collection = EqualIndexWithListFieldModel2.collection
        obj1 = EqualIndexWithListFieldModel2(foo=['FOO1', 'FOO2'], bar=['BAR1', 'BAR2'], baz='baz')
        pk1 = obj1.pk.get()
        obj2 = EqualIndexWithListFieldModel2(foo=['FOO1'], bar=['BAR1', 'BAR2'], baz='baz')
        pk2 = obj2.pk.get()
        obj3 = EqualIndexWithListFieldModel2(foo=['FOO3'], bar=['BAR2'], baz='baz')
        pk3 = obj3.pk.get()
        EqualIndexWithListFieldModel2(foo=['FOO3'], bar=['BAR3'], baz='baz')
        self.assertSetEqual(set(collection(foo='FOO1', bar='BAR1', baz='baz')), {pk1, pk2})
        self.assertSetEqual(set(collection(foo='FOO2', bar='BAR1', baz='baz')), {pk1})
        self.assertSetEqual(set(collection(foo__in=['FOO1', 'FOO2', 'FOO3'], bar__in=['BAR1', 'BAR2'], baz='baz')), {pk1, pk2, pk3})

    def test_with_hashfield(self):
        # no unique if hashfield
        with self.assertRaises(ImplementationError):
            class EqualIndexWithUniqueWithHashFieldModel(TestRedisModel):
                foo = fields.InstanceHashField(
                    indexable=True,
                    indexes=[
                        EqualIndexWith.configure(other_fields=['bar', 'baz'], unique=True),
                        EqualIndex,
                    ]
                )
                bar = fields.HashField()
                baz = fields.InstanceHashField()

        class EqualIndexWithHashFieldModel(TestRedisModel):
            foo = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['bar', 'baz']),
                    EqualIndex,
                ]
            )
            bar = fields.HashField()
            baz = fields.HashField()

        collection = EqualIndexWithHashFieldModel.collection

        obj1 = EqualIndexWithHashFieldModel(foo='foo', bar={'BAR1': 'BAR11', 'BAR2': 'BAR22'}, baz={'BAZ1': 'BAZ11'})
        pk1 = obj1.pk.get()
        obj2 = EqualIndexWithHashFieldModel(foo='foo', bar={'BAR1': 'BAR11', 'BAR2': 'BAR22x'}, baz={'BAZ1': 'BAZ11'})
        pk2 = obj2.pk.get()
        obj3 = EqualIndexWithHashFieldModel(foo='foo', bar={'BAR1': 'BAR11x', 'BAR2': 'BAR22'}, baz={'BAZ1': 'BAZ11'})
        pk3 = obj3.pk.get()
        EqualIndexWithHashFieldModel(foo='foo', bar={'BAR1': 'BAR11x'}, baz={'BAZ1': 'BAZ11x'})
        self.assertSetEqual(set(collection(foo='foo', bar__BAR1='BAR11', baz__BAZ1='BAZ11')), {pk1, pk2})
        self.assertSetEqual(set(collection(foo='foo', bar__BAR2='BAR22', baz__BAZ1='BAZ11')), {pk1, pk3})
        self.assertSetEqual(set(collection(foo='foo', bar__BAR2='BAR22x', baz__BAZ1='BAZ11')), {pk2})
        self.assertSetEqual(set(collection(foo='foo', bar__BAR2__in=['BAR22', 'BAR22x'], baz__BAZ1='BAZ11')), {pk1, pk2, pk3})

        # with main field being a hashfield too
        class EqualIndexWithHashFieldModel2(TestRedisModel):
            foo = fields.HashField(
                indexable=True,
                indexes=[
                    EqualIndexWith.configure(other_fields=['bar', 'baz']),
                    EqualIndex,
                ]
            )
            bar = fields.HashField()
            baz = fields.InstanceHashField()

        collection = EqualIndexWithHashFieldModel2.collection

        obj1 = EqualIndexWithHashFieldModel2(foo={'FOO1': 'FOO11', 'FOO2': 'FOO22'}, bar={'BAR1': 'BAR11', 'BAR2': 'BAR22'}, baz='baz')
        pk1 = obj1.pk.get()
        obj2 = EqualIndexWithHashFieldModel2(foo={'FOO1': 'FOO11', 'FOO2': 'FOO22x'}, bar={'BAR1': 'BAR11', 'BAR2': 'BAR22x'}, baz='baz')
        pk2 = obj2.pk.get()
        obj3 = EqualIndexWithHashFieldModel2(foo={'FOO1': 'FOO11x', 'FOO2': 'FOO22'}, bar={'BAR1': 'BAR11', 'BAR2': 'BAR22'}, baz='baz')
        pk3 = obj3.pk.get()
        EqualIndexWithHashFieldModel2(foo={'FOO1': 'FOO11x'}, bar={'BAR1': 'BAR11'}, baz='baz')
        self.assertSetEqual(set(collection(foo__FOO1='FOO11', bar__BAR1='BAR11', baz='baz')), {pk1, pk2})
        self.assertSetEqual(set(collection(foo__FOO2='FOO22', bar__BAR1='BAR11', baz='baz')), {pk1, pk3})
        self.assertSetEqual(set(collection(foo__FOO1='FOO11x', bar__BAR2='BAR22', baz='baz')), {pk3})
        self.assertSetEqual(set(collection(foo__FOO1__in=['FOO11', 'FOO11x'], bar__BAR2__in=['BAR22', 'BAR22x'], baz='baz')), {pk1, pk2, pk3})

        class Queue(TestRedisModel):
            name = fields.InstanceHashField(
                indexable=True,
                indexes=[
                    EqualIndex,  # allow to filter on ``name`` only
                    EqualIndexWith.configure(other_fields=['priority'], unique=True),  # name and priority are unique together
                ]
            )
            priority = fields.InstanceHashField()

        queue = Queue(name='foo', priority=1)
        Queue(name='foo', priority=2)
        list
