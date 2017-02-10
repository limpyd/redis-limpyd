# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from limpyd import fields
from limpyd.contrib.indexes import MultiIndexes
from limpyd.exceptions import ImplementationError, UniquenessError
from limpyd.indexes import BaseIndex, NumberRangeIndex, TextRangeIndex, EqualIndex

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
