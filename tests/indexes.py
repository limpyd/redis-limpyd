# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from limpyd import fields
from limpyd.database import RedisDatabase
from limpyd.exceptions import ImplementationError, UniquenessError
from limpyd.indexes import EqualIndex, TextRangeIndex

from .base import LimpydBaseTest, TEST_CONNECTION_SETTINGS, skip_if_no_zrangebylex
from .model import Bike, Email, TestRedisModel


class ReverseEqualIndex(EqualIndex):
    handled_suffixes = {'reverse_eq'}
    index_key_name = 'reverse-equal'

    def transform_normalized_value_for_storage(self, value):
        return value[::-1]


class ShortcutTestCase(LimpydBaseTest):

    def test_shortcuts_for_instance_index(self):
        bike = Bike(name='rosalie')
        instance_index = EqualIndex(bike.get_field('name'))
        self.assertIs(instance_index.connection, bike.database.connection)
        self.assertIs(instance_index.model, Bike)
        self.assertIs(instance_index.instance, bike)

    def test_shortcuts_for_model_index(self):
        instance_index = EqualIndex(Bike.get_field('name'))
        self.assertIs(instance_index.connection, Bike.database.connection)
        self.assertIs(instance_index.model, Bike)
        with self.assertRaises(AttributeError):
            instance_index.instance


class EqualIndexKeyTestCase(unittest.TestCase):

    def test_equal_index_key(self):
        index = EqualIndex(Bike.get_field('name'))
        key = index.get_storage_key('rosalie')
        self.assertEqual(key, 'tests:bike:name:rosalie')
        filter_key, key_type, is_tmp = index.get_filtered_key(None, 'rosalie')
        self.assertEqual(filter_key, 'tests:bike:name:rosalie')
        self.assertEqual(key_type, 'set')
        self.assertFalse(is_tmp)

    def test_other_normalize_value_method(self):
        index = ReverseEqualIndex(Bike.get_field('name'))
        key = index.get_storage_key('rosalie')
        self.assertEqual(key, 'tests:bike:name:reverse-equal:eilasor')

    def test_number_value(self):
        index = EqualIndex(Bike.get_field('name'))
        key = index.get_storage_key(123)
        self.assertEqual(key, 'tests:bike:name:123')

    def test_bytes_value(self):
        index = EqualIndex(Bike.get_field('name'))
        key = index.get_storage_key(b'rosalie')
        self.assertEqual(key, 'tests:bike:name:rosalie')

    def test_works_for_hash_fields(self):
        index = EqualIndex(Email.get_field('headers'))
        key = index.get_storage_key('from', 'foo@bar.com')
        self.assertEqual(key, 'tests:email:headers:from:foo@bar.com')

# Below some indexes,, fields, and a database for DefaultIndexesTestCase


class TestDefaultIndexForDatabase(EqualIndex):
    pass


class TestDefaultIndexForModel(EqualIndex):
    pass


class TestDefaultIndexForField(EqualIndex):
    pass


class TestDatabase(RedisDatabase):
    default_indexes = [TestDefaultIndexForDatabase]


class TestStringField(fields.StringField):
    default_indexes = [TestDefaultIndexForField]


class DefaultIndexesTestCase(LimpydBaseTest):

    def test_default_is_EqualIndex(self):
        field = Bike.get_field('name')
        index = field._indexes[0]
        self.assertIs(index.__class__, EqualIndex)

    def test_from_field_if_defined(self):
        class TestDefaultIndexModel1(TestRedisModel):
            database = TestDatabase(**TEST_CONNECTION_SETTINGS)
            default_indexes = [TestDefaultIndexForModel]
            name = TestStringField(indexable=True)

        field = TestDefaultIndexModel1.get_field('name')
        index = field._indexes[0]
        self.assertIs(index.__class__, TestDefaultIndexForField)

    def test_from_model_if_defined(self):
        class TestDefaultIndexModel2(TestRedisModel):
            database = TestDatabase(**TEST_CONNECTION_SETTINGS)
            default_indexes = [TestDefaultIndexForModel]
            name = fields.StringField(indexable=True)

        field = TestDefaultIndexModel2.get_field('name')
        index = field._indexes[0]
        self.assertIs(index.__class__, TestDefaultIndexForModel)

    def test_from_database_if_defined(self):
        class TestDefaultIndexModel3(TestRedisModel):
            database = TestDatabase(**TEST_CONNECTION_SETTINGS)
            name = fields.StringField(indexable=True)

        field = TestDefaultIndexModel3.get_field('name')
        index = field._indexes[0]
        self.assertIs(index.__class__, TestDefaultIndexForDatabase)


class PassIndexesToField(LimpydBaseTest):

    def test_cannot_pass_indexes_if_not_indexable(self):
        with self.assertRaises(ImplementationError):
            class TestPassIndexesModel1(TestRedisModel):
                name = fields.StringField(indexable=False, indexes=[EqualIndex])

    def test_default_indexes_not_used_if_passed_to_field(self):
        class TestPassIndexesModel2(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[ReverseEqualIndex])

        field = TestPassIndexesModel2.get_field('name')
        index = field._indexes[0]
        self.assertIs(index.__class__, ReverseEqualIndex)

    def test_many_indexes_should_be_used_correctly(self):
        class TestPassIndexesModel3(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[EqualIndex, ReverseEqualIndex])

        obj1 = TestPassIndexesModel3(name='foo')
        pk1 = obj1.pk.get()
        obj2 = TestPassIndexesModel3(name='oof')
        pk2 = obj2.pk.get()

        name1 = obj1.get_field('name')
        self.assertEqual(len(name1._indexes), 2)
        self.assertIs(name1._indexes[0].__class__, EqualIndex)
        self.assertIs(name1._indexes[1].__class__, ReverseEqualIndex)

        self.assertEqual(set(TestPassIndexesModel3.collection(name='foo')), {pk1})
        self.assertEqual(set(TestPassIndexesModel3.collection(name__eq='foo')), {pk1})
        self.assertEqual(set(TestPassIndexesModel3.collection(name__eq='oof')), set(pk2))
        self.assertEqual(set(TestPassIndexesModel3.collection(name__reverse_eq='oof')), {pk1})
        self.assertEqual(set(TestPassIndexesModel3.collection(name__reverse_eq='foo')), {pk2})
        self.assertEqual(set(TestPassIndexesModel3.collection(name='foo', name__reverse_eq='oof')), {pk1})
        self.assertEqual(set(TestPassIndexesModel3.collection(name='foo', name__reverse_eq='foo')), set())


class TextRangeIndexTestModel(TestRedisModel):
    name = fields.StringField(indexable=True, indexes=[TextRangeIndex])
    category = fields.StringField(indexable=True)


@unittest.skipIf(*skip_if_no_zrangebylex)
class TextRangeIndexTestCase(LimpydBaseTest):

    def setUp(self):
        super(TextRangeIndexTestCase, self).setUp()
        self.obj1 = TextRangeIndexTestModel(name='foo')
        self.pk1 = self.obj1.pk.get()
        self.obj2 = TextRangeIndexTestModel(name='bar')
        self.pk2 = self.obj2.pk.get()
        self.obj3 = TextRangeIndexTestModel(name='foobar')
        self.pk3 = self.obj3.pk.get()
        self.obj4 = TextRangeIndexTestModel(name='foobar')  # same as before
        self.pk4 = self.obj4.pk.get()
        self.obj5 = TextRangeIndexTestModel(name='qux')
        self.pk5 = self.obj5.pk.get()

    def test_storage_key_for_single_field(self):

        field = self.obj1.get_field('name')
        index = field._indexes[0]
        key = index.get_storage_key('foo')

        self.assertEqual(key, 'tests:textrangeindextestmodel:name:text-range')

    def test_storage_key_for_hash_field(self):
        class TextRangeIndexTestModel1(TestRedisModel):
            data = fields.HashField(indexable=True, indexes=[TextRangeIndex])

        obj = TextRangeIndexTestModel1(data={'foo': 'bar'})

        field = obj.get_field('data')
        index = field._indexes[0]
        key = index.get_storage_key('foo', 'bar')

        self.assertEqual(key, 'tests:textrangeindextestmodel1:data:foo:text-range')

    def test_stored_data(self):

        field = self.obj1.get_field('name')
        index = field._indexes[0]
        key = index.get_storage_key(None)  # value not used in this index for the storage key

        key_type = self.connection.type(key)
        self.assertEqual(key_type, 'zset')

        data = self.connection.zrange(key, 0, -1, withscores=True)

        # all entries should:
        # - have the same score of 0
        # - be returned in a lexicographical order
        # - have the pk appended
        self.assertEqual(data, [
            ('bar:TEXT-RANGE-SEPARATOR:%s' % self.pk2, 0.0),
            ('foo:TEXT-RANGE-SEPARATOR:%s' % self.pk1, 0.0),
            ('foobar:TEXT-RANGE-SEPARATOR:%s' % self.pk3, 0.0),
            ('foobar:TEXT-RANGE-SEPARATOR:%s' % self.pk4, 0.0),
            ('qux:TEXT-RANGE-SEPARATOR:%s' % self.pk5, 0.0),
        ])

    def test_uniqueness(self):
        class TextRangeIndexTestModel2(TestRedisModel):
            name = fields.StringField(indexable=True, unique=True, indexes=[TextRangeIndex])

        # first object with foo
        TextRangeIndexTestModel2(name='foo')

        # new object with foobar but update to foo: should fail
        obj = TextRangeIndexTestModel2(name='foobar')
        with self.assertRaises(UniquenessError):
            obj.name.set('foo')

        # new object with foo: should fail
        with self.assertRaises(UniquenessError):
            TextRangeIndexTestModel2(name='foo')

    def test_get_filtered_key(self):

        index = TextRangeIndexTestModel.get_field('name')._indexes[0]

        with self.assertRaises(ImplementationError):
            index.get_filtered_key('gt', 'bar', accepted_key_types={'list'})

        index_key, key_type, is_tmp = index.get_filtered_key('gt', 'bar', accepted_key_types={'set'})
        self.assertEqual(self.connection.type(index_key), 'set')
        self.assertEqual(key_type, 'set')
        self.assertTrue(is_tmp)
        data = self.connection.smembers(index_key)
        self.assertEqual(data, {
            self.pk1,  # foo gt bar
            self.pk3,  # foobar gt bar
            self.pk4,  # foobar gt bar
            self.pk5,  # qux gt bar
        })

        index_key, key_type, is_tmp = index.get_filtered_key('lte', 'foo', accepted_key_types={'zset'})
        self.assertEqual(self.connection.type(index_key), 'zset')
        self.assertEqual(key_type, 'zset')
        self.assertTrue(is_tmp)
        data = self.connection.zrange(index_key, 0, -1, withscores=1)
        self.assertEqual(data, [
            (self.pk2, 0.0),  # bar lte foo
            (self.pk1, 1.0),  # foo lte foo
        ])

    def test_eq(self):
        # without suffix
        data = set(TextRangeIndexTestModel.collection(name__eq='foo'))
        self.assertSetEqual(data, {self.pk1})
        data = set(TextRangeIndexTestModel.collection(name__eq='foobar'))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(TextRangeIndexTestModel.collection(name__eq='barbar'))
        self.assertSetEqual(data, set())

        # with eq suffix
        data = set(TextRangeIndexTestModel.collection(name__eq='foo'))
        self.assertSetEqual(data, {self.pk1})
        data = set(TextRangeIndexTestModel.collection(name__eq='foobar'))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(TextRangeIndexTestModel.collection(name__eq='barbar'))
        self.assertSetEqual(data, set())

    def test_gt(self):
        data = set(TextRangeIndexTestModel.collection(name__gt='foo'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar gt foo
            self.pk4,  # foobar gt foo
            self.pk5,  # qux gt foo
        })
        data = set(TextRangeIndexTestModel.collection(name__gt='foobar'))
        self.assertSetEqual(data, {
            self.pk5,  # qux gt foo
        })
        data = set(TextRangeIndexTestModel.collection(name__gt='qux'))
        self.assertSetEqual(data, set())
        data = set(TextRangeIndexTestModel.collection(name__gt='zzz'))
        self.assertSetEqual(data, set())

    def test_gte(self):
        data = set(TextRangeIndexTestModel.collection(name__gte='foo'))
        self.assertSetEqual(data, {
            self.pk1,  # foo gte foo
            self.pk3,  # foobar gte foo
            self.pk4,  # foobar gte foo
            self.pk5,  # qux gte foo
        })
        data = set(TextRangeIndexTestModel.collection(name__gte='foobar'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar gte foo
            self.pk4,  # foobar gte foo
            self.pk5,  # qux gte foo
        })
        data = set(TextRangeIndexTestModel.collection(name__gte='qux'))
        self.assertSetEqual(data, {
            self.pk5,  # qux gte qux
        })
        data = set(TextRangeIndexTestModel.collection(name__gte='zzz'))
        self.assertSetEqual(data, set())

    def test_lt(self):
        data = set(TextRangeIndexTestModel.collection(name__lt='foo'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lt foo
        })
        data = set(TextRangeIndexTestModel.collection(name__lt='foobar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lt foobar
            self.pk1,  # foo lt foobar
        })
        data = set(TextRangeIndexTestModel.collection(name__lt='bar'))
        self.assertSetEqual(data, set())
        data = set(TextRangeIndexTestModel.collection(name__lt='aaa'))
        self.assertSetEqual(data, set())

    def test_lte(self):
        data = set(TextRangeIndexTestModel.collection(name__lte='foo'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte foo
            self.pk1,  # foo lte foo
        })
        data = set(TextRangeIndexTestModel.collection(name__lte='foobar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte foobar
            self.pk1,  # foo lte foobar
            self.pk3,  # foobar lte foobar
            self.pk4,  # foobar lte foobar
        })
        data = set(TextRangeIndexTestModel.collection(name__lte='bar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte bar
        })
        data = set(TextRangeIndexTestModel.collection(name__lte='aaa'))
        self.assertSetEqual(data, set())

    def test_startswith(self):
        data = set(TextRangeIndexTestModel.collection(name__startswith='foo'))
        self.assertSetEqual(data, {
            self.pk1,  # foo startswith foo
            self.pk3,  # foobar startswith foo
            self.pk4,  # foobar startswith foo
        })
        data = set(TextRangeIndexTestModel.collection(name__startswith='foobar'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar startswith foobar
            self.pk4,  # foobar startswith foobar
        })
        data = set(TextRangeIndexTestModel.collection(name__startswith='quz'))
        self.assertSetEqual(data, set())

    def test_many_filters(self):
        data = set(TextRangeIndexTestModel.collection(name__gt='bar', name__lte='foobar'))
        self.assertSetEqual(data, {
            self.pk1,  # foo gt bar, lte foobar
            self.pk3,  # foobar gt bar, lte foobar
            self.pk4,  # foobar gt bar, lte foobar
        })
        data = set(TextRangeIndexTestModel.collection(name__lt='foo', name__lte='foo'))
        self.assertEqual(data, {
            self.pk2,  # bar is the only one lt foo and lte foo
        })
        data = set(TextRangeIndexTestModel.collection(name__gte='foobar', name__lt='foo'))
        self.assertEqual(data, set())

        self.obj1.category.set('cat1')
        self.obj3.category.set('cat1')
        self.obj4.category.set('cat2')

        with self.assertRaises(ImplementationError):
            # not the right index for category
            TextRangeIndexTestModel.collection(name__gte='foo', category__startswith='cat')

        data = set(TextRangeIndexTestModel.collection(name__gte='foo', category='cat1'))
        self.assertEqual(data, {
            self.pk1,  # foo and cat1
            self.pk3,  # foobar and cat1
        })
