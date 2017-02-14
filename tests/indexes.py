# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from limpyd import fields
from limpyd.database import RedisDatabase
from limpyd.exceptions import ImplementationError, UniquenessError
from limpyd.indexes import EqualIndex, TextRangeIndex, NumberRangeIndex

from .base import LimpydBaseTest, TEST_CONNECTION_SETTINGS, skip_if_no_zrangebylex
from .model import Bike, Email, TestRedisModel


class ReverseEqualIndex(EqualIndex):
    handled_suffixes = {'reverse_eq'}
    key = 'reverse-equal'

    @staticmethod
    def transform(value):
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
        filter_key, key_type, is_tmp = index.get_filtered_keys(None, 'rosalie')[0]
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


class PassIndexesToFieldTestCase(LimpydBaseTest):

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


class ConfigureClassMethodTestCase(LimpydBaseTest):

    def test_configure_class_method(self):

        # test a transform method with only the value
        def reverse_value(value):
            return value[::-1]

        # test a transform method with also self
        def reverse_value_self(self, value):
            # prefix "strange" and value "bar" => "egnarts" + "bar
            return self.prefix[::-1] + value

        class TestIndexConfigureModel(TestRedisModel):
            name = fields.StringField(indexable=True, indexes=[
                EqualIndex,
                EqualIndex.configure(prefix='reverse', transform=reverse_value),
            ])
            lastname = fields.StringField(indexable=True, indexes=[
                EqualIndex,
                EqualIndex.configure(prefix='strange', transform=reverse_value_self),
            ])

        obj1 = TestIndexConfigureModel(name='foo', lastname='foofoo')
        pk1 = obj1.pk.get()
        obj2 = TestIndexConfigureModel(name='bar', lastname='barbar')
        pk2 = obj2.pk.get()

        self.assertEqual(set(TestIndexConfigureModel.collection(name='foo')), {pk1})
        self.assertEqual(set(TestIndexConfigureModel.collection(name__eq='foo')), {pk1})
        self.assertEqual(set(TestIndexConfigureModel.collection(name='oof')), set())
        self.assertEqual(set(TestIndexConfigureModel.collection(name__reverse='rab')), {pk2})
        self.assertEqual(set(TestIndexConfigureModel.collection(name__reverse__eq='rab')), {pk2})
        self.assertEqual(set(TestIndexConfigureModel.collection(name__reverse='bar')), set())

        self.assertEqual(set(TestIndexConfigureModel.collection(lastname__strange='egnartsbarbar')), {pk2})

class RangeIndexTestModel(TestRedisModel):
    name = fields.StringField(indexable=True, indexes=[TextRangeIndex])
    category = fields.StringField(indexable=True)
    value = fields.StringField(indexable=True, indexes=[NumberRangeIndex])


@unittest.skipIf(*skip_if_no_zrangebylex)
class TextRangeIndexTestCase(LimpydBaseTest):

    def setUp(self):
        super(TextRangeIndexTestCase, self).setUp()
        self.obj1 = RangeIndexTestModel(name='foo')
        self.pk1 = self.obj1.pk.get()
        self.obj2 = RangeIndexTestModel(name='bar')
        self.pk2 = self.obj2.pk.get()
        self.obj3 = RangeIndexTestModel(name='foobar')
        self.pk3 = self.obj3.pk.get()
        self.obj4 = RangeIndexTestModel(name='foobar')  # same as before
        self.pk4 = self.obj4.pk.get()
        self.obj5 = RangeIndexTestModel(name='qux')
        self.pk5 = self.obj5.pk.get()

    def test_storage_key_for_single_field(self):

        field = self.obj1.get_field('name')
        index = field._indexes[0]
        key = index.get_storage_key('foo')

        self.assertEqual(key, 'tests:rangeindextestmodel:name:text-range')

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

        index = RangeIndexTestModel.get_field('name')._indexes[0]

        with self.assertRaises(ImplementationError):
            index.get_filtered_keys('gt', 'bar', accepted_key_types={'list'})

        index_key, key_type, is_tmp = index.get_filtered_keys('gt', 'bar', accepted_key_types={'set'})[0]
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

        index_key, key_type, is_tmp = index.get_filtered_keys('lte', 'foo', accepted_key_types={'zset'})[0]
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
        data = set(RangeIndexTestModel.collection(name='foo'))
        self.assertSetEqual(data, {self.pk1})
        data = set(RangeIndexTestModel.collection(name='foobar'))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(RangeIndexTestModel.collection(name='barbar'))
        self.assertSetEqual(data, set())

        # with eq suffix
        data = set(RangeIndexTestModel.collection(name__eq='foo'))
        self.assertSetEqual(data, {self.pk1})
        data = set(RangeIndexTestModel.collection(name__eq='foobar'))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(RangeIndexTestModel.collection(name__eq='barbar'))
        self.assertSetEqual(data, set())

    def test_gt(self):
        data = set(RangeIndexTestModel.collection(name__gt='foo'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar gt foo
            self.pk4,  # foobar gt foo
            self.pk5,  # qux gt foo
        })
        data = set(RangeIndexTestModel.collection(name__gt='foobar'))
        self.assertSetEqual(data, {
            self.pk5,  # qux gt foo
        })
        data = set(RangeIndexTestModel.collection(name__gt='qux'))
        self.assertSetEqual(data, set())
        data = set(RangeIndexTestModel.collection(name__gt='zzz'))
        self.assertSetEqual(data, set())

    def test_gte(self):
        data = set(RangeIndexTestModel.collection(name__gte='foo'))
        self.assertSetEqual(data, {
            self.pk1,  # foo gte foo
            self.pk3,  # foobar gte foo
            self.pk4,  # foobar gte foo
            self.pk5,  # qux gte foo
        })
        data = set(RangeIndexTestModel.collection(name__gte='foobar'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar gte foo
            self.pk4,  # foobar gte foo
            self.pk5,  # qux gte foo
        })
        data = set(RangeIndexTestModel.collection(name__gte='qux'))
        self.assertSetEqual(data, {
            self.pk5,  # qux gte qux
        })
        data = set(RangeIndexTestModel.collection(name__gte='zzz'))
        self.assertSetEqual(data, set())

    def test_lt(self):
        data = set(RangeIndexTestModel.collection(name__lt='foo'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lt foo
        })
        data = set(RangeIndexTestModel.collection(name__lt='foobar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lt foobar
            self.pk1,  # foo lt foobar
        })
        data = set(RangeIndexTestModel.collection(name__lt='bar'))
        self.assertSetEqual(data, set())
        data = set(RangeIndexTestModel.collection(name__lt='aaa'))
        self.assertSetEqual(data, set())

    def test_lte(self):
        data = set(RangeIndexTestModel.collection(name__lte='foo'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte foo
            self.pk1,  # foo lte foo
        })
        data = set(RangeIndexTestModel.collection(name__lte='foobar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte foobar
            self.pk1,  # foo lte foobar
            self.pk3,  # foobar lte foobar
            self.pk4,  # foobar lte foobar
        })
        data = set(RangeIndexTestModel.collection(name__lte='bar'))
        self.assertSetEqual(data, {
            self.pk2,  # bar lte bar
        })
        data = set(RangeIndexTestModel.collection(name__lte='aaa'))
        self.assertSetEqual(data, set())

    def test_startswith(self):
        data = set(RangeIndexTestModel.collection(name__startswith='foo'))
        self.assertSetEqual(data, {
            self.pk1,  # foo startswith foo
            self.pk3,  # foobar startswith foo
            self.pk4,  # foobar startswith foo
        })
        data = set(RangeIndexTestModel.collection(name__startswith='foobar'))
        self.assertSetEqual(data, {
            self.pk3,  # foobar startswith foobar
            self.pk4,  # foobar startswith foobar
        })
        data = set(RangeIndexTestModel.collection(name__startswith='quz'))
        self.assertSetEqual(data, set())

    def test_many_filters(self):
        data = set(RangeIndexTestModel.collection(name__gt='bar', name__lte='foobar'))
        self.assertSetEqual(data, {
            self.pk1,  # foo gt bar, lte foobar
            self.pk3,  # foobar gt bar, lte foobar
            self.pk4,  # foobar gt bar, lte foobar
        })
        data = set(RangeIndexTestModel.collection(name__lt='foo', name__lte='foo'))
        self.assertEqual(data, {
            self.pk2,  # bar is the only one lt foo and lte foo
        })
        data = set(RangeIndexTestModel.collection(name__gte='foobar', name__lt='foo'))
        self.assertEqual(data, set())

        self.obj1.category.set('cat1')
        self.obj3.category.set('cat1')
        self.obj4.category.set('cat2')

        with self.assertRaises(ImplementationError):
            # not the right index for category
            RangeIndexTestModel.collection(name__gte='foo', category__startswith='cat')

        data = set(RangeIndexTestModel.collection(name__gte='foo', category='cat1'))
        self.assertEqual(data, {
            self.pk1,  # foo, and cat1
            self.pk3,  # foobar, and cat1
        })


@unittest.skipIf(*skip_if_no_zrangebylex)
class NumberRangeIndexTestCase(LimpydBaseTest):

    def setUp(self):
        super(NumberRangeIndexTestCase, self).setUp()
        self.obj1 = RangeIndexTestModel(value=-15)
        self.pk1 = self.obj1.pk.get()
        self.obj2 = RangeIndexTestModel(value=-25)
        self.pk2 = self.obj2.pk.get()
        self.obj3 = RangeIndexTestModel(value=30)
        self.pk3 = self.obj3.pk.get()
        self.obj4 = RangeIndexTestModel(value=30)  # same as before
        self.pk4 = self.obj4.pk.get()
        self.obj5 = RangeIndexTestModel(value=123)
        self.pk5 = self.obj5.pk.get()

    def test_storage_key_for_single_field(self):

        field = self.obj1.get_field('value')
        index = field._indexes[0]
        key = index.get_storage_key(-25)

        self.assertEqual(key, 'tests:rangeindextestmodel:value:number-range')

    def test_storage_key_for_hash_field(self):
        class NumberRangeIndexTestModel1(TestRedisModel):
            data = fields.HashField(indexable=True, indexes=[NumberRangeIndex])

        obj = NumberRangeIndexTestModel1(data={'foo': 123})

        field = obj.get_field('data')
        index = field._indexes[0]
        key = index.get_storage_key('foo', 123)

        self.assertEqual(key, 'tests:numberrangeindextestmodel1:data:foo:number-range')

    def test_stored_data(self):

        field = self.obj1.get_field('value')
        index = field._indexes[0]
        key = index.get_storage_key(None)  # value not used in this index for the storage key

        key_type = self.connection.type(key)
        self.assertEqual(key_type, 'zset')

        data = self.connection.zrange(key, 0, -1, withscores=True)

        # all entries should:
        # - have the value as score
        # - be returned in a score order
        # - have the pk
        self.assertEqual(data, [
            (self.pk2, -25.0),
            (self.pk1, -15.0),
            (self.pk3, 30.0),
            (self.pk4, 30.0),
            (self.pk5, 123.0),
        ])

    def test_uniqueness(self):
        class NumberRangeIndexTestModel2(TestRedisModel):
            value = fields.StringField(indexable=True, unique=True, indexes=[TextRangeIndex])

        # first object with -15
        NumberRangeIndexTestModel2(value=-15)

        # new object with 30 but update to -15: should fail
        obj = NumberRangeIndexTestModel2(value=30)
        with self.assertRaises(UniquenessError):
            obj.value.set(-15)

        # new object with -15: should fail
        with self.assertRaises(UniquenessError):
            NumberRangeIndexTestModel2(value=-15)

    def test_get_filtered_key(self):

        index = RangeIndexTestModel.get_field('value')._indexes[0]

        with self.assertRaises(ImplementationError):
            index.get_filtered_keys('gt', -25, accepted_key_types={'list'})

        index_key, key_type, is_tmp = index.get_filtered_keys('gt', -25, accepted_key_types={'set'})[0]
        self.assertEqual(self.connection.type(index_key), 'set')
        self.assertEqual(key_type, 'set')
        self.assertTrue(is_tmp)
        data = self.connection.smembers(index_key)
        self.assertEqual(data, {
            self.pk1,  # -15 > -25
            self.pk3,  # 30 > -25
            self.pk4,  # 30 > -25
            self.pk5,  # 123 > -25
        })

        index_key, key_type, is_tmp = index.get_filtered_keys('lte', -15, accepted_key_types={'zset'})[0]
        self.assertEqual(self.connection.type(index_key), 'zset')
        self.assertEqual(key_type, 'zset')
        self.assertTrue(is_tmp)
        data = self.connection.zrange(index_key, 0, -1, withscores=1)
        self.assertEqual(data, [
            (self.pk2, 0.0),  # -25 <= -15
            (self.pk1, 1.0),  # -15 <= -15
        ])

    def test_eq(self):
        # without suffix
        data = set(RangeIndexTestModel.collection(value=-15))
        self.assertSetEqual(data, {self.pk1})
        data = set(RangeIndexTestModel.collection(value=30))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(RangeIndexTestModel.collection(value=17))
        self.assertSetEqual(data, set())

        # with eq suffix
        data = set(RangeIndexTestModel.collection(value__eq=-15))
        self.assertSetEqual(data, {self.pk1})
        data = set(RangeIndexTestModel.collection(value__eq=30))
        self.assertSetEqual(data, {self.pk3, self.pk4})
        data = set(RangeIndexTestModel.collection(value__eq=15))
        self.assertSetEqual(data, set())

    def test_gt(self):
        data = set(RangeIndexTestModel.collection(value__gt=-15))
        self.assertSetEqual(data, {
            self.pk3,  # 30 > -15
            self.pk4,  # 30 > -15
            self.pk5,  # 123 > -15
        })
        data = set(RangeIndexTestModel.collection(value__gt=30))
        self.assertSetEqual(data, {
            self.pk5,  # 123 > -15
        })
        data = set(RangeIndexTestModel.collection(value__gt=123))
        self.assertSetEqual(data, set())
        data = set(RangeIndexTestModel.collection(value__gt=1000))
        self.assertSetEqual(data, set())

    def test_gte(self):
        data = set(RangeIndexTestModel.collection(value__gte=-15))
        self.assertSetEqual(data, {
            self.pk1,  # -15 >= -15
            self.pk3,  # 30 >= -15
            self.pk4,  # 30 >= -15
            self.pk5,  # 123 >= -15
        })
        data = set(RangeIndexTestModel.collection(value__gte=30))
        self.assertSetEqual(data, {
            self.pk3,  # 30 >= -15
            self.pk4,  # 30 >= -15
            self.pk5,  # 123 >= -15
        })
        data = set(RangeIndexTestModel.collection(value__gte=123))
        self.assertSetEqual(data, {
            self.pk5,  # 123 >= 123
        })
        data = set(RangeIndexTestModel.collection(value__gte=1000))
        self.assertSetEqual(data, set())

    def test_lt(self):
        data = set(RangeIndexTestModel.collection(value__lt=-15))
        self.assertSetEqual(data, {
            self.pk2,  # -25 < -15
        })
        data = set(RangeIndexTestModel.collection(value__lt=30))
        self.assertSetEqual(data, {
            self.pk2,  # -25 < 30
            self.pk1,  # -15 < 30
        })
        data = set(RangeIndexTestModel.collection(value__lt=-25))
        self.assertSetEqual(data, set())
        data = set(RangeIndexTestModel.collection(value__lt=-123))
        self.assertSetEqual(data, set())

    def test_lte(self):
        data = set(RangeIndexTestModel.collection(value__lte=-15))
        self.assertSetEqual(data, {
            self.pk2,  # -25 <= -15
            self.pk1,  # -15 <= -15
        })
        data = set(RangeIndexTestModel.collection(value__lte=30))
        self.assertSetEqual(data, {
            self.pk2,  # -25 <= 30
            self.pk1,  # 20 <= 30
            self.pk3,  # 30 <= 30
            self.pk4,  # 30 <= 30
        })
        data = set(RangeIndexTestModel.collection(value__lte=-25))
        self.assertSetEqual(data, {
            self.pk2,  # -25 <= 10
        })
        data = set(RangeIndexTestModel.collection(value__lte=-123))
        self.assertSetEqual(data, set())

    def test_many_filters(self):
        data = set(RangeIndexTestModel.collection(value__gt=-25, value__lte=30))
        self.assertSetEqual(data, {
            self.pk1,  # -15 > -25, <= 30
            self.pk3,  # 30 > -25, <= 30
            self.pk4,  # 30 > -25, <= 30
        })
        data = set(RangeIndexTestModel.collection(value__lt=-15, value__lte=-15))
        self.assertEqual(data, {
            self.pk2,  # -25 is the only one < -15 and <= -15
        })
        data = set(RangeIndexTestModel.collection(value__gte=30, value__lt=-15))
        self.assertEqual(data, set())

        self.obj1.category.set('cat1')
        self.obj1.name.set('foo')
        self.obj3.category.set('cat1')
        self.obj4.category.set('cat2')

        with self.assertRaises(ImplementationError):
            # not the right index for category
            RangeIndexTestModel.collection(value__gte=-15, category__startswith='cat')

        data = set(RangeIndexTestModel.collection(value__gte=-15, category='cat1'))
        self.assertEqual(data, {
            self.pk1,  # -15 and cat1
            self.pk3,  # 30 and cat1
        })

        # three index types
        data = set(RangeIndexTestModel.collection(value__gte=-15, category='cat1', name__lte='fooa'))
        self.assertEqual(data, {
            self.pk1,  # -15, and cat1, and foo lte fooa
        })


class CleanTestCase(LimpydBaseTest):

    def test_equal_index(self):

        class CleanModel1(TestRedisModel):
            field = fields.StringField(indexable=True)
            other_field = fields.StringField(indexable=True)
            key_field = fields.StringField(indexable=True, indexes=[EqualIndex.configure(key='foo')])
            prefix_field = fields.StringField(indexable=True, indexes=[EqualIndex.configure(prefix='bar')])
            key_prefix_field = fields.StringField(indexable=True, indexes=[EqualIndex.configure(key='baz', prefix='qux')])
            hash_field = fields.HashField(indexable=True)
            two_indexes_field = fields.StringField(indexable=True, indexes=[
                EqualIndex.configure(prefix='one'),
                EqualIndex.configure(prefix='two', transform=lambda value: value[::-1]),
            ])

        pk1 = CleanModel1(
            field='a', other_field='aa', key_field='aaa', prefix_field='aaaa', key_prefix_field='aaaaa',
            hash_field={'aaaaaa1': 'AAAAAA1', 'aaaaaa2': 'AAAAAA2'},
            two_indexes_field='aaaaaaX',
        ).pk.get()
        pk2 = CleanModel1(
            field='b', other_field='bb', key_field='bbb', prefix_field='bbbb', key_prefix_field='bbbbb',
            hash_field={'bbbbbb1': 'BBBBBB1', 'bbbbbb2': 'BBBBBB2'},
            two_indexes_field='bbbbbbX',
        ).pk.get()

        ### check simple index
        index = CleanModel1.get_field('field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:field:a',
            'tests:cleanmodel1:field:b',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(field='a')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel1.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(field='b')), {pk2})

        # now for index with key
        index = CleanModel1.get_field('key_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:key_field:foo:aaa',
            'tests:cleanmodel1:key_field:foo:bbb',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel1.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_field='bbb')), {pk2})

        ### now for index with prefix
        index = CleanModel1.get_field('prefix_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:prefix_field:bar:aaaa',
            'tests:cleanmodel1:prefix_field:bar:bbbb',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel1.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='bbbb')), {pk2})

        ### now for index with key and prefix
        index = CleanModel1.get_field('key_prefix_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:key_prefix_field:qux:baz:aaaaa',
            'tests:cleanmodel1:key_prefix_field:qux:baz:bbbbb',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel1.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='bbbbb')), {pk2})

        ### now for index for hashfield
        index = CleanModel1.get_field('hash_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:hash_field:aaaaaa1:AAAAAA1',
            'tests:cleanmodel1:hash_field:aaaaaa2:AAAAAA2',
            'tests:cleanmodel1:hash_field:bbbbbb1:BBBBBB1',
            'tests:cleanmodel1:hash_field:bbbbbb2:BBBBBB2',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), set())
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), set())
        self.assertSetEqual(set(CleanModel1.collection(hash_field__bbbbbb1='BBBBBB1')), set())
        self.assertSetEqual(set(CleanModel1.collection(hash_field__bbbbbb2='BBBBBB2')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel1.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__bbbbbb1='BBBBBB1')), {pk2})
        self.assertSetEqual(set(CleanModel1.collection(hash_field__bbbbbb2='BBBBBB2')), {pk2})

        ### now for multi-indexes
        index = CleanModel1.get_field('two_indexes_field')._indexes[1]  # the reverse one

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel1:two_indexes_field:two:Xaaaaaa',
            'tests:cleanmodel1:two_indexes_field:two:Xbbbbbb',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), set())

        # but other index still present
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__two='Xaaaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})

        # and other index still present
        self.assertSetEqual(set(CleanModel1.collection(two_indexes_field__one='aaaaaaX')), {pk1})

        # both methods cannot be called from instance index
        with self.assertRaises(AssertionError):
            CleanModel1().get_field('field')._indexes[0].clear()
        with self.assertRaises(AssertionError):
            CleanModel1().get_field('field')._indexes[0].rebuild()

    @unittest.skipIf(*skip_if_no_zrangebylex)
    def test_range_index(self):

        class CleanModel2(TestRedisModel):
            field = fields.StringField(indexable=True, indexes=[TextRangeIndex])
            other_field = fields.StringField(indexable=True, indexes=[TextRangeIndex])
            key_field = fields.StringField(indexable=True, indexes=[TextRangeIndex.configure(key='foo')])
            prefix_field = fields.StringField(indexable=True, indexes=[TextRangeIndex.configure(prefix='bar')])
            key_prefix_field = fields.StringField(indexable=True, indexes=[TextRangeIndex.configure(key='baz', prefix='qux')])
            hash_field = fields.HashField(indexable=True, indexes=[TextRangeIndex])
            two_indexes_field = fields.StringField(indexable=True, indexes=[
                TextRangeIndex.configure(prefix='one'),
                TextRangeIndex.configure(prefix='two', transform=lambda value: value[::-1]),
            ])

        pk1 = CleanModel2(
            field='a', other_field='aa', key_field='aaa', prefix_field='aaaa', key_prefix_field='aaaaa',
            hash_field={'aaaaaa1': 'AAAAAA1', 'aaaaaa2': 'AAAAAA2'},
            two_indexes_field='aaaaaaX',
        ).pk.get()
        pk2 = CleanModel2(
            field='b', other_field='bb', key_field='bbb', prefix_field='bbbb', key_prefix_field='bbbbb',
            hash_field={'bbbbbb1': 'BBBBBB1', 'bbbbbb2': 'BBBBBB2'},
            two_indexes_field='bbbbbbX',
        ).pk.get()

        ### check simple index
        index = CleanModel2.get_field('field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:field:text-range',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(field='a')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel2.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(field='b')), {pk2})

        # now for index with key
        index = CleanModel2.get_field('key_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:key_field:foo',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel2.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_field='bbb')), {pk2})

        ### now for index with prefix
        index = CleanModel2.get_field('prefix_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:prefix_field:bar:text-range',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel2.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='bbbb')), {pk2})

        ### now for index with key and prefix
        index = CleanModel2.get_field('key_prefix_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:key_prefix_field:qux:baz',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel2.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='bbbbb')), {pk2})

        ### now for index for hashfield
        index = CleanModel2.get_field('hash_field')._indexes[0]

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:hash_field:aaaaaa1:text-range',
            'tests:cleanmodel2:hash_field:aaaaaa2:text-range',
            'tests:cleanmodel2:hash_field:bbbbbb1:text-range',
            'tests:cleanmodel2:hash_field:bbbbbb2:text-range',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), set())
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), set())
        self.assertSetEqual(set(CleanModel2.collection(hash_field__bbbbbb1='BBBBBB1')), set())
        self.assertSetEqual(set(CleanModel2.collection(hash_field__bbbbbb2='BBBBBB2')), set())

        # but index for other fields are still present
        self.assertSetEqual(set(CleanModel2.collection(other_field='aa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(field='a')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_field='aaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(prefix_field__bar='aaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(key_prefix_field__qux='aaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa1='AAAAAA1')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__aaaaaa2='AAAAAA2')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__bbbbbb1='BBBBBB1')), {pk2})
        self.assertSetEqual(set(CleanModel2.collection(hash_field__bbbbbb2='BBBBBB2')), {pk2})

        ### now for multi-indexes
        index = CleanModel2.get_field('two_indexes_field')._indexes[1]  # the reverse one

        # check we have the keys
        self.assertSetEqual(index.get_all_storage_keys(), {
            'tests:cleanmodel2:two_indexes_field:two:text-range',
        })

        # check that they are deleted
        index.clear()
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), set())

        # but other index still present
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})

        # check the index is rebuilt
        index.rebuild()
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__two='Xaaaaaa')), {pk1})
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})

        # and other index still present
        self.assertSetEqual(set(CleanModel2.collection(two_indexes_field__one='aaaaaaX')), {pk1})

    def test_from_field(self):

        class CleanModel3(TestRedisModel):
            two_indexes_field = fields.StringField(indexable=True, indexes=[
                EqualIndex.configure(prefix='one'),
                EqualIndex.configure(prefix='two', transform=lambda value: value[::-1]),
            ])

        pk1 = CleanModel3(two_indexes_field='aX').pk.get()
        pk2 = CleanModel3(two_indexes_field='bX').pk.get()

        # we clear all indexes
        CleanModel3.get_field('two_indexes_field').clear_indexes()
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__one='aX')), set())
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__two='Xa')), set())
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__one='bX')), set())
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__two='Xb')), set())

        # and rebuild them
        CleanModel3.get_field('two_indexes_field').rebuild_indexes()
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__one='aX')), {pk1})
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__two='Xa')), {pk1})
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__one='bX')), {pk2})
        self.assertSetEqual(set(CleanModel3.collection(two_indexes_field__two='Xb')), {pk2})

        # this doesn't work from an instance
        with self.assertRaises(AssertionError):
            CleanModel3().two_indexes_field.clear_indexes()
        with self.assertRaises(AssertionError):
            CleanModel3().two_indexes_field.rebuild_indexes()
