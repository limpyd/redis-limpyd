# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from limpyd import fields
from limpyd.database import RedisDatabase
from limpyd.exceptions import ImplementationError
from limpyd.indexes import EqualIndex

from .base import LimpydBaseTest, TEST_CONNECTION_SETTINGS
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
