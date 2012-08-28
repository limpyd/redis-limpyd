# -*- coding:utf-8 -*-

from limpyd import fields
from limpyd.exceptions import *
from limpyd.contrib.related import (RelatedModel, RelatedCollection,
                                    FKStringField, FKHashableField, M2MSetField,
                                    M2MListField, M2MSortedSetField)

from ..base import LimpydBaseTest


class TestRedisModel(RelatedModel):
    """
    Use it as a base for all RelatedModel created for tests
    """
    database = LimpydBaseTest.database
    abstract = True
    namespace = "related-tests"


class Person(TestRedisModel):
    name = fields.PKField()
    prefered_group = FKStringField('Group')
    following = M2MSetField('self', related_name='followers')


class Group(TestRedisModel):
    name = fields.PKField()
    status = fields.StringField(indexable=True)
    owner = FKHashableField(Person, related_name='owned_groups')
    parent = FKStringField('self', related_name='children')
    members = M2MSetField(Person, related_name='membership')


class RelatedToTest(LimpydBaseTest):
    """ Test the "to" attribute of related fields """

    def test_to_as_model_should_be_converted(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-model'

        class Bar(TestRedisModel):
            namespace = 'related-to-model'
            foo = FKStringField(Foo)

        self.assertEqual(Bar._redis_attr_foo.related_to, 'related-to-model:foo')

    def test_to_as_model_name_should_be_converted(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-name'

        class Bar(TestRedisModel):
            namespace = 'related-to-name'
            foo = FKStringField('Foo')

        self.assertEqual(Bar._redis_attr_foo.related_to, 'related-to-name:foo')

    def test_to_as_full_name_should_be_kept(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-full'

        class Bar(TestRedisModel):
            namespace = 'related-to-full'
            foo = FKStringField('related-to-full:Foo')

        self.assertEqual(Bar._redis_attr_foo.related_to, 'related-to-full:foo')

    def test_to_as_self_should_be_converted(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-self'
            myself = FKStringField('self')

        self.assertEqual(Foo._redis_attr_myself.related_to, 'related-to-self:foo')


class RelatedNameTest(LimpydBaseTest):
    """ Test the "related_name" attribute of related fields """

    def test_undefined_related_name_should_be_auto_created(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        ybon.prefered_group.set(core_devs._pk)

        self.assertEqual(set(core_devs.person_set()), set([ybon._pk]))

    def test_defined_related_name_should_exists_as_collection(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        core_devs.owner.hset(ybon._pk)

        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))
        self.assertEqual(set(ybon.owned_groups()), set(Group.collection(owner=ybon._pk)))

    def test_placeholders_in_related_name_should_be_replaced(self):
        class PersonTest(TestRedisModel):
            namespace = 'related-name'
            name = fields.PKField()
            most_hated_group = FKStringField('related-tests:Group', related_name='%(namespace)s_%(model)s_set')

        ms_php = Group(name='microsoft php')
        ybon = PersonTest(name='ybon')
        ybon.most_hated_group.set(ms_php._pk)

        self.assertTrue(hasattr(ms_php, 'related_name_persontest_set'))
        self.assertEqual(set(ms_php.related_name_persontest_set()), set([ybon._pk]))

    def test_related_name_should_follow_namespace(self):
        class SubTest():
            """
            A class to create another model with the name "Group"
            """

            class Group(TestRedisModel):
                namespace = "related-name-ns"
                name = fields.PKField()

            class PersonTest(TestRedisModel):
                namespace = "related-name-ns"
                name = fields.PKField()
                first_group = FKStringField("related-tests:Group")
                second_group = FKStringField('Group')

            @staticmethod
            def run():
                group1 = Group(name='group1')  # namespace "related-name"
                group2 = SubTest.Group(name='group2')  # namespace "related-name-ns"

                person = SubTest.PersonTest(name='person')
                person.first_group.set(group1._pk)
                person.second_group.set(group2._pk)

                self.assertEqual(set(group1.persontest_set()), set([person._pk]))
                self.assertEqual(set(group2.persontest_set()), set([person._pk]))

        SubTest.run()

    def test_related_names_should_be_unique_for_a_model(self):
        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKHashableField('self')
                mother = FKHashableField('self')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKHashableField('self', related_name='parent')
                mother = FKHashableField('self', related_name='parent')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKHashableField('self', related_name='%(namespace)s_%(model)s_set')
                mother = FKHashableField('self', related_name='%(namespace)s_%(model)s_set')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKHashableField('Bar')
                mother = FKHashableField('Bar')

            class Bar(TestRedisModel):
                namespace = 'related-name-uniq'

    def test_related_names_should_work_with_subclasses(self):

        class Base(TestRedisModel):
            abstract = True
            namespace = 'related-name-sub'
            name = fields.PKField()
            a_field = FKStringField('Other', related_name='%(namespace)s_%(model)s_related')

        class ChildA(Base):
            pass

        class ChildB(Base):
            pass

        class Other(TestRedisModel):
            namespace = 'related-name-sub'
            name = fields.PKField()

        other = Other(name='foo')
        childa = ChildA(name='bar', a_field=other._pk)
        childb = ChildB(name='baz', a_field=other._pk)

        self.assertTrue(hasattr(other, 'related_name_sub_childa_related'))
        self.assertTrue(hasattr(other, 'related_name_sub_childb_related'))
        self.assertEqual(set(other.related_name_sub_childa_related()), set([childa._pk]))
        self.assertEqual(set(other.related_name_sub_childb_related()), set([childb._pk]))

    def test_related_name_as_invalid_identifier_should_raise(self):
        with self.assertRaises(ImplementationError):
            class PersonTest(TestRedisModel):
                namespace = 'related-name-inv'
                group = FKStringField('related-tests:Group', related_name='list-of-persons')


class RelatedCollectionTest(LimpydBaseTest):
    """ Test the reverse side of related field """

    def test_related_collection_are_collections(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        core_devs.members.sadd(ybon)

        self.assertTrue(isinstance(ybon.membership, RelatedCollection))
        self.assertTrue(hasattr(ybon.membership(), '_lazy_collection'))

    def test_return_value_of_related_collection(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        core_devs.members.sadd(ybon)

        test1 = set(Group.collection(members=ybon._pk))
        test2 = set(ybon.membership())

        self.assertEqual(test1, test2)

    def test_additional_filters_of_related_collection(self):
        core_devs = Group(name='limpyd core devs', status='private')
        fan_boys = Group(name='limpyd fan boys')
        ybon = Person(name='ybon')
        core_devs.members.sadd(ybon)
        fan_boys.members.sadd(ybon)

        test1 = set(Group.collection(members=ybon._pk, status='private'))
        test2 = set(ybon.membership(status='private'))

        self.assertEqual(test1, test2)
        self.assertEqual(test2, set([core_devs._pk]))


class FKTest(LimpydBaseTest):

    def test_fk_can_be_given_as_object(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        # test with FKHashableField
        core_devs.owner.hset(ybon)
        self.assertEqual(core_devs.owner.hget(), ybon._pk)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

        # test with FKStringField
        ybon.prefered_group.set(core_devs)
        self.assertEqual(ybon.prefered_group.get(), core_devs._pk)
        self.assertEqual(set(core_devs.person_set()), set([ybon._pk]))

    def test_can_update_fk(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.owner.hset(ybon)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

        core_devs.owner.hset(twidi)
        self.assertEqual(set(ybon.owned_groups()), set())
        self.assertEqual(set(twidi.owned_groups()), set([core_devs._pk]))

    def test_many_fk_can_be_set_on_same_object(self):
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')
        twidi = Person(name='twidi')

        core_devs.owner.hset(twidi)
        fan_boys.owner.hset(twidi)
        self.assertEqual(set(twidi.owned_groups()), set([core_devs._pk, fan_boys._pk]))

    def test_fk_can_be_set_on_same_model(self):
        main_group = Group(name='limpyd groups')
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')

        core_devs.parent.set(main_group)
        fan_boys.parent.set(main_group)
        self.assertEqual(set(main_group.children()), set([core_devs._pk, fan_boys._pk]))

    def test_deleting_an_object_must_clear_the_fk(self):
        main_group = Group(name='limpyd groups')
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        ybon.delete()
        self.assertIsNone(core_devs.owner.hget())

        core_devs.parent.set(main_group)
        fan_boys.parent.set(main_group)
        main_group.delete()
        self.assertIsNone(core_devs.parent.get())
        self.assertIsNone(fan_boys.parent.get())

    def test_deleting_a_fk_must_clean_the_collection(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        core_devs.delete()
        self.assertEqual(set(ybon.owned_groups()), set())


class M2MSetTest(LimpydBaseTest):

    def test_set_m2m_values_can_be_given_as_object(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.sadd(ybon._pk, twidi)

        self.assertEqual(core_devs.members.smembers(), set([twidi._pk, ybon._pk]))
        self.assertEqual(set(ybon.membership()), set([core_devs._pk]))
        self.assertEqual(set(twidi.membership()), set([core_devs._pk]))

    def test_removed_m2m_values_should_update_related_collection(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.sadd(ybon, twidi)
        core_devs.members.srem(ybon)

        self.assertEqual(core_devs.members.smembers(), set([twidi._pk]))
        self.assertEqual(set(ybon.membership()), set())
        self.assertEqual(set(twidi.membership()), set([core_devs._pk]))

    def test_m2m_can_be_set_on_the_same_model(self):
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        twidi.following.sadd(ybon)

        self.assertEqual(twidi.following.smembers(), set([ybon._pk]))
        self.assertEqual(set(ybon.followers()), set([twidi._pk]))

    def test_deleting_an_object_must_clean_m2m(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.sadd(ybon._pk, twidi)
        ybon.delete()

        self.assertEqual(core_devs.members.smembers(), set([twidi._pk]))
        self.assertEqual(set(twidi.membership()), set([core_devs._pk]))

    def test_deleting_a_m2m_should_clear_collections(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.sadd(ybon._pk, twidi)
        core_devs.delete()

        self.assertEqual(set(twidi.membership()), set([]))
        self.assertEqual(set(ybon.membership()), set([]))


class M2MListTest(LimpydBaseTest):

    class Group2(TestRedisModel):
        name = fields.PKField()
        members = M2MListField(Person, related_name='members_set2')

    def test_list_m2m_values_can_be_given_as_object(self):
        core_devs = M2MListTest.Group2(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.rpush(ybon._pk, twidi)

        self.assertEqual(core_devs.members.lrange(0, -1), [ybon._pk, twidi._pk])
        self.assertEqual(set(ybon.members_set2()), set([core_devs._pk]))
        self.assertEqual(set(twidi.members_set2()), set([core_devs._pk]))


class M2MSortedSetTest(LimpydBaseTest):

    class Group3(TestRedisModel):
        name = fields.PKField()
        members = M2MSortedSetField(Person, related_name='members_set3')

    def test_zset_m2m_values_can_be_given_as_object(self):
        core_devs = M2MSortedSetTest.Group3(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.zadd(20, ybon, 10, twidi._pk)

        self.assertEqual(core_devs.members.zrange(0, -1), [twidi._pk, ybon._pk])
        self.assertEqual(set(ybon.members_set3()), set([core_devs._pk]))
        self.assertEqual(set(twidi.members_set3()), set([core_devs._pk]))

    def test_zset_m2m_values_are_scored(self):
        core_devs = M2MSortedSetTest.Group3(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.members.zadd(20, ybon, 10, twidi)

        self.assertEqual(core_devs.members.zrange(0, -1, withscores=True), [(twidi._pk, 10.0), (ybon._pk, 20.0)])
        self.assertEqual(core_devs.members.zscore(twidi), 10.0)
        self.assertEqual(core_devs.members.zrevrangebyscore(25, 15), [ybon._pk])
