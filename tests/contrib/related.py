# -*- coding:utf-8 -*-

from limpyd import model, fields
from limpyd.exceptions import *
from limpyd.contrib.related import (RelatedModel, RelatedCollection,
                                    FKStringField, FKInstanceHashField, M2MSetField,
                                    M2MListField, M2MSortedSetField)
from limpyd.contrib.collection import ExtendedCollectionManager

from ..base import LimpydBaseTest, TEST_CONNECTION_SETTINGS


class TestRedisModel(RelatedModel):
    """
    Use it as a base for all RelatedModel created for tests
    """
    database = LimpydBaseTest.database
    abstract = True
    namespace = "related-tests"


class Person(TestRedisModel):
    name = fields.PKField()
    age = fields.StringField(indexable=True)
    prefered_group = FKStringField('Group')
    following = M2MSetField('self', related_name='followers')


class Group(TestRedisModel):
    name = fields.PKField()
    status = fields.StringField(indexable=True)
    owner = FKInstanceHashField(Person, related_name='owned_groups')
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

        self.assertEqual(Bar.get_field('foo').related_to, 'related-to-model:foo')

    def test_to_as_model_name_should_be_converted(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-name'

        class Bar(TestRedisModel):
            namespace = 'related-to-name'
            foo = FKStringField('Foo')

        self.assertEqual(Bar.get_field('foo').related_to, 'related-to-name:foo')

    def test_to_as_full_name_should_be_kept(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-full'

        class Bar(TestRedisModel):
            namespace = 'related-to-full'
            foo = FKStringField('related-to-full:Foo')

        self.assertEqual(Bar.get_field('foo').related_to, 'related-to-full:foo')

    def test_to_as_self_should_be_converted(self):
        class Foo(TestRedisModel):
            namespace = 'related-to-self'
            myself = FKStringField('self')

        self.assertEqual(Foo.get_field('myself').related_to, 'related-to-self:foo')


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
                father = FKInstanceHashField('self')
                mother = FKInstanceHashField('self')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKInstanceHashField('self', related_name='parent')
                mother = FKInstanceHashField('self', related_name='parent')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKInstanceHashField('self', related_name='%(namespace)s_%(model)s_set')
                mother = FKInstanceHashField('self', related_name='%(namespace)s_%(model)s_set')

        with self.assertRaises(ImplementationError):
            class Foo(TestRedisModel):
                namespace = 'related-name-uniq'
                father = FKInstanceHashField('Bar')
                mother = FKInstanceHashField('Bar')

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


class MultiValuesCollectionTest(LimpydBaseTest):

    def setUp(self):
        super(MultiValuesCollectionTest, self).setUp()
        self.core_devs = Group(name='limpyd core devs')
        self.ybon = Person(name='ybon', age=30)
        self.twidi = Person(name='twidi', age=35)
        self.core_devs.members.sadd(self.ybon)
        self.core_devs.members.sadd(self.twidi)

    def test_calling_a_multivaluesrelatedfield_should_return_a_collection(self):
        members = self.core_devs.members()
        self.assertTrue(isinstance(members, ExtendedCollectionManager))

    def test_return_value_of_collection(self):
        members_pk = set(self.core_devs.members())
        self.assertTrue(members_pk, set(['twidi', 'ybon']))
        members_instances = list(self.core_devs.members().instances())
        self.assertEqual(len(members_instances), 2)
        self.assertTrue(isinstance(members_instances[0], Person))

    def test_additional_filters(self):
        members_pk = set(self.core_devs.members(name='twidi'))
        self.assertEqual(members_pk, set(['twidi']))

        members_pk = set(self.core_devs.members(name='diox'))
        self.assertEqual(members_pk, set())

    def test_sort(self):
        members_pk = list(self.core_devs.members().sort(by='age'))
        self.assertEqual(members_pk, ['ybon', 'twidi'])
        members_pk = list(self.core_devs.members().sort(by='-age'))
        self.assertEqual(members_pk, ['twidi', 'ybon'])

    def test_should_work_with_listfield(self):

        class GroupAsList(TestRedisModel):
            name = fields.PKField()
            members = M2MListField(Person, related_name='members_list')

        core_devs = GroupAsList(name='limpyd core devs')
        core_devs.members.rpush(self.ybon)
        core_devs.members.rpush(self.twidi)

        members_pk = set(core_devs.members())
        self.assertTrue(members_pk, set(['twidi', 'ybon']))
        members_instances = list(core_devs.members().instances())
        self.assertEqual(len(members_instances), 2)
        self.assertTrue(isinstance(members_instances[0], Person))

    def test_should_work_with_sortedsetfield(self):

        class GroupAsSortedSet(TestRedisModel):
            name = fields.PKField()
            members = M2MSortedSetField(Person, related_name='members_zset')

        core_devs = GroupAsSortedSet(name='limpyd core devs')
        core_devs.members.zadd(100, self.ybon)
        core_devs.members.zadd(50, self.twidi)

        members_pk = set(core_devs.members())
        self.assertTrue(members_pk, set(['twidi', 'ybon']))
        members_instances = list(core_devs.members().instances())
        self.assertEqual(len(members_instances), 2)
        self.assertTrue(isinstance(members_instances[0], Person))

    def test_collection_should_be_alias_of_call(self):
        """
        Test that we can use obj.field.collection() the same way we can use
        obj.field()
        """
        members1 = self.core_devs.members()
        self.assertTrue(isinstance(members1, ExtendedCollectionManager))

        members2 = self.core_devs.members.collection()
        self.assertTrue(isinstance(members1, ExtendedCollectionManager))

        self.assertEqual(list(members1), list(members2))


class FKTest(LimpydBaseTest):

    def test_fk_can_be_given_as_object(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        # test with FKInstanceHashField
        core_devs.owner.hset(ybon)
        self.assertEqual(core_devs.owner.hget(), ybon._pk)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

        # test with FKStringField
        ybon.prefered_group.set(core_devs)
        self.assertEqual(ybon.prefered_group.get(), core_devs._pk)
        self.assertEqual(set(core_devs.person_set()), set([ybon._pk]))

    def test_fk_can_be_given_as_fk(self):
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        fan_boys.owner.hset(core_devs.owner)
        self.assertEqual(fan_boys.owner.hget(), ybon._pk)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk, fan_boys._pk]))

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

    def test_calling_instance_on_fkfield_should_retrieve_the_related_instance(self):
        twidi = Person(name='twidi')
        main_group = Group(name='limpyd groups')
        core_devs = Group(name='limpyd core devs', owner=twidi, parent=main_group)

        # test FKStrinField
        owner = core_devs.owner.instance()
        self.assertEqual(owner._pk, twidi._pk)
        self.assertTrue(isinstance(owner, Person))
        # test FKInstanceHashField
        parent = core_devs.parent.instance()
        self.assertEqual(parent._pk, main_group._pk)
        self.assertTrue(isinstance(parent, Group))

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

    def test_set_m2m_values_can_be_given_as_fk(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        core_devs.members.sadd(core_devs.owner)
        self.assertEqual(core_devs.members.smembers(), set([ybon._pk]))

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


class DatabaseTest(LimpydBaseTest):
    def test_database_could_transfer_its_models_and_relations_to_another(self):
        """
        Move of models is tested in tests.models.DatabaseTest.test_database_could_transfer_its_models_to_another
        The move of relations is tested here.
        """
        db1 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db2 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db3 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)

        class M(RelatedModel):
            namespace = 'transfert-db-relations'
            abstract = True
            foo = fields.StringField()

        class A(M):
            database = db1
            b = FKStringField('B', related_name='a_set')

        class B(M):
            database = db1
            a = FKStringField(A, related_name='b_set')

        class C(M):
            database = db2
            b = FKStringField(B, related_name='c_set')  # link to a model on another database !

        # getting list of linked C objects from a B object will fail because
        # both models are not on the same database, so B is not aware of a link
        # to him made on C. In fact C has created a relation on a B field on its
        # database, but which is not defined
        b = B(foo='bar')
        with self.assertRaises(AttributeError):
            b.c_set()

        # the link A <-> B should work
        self.assertEqual(list(b.a_set()), [])

        # move B to db2 to allow relation to work
        B.use_database(db2)
        b = B(foo='bar')
        self.assertEqual(list(b.c_set()), [])

        # now the link A <-> B should be broken
        with self.assertRaises(AttributeError):
            b.a_set()

        # move all to db3
        A.use_database(db3)
        B.use_database(db3)
        C.use_database(db3)

        # create and link objects
        a = A(foo='bar')
        b = B(foo='bar')
        c = C(foo='bar')
        a.b.set(b)
        b.a.set(a)
        c.b.set(b)

        # all relation should work
        self.assertEqual(list(a.b_set()), [b._pk])
        self.assertEqual(list(b.a_set()), [a._pk])
        self.assertEqual(list(b.c_set()), [c._pk])
