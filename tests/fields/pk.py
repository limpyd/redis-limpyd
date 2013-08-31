from limpyd import fields
from limpyd.exceptions import UniquenessError, ImplementationError

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class PKFieldTest(LimpydBaseTest):

    class AutoPkModel(TestRedisModel):
        name = fields.StringField(indexable=True)

    class RedefinedAutoPkModel(AutoPkModel):
        id = fields.AutoPKField()

    class NotAutoPkModel(TestRedisModel):
        pk = fields.PKField()
        name = fields.StringField(indexable=True)

    class ExtendedNotAutoPkField(NotAutoPkModel):
        pass

    class RedefinedNotAutoPkField(AutoPkModel):
        id = fields.PKField()

    def test_pk_value_for_default_pk_field(self):
        obj = self.AutoPkModel(name="foo")
        self.assertEqual(obj._pk, '1')
        self.assertEqual(obj.pk.get(), obj._pk)
        same_obj = self.AutoPkModel.get(obj._pk)
        self.assertEqual(same_obj._pk, obj._pk)
        always_same_obj = self.AutoPkModel.get(pk=obj._pk)
        self.assertEqual(always_same_obj._pk, obj._pk)
        obj2 = self.AutoPkModel(name="bar")
        self.assertEqual(obj2._pk, '2')

    def test_pk_value_for_redefined_auto_pk_field(self):
        obj = self.RedefinedAutoPkModel(name="foo")
        self.assertEqual(obj._pk, '1')
        self.assertEqual(obj.pk.get(), obj._pk)
        self.assertEqual(obj.id.get(), obj._pk)
        same_obj = self.RedefinedAutoPkModel.get(obj._pk)
        self.assertEqual(same_obj._pk, obj._pk)
        always_same_obj = self.RedefinedAutoPkModel.get(pk=obj._pk)
        self.assertEqual(always_same_obj._pk, obj._pk)
        obj2 = self.RedefinedAutoPkModel(name="bar")
        self.assertEqual(obj2._pk, '2')

    def test_pk_value_for_not_auto_increment_pk_field(self):
        obj = self.NotAutoPkModel(name="evil", pk=666)
        self.assertEqual(obj._pk, '666')
        self.assertEqual(obj.pk.get(), obj._pk)
        # test with real string
        obj2 = self.NotAutoPkModel(name="foo", pk="bar")
        self.assertEqual(obj2._pk, "bar")
        self.assertEqual(obj2.pk.get(), obj2._pk)
        same_obj2 = self.NotAutoPkModel.get("bar")
        self.assertEqual(obj2._pk, same_obj2.pk.get())
        # test uniqueness
        with self.assertRaises(UniquenessError):
            self.NotAutoPkModel(name="baz", pk="666")

    def test_cannot_define_already_user_defined_pk_field(self):
        with self.assertRaises(ImplementationError):
            class InvalidAutoPkModel(self.RedefinedAutoPkModel):
                uid = fields.AutoPKField()

    def test_cannot_set_pk_for_auto_increment_pk_field(self):
        with self.assertRaises(ValueError):
            self.AutoPkModel(name="foo", pk=1)
        with self.assertRaises(ValueError):
            self.RedefinedAutoPkModel(name="bar", pk=2)

    def test_forced_to_set_pk_for_not_auto_increment_pk_field(self):
        with self.assertRaises(ValueError):
            self.NotAutoPkModel(name="foo")
        with self.assertRaises(ValueError):
            self.ExtendedNotAutoPkField(name="foo")

    def test_no_collision_between_pk(self):
        self.NotAutoPkModel(name="foo", pk=1000)
        # same model, same pk
        with self.assertRaises(UniquenessError):
            self.NotAutoPkModel(name="bar", pk=1000)
        # other model, same pk
        self.assertEqual(self.ExtendedNotAutoPkField(name="bar", pk=1000)._pk, '1000')

    def test_collections_filtered_by_pk(self):
        # default auto pk
        self.AutoPkModel(name="foo")
        self.AutoPkModel(name="foo")
        self.assertEqual(set(self.AutoPkModel.collection(name="foo")), set(['1', '2']))
        self.assertEqual(set(self.AutoPkModel.collection(pk=1)), set(['1', ]))
        self.assertEqual(set(self.AutoPkModel.collection(name="foo", pk=1)), set(['1', ]))
        self.assertEqual(set(self.AutoPkModel.collection(name="foo", pk=3)), set())
        self.assertEqual(set(self.AutoPkModel.collection(name="bar", pk=1)), set())
        # specific pk
        self.NotAutoPkModel(name="foo", pk="100")
        self.NotAutoPkModel(name="foo", pk="200")
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo")), set(['100', '200']))
        self.assertEqual(set(self.NotAutoPkModel.collection(pk=100)), set(['100', ]))
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo", pk=100)), set(['100', ]))
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo", pk=300)), set())
        self.assertEqual(set(self.NotAutoPkModel.collection(name="bar", pk=100)), set())

    def test_pk_cannot_be_updated(self):
        obj = self.AutoPkModel(name="foo")
        with self.assertRaises(ValueError):
            obj.pk.set(2)
        obj2 = self.RedefinedAutoPkModel(name="bar")
        with self.assertRaises(ValueError):
            obj2.pk.set(2)
        with self.assertRaises(ValueError):
            obj2.id.set(2)
        with self.assertRaises(ValueError):
            obj2.id.set(3)
        obj3 = self.NotAutoPkModel(name="evil", pk=666)
        with self.assertRaises(ValueError):
            obj3.pk.set(777)

    def test_can_access_pk_with_two_names(self):
        # create via pk, get via id or pk
        self.RedefinedNotAutoPkField(name="foo", pk=1)
        same_obj = self.RedefinedNotAutoPkField.get(pk=1)
        same_obj2 = self.RedefinedNotAutoPkField.get(id=1)
        self.assertEqual(same_obj.pk.get(), same_obj2.pk.get())
        self.assertEqual(same_obj.id.get(), same_obj2.id.get())
        # create via id, get via id or pk
        self.RedefinedNotAutoPkField(name="foo", id=2)
        same_obj = self.RedefinedNotAutoPkField.get(pk=2)
        same_obj2 = self.RedefinedNotAutoPkField.get(id=2)
        self.assertEqual(same_obj._pk, same_obj2._pk)
        self.assertEqual(same_obj.id.get(), same_obj2.id.get())
        # collection via pk or id
        self.assertEqual(set(self.RedefinedNotAutoPkField.collection(pk=1)), set(['1', ]))
        self.assertEqual(set(self.RedefinedNotAutoPkField.collection(id=2)), set(['2', ]))

    def test_cannot_set_pk_with_two_names(self):
        with self.assertRaises(ValueError):
            self.RedefinedNotAutoPkField(name="foo", pk=1, id=2)

    def test_pk_cannot_be_deleted(self):
        obj = self.AutoPkModel(name="foo")
        with self.assertRaises(ImplementationError):
            obj.pk.delete()

        obj = self.RedefinedAutoPkModel(name="foo")
        with self.assertRaises(ImplementationError):
            obj.pk.delete()
        with self.assertRaises(ImplementationError):
            obj.id.delete()

        obj = self.NotAutoPkModel(name="evil", pk=666)
        with self.assertRaises(ImplementationError):
            obj.pk.delete()

        obj = self.RedefinedNotAutoPkField(name="foo", pk=1)
        with self.assertRaises(ImplementationError):
            obj.pk.delete()
        with self.assertRaises(ImplementationError):
            obj.id.delete()
