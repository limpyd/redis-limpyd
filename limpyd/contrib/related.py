# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import map
from future.builtins import str
from future.builtins import object
from future.utils import with_metaclass

import re
from copy import copy

from limpyd import model, fields
from limpyd.exceptions import *
from limpyd.contrib.collection import ExtendedCollectionManager

# used to validate a related_name
re_identifier = re.compile(r"\W")


class RelatedCollection(object):
    """
    When a related field is added, a related collection is created on the
    related model on the other side.
    It's simply a shortcut to a collection with a predefined filter.

    Exemple with these two related classes :

        class Person(RelatedModel):
            name = FKStringField()
            group = FKStringField('Group', related_name='members')

        class Group(RelatedField):
            name = FKStringField()

    Defining these objets :

        group = Group(name='a group')
        person = Person(name='a person', group=group)

    You can access members of the group via the main way:

        members = Person.collection(group=group._pk)

    Or with the related collection:

        members = group.members()

    Note that you can pass filters the same way you can pass them to a collection:
        members = group.members(a_filter=a_value, another_filter=another_value)

    The result is a real collection, with lazy loading, sorting...
    """

    def __init__(self, instance, related_field):
        """
        Create the RelatedCollection on the 'instance', related to the field
        'related_field'
        """
        self.instance = instance
        self.related_field = related_field

    def __call__(self, **filters):
        """
        Return a collection on the related model, given the current instance as
        a filter for the related field.
        """
        if not filters:
            filters = {}
        filters[self.related_field.name] = self.instance._pk
        return self.related_field._model.collection(**filters)

    def remove_instance(self):
        """
        Remove the instance from the related fields (delete the field if it's
        a simple one, or remove the instance from the field if it's a set/list/
        sorted_set)
        """
        with fields.FieldLock(self.related_field):
            related_pks = self()
            for pk in related_pks:

                # get the real related field
                related_instance = self.related_field._model(pk)
                related_field = getattr(related_instance, self.related_field.name)

                # check if we have a dedicated remove method
                remover = getattr(related_field, '_related_remover', None)

                # then remove the instance from the related field
                if remover is not None:
                    # if we have a remover method, it wants the instance as argument
                    # (the related field may be a set/list/sorted_set)
                    getattr(related_field, remover)(self.instance._pk)
                else:
                    # no remover method, simple delete the field
                    related_field.delete()


class RelatedModel(model.RedisModel):
    """
    This subclass of RedisModel handles creation of related collections, and
    propagates to them the deletion of the instance. So it's needed for models
    with related fields to subclass this RelatedModel instead of RedisModel.
    """

    abstract = True
    collection_manager = ExtendedCollectionManager

    def __init__(self, *args, **kwargs):
        """
        Create the instance then add all related collections (link between this
        instance and related fields on other models)
        """
        super(RelatedModel, self).__init__(*args, **kwargs)

        # create the related collections
        self.related_collections = []
        relations = getattr(self.database, '_relations', {}).get(self._name.lower(), [])
        for relation in relations:
            # get the related field
            model_name, field_name, _ = relation
            related_model = self.database._models[model_name]
            related_field = related_model.get_field(field_name)

            # add the collection
            collection = related_field.related_collection_class(self, related_field)
            setattr(self, related_field.related_name, collection)
            self.related_collections.append(related_field.related_name)

    def delete(self):
        """
        When the instance is deleted, we propagate the deletion to the related
        collections, which will remove it from the related fields.
        """
        for related_collection_name in self.related_collections:
            related_collection = getattr(self, related_collection_name)
            related_collection.remove_instance()
        return super(RelatedModel, self).delete()

    @classmethod
    def use_database(cls, database):
        """
        Move model and its submodels to the new database, as the original
        use_database method. And manage relations too (done here instead of
        the database because the database class is not aware of relations)
        """
        original_database = getattr(cls, 'database', None)
        impacted_models = super(RelatedModel, cls).use_database(database)

        # no relation on the current database, so nothing nore to transfer
        if not original_database or not getattr(original_database, '_relations', {}):
            return impacted_models

        # prepare relations to remove impacted ones
        # relations in original_database._relations have this format
        # related_model => (original_model, original_field, related_name)
        # Here we want all relations with an original_model in our list of
        # models impacted by the change of database (impacted_models), to move
        # these relation on the new database
        reverse_relations = {}
        for related_model_name, relations in original_database._relations.items():
            for relation in relations:
                reverse_relations.setdefault(relation[0], []).append((related_model_name, relation))

        # create an dict to store relations in the new database
        if not hasattr(database, '_relations'):
            database._relations = {}

        # move relation for all impacted models
        for _model in impacted_models:
            if _model.abstract:
                continue
            for related_model_name, relation in reverse_relations[_model._name]:
                # if the related model name is already used as a relation, check
                # if it's not already used with the related_name of the relation
                if related_model_name in database._relations:
                    field = _model.get_field(relation[1])
                    field._assert_relation_does_not_exists()
                # move the relation from the original database to the new
                original_database._relations[related_model_name].remove(relation)
                database._relations.setdefault(related_model_name, []).append(relation)

        return impacted_models


class RelatedFieldMetaclass(fields.MetaRedisProxy):
    """
    Metaclass for RelatedFieldMixin that will create methods for all commands
    defined in "_commands_with_single_value_from_python" and
    "_commands_with_many_values_from_python", to override the default
    behaviour: convert values given as object to their pk, before calling the
    super method
    """

    def __new__(mcs, name, base, dct):
        it = super(RelatedFieldMetaclass, mcs).__new__(mcs, name, base, dct)
        for command_name in it._commands_with_single_value_from_python:
            setattr(it, command_name, it._make_command_method(command_name, many=False))
        for command_name in it._commands_with_many_values_from_python:
            setattr(it, command_name, it._make_command_method(command_name, many=True))
        return it


class RelatedFieldMixin(with_metaclass(RelatedFieldMetaclass)):
    """
    Base mixin for all fields holding related instances.
    This mixin provides:
    - force indexable to True
    - a "from_python" method that can translate a RedisModel instance, or a
      FK field in its pk (useful to pass object instead of "object._pk" when
      adding/removing a FK)
      All commands that may receive objects as arguments must call this
      "from_python" method (or "from_python_many" if many values to convert).
      To do this automatically, simply add command names that accept only one
      value in "_commands_with_single_value_from_python" and ones that accept
      many values (without any other arguments) in
      "_commands_with_many_values_from_python" (see RelatedFieldMetaclass)
    - management of related parameters: "to" and "related_name"
    """

    _copy_conf = copy(fields.RedisField._copy_conf)
    _copy_conf['kwargs'] += [('to', 'related_to'), 'related_name']

    _commands_with_single_value_from_python = []
    _commands_with_many_values_from_python = []

    related_collection_class = RelatedCollection

    def __init__(self, to, *args, **kwargs):
        """
        Force the field to be indexable and save related arguments.
        """
        kwargs['indexable'] = True
        super(RelatedFieldMixin, self).__init__(*args, **kwargs)

        self.related_to = to
        self.related_name = kwargs.pop('related_name', None)

    def _attach_to_model(self, model):
        """
        When we have a model, save the relation in the database, to later create
        RelatedCollection objects in the related model
        """
        super(RelatedFieldMixin, self)._attach_to_model(model)

        if model.abstract:
            # do not manage the relation if it's an abstract model
            return

        # now, check related_name and save the relation in the database

        # get related parameters to identify the relation
        self.related_name = self._get_related_name()
        self.related_to = self._get_related_model_name()

        # create entry for the model in the _relations list of the database
        if not hasattr(self.database, '_relations'):
            self.database._relations = {}
        self.database._relations.setdefault(self.related_to, [])

        # check unicity of related name for related model
        self._assert_relation_does_not_exists()

        # the relation didn't exists, we can save it
        relation = (self._model._name, self.name, self.related_name)
        self.database._relations[self.related_to].append(relation)

    def _assert_relation_does_not_exists(self):
        """
        Check if a relation with the current related_name doesn't already exists
        for the related model
        """
        relations = self.database._relations[self.related_to]
        existing = [r for r in relations if r[2] == self.related_name]
        if existing:
            error = ("The related name defined for the field '%s.%s', named '%s', already exists "
                     "on the model '%s' (tied to the field '%s.%s')")
            raise ImplementationError(error % (self._model._name, self.name, self.related_name,
                                      self.related_to, existing[0][1], existing[0][0]))

    def _get_related_model_name(self):
        """
        Return the name of the related model, as used to store all models in the
        database object, in the following format: "%(namespace)s:%(class_name)"
        The result is computed from the "to" argument of the RelatedField
        constructor, stored in the "related_to" attribute, based on theses rules :
        - if a "RelatedModel" subclass, get its namespace:name
        - if a string :
          - if "self", get from the current model (relation on self)
          - if a RelatedModel class name, keep it, and if no namesace, use the
            namespace of the current model
        """
        if isinstance(self.related_to, type) and issubclass(self.related_to, RelatedModel):
            model_name = self.related_to._name

        elif isinstance(self.related_to, str):
            if self.related_to == 'self':
                model_name = self._model._name
            elif ':' not in self.related_to:
                model_name = ':'.join((self._model.namespace, self.related_to))
            else:
                model_name = self.related_to

        else:
            raise ImplementationError("The `to` argument to a related field "
                                      "must be a RelatedModel as a class or as "
                                      "a string (with or without namespace). "
                                      "Or simply 'self'.")

        return model_name.lower()

    def _get_related_name(self):
        """
        Return the related name to use to access this related field.
        If the related_name argument is not defined in its declaration,
        a new one will be computed following this format: '%s_set' with %s the
        name of the model owning the related.field.
        If the related_name argument exists, it can be the exact name to use (
        be careful to use a valid python attribute name), or a string where you
        can set placeholder for the namespace and the model of the current
        model. It's useful if the current model is abstract with many subclasses

        Exemples:
            class Base(RelatedModel):
                abstract = True
                namespace = 'project'
                a_field = FKStringField('Other', related_name='%(namespace)s_%(model)s_related')
                    # => related names accessible from Other are
                    #    "project_childa_related" and "project_childb_related"

            class ChildA(Base):
                pass

            class ChildB(Base):
                pass

            class Other(RelatedModel):
                namespace = 'project'
                field_a = FKStringField(ChildA)
                    # => related name accessible from ChildA and ChildB will be "other_related"
                field_b = FKStringField(ChildB, related_name='some_others')
                    # => related name accessible from ChildA and ChildB will be "some_others"
        """
        related_name = self.related_name or '%(model)s_set'
        related_name = related_name % {
            'namespace': re_identifier.sub('_', self._model.namespace),
            'model': self._model.__name__
        }
        if re_identifier.sub('', related_name) != related_name:
            raise ImplementationError('The related_name "%s" is not a valid '
                                      'python identifier' % related_name)

        return related_name.lower()

    def from_python(self, value):
        """
        Provide the ability to pass a RedisModel instances or a FK field as
        value instead of passing the PK. The value will then be translated in
        the real PK.
        """
        if isinstance(value, model.RedisModel):
            value = value._pk
        elif isinstance(value, SimpleValueRelatedFieldMixin):
            value = value.proxy_get()
        return value

    def from_python_many(self, *values):
        """
        Apply the from_python to each values and return the final list
        """
        return list(map(self.from_python, values))

    @classmethod
    def _make_command_method(cls, command_name, many=False):
        """
        Return a function which will convert objects to their pk, then call the
        super method for the given name.
        The "many" attribute indicates that the command accept one or many
        values as arguments (in *args)
        """
        def func(self, *args, **kwargs):
            if many:
                args = self.from_python_many(*args)
            else:
                if 'value' in kwargs:
                    kwargs['value'] = self.from_python(kwargs['value'])
                else:
                    args = list(args)
                    args[0] = self.from_python(args[0])

            # call the super method, with real pk
            sup_method = getattr(super(cls, self), command_name)
            return sup_method(*args, **kwargs)
        return func


class SimpleValueRelatedFieldMixin(RelatedFieldMixin):
    """
    Mixin for all related fields storing one unique value.
    Add a instance method to get the related object.
    Example:

        class Person(RelatedModel):
            name = PKField()

        class Group(RelatedModel):
            name = PKField()
            owner = FKStringField(Person, related_name='owned_groups')

        person = Person(name='person')
        group = Group(name='group', owner=person)

        # returns the owner's pk
        group.owner.get()

        # return the full person instance
        group.owner.instance()

    """
    def instance(self, skip_exist_test=False):
        """
        Returns the instance of the related object linked by the field.
        """
        model = self.database._models[self.related_to]
        meth = model.lazy_connect if skip_exist_test else model
        return meth(self.proxy_get())


class FKStringField(SimpleValueRelatedFieldMixin, fields.StringField):
    """ Related field based on a StringField, acting as a Foreign Key """
    _commands_with_single_value_from_python = ['set', 'setnx', 'getset', ]


class FKInstanceHashField(SimpleValueRelatedFieldMixin, fields.InstanceHashField):
    """ Related field based on a InstanceHashField, acting as a Foreign Key """
    _commands_with_single_value_from_python = ['hset', 'hsetnx', ]


class MultiValuesRelatedFieldMixin(RelatedFieldMixin):
    """
    Mixin for all related fields based on MultiValuesField.
    Add a __call__ method creating a collection to use the field the same way
    we can use the RelatedCollection on the other side.
    Example showing both sides:

        class Person(RelatedModel):
            name = PKField()

        class Group(RelatedModel):
            name = PKField()
            members = M2MSetField(Person, related_name='membership')

        person1 = Person(name='person1')
        person2 = Person(name='person2')
        group1 = Group(name='group1')
        group2 = Group(name='group2')

        group1.members.sadd(person1, person2)
        group2.members.sadd(person1, person2)

        # A RelatedColleciton that return a collection with set(['person1', 'person2'])
        person1.membership()
        # A M2MSetField call, that return a collection with set(['group1', 'group2'])
        group1.members()

    """

    def __call__(self, **filters):
        """
        When calling (via `()` or via `.collection()`) a MultiValuesRelatedField,
        we return a collection, filtered with given arguments, the result beeing
        "intersected" with the members of the current field.
        """
        model = self.database._models[self.related_to]
        collection = model.collection_manager(model)
        return collection(**filters).intersect(self)

    # calling obj.field.collection() is the same as calling obj.field()
    collection = __call__


class M2MSetField(MultiValuesRelatedFieldMixin, fields.SetField):
    """ Related field based on a SetField, acting as a M2M """
    _commands_with_single_value_from_python = ['sismember', ]
    _commands_with_many_values_from_python = ['sadd', 'srem', ]
    _related_remover = 'srem'


class M2MListField(MultiValuesRelatedFieldMixin, fields.ListField):
    """ Related field based on a ListField, acting as a sorted M2M """
    _commands_with_single_value_from_python = ['lpushx', 'rpushx', ]
    _commands_with_many_values_from_python = ['lpush', 'rpush', ]
    _related_remover = 'lrem'

    def linsert(self, where, refvalue, value):
        value = self.from_python(value)
        return super(M2MListField, self).linsert(where, refvalue, value)

    def lrem(self, count, value):
        value = self.from_python(value)
        return super(M2MListField, self).lrem(count, value)

    def lset(self, index, value):
        value = self.from_python(value)
        return super(M2MListField, self).lset(index, value)


class M2MSortedSetField(MultiValuesRelatedFieldMixin, fields.SortedSetField):
    """ Related field based on a SortesSetField, acting as a M2M with scores """
    _commands_with_single_value_from_python = ['zscore', 'zrank', 'zrevrank']
    _commands_with_many_values_from_python = ['zrem']
    _related_remover = 'zrem'

    def zadd(self, *args, **kwargs):
        """
        Parse args and kwargs to check values to pass them through the
        from_python method.
        We pass the parsed args/kwargs as args in the super call, to avoid
        doing the same calculation on kwargs one more time.
        """
        if 'values_callback' not in kwargs:
            kwargs['values_callback'] = self.from_python_many
        pieces = fields.SortedSetField.coerce_zadd_args(*args, **kwargs)
        return super(M2MSortedSetField, self).zadd(*pieces)

    def zincrby(self, value, amount=1):
        value = self.from_python(value)
        return super(M2MSortedSetField, self).zincrby(value, amount)
