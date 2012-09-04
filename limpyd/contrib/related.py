# -*- coding:utf-8 -*-

import re
from copy import copy
from redis.exceptions import RedisError

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
            related_field = getattr(related_model, '_redis_attr_%s' % field_name)

            # add the collection
            collection = RelatedCollection(self, related_field)
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


class RelatedFieldMixin(fields.RedisField):
    """
    Base mixin for all fields holding related instances.
    This mixin provides:
    - force indexable to True
    - a "from_python" method that can translate RedisModel instances in their pk
      (useful to pass object insteand of "object._pk" when adding/removing a FK)
    All commands that may receive objects as arguments must call This
    "from_python" method. To do this automatically, simple add command names
    that accept only one value in "_commands_with_single_value_from_python" and
    ones that accept many values (without any other arguments) in
    "_commands_with_many_values_from_python" (see RelatedFieldMetaclass)
    - management of related parameters: "to" and "related_name"
    """
    __metaclass__ = RelatedFieldMetaclass

    _copy_conf = copy(fields.RedisField._copy_conf)
    _copy_conf['kwargs'] += [('to', 'related_to'), 'related_name']

    _commands_with_single_value_from_python = []
    _commands_with_many_values_from_python = []

    def __init__(self, to, *args, **kwargs):
        """
        Force the field to be indexable and save related arguments.
        We also disable caching because cache is instance-related, and when
        we delete a object linked to a related field, we need to instanciate
        all instances linked to it to remove the link. But with cache enabled,
        the cache of just created instances is cleared, but not ones of
        already existing ones. Test "test_deleting_an_object_must_clear_the_fk"
        fails with caching enabled. Consider moving cache from instances to the
        database could be an option.
        """
        kwargs['indexable'] = True
        kwargs['cacheable'] = False
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

        # relation to save in the database
        relation = (self._model._name, self.name, self.related_name)

        # check if a relation with the current related_name doesn't already
        # exists for the related model
        existing = [r for r in self.database._relations[self.related_to] if r[2] == self.related_name]
        if existing:
            raise ImplementationError(
                "The related name defined for the field '%s.%s', named '%s', already exists on the model '%s'  (tied to the field '%s.%s')"
                 % (self._model._name, self.name, self.related_name, self._model._name, existing[0][1], existing[0][0]))

        # the relation didn't exists, we can save it
        self.database._relations[self.related_to].append(relation)

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

        elif isinstance(self.related_to, basestring):
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

    def from_python(self, values):
        """
        Provide the ability to pass RedisModel instances as values. They are
        translated to their own pk.
        """
        result = []
        for value in values:
            if isinstance(value, model.RedisModel):
                value = value._pk
            result.append(value)
        return result

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
                args = self.from_python(args)
            else:
                if 'value' in kwargs:
                    kwargs['value'] = self.from_python([kwargs['value']])[0]
                else:
                    args = list(args)
                    args[0] = self.from_python([args[0]])[0]

            # call the super method, with real pk
            sup_method = getattr(super(cls, self), command_name)
            return sup_method(*args, **kwargs)
        return func


class FKStringField(RelatedFieldMixin, fields.StringField):
    """ Related field based on a StringField, acting as a Foreign Key """
    _commands_with_single_value_from_python = ['set', 'setnx', 'getset', ]


class FKHashableField(RelatedFieldMixin, fields.HashableField):
    """ Related field based on a HashableField, acting as a Foreign Key """
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
        When calling a MultiValuesRelatedField, we return a collection,
        filtered with given arguments, the result beeing "intersected" with the
        members of the current field.
        """
        model = self.database._models[self.related_to]
        manager = ExtendedCollectionManager(model)
        collection = manager(**filters)
        collection.intersect(self)
        return collection


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
        value = self.from_python([value])[0]
        return super(M2MListField, self).linsert(where, refvalue, value)

    def lrem(self, count, value):
        value = self.from_python([value])[0]
        return super(M2MListField, self).lrem(count, value)

    def lset(self, index, value):
        value = self.from_python([value])[0]
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
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(args)
        for pair in kwargs.iteritems():
            pieces.append(pair[1])
            pieces.append(pair[0])

        values = self.from_python(pieces[1::2])
        scores = pieces[0::2]

        pieces = []
        for z in zip(scores, values):
            pieces.extend(z)

        return super(M2MSortedSetField, self).zadd(*pieces)

    def zincrby(self, value, amount=1):
        value = self.from_python[value]
        return super(M2MSortedSetField, self).zincrby(value, amout)
