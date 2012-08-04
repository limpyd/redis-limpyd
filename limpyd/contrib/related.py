# -*- coding:utf-8 -*-

from limpyd import model, fields
from redis.exceptions import RedisError


class RelatedCollection(object):

    def __init__(self, model, field):
        pass


class MetaRelatedModel(model.MetaRedisModel):
    pass


class RelatedModel(model.RedisModel):
    __metaclass__ = MetaRelatedModel

    abstract = True


class RelatedFieldMixin(object):
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
    "_commands_with_many_values_from_python"
    """

    _commands_with_single_value_from_python = []
    _commands_with_many_values_from_python = []

    def __init__(self, *args, **kwargs):
        """
        Force the field to be indexable
        """
        kwargs['indexable'] = True
        super(RelatedFieldMixin, self).__init__(*args, **kwargs)

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

    def _traverse_command(self, name, *args, **kwargs):
        """
        Handle the call to the "from_python" methods for all commands defined in
        _commands_with_single_value_from_python or _commands_with_many_values_from_python
        """
        if name in self._commands_with_single_value_from_python:
            if 'value' in kwargs:
                kwargs['value'] = self.from_python([kwargs['value']])[0]
            else:
                args = list(args)
                args[0] = self.from_python([args[0]])[0]
        elif name in self._commands_with_many_values_from_python:
            args = self.from_python(args)
        return super(RelatedFieldMixin, self)._traverse_command(name, *args, **kwargs)


class FKStringField(RelatedFieldMixin, fields.StringField):
    _commands_with_single_value_from_python = ['set', 'setnx', 'getset', ]


class FKHashableField(RelatedFieldMixin, fields.HashableField):
    _commands_with_single_value_from_python = ['hset', 'hsetnx', ]


class M2MSetField(RelatedFieldMixin, fields.SetField):
    _commands_with_many_values_from_python = ['sadd', 'srem', ]


class M2MListField(RelatedFieldMixin, fields.ListField):
    _commands_with_many_values_from_python = ['lpush', 'rpush', 'lpushx', 'rpushx', ]

    def linsert(self, where, refvalue, value):
        value = self.from_python([value])[0]
        return super(M2MListField, self).linsert(where, refvalue, value)

    def lrem(self, count, value):
        value = self.from_python([value])[0]
        return super(M2MListField, self).lrem(count, value)

    def lset(self, index, value):
        value = self.from_python([value])[0]
        return super(M2MListField, self).lset(index, value)


class M2MSortedSetField(RelatedFieldMixin, fields.SortedSetField):
    _commands_with_many_values_from_python = ['zrem', ]

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
