# -*- coding:utf-8 -*-

from limpyd import model, fields


class RelatedCollection(object):

    def __init__(self, model, field):
        pass


class MetaRelatedModel(model.MetaRedisModel):
    pass


class RelatedModel(model.RedisModel):
    __metaclass__ = MetaRelatedModel

    abstract = True


class RelatedFieldMixin(object):
    pass


class FKStringField(RelatedFieldMixin, fields.StringField):
    pass


class FKHashableField(RelatedFieldMixin, fields.HashableField):
    pass


class M2MSetField(RelatedFieldMixin, fields.SetField):
    pass


class M2MListField(RelatedFieldMixin, fields.ListField):
    pass


class M2MSortedSetField(RelatedFieldMixin, fields.SortedSetField):
    pass
