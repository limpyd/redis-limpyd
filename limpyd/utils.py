from __future__ import unicode_literals
from future.builtins import str, bytes, object

import uuid

from logging import getLogger

log = getLogger(__name__)


def make_key(*args):
    """Create the key concatening all args with `:`."""
    return u":".join(str(arg) for arg in args)


def unique_key(connection):
    """
    Generate a unique keyname that does not exists is the connection
    keyspace.
    """
    while 1:
        key = str(uuid.uuid4().hex)
        if not connection.exists(key):
            break
    return key


def normalize(value):
    """
    Simple method to always have the same kind of value
    """
    if value and isinstance(value, bytes):
        value = value.decode('utf-8')
    return value


class cached_property(object):
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    Optional ``name`` argument allows you to make cached properties of other
    methods. (e.g.  url = cached_property(get_absolute_url, name='url') )

    From https://github.com/django/django/blob/27793431cf21a82809c0c39a7c0188a2d83bf475/django/utils/functional.py#L15
    """
    def __init__(self, func, name=None):
        self.func = func
        self.__doc__ = getattr(func, '__doc__')
        self.name = name or func.__name__

    def __get__(self, instance, cls=None):
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res
