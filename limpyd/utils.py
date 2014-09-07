from __future__ import unicode_literals
from future.builtins import str, bytes

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
