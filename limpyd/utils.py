import uuid

from functools import wraps
from logging import getLogger

log = getLogger(__name__)


def make_key(*args):
    """Create the key concatening all args with `:`."""
    return u":".join(unicode(arg) for arg in args)


def unique_key(connection):
    """
    Generate a unique keyname that does not exists is the connection
    keyspace.
    """
    while 1:
        key = uuid.uuid4().hex
        if not connection.exists(key):
            break
    return key
