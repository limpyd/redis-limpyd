from logging import getLogger

from limpyd import get_connection
from limpyd.utils import make_key

log = getLogger(__name__)

__all__ = [
    'HashableField',
    'RedisField',
    'RedisProxyCommand',
    'SortedSetField',
    'StringField',
]


class RedisProxyCommand(object):

    def __getattr__(self, name):
        """
        Return the function in redis when not found in the abstractmodel.
        """
        return lambda *args, **kwargs: self._traverse_command(name, *args, **kwargs)

    def _traverse_command(self, name, *args, **kwargs):
        """Add the key to the args and call the Redis command."""
        # TODO: implement instance level cache
        attr = getattr(self.connection(), "%s" % name)
        key = self.key
        log.debug(u"Requesting %s with key %s and args %s" % (name, key, args))
        return attr(key, *args, **kwargs)


class RedisField(RedisProxyCommand):
    """
    Wrapper to help use the redis data structures.
    """

    def __init__(self, *args, **kwargs):
        self.indexable = False
        self._instance = None

    @property
    def key(self):
        return self.make_key(
            self._instance.__class__.__name__.lower(),
            self._instance.pk,
            self.name,
        )

    def connection(self):
        if self._instance:
            return self._instance.connection()
        else:
            return get_connection()

    def exists(self, value):
        raise NotImplementedError("Only indexable fields can be used")

    def __copy__(self):
        new_copy = self.__class__()
        new_copy.__dict__ = self.__dict__
        return new_copy

    def make_key(self, *args):
        return make_key(*args)


class IndexableField(RedisField):
    """
    Base field for the indexable fields.

    Store data in index at save.
    Retrieve instances from these indexes.
    """

    def __init__(self, *args, **kwargs):
        super(IndexableField, self).__init__(*args, **kwargs)
        self.indexable = "indexable" in kwargs and kwargs["indexable"] or False

    def _traverse_command(self, name, *args, **kwargs):
        # TODO manage transaction
        # TODO better handling of "set" actions
        if self.indexable and ("set" in name or "append" in name):
            self.deindex()
        result = super(IndexableField, self)._traverse_command(name, *args, **kwargs)
        if self.indexable and ("set" in name or "append" in name):
            self.index()
        return result

    def index(self):
        # TODO: manage uniqueness
        value = self.get().decode('utf-8')
        key = self.index_key(value)
#        print "indexing %s with key %s" % (key, self._instance.pk)
        return self.connection().set(key, self._instance.pk)

    def deindex(self):
        """
        Remove stored index if needed.
        """
        value = self.get()
        if value:
            value = value.decode('utf-8')
            key = self.index_key(value)
            return self.connection().delete(key)
        else:
            return True  # True?

    def index_key(self, value):
        # Ex. bikemodel:name:whatabike
        return self.make_key(
            self._parent_class,
            self.name,
            value,
        )

    def populate_instance_pk_from_index(self, value):
        key = self.index_key(value)
#        print "Looking for pk from index key %s" % key
        pk = self.connection().get(key)
        if pk:
            self._instance._pk = pk
        else:
            raise ValueError("Can't retrieve instance pk with %s = %s" % (self.name, value))

    def exists(self, value):
        # TODO factorize with the previous
        if not self.indexable:
            raise ValueError("Only indexable fields can be used")
        key = self.index_key(value)
        pk = self.connection().get(key)
        return pk is not None


class StringField(IndexableField):
    pass


class SortedSetField(RedisField):
    pass


class HashableField(IndexableField):
    """Field stored in the parent object hash."""

    @property
    def key(self):
        return self._instance.key

    def _traverse_command(self, name, *args, **kwargs):
        """Add key AND the hash field to the args, and call the Redis command."""
        # self.name is the name of the hash key field
        args = list(args)
        args.insert(0, self.name)
        return super(HashableField, self)._traverse_command(name, *args, **kwargs)

    def get(self):
        """get and set are made generic, to be used as a common API."""
        return self.hget()
