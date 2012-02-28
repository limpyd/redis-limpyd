from limpyd import get_connection
from limpyd.utils import make_key

__all__ = ['StringField', 'SortedSetField', 'RedisField']

class RedisField(object):
    """
    Wrapper to help use the redis data structures.
    """
    

    def __init__(self, *args, **kwargs):
        self.indexable = False
        self._instance = None

    def __getattr__(self, name):
        """
        Return the function in redis when not found in the abstractmodel.
        """
        return lambda *args, **kwargs: self._traverse_command(name, *args, **kwargs)

    def _traverse_command(self, name, *args, **kwargs):
        attr = getattr(self.connection(), "%s" % name)
        key = self.key()
        return attr(key, *args, **kwargs)

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


class StringField(RedisField):

    def __init__(self, *args, **kwargs):
        super(StringField, self).__init__(*args, **kwargs)
        self.indexable = "indexable" in kwargs and kwargs["indexable"] or False

    def _traverse_command(self, name, *args, **kwargs):
        # TODO manage transaction
        result = super(StringField, self)._traverse_command(name, *args, **kwargs)
        if self.indexable and ("set" in name or "append" in name):
            self.index()
        return result

    def index(self):
        value = self.get().decode('utf-8')
        key = self.index_key(value)
#        print "indexing %s with key %s" % (key, self._instance.pk)
        return self.connection().set(key, self._instance.pk)

    def index_key(self, value):
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


class SortedSetField(RedisField):
    pass        


