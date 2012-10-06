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


def make_cache_key(*args, **kwargs):
    """
    Make a cache key with args and kwargs, unique for the same set of values
    in args and kwargs (), even if order (in list) is different
    """
    _args = []
    for arg in args:
        if isinstance(arg, (list, tuple)):
            arg = make_cache_key(*arg)
        elif isinstance(arg, dict):
            arg = make_cache_key(**arg)
        _args.append(arg)
    _kwargs = {}
    for key, value in kwargs.iteritems():
        if isinstance(value, (list, tuple)):
            value = make_cache_key(*value)
        elif isinstance(value, dict):
            value = make_cache_key(**value)
        _kwargs[key] = value
    return frozenset(tuple(_args) + tuple(_kwargs.items()))


class memoize_command(object):
    def __call__(self, func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # self here is a field instance
            command_name = args[0]

            if (not self.cacheable or self.database.discard_cache or
                    command_name in self.no_cache_getters):
                return func(self, *args, **kwargs)

            haxh = make_cache_key(*args, **kwargs)
            # "name" for a field, or "_name" for a model
            name = getattr(self, 'name', getattr(self, '_name', self))

            # Cache per object name to be able to flush per object (instance or field)
            if not self.has_cache():
                self.init_cache()
            cache = self.get_cache()
            # Warning: Some commands are both setter and modifiers (getset)
            command_name = args[0]
            if command_name in self.available_modifiers:
                # clear cache each time a modifier affects the field
                log.debug("Clearing cache for %s" % name)
                cache.clear()
            if haxh not in cache:
                # Run command and store result
                # It will be run only first time for getters and every time for
                # modifiers
                result = func(self, *args, **kwargs)
                if command_name in self.available_getters:
                    # Populate the cache if getter
                    log.debug("Storing key %s for %s" % (haxh, name))
                    cache[haxh] = result
            else:
                result = cache[haxh]
            return result
        return wrapper
