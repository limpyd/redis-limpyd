*****
Cache
*****

As we don't store field values in the object, and to avoid querying Redis_ each time we need a value, `limpyd` implements a level of local cache.


On the model
============

This cache is activated by default for each model. To deactivate it, it's as simple as adding the attribute `cacheable` to False on the model::

    class Example(model.RedisModel):
        database = main_database
        cacheable = False

        a_field = fields.StringField()

The use of the cache is transparent. If you got a value from a field, without updating it after that, the next time you'll get it, the value will be fetched from the cache. When a field is updated, its cached is cleared.

Example::

    >>> example = Example()
    >>> example.a_field.set('foo')
    True
    >>> example.a_field.get()  # call Redis_
    'foo'
    >>> example.a_field.get()  # hit the cache
    'foo'
    >>> example.a_field.set('bar')  # clear the cache
    True
    >>> example.a_field.get()  # call Redis_
    'bar'


On fields
=========

If the cache is activated on the model, you can deactivate it at the field level. The reverse is not True (if the cache is deactivated for the model, you cannot activate it for a field).

To deactivate it for the field, just set the `cacheable` argument to True::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField()
        bar = fields.StringField(cacheable=False)

Here the cache is activated for `foo` but not for `bar`.


WARNING
=======

Be careful that the cache is on the instance itself. If you create another instance on the same object, update a field, the cache from the first instance will not be cleared. It's also obviously the case if you work with multiple threads of workers.

