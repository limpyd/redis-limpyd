
******
Fields
******

The core module of `limpyd` provides 6 fields types, matching the ones in Redis_:

- StringField_, for the main data type in `Redis`, strings
- HashField_, for dicts
- InstanceHashField_, for hashes
- SetField_, for sets
- ListField_, for lists
- SortedSetField_, for sorted sets

You can also manage primary keys with these too fields:

- PKField_, based on StringField_
- AutoPKField_, same as PKField_ but auto-incremented.

All these fields can be indexed, and they manage the keys for you (they take the same arguments as the real Redis_ ones, as defined in the `StrictRedis` class of redis-py_, but without the `key` parameter).

Another thing all fields have in common, is the way to delete them: use the `delete` method on a field, and both the field and its value will be removed from Redis_.


Field attributes
================

When adding fields to a model, you can configure it with some attributes:

**default**

It's possible to set default values for fields of type StringField_ and InstanceHashField_::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(default='FOO')
        bar = fields.StringField()

    >>> example = Example(bar='BAR')
    >>> example.foo.get()
    'FOO'

When setting a default value, the field will be saved when creating the instance. If you defined a PKField_ (not AutoPKField_), don't forget to pass a value for it when creating the instance, it's needed to store other fields.


**indexable**

Sometimes getting objects from Redis_ by its primary key is not what you want. You may want to search for objects with a specific value for a specific field. 

By setting the `indexable` argument to True when defining the field, this functionnality is automatically activated, and you'll be able to retrieve objects by filtering on this field using :doc:`collections`.

To activate it, just set the `indexable` argument to True::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(indexable=True)
        bar = fields.StringField()

In this example you will be able to filter on the field `foo` but not on `bar`.

See :doc:`collections` to know how to filter objects.

When updating an indexable field, a lock is acquired on Redis on this field, for all instances of the model. It wasn't possible to use pipeline or redis scripting, because both need to know in advance keys to update, but we don't always know since keys for indexes are based on values. So a all *writing* operations on an indexable field are protected, to ensure consistensy if many threads, process, servers are working on the same redis database.

If you are sure you have only one thread, or you don't want to ensure consistency, you can disable locking by setting to False the `lockable` argument when creating a field, or the `lockable` attribute of a model to inactive the lock for all of its fields.

**unique**

The `unique` argument is the same as the `indexable` one, except it will ensure that you can't have multiple objects with the same value for some fields. `unique` fields are also indexed, and can be filtered, as for the `indexable` argument.

Example::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(indexable=True)
        bar = fields.StringField(unique=True)

    >>> example1 = Example(foo='FOO', bar='BAR')
    True
    >>> example2 = Example(foo='FOO', bar='BAR')
    UniquenessError: Key :example:bar:BAR already exists (for instance 1)

See :doc:`collections` to know how to filter objects, as for `indexable`.

**lockable**

You can set this argument to False if you don't want a lock to be acquired on this field for all instances of the model. See `indexable` for more informations about locking.

If not specified, it's default to True, except if the `lockable` attribute of the model is False, in which case it's forced to False for all fields.



Field types
===========

.. _StringField:

StringField
-----------

StringField_ based fields allow the storage of strings, but some `Redis string commands <http://redis.io/commands#string>`_ allow to treat them as integer, float or bits.

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        name = fields.StringField()

You can use this model like this::
    
    >>> example = Example(name='foo')
    >>> example.name.get()
    'foo'
    >>> example.name.set('bar')
    >>> example.name.get()
    'bar'
    >> example.delete()

The StringField_ type support these `Redis string commands`_:

**Getters:**

- `bitcount`
- `get`
- `getbit`
- `getrange`
- `getset`
- `strlen`

**Modifiers:**

- `append`
- `decr`
- `getset`
- `incr`
- `incrbyfloat`
- `set`
- `setbit`
- `setnx`
- `setrange`


.. _HashField:

HashField
---------

HashField allows storage of a dict in Redis.

Example::

    class Email(model.RedisModel):
        database = main_database
        headers = fields.HashField()

    >>> email = Email()
    >>> headers = {'from': 'foo@bar.com', 'to': 'me@world.org'}
    >>> email.headers.hmset(**headers)
    >>> email.headers.hget('from')
    'foo@bar.com'

Supported commands:

**Getters:**

- `hget`
- `hgetall`
- `hmget`
- `hkeys`
- `hvals`
- `hexists`
- `hlen`


**Modifiers:**

- `hdel`
- `hmset`
- `hsetnx`
- `hset`
- `hincrby`
- `hincrbyfloat`

.. _InstanceHashField:

InstanceHashField
-----------------

As for StringField_, InstanceHashField_ based fields allow the storage of strings. But all the `InstanceHashField` fields of an instance are stored in the same Redis_ hash, the name of the field being the key in the hash.

To fully use the power of Redis_ hashes, we also provide two methods to get and set multiples field in one operation (see hmget_ and hmset_). It's usually cheaper to store fields in hash that in strings. And it's faster to set/retrieve them using these two commands.

Example with simple commands::

    class Example(model.RedisModel):
        database = main_database

        foo = fields.InstanceHashField()
        bar = fields.InstanceHashField()

    >>> example.foo.hset('FOO')
    1  # 1 because the hash field was created
    >>> example.foo.hget()
    'FOO'

The InstanceHashField_ type support these `Redis hash commands <http://redis.io/commands#hash>`_:

**Getters:**

- hget

**Modifiers:**

- `hincrby`
- `hincrbyfloat`
- `hset`
- `hsetnx`

**Deleter:**

* Note that to delete the value of a InstanceHashField_, you can use the `hdel` command, which do the same as the main `delete` one.

**Multi:**

The following commands are not called on the fields themselves, but on an instance:

- hmget_
- hmset_
- hgetall_
- hkeys_
- hvals_
- hlen_

.. _InstanceHashField-hmget:

hmget
"""""

hmget_ is called directly on an instance, and expects a list of field names to retrieve.

The result will be, as in Redis_, a list of all values, in the same order.

If no names are provided, nothing will be fetched. Use hvals_, or better, hgetall_ to get values for all InstanceHashFields

It's up to you to associate names and values, but you can find an example below::

    class Example(model.RedisModel):
        database = main_database

        foo = fields.InstanceHashField()
        bar = fields.InstanceHashField()

        def hmget_dict(self, *args):
            """
            A call to hmget but which return a dict with field names as keys, instead
            of only a list of values
            """
            values = self.hmget(*args)
            keys = args or self._hashable_fields
            return dict(zip(keys, values))


    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hmget('foo', 'bar')
    ['FOO', 'BAR']
    >>> example.hmget_dict('foo', 'bar')
    {'bar': 'BAR', 'foo': 'FOO'}

hmset
"""""

hmset_ is the reverse of hmget_, and also called directly on an instance, and expects
named arguments with field names as keys, and new values to set as values.

Example (with same model as for hmget_)::

    >>> example = Example()
    >>> example.hmset(foo='FOO', bar='BAR')
    True
    >>> example.hmget('foo', 'bar')
    ['FOO', 'BAR']

hgetall
"""""""

hgetall_ must be called directly on an instance, and will return a dictionary containing names and values of all InstanceHashField with a stored value.
If a field has no stored value, it will not appear in the result of hgetall_.

Example (with same model as for hmget_)::

    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hgetall()
    {'foo': 'FOO', 'bar': 'BAR'}
    >>> example.foo.hdel()
    >>> example.hgetall()
    {bar': 'BAR'}

hkeys
"""""

hkeys_ must be called on an instance and will return the name of all the InstanceHashField with a stored value.
If a field has no stored value, it will not appear in the result of hkeys_.
Note that the result is not ordered in any way.

Example (with same model as for hmget_)::

    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hkeys()
    ['foo', 'bar']
    >>> example.foo.hdel()
    >>> example.hkeys()
    ['bar']

hvals
"""""

hkeys_ must be called on an instance and will return the value of all the InstanceHashField with a stored value.
If a field has no stored value, it will not appear in the result of hvals_.
Note that the result is not ordered in any way.

Example (with same model as for hmget_)::

    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hvals()
    ['FOO', 'BAR']
    >>> example.foo.hdel()
    >>> example.hvals()
    ['BAR']

hlen
""""
hlen_ must be called on an instance and will return the number of InstanceHashField with a stored value.
If a field has no stored value, it will not be count in the result of hlen_.

Example (with same model as for hmget_)::

    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hlen()
    2
    >>> example.foo.hdel()
    >>> example.hlen()
    1


.. _SetField:

SetField
--------

SetField_ based fields can store many values in one field, using the set data type of Redis_, an unordered set (with unique values).

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.SetField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.sadd('foo', 'bar')
    2  # number of values really added to the set
    >>> example.stuff.smembers()
    set(['foo', 'bar'])
    >>> example.stuff.sismember('bar')
    True
    >>> example.stuff.srem('bar')
    True
    >>> example.stuff.smembers()
    set(['foo'])
    >>> example.stuff.delete()
    True

The SetField_ type support these `Redis set commands <http://redis.io/commands#set>`_:

**Getters:**

- `scard`
- `sismember`
- `smembers`
- `srandmember`

**Modifiers:**

- `sadd`
- `spop`
- `srem`


.. _ListField:

ListField
---------

ListField_ based fields can store many values in one field, using the list data type of Redis_. Values are ordered, and are not unique (you can push many times the same value).

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.ListField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.rpush('foo', 'bar')
    2  # number of values added to the list
    >>> example.stuff.lrange(0, -1)
    ['foo', 'bar']
    >>> example.stuff.lindex(1)
    'bar'
    >>> example.stuff.lrem(1, 'bar')
    1  # number of values really removed
    >>> example.stuff.lrange(0, -1)
    ['foo']
    >>> example.stuff.delete()
    True

The ListField_ type support these `Redis list commands <http://redis.io/commands#list>`_:

**Getters:**

- `lindex`
- `llen`
- `lrange`

**Modifiers:**

- `linsert`
- `lpop`
- `lpush`
- `lpushx`
- `lrem`
- `lset`
- `ltrim`
- `rpop`
- `rpush`
- `rpushx`


.. _SortedSetfield:

SortedSetField
--------------

SortedSetField_ based fields can store many values, each scored, in one field using the sorted-set data type of Redis_. Values are unique (it's a set), and are ordered by their score.

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.SortedSetField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.zadd(foo=2.5, bar=1.1)
    2  # number of values added to the sorted set
    >>> example.stuff.zrange(0, -1)
    ['bar', 'foo']
    >>> example.stuff.zrangebyscore(1, 2, withscores=True)
    [('bar', 1.1)]
    >>> example.stuff.zrem('bar')
    1  # number of values really removed
    >>> example.stuff.zrangebyscore(1, 2, withscores=True)
    []
    >>> example.stuff.delete()
    True

The SortedSetField_ type support these `Redis sorted set commands <http://redis.io/commands#sorted_set>`_:

**Getters:**

- `zcard`
- `zcount`
- `zrange`
- `zrangebyscore`
- `zrank`
- `zrevrange`
- `zrevrangebyscore`
- `zrevrank`
- `zscore`

**Modifiers:**

- `zadd`
- `zincrby`
- `zrem`
- `zremrangebyrank`
- `zremrangebyscore`


.. _PKField:

PKField
-------

PKField_ is a special subclass of StringField_ that manage primary keys of models. The PK of an object cannot be updated, as it serves to create keys of all its stored fields. It's this PK that is returned, with others, in :doc:`collections`.

A PK can contain any sort of string you want: simple integers, float, long uuid, names...

If you want a PKField which will be automatically filled, and auto-incremented, see AutoPKField_. Otherwise, with standard PKField_, you must assign a value to it when creating an instance.

By default, a model has a AutoPKField_ attached to it, named `pk`. But you can redefine the nameand type of PKField you want.

Examples::

    class Foo(model.RedisModel):
        """
        The PK field is `pk`, and will be auto-incremented.
        """
        database = main_database

    class Bar(model.RedisModel):
        """
        The PK field is `id`, and will be auto-incremented.
        """
        database = main_database
        id = fields.AutoPKField()

    class Baz(model.RedisModel):
        """
        The PK field is `name`, and won't be auto-incremented, so you must assign it a value when creating an instance.
        """
        database = main_database
        name = fields.PKField()

Note that wathever name you use for the PKField_ (or AutoPKField_), you can always access it via the name `pk` (but also we its real name). It's easier for abstraction.

To access the pk value of an object, you have many ways::

    class Example(model.RedisModel):
        database = main_database
        id = fields.AutoPKField()
        name = fields.StringField()

    >>> example = Example(name='foobar')
    >>> example.get_pk()
    1
    >>> example.pk.get()
    1
    >>> example.id.get()
    1


AutoPKField
-----------

A AutoPKField_ field is a PKField_ filled with auto-incremented integers, starting to 1. Assigning a value to of AutoPKField_ is forbidden.

It's a AutoPKField_ that is attached by default to every model, if no other one is defined.

See PKField_ for more details.

.. _Redis: http://redis.io
.. _redis-py: https://github.com/andymccurdy/redis-py

