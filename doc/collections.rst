***********
Collections
***********

The main and obvious way to get data from Redis via ``limpyd`` is to know the primary key of objects and instantiate them one by one.

But some fields can be indexed, passing them the ``indexable`` or ``unique`` attribute.

If fields are indexed, it's possible to make queries to retrieve many of them, using the ``collection`` method on the models.

The filtering has some limitations:

- you can only filter on fields with ``indexable`` and/or ``unique`` attributes set to ``True``
- the filtering capabilities are limited and must be thought at the beginning
- all filters are ``and``-ed
- no ``not`` (only able to find matching fields, not to exclude some)
- no `join`` (filter on one model only)

The result of a call to the ``collection`` is lazy. The query is only sent to ``Redis`` when data is really needed, to display or do computation with them.

By default, a collection returns a list of primary keys for all the matching objects, but you can sort them, retrieve only a part, and/or directly get full instances instead of primary keys.

We will explain Filtering_, Sorting_, Slicing_, Instantiating_, Indexing_, and Laziness_ below, based on this example:

.. code:: python

    class Person(model.RedisModel):
        database = main_database
        firstname = fields.InstanceHashField(indexable=True)
        lastname = fields.InstanceHashField(indexable=True)
        nickname = fields.InstanceHashField(indexable=True, indexes=[TextRangeIndex])
        birth_year = fields.InstanceHashField(indexable=True, indexes=[NumberRangeIndex])

        def __repr__(self):
            return '<[%s] %s "%s" %s (%s)>' % tuple([self.pk.get()] + self.hmget('firstname', 'nickname', 'lastname', 'birth_year'))

    >>> Person(firstname='John', lastname='Smith', nickname='Joe', birth_year=1960)
    <[1] John "Joe" Smith (1960)>
    >>> Person(firstname='John', lastname='Doe', nickname='Jon', birth_year=1965)
    <[2] John "Jon" Doe (1965)>
    >>> Person(firstname='Emily', lastname='Smith', nickname='Emma', birth_year=1950)
    <[3] Emily "Emma" Smith (1950)>
    >>> Person(firstname='Susan', lastname='Doe', nickname='Sue', birth_year=1960)
    <[4] Susan "Sue" Doe (1960)>


Filtering
=========

To filter, simply call the ``collection`` (class)method with fields you want to filter as keys, and wanted values as values:

.. code:: python

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='john', lastname='Smith')
    ['1']
    >>> Person.collection(birth_year=1965)
    ['2']
    >>> Person.collection(birth_year=1965, lastname='Smith')
    []

You cannot pass two filters with the same name. All filters are ``and``-ed.

In Indexing_ you'll see more filtering capabilities.


Slicing
=======

To slice the result, simply act as if the result of a collection is a list:

.. code:: python

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John')[1:2]
    ['2']


.. _collection-sorting:

Sorting
=======

With the help of the ``sort`` command of Redis_, ``limpyd`` is able to sort the result of collections.

It's as simple as calling the ``sort`` method of the collection. Use the ``by`` argument to specify on which field to sort.

Redis_ default sort is numeric. If you want to sort values lexicographically, set the ``alpha`` parameter to ``True``.

Example:

.. code:: python

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)
    ['2', '1']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)[1:2]
    ['1']
    >>> Person.collection().sort(by='birth_year')
    ['3', '1', '4', '2']

Note: using ``by='pk'`` (or the real name of the ``pk`` field) is the same as not using ``by``: it will sort by primary keys,
using a numeric filter (use ``alpha=True`` if your ``pk`` is not numeric)


Instantiating
=============

If you want to retrieve already instantiated objects, instead of only primary keys and having to do instantiation yourself, you simply have to call ``instances()`` on the result of the collection. The result of the collection and its methods (``sort`` and ``instances``) return a collection, so you can chain calls:

.. code:: python

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>]
    >>> Person.collection(firstname='John').instances().sort(by='lastname', alpha=True)
    [<[2] John "Jon" Doe (1965)>, <[1] John "Joe" Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()
    [<[2] John "Jon" Doe (1965)>, <[1] John "Joe" Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()[0]
    [<[2] John "Jon" Doe (1965)>

Note that for each primary key got from Redis, a real instance is created, with a check for ``pk`` existence. As it can lead to a lot of Redis calls (one for each instance), if you are sure that all primary keys really exists (it must be the case if nothing special was done), you can skip these tests by passing the ``skip_exist_test`` named argument to ``True`` when calling ``instances``:

.. code:: python

    >>> Person.collection().instances(skip_exist_test=True)

Note that when you'll update an instance got with ``skip_exist_test`` set to ``True``, the existence of the primary key will be done before the update, raising an exception if not found.

To cancel retrieving instances and get the default return format, call the ``primary_keys`` method:

.. code:: python

    >>> Person.collection(firstname='John').instances().primary_keys()
    >>> ['1', '2']

.. code:: python

    >>> Person.collection().instances(skip_exist_test=True).primary_keys()

Indexing
========

By default, all fields with ``indexable=True`` use the default index, ``EqualIndex``.

It only allows equality filtering (the only legacy index type supported by ``limpyd``), but it is fast.

To filter using this index, you simply pass the field and a value in the collection call:

.. code:: python

    >>> Person.collection(firstname='John').instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>]

But you can also be more specific about the fact that you want an equality by using the `__eq` suffix. All other indexes use different suffixes.

This design is inspired by ``Django``.

.. code:: python

    >>> Person.collection(firstname__eq='John').instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>]

If you want to do more advanced lookup on a field that contains text, you can use the ``TextRangeIndex`` (to import from ``limpyd.indexes``), as we did for the ``nickname`` field.

It allows the same filtering as the default index, ie equality without suffix or with the ``__eq`` suffix, but it is not as efficient.

So if your only usage is equality filtering, prefer ``EqualIndex`` (which is the default)

But if not, you can take advantage of its capabilities, depending on the suffix you'll use:

- ``__gt``: text "Greater Than" the given value
- ``__gte``: "Greater Than or Equal"
- ``__lt``: "Less Than"
- ``__lte``: "Less Than or Equal"
- ``__startswith``: text that starts with the given value

Texts are compared in a lexicographical way, as viewed by Redis and explained this way:

    The elements are considered to be ordered from lower to higher strings as compared byte-by-byte using the memcmp() C function. Longer strings are considered greater than shorter strings if the common part is identical.

Some examples:

.. code:: python

    >>> Person.collection(nickname__startswith='Jo').instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>]
    >>> Person.collection(nickname__gte='Jo').instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>, <[4] Susan "Sue" Doe (1960)>]
    >>> Person.collection(nickname__gt='Jo').instances()
    [<[4] Susan "Sue" Doe (1960)>]

You can filter many times on the same field (more than two times doesn't really make sense):

.. code:: python

    >>> Person.collection(nickname__gte='E', nickname__lte='J').instances()
    [<[3] Emily "Emma" Smith (1950)>, <[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>]

This index works well for text but not for numbers, because lexicographically, ``1000 < 11``.

For numbers, you can use the ``NumberRangeIndex`` (to import from ``limpyd.indexes``).

It supports the same suffixes than ``TextRangeIndex`` excepted for ``startswith``.

Some things to know about this index:

- values of a field that cannot be casted to a float are converted to 0 for indexing (the stored value doesn't change).
- negative numbers are, of course, supported
- numbers are saved as the score of a Redis sorted set, so a number is, in the index:

    represented as an IEEE 754 floating point number, that is able to represent precisely integer numbers between -(2^53) and +(2^53) included.

    In more practical terms, all the integers between -9007199254740992 and 9007199254740992 are perfectly representable.

    Larger integers, or fractions, are internally represented in exponential form, so it is possible that you get only an approximation of the decimal number, or of the very big integer.

Some examples:

.. code:: python

    >>> Person.collection(birth_year__eq=1960).instances()
    [<[1] John "Joe" Smith (1960)>, <[4] Susan "Sue" Doe (1960)>]
    >>> Person.collection(birth_year__gt=1960).instances()
    [<[2] John "Jon" Doe (1965)>]
    >>> Person.collection(birth_year__gte=1960).instances()
    [<[1] John "Joe" Smith (1960)>, <[2] John "Jon" Doe (1965)>, <[4] Susan "Sue" Doe (1960)>]
    >>> Person.collection(birth_year__gt=1940, birth_year__lte=1950).instances()
    [<[3] Emily "Emma" Smith (1950)>]

And, of course, you can use fields with different indexes in the same query:

.. code:: python

    >>> Person.collection(birth_year__gte=1960, lastname='Doe', nickname__startswith='S').instances()
    [<[4] Susan "Sue" Doe (1960)>]

Configuration
-------------

If you want to use an index with a different behavior, you can use the ``configure`` class method of the index. Note that you can also create a new class by yourself but we provide this ability.

It accepts one or many arguments (``prefix``, ``transform`` and ``handle_uniqueness``) and returns a new index class to be passed to the ``indexes`` argument of the field.

About the ``prefix`` argument:

If you use two indexes accepting the same suffix, for example ``eq``, you can specify which one to use on the collection by assigning a prefix to the index:

.. code:: python

    class MyModel(model.RedisModel):
        myfield = fields.StringField(indexable=True, indexes=[
            EqualIndex,
            MyOtherIndex.configure(prefix='foo')
        ])

    >>> MyModel.collection(myfield='bar')  # will use EqualIndex
    >>> MyModel.collection(myfield__foo='bar')  # will use MyOtherIndex


About the ``transform`` argument:

If you want to index on a value different than the one stored on the field, you can transform it by assigning a transform function to the index.

This function accepts a value as argument and should return the value to store (which will be "normalized", ie converted to string for ``EqualIndex`` and ``TextRangeIndex`` and to float for ``NumberRangeIndex``)

.. code:: python

    def reverse_value(value):
        return value[::-1]

    class MyModel(model.RedisModel):
        myfield = fields.StringField(indexable=True, indexes=[EqualIndex.configure(transform=reverse_value)])

    >>> MyModel.collection(myfield__foo='rab')  # query with the expected transformed value

If you need this function to behave like a method of the index class, you can make it accepts two arguments, ``self`` and ``value``.

About the ``handle_uniqueness`` argument:

It will simply override the default value set on the index class. Useful if your ``transform`` function make the value not suitable to check uniqueness, so you can pass it to ``False``.

Note that if your field is marked as ``unique``, you'll need to have at least one index capable of handling uniqueness.


Clean and rebuild
-----------------

Before removing an index from the field declaration, you have to clean it, else the data will stay in redis.

For this, use the ``clean_indexes`` method of the field.

.. code:: python

    >>> MyModel.get_field('myfield').clean_indexes()


You can also rebuild them. It is useful if you decide to index a field with existing data that was not indexed before.

.. code:: python

    >>> MyModel.get_field('myfield').rebuild_indexes()


You can pass the named argument ``index_class`` to limit the clean/rebuild to only indexes of this class.

Say you defined your own index:

.. code:: python

    MyIndex = EqualIndex(key='yolo', transform=lambda value: 'yolo' + value)
    class MyModel(RedisModel):
        myfield = model.StringField(indexable=True, indexes=[TextRangeIndex, EqualIndex])

You can clear/rebuild only your own index this way:

.. code:: python

    >>> MyModel.get_field('myfield').clear(index_class=MyIndex)


Laziness
========

The result of a collection is lazy. In fact it's the collection itself, it's why we can chain calls to ``sort`` and ``instances``.

The query is sent to Redis only when the data are needed. In the previous examples, data was needed to display them.

But if you do something like:

.. code:: python

    >>> results = Person.collection(firstname='John').instances()

nothing will be done while results is not printed, iterated...


.. _collection-subclassing:

Subclassing
===========

The collection stuff is managed by a class named ``CollectionManager``, available in ``limpyd.collection``.

If you want to use another class (you own subclass or one provided in contrib, see :ref:`Extended collection <ExtendedCollectionManager>`), you can do it simple by declaring the ``collection_manager`` attribute of the model:

.. code:: python

    class MyOwnCollectionManager(CollectionManager):
        pass

    class Person(model.RedisModel):
        database = main_database
        collection_manager = MyOwnCollectionManager

        firstname = fields.InstanceHashField(indexable=True)
        lastname = fields.InstanceHashField(indexable=True)
        birth_year = fields.InstanceHashField(indexable=True)

You can also do it on each call to the ``collection`` method, by passing the class to the ``manager`` argument (useful if you want to keep the default manager in the model):

.. code:: python

    >>> Person.collection(firstname='John', manager=MyOwnCollectionManager)

.. _Redis: http://redis.io
