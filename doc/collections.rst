

***********
Collections
***********

The main and obvious way to get data from Redis via `limpyd` is to know the primary key of objects and instantiate them one by one.

But some fields can be indexed, passing them the `indexable` or `unique` attribute. 

If fields are indexed, it's possible to make query to retrieve many of them, using the `collection` method on the models.

The filtering has some limitations:

- you can only filter on fields with `indexable` and/or `unique` attributes set to True
- you can only filter on full values (`limyd` doesn't provide filters like "startswith", "contains"...)
- all filters are "and"ed
- no "not" (only able to find mathing fields, not to exlude some)
- no "join" (filter on one model only)

The result of a call to the `collection` is lazy. The query is only sent to Redis when data is really needed, to display or do computation with them.

By default, a collection returns a list of primary keys for all the matching objects, but you can sort them, retrieve only a part, and/or directly get full instances instead of primary keys.

We will explain Filtering_, Sorting_, Slicing_, Instanciating_, and Lazyness_ below, based on this example::

    class Person(model.RedisModel):
        database = main_database
        firstname = fields.HashableField(indexable=True)
        lastname = fields.HashableField(indexable=True)
        birth_year = fields.HashableField(indexable=True)

        def __repr__(self):
            return "<[%s] %s %s (%s)>" % tuple([self.get_pk()] + self.hmget('firstname', 'lastname', 'birth_year'))

    >>> Person(firstname='John', lastname='Smith', birth_year=1960)
    <[1] John Smith (1960)>
    >>> Person(firstname='John', lastname='Doe', birth_year=1965)
    <[2] John Doe (1965)>
    >>> Person(firstname='Emily', lastname='Smith', birth_year=1950)
    <[3] Emily Smith (1950)>
    >>> Person(firstname='Susan', lastname='Doe', birth_year=1960)
    <[4] Susan Doe (1960)>

Note that for each primary key got from redis, a real instance is created, with a check for pk existence. As it can lead to a lot of redis calls (one for each instance), if you are sure that all primary keys really exists (it must be the case if nothing special was done), you can skip these tests by passing the `skip_exist_test` named argument to True when calling `instances`::

    >>> Person.collection().instances(skip_exist_test=True)

Note that when you'll update an instance got with `skip_exist_test` set to True, the existence of the primary key will be done before the update, raising an exception if not found.

To cancel retrieving instances and get the default return format, call the `primary_keys` method::

    >>> Person.collection(firstname='John').instances().primary_keys()
    >>> ['1', '2']


Filtering
=========

To filter, simply call the `collection` (class)method with fields you want to filter as keys, and wanted values as values::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='john', lastname='Smith')
    ['1']
    >>> Person.collection(birth_year=1965)
    ['2']
    >>> Person.collection(birth_year=1965, lastname='Smith')
    []

You cannot pass two filters with the same name. All filters are "and"ed.


Slicing
=======

To slice the result, simply act as it's the result of a collection is a list::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John')[1:2]
    ['2']


Sorting
=======

With the help of the `sort` command of Redis_, `limpyd` is able to sort the result of collections.

It's as simple as calling the `sort` method of the collection. Use the `by` argument to specify on which field to sort.

Redis_ default sorting is numeric. If you want to sort values lexicographically, set the `alpha` parameter to True.

Example::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)
    ['2', '1']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)[1:2]
    [1']
    >>> Person.collection().sort(by='birth_year')
    ['3', '1', '4', '2']




Instanciating
=============

If you want to retrieve already instanciated objects, instead of only primary keys and having to do instanciation yourself, you simply have to call `instances()` on the result of the collection. The result of the collection and its methods (`sort` and `instances`) return a collection, so you can do chaining::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').instances()
    [<[1] John Smith (1960)>, <[2] John Doe (1965)>]
    >>> Person.collection(firstname='John').instances().sort(by='lastname', alpha=True)
    [<[2] John Doe (1965)>, <[1] John Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()
    [<[2] John Doe (1965)>, <[1] John Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()[0]
    [<[2] John Doe (1965)>


Lazyness
========

The result of a collection is lazy. In fact it's the collection itself, it's why we can chain calls to `sort` and `instances`.

The query is sent to Redis_ only when the data are needed. In the previous examples, data was needed to display them.

But if you do something like::

    >>> results = Person.collection(firstname='John').instances())

nothing will be done while results is not printed, iterated...


Subclassing
===========

The collection stuff is managed by a class named `CollectionManager`, available in `limpyd.collection`.

If you want to use another class (you own subclass or one provided in contrib, see `Extended collection`_), you can do it simple by declaring the `collection_manager` attribute of the model::

    class MyOwnCollectionManager(CollectionManager):
        pass

    class Person(model.RedisModel):
        database = main_database
        collection_manager = MyOwnCollectionManager

        firstname = fields.HashableField(indexable=True)
        lastname = fields.HashableField(indexable=True)
        birth_year = fields.HashableField(indexable=True)

You can also do it on each call to the `collection` method, by passing the class to the `manager` argument (useful if you want to keep the default manager in the model)::

    >>> Person.collection(firstname='John', manager=MyOwnCollectionManager)
